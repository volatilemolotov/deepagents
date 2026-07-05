# Copyright 2026 The Kubernetes Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from __future__ import annotations

import base64
import logging
import posixpath
import re
import shlex
import threading
import weakref
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Callable, Iterable, Optional, Dict, List, Union, Tuple, Any, cast

from k8s_agent_sandbox import SandboxClient
from k8s_agent_sandbox.sandbox import Sandbox

from deepagents.backends.protocol import (
    EditResult,
    ExecuteResponse,
    FileData,
    FileDownloadResponse,
    FileInfo,
    FileUploadResponse,
    GlobResult,
    GrepMatch,
    GrepResult,
    LsResult,
    ReadResult,
    SandboxBackendProtocol,
    WriteResult,
)

from deepagents.backends.sandbox import (
    BaseSandbox,
)
from deepagents.backends.utils import create_file_data

try:
    from langgraph._internal._constants import CONFIG_KEY_SEND
    from langgraph.config import get_config
except (ImportError, ModuleNotFoundError):
    CONFIG_KEY_SEND = None  # type: ignore[assignment]
    get_config = None  # type: ignore[assignment]

from .settings import (
    K8sAgentSandboxClientSettings,
    K8sAgentSandboxSettings,
)

logger = logging.getLogger(__name__)


class AgentSandboxBackend(BaseSandbox):
    """DeepAgents backend adapter for agent-sandbox runtimes.

    Implements the DeepAgents SandboxBackendProtocol by wrapping a Sandbox handle.
    All file operations are virtualized under `root_dir` (default: /app).

    Requirements:
        - Sandbox image must have GNU coreutils: sh, grep, find (with -printf), mkdir, test
        - Sandbox handle must be connected before use

    Note:
        The backend can optionally manage the sandbox lifecycle when created via from_template().
        When manage_lifecycle=True, the backend must be used as a context manager.
    """

    # SESSION_LABEL_KEY = "agent-sandbox.sigs.k8s.io/session-id"
    SESSION_LABEL_KEY = "langchain-deepagents/session-id"
    """Kubernetes label key used for session-based sandbox reattach."""

    def __init__(
        self,
        # lifecycle_manager: K8sAgentSandboxLifecycleManager,
        sandbox: Sandbox,
        root_dir: str = "/app",
        allow_absolute_paths: bool = False,
        runtime_root: str = "/app",
        default_timeout_seconds: int = 30 * 60,
    ) -> None:
        if not root_dir.startswith("/"):
            raise ValueError(f"root_dir must be an absolute path, got: {root_dir}")
        if not runtime_root.startswith("/"):
            raise ValueError(f"runtime_root must be an absolute path, got: {runtime_root}")
        self._root_dir = posixpath.normpath(root_dir)
        # Path the sandbox runtime treats as its filesystem base — the
        # SDK file APIs (``files.read`` / ``files.list`` / ``files.exists``
        # / ``files.write``) interpret their path argument as relative to
        # this directory. Default ``/app`` matches the example runtime.
        self._runtime_root = posixpath.normpath(runtime_root)
        self._allow_absolute_paths = allow_absolute_paths
        self._default_timeout_seconds = default_timeout_seconds

        # self._lifecycle_manager = lifecycle_manager
        self._sandbox = sandbox

    def _send_files_update(self, update: dict[str, Any]) -> None:
        """Queue a ``files`` channel write via LangGraph's CONFIG_KEY_SEND.

        Follows the same contract as ``StateBackend._send_files_update`` so
        that ``state["files"]`` stays in sync with the sandbox filesystem.
        Silently no-ops when called outside a LangGraph graph context (e.g.
        unit tests, standalone scripts).
        """
        if get_config is None or CONFIG_KEY_SEND is None:
            return
        try:
            config = get_config()
        except RuntimeError:
            return
        send = config.get("configurable", {}).get(CONFIG_KEY_SEND)
        if send is None:
            return
        send([("files", update)])

    @classmethod
    def from_labels_scope(
        cls,
        # client_settings: K8sAgentSandboxClientSettings,
        # sandbox_settings: K8sAgentSandboxSettings,
        client: SandboxClient,
        sandbox_settings: K8sAgentSandboxSettings,
        scope: dict[str, str] | Callable,
        scope_labels_prefix: str = "scope.langchain-deepagents",
        multiple_sandbox_found_handler: Callable[[list[str]], None] | None = None,
        root_dir: str = "/app",
        allow_absolute_paths: bool = False,
        runtime_root: str = "/app",
    ):

        if callable(scope):
            final_scope = scope()
        else:
            final_scope = scope

        labels = {f"{scope_labels_prefix}/{k}":v for k,v in final_scope.items()} 
        
        label_selector=",".join([ f"{k}={v}" for k,v in labels.items()])

        claim_name = _find_sandbox_claim_by_label_selector(
            client,
            sandbox_settings.namespace,
            label_selector,
            multiple_sandbox_found_handler,
        )


        if claim_name is not None:
            sandbox = client.get_sandbox(
                claim_name, 
                namespace=sandbox_settings.namespace
            )
        else:
            sandbox = client.create_sandbox(
                sandbox_settings.warmpool,
                namespace=sandbox_settings.namespace,
                labels=labels,
            )

        return cls(
            sandbox,
            root_dir=root_dir,
            allow_absolute_paths=allow_absolute_paths,
            runtime_root=runtime_root,
        )
    
    @classmethod
    def from_existing_sandbox(
        cls,
        sandbox: Sandbox,
        root_dir: str = "/app",
        allow_absolute_paths: bool = False,
        runtime_root: str = "/app",

    ) -> "AgentSandboxBackend":

        return cls(
            sandbox,
            root_dir=root_dir,
            allow_absolute_paths=allow_absolute_paths,
            runtime_root=runtime_root,
        )


    @classmethod
    def from_existing_claim_name(
        cls,
        client_settings: K8sAgentSandboxClientSettings,
        claim_name: str,
        namespace: str,
        root_dir: str = "/app",
        allow_absolute_paths: bool = False,
        runtime_root: str = "/app",
    ) -> "AgentSandboxBackend":

        """Wrap an existing, already-connected Sandbox handle.

        Use this when the caller manages the sandbox lifecycle externally
        (e.g. via ``SandboxClient.create_sandbox()`` or
        ``SandboxClient.get_sandbox()``).  The backend will **not**
        create or delete the sandbox on enter/exit.

        Args:
            sandbox: A connected Sandbox instance.
            root_dir: Virtual root for file operations.
            allow_absolute_paths: If True, write/upload operations may
                target absolute paths outside root_dir.

        Returns:
            AgentSandboxBackend in unmanaged mode.
        """

        sandbox = client_settings.client.get_sandbox(
            claim_name,
            namespace=namespace,
        )

        return cls.from_existing_sandbox(
            sandbox,
            root_dir=root_dir,
            allow_absolute_paths=allow_absolute_paths,
            runtime_root=runtime_root,
        )


    def execute(
        self,
        command: str,
        *,
        timeout: Optional[int] = None,
    ) -> ExecuteResponse:
        """Execute a shell command in the sandbox.

        The command runs with its working directory set to ``root_dir``
        so that relative paths in shell commands resolve consistently
        with the paths exposed by ``ls`` / ``read`` / ``write``.

        Args:
            command: Shell command to execute.
            timeout: Maximum time in seconds to wait for the command to complete.
                If None, no explicit timeout is passed and the underlying
                sandbox client's default applies.

        Returns:
            ExecuteResponse with combined stdout/stderr output and exit code.
            On timeout, exit_code=-2 and output is prefixed with "Timed out".
            On other failures, exit_code=-1.
        """

        wrapped = f"sh -c {shlex.quote(f'cd {self._root_dir} && {command}')}"

        logger.error(wrapped)

        effective_timeout = timeout or self._default_timeout_seconds
        try:
            result = self._sandbox.commands.run(wrapped, timeout=effective_timeout)
        except Exception as e:
            logger.error("execute failed: %s", e)
            return ExecuteResponse(
                output=f"Error: {e}",
                exit_code=-1,
                truncated=False,
            )
        combined = result.stdout
        if result.stderr:
            combined = f"{combined}\n<stderr>\n{result.stderr}\n</stderr>" if combined else result.stderr
        return ExecuteResponse(
            output=combined,
            exit_code=result.exit_code,
            truncated=False,
        )


    def upload_files(self, files: list[tuple[str, bytes]]) -> list[FileUploadResponse]:
        """Upload multiple files to the sandbox.

        Args:
            files: Dict or iterable of (path, content) pairs.

        Returns:
            List of FileUploadResponse for each file.
        """
        responses: List[FileUploadResponse] = []
        for path, content in files:
            try:
                internal_path = self._resolve_write_path(path)
            except ValueError as e:
                logger.warning("Invalid path '%s': %s", path, e)
                responses.append(FileUploadResponse(path=path, error="invalid_path"))
                continue

            state = self._file_state(internal_path)
            if state == "error":
                responses.append(FileUploadResponse(path=path, error=cast(Any, "upload_failed")))
                continue
            if state == "dir":
                responses.append(FileUploadResponse(path=path, error="is_directory"))
                continue
            if state == "denied":
                responses.append(FileUploadResponse(path=path, error="permission_denied"))
                continue

            parent_dir = posixpath.dirname(internal_path)
            parent_state = self._dir_state(parent_dir)
            if parent_state == "error":
                responses.append(FileUploadResponse(path=path, error=cast(Any, "upload_failed")))
                continue
            if parent_state == "not_dir":
                responses.append(FileUploadResponse(path=path, error="invalid_path"))
                continue
            if parent_state == "denied":
                responses.append(FileUploadResponse(path=path, error="permission_denied"))
                continue
            if parent_state == "missing":
                try:
                    self._ensure_parent_dir(internal_path)
                except Exception as e:
                    logger.error(
                        "Failed to create parent dir for upload '%s': %s: %s",
                        path, type(e).__name__, e,
                    )
                    responses.append(FileUploadResponse(path=path, error=cast(Any, "upload_failed")))
                    continue

            try:
                self._write_bytes_at(internal_path, payload)
                responses.append(FileUploadResponse(path=path, error=None))
            except Exception as e:
                logger.error("Upload failed for '%s': %s: %s", path, type(e).__name__, e)
                responses.append(FileUploadResponse(path=path, error=cast(Any, "upload_failed")))

        # Batch-send state updates for all successfully uploaded files.
        files_update: dict[str, Any] = {}
        for resp, (orig_path, payload) in zip(responses, pairs):
            if resp.error is None:
                try:
                    text = payload.decode("utf-8")
                except UnicodeDecodeError:
                    import base64 as _b64
                    text = _b64.b64encode(payload).decode("ascii")
                files_update[orig_path] = create_file_data(text)
        if files_update:
            self._send_files_update(files_update)

        return responses

    def download_files(self, paths: Iterable[str]) -> List[FileDownloadResponse]:
        """Download multiple files from the sandbox.

        Args:
            paths: Iterable of file paths to download.

        Returns:
            List of FileDownloadResponse for each file.
        """
        sandbox = self._assert_sandbox()
        with self._track_op():
            responses: List[FileDownloadResponse] = []
            for path in paths:
                try:
                    internal_path = self._to_internal(path)
                except ValueError as e:
                    logger.warning("Invalid path '%s': %s", path, e)
                    responses.append(FileDownloadResponse(path=path, content=None, error="invalid_path"))
                    continue
                state = self._file_state(internal_path)
                if state == "error":
                    responses.append(FileDownloadResponse(path=path, content=None, error=cast(Any, "download_failed")))
                    continue
                if state == "missing":
                    responses.append(FileDownloadResponse(path=path, content=None, error="file_not_found"))
                    continue
                if state == "dir":
                    responses.append(FileDownloadResponse(path=path, content=None, error="is_directory"))
                    continue
                if state == "denied":
                    responses.append(FileDownloadResponse(path=path, content=None, error="permission_denied"))
                    continue
                try:
                    content = self._read_bytes_at(internal_path)
                    responses.append(FileDownloadResponse(path=path, content=content, error=None))
                except Exception as e:
                    logger.error("Download failed for '%s': %s: %s", path, type(e).__name__, e)
                    responses.append(FileDownloadResponse(path=path, content=None, error=cast(Any, "download_failed")))
            return responses

    def _ensure_parent_dir(self, path: str) -> None:
        parent = posixpath.dirname(path)
        command = shlex.join(["mkdir", "-p", parent])
        result = self._sandbox.commands.run(command)
        if result.exit_code != 0:
            error_msg = result.stderr.strip() if result.stderr else f"mkdir failed with exit code {result.exit_code}"
            raise RuntimeError(f"Cannot create parent directory '{parent}': {error_msg}")


    def _path_exists_at(self, internal_path: str) -> bool:
        """Check whether ``internal_path`` exists via file API or shell fallback."""
        assert self._sandbox is not None
        try:
            runtime_rel = self._to_runtime_relative(internal_path)
        except ValueError:
            runtime_rel = None
        if runtime_rel is not None:
            return self._sandbox.files.exists(runtime_rel)
        command = f"sh -c {shlex.quote(f'test -e {shlex.quote(internal_path)}')}"
        result = self._sandbox.commands.run(command)
        return result.exit_code == 0

    def _to_public(self, internal_path: str) -> str:
        rel = posixpath.relpath(internal_path, self._root_dir)
        if rel == ".":
            return "/"
        return "/" + rel

    def _normalize_public_dir(self, path: str) -> str:
        if not path or path == "/":
            return "/"
        if path == self._root_dir or path.startswith(self._root_dir + "/"):
            path = path[len(self._root_dir):]
        return "/" + path.strip("/")

    def _probe_state(
        self, check_script: str, valid: frozenset, label: str,
    ) -> str:
        """Run a shell probe and validate the single-word stdout result."""
        assert self._sandbox is not None
        result = self._sandbox.commands.run(f"sh -c {shlex.quote(check_script)}")
        output = result.stdout.strip()
        if not output and result.exit_code != 0:
            logger.warning(
                "_%s command failed: exit_code=%d, stderr=%s",
                label, result.exit_code, result.stderr,
            )
            return "error"
        state = output or "missing"
        if state not in valid:
            logger.warning("_%s returned unexpected value %r", label, state)
            return "error"
        return state

    _FILE_STATE_VALID = frozenset({"missing", "dir", "file", "denied"})

    def _file_state(self, internal_path: str) -> str:
        """Returns: 'missing', 'dir', 'file', 'denied', or 'error'."""
        check = (
            f"if [ ! -e {shlex.quote(internal_path)} ]; then echo missing; exit 0; fi; "
            f"if [ -d {shlex.quote(internal_path)} ]; then echo dir; exit 0; fi; "
            f"if [ -r {shlex.quote(internal_path)} ]; then echo file; else echo denied; fi"
        )
        return self._probe_state(check, self._FILE_STATE_VALID, "file_state")

    _DIR_STATE_VALID = frozenset({"missing", "writable", "denied", "not_dir"})

    def _dir_state(self, internal_path: str) -> str:
        """Returns: 'missing', 'writable', 'denied', 'not_dir', or 'error'."""
        check = (
            f"if [ ! -e {shlex.quote(internal_path)} ]; then echo missing; exit 0; fi; "
            f"if [ -d {shlex.quote(internal_path)} ]; then "
            f"if [ -w {shlex.quote(internal_path)} ]; then echo writable; else echo denied; fi; "
            f"exit 0; fi; "
            f"echo not_dir"
        )
        return self._probe_state(check, self._DIR_STATE_VALID, "dir_state")

    @property
    def id(self) -> str:
        """Return a namespace-qualified sandbox identifier.

        Format: ``{namespace}/{claim_name}``. Namespace-qualification
        prevents collisions when multiple namespaces are in use.
        Falls back to ``"agent-sandbox"`` before ``__enter__`` is called.
        """
        if self._sandbox is not None:
            ns = getattr(self._sandbox, "namespace", None) or "default"
            claim = getattr(self._sandbox, "claim_name", None)
            if claim:
                return f"{ns}/{claim}"
        return "agent-sandbox"


def _find_sandbox_claim_by_label_selector(
    client: SandboxClient, 
    namespace: str,
    label_selector: str,
    multiple_sandbox_found_handler: Callable[[list[str]], None] | None = None,
) -> str | None:

    found = client.list_all_sandboxes(
        namespace=namespace,
        label_selector=label_selector,
    )
 
    if len(found) == 1:
        return found[0]

    if len(found) == 0:
        return None

    if multiple_sandbox_found_handler is None:
        raise RuntimeError("")

    return multiple_sandbox_found_handler(found)


