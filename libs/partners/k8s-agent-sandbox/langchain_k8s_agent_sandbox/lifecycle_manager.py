from abc import abstractmethod
from collections.abc import Callable
import logging

from k8s_agent_sandbox import SandboxClient, SandboxNotFoundError
from k8s_agent_sandbox.sandbox import Sandbox

from langchain_k8s_agent_sandbox.settings import K8sAgentSandboxSettings


logger = logging.getLogger(__name__)


class K8sAgentSandboxLifecycleManager:
    """A helper class that takes care if managing the sandbox instance."""
    def __init__(self) -> None:
        self._sandbox = None

    def get_sandbox(self) -> Sandbox:
        if self._sandbox is not None:
            return self._sandbox

        self._sandbox = self._get_sandbox()
        return self._sandbox

    @abstractmethod
    def _get_sandbox(self) -> Sandbox:
        pass

class ExistingSandboxInstanceLifecycleManager(K8sAgentSandboxLifecycleManager):
    """Simple manager that uses existing sandbox instance without managing it."""
    def __init__(self, sandbox: Sandbox) -> None:
        super().__init__()
        self._sandbox = sandbox

    def _get_sandbox(self) -> Sandbox:
        return self._sandbox


class ExistingSandboxClaimLifecycleManager(K8sAgentSandboxLifecycleManager):
    """Simple manager that uses existing sandbox by its claim, without managing it."""
    def __init__(
        self,
        client: SandboxClient,
        claim_name: str,
        namespace: str,
    ) -> None:
        super().__init__()
        self._client = client
        self._claim_name =claim_name
        self._namespace = namespace

    def _get_sandbox(self) -> Sandbox:
        return self._client.get_sandbox(
            self._claim_name, 
            namespace=self._namespace
        )


class LabelScopedLifecycleManager(K8sAgentSandboxLifecycleManager):
    """Fully manager the lifecycle of a sandbox based on the provided scope."""
    def __init__(
        self, 
        client: SandboxClient,
        sandbox_settings: K8sAgentSandboxSettings,
        scope: dict[str, str],
        scope_labels_prefix: str,
    ) -> None:
        super().__init__()
        self._client = client
        self._sandbox_settings = sandbox_settings

        self._labels = {
            f"{scope_labels_prefix}/{k}":v for k,v in scope.items()
        }
        
        self._label_selector=",".join(
            [ f"{k}={v}" for k,v in self._labels.items()]
        )

    def _get_sandbox(self) -> Sandbox:
        claim_name = self._find_sandbox_claim_by_label_selector()

        if claim_name is not None:
            try:
                return self._client.get_sandbox(
                    claim_name, 
                    namespace=self._sandbox_settings.namespace
                )
            except SandboxNotFoundError:
                pass

        return self._client.create_sandbox(
            self._sandbox_settings.warmpool,
            namespace=self._sandbox_settings.namespace,
            labels=self._labels,
        )
        
    def _find_sandbox_claim_by_label_selector(self) -> str | None:
    
        found = self._client.list_all_sandboxes(
            namespace=self._sandbox_settings.namespace,
            label_selector=self._label_selector,
        )

        if len(found) > 1:
            raise RuntimeError(
                "Multiple sandboxes with matching scopes have been found. "
                f"Relete the orphan sandboxes manualy.\nScope labels: {self._labels}"
            )
     
        if len(found) == 1:
            return found[0]
    
        if len(found) == 0:
            return None
