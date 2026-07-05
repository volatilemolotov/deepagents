from collections.abc import Callable
from enum import Enum

from k8s_agent_sandbox.sandbox_client import SandboxClient


SESSION_LABEL_KEY="langchain-deepagents/session-id"


def get_or_create_sandbox(
    client: SandboxClient, 
    warmpool: str,
    namespace: str,
    label_selector: str,
    multiple_sandbox_found_handler: Callable[[list[str]], None] | None = None,
):

    claim_name = _find_sandbox_claim_by_label_selector(
        client,
        namespace,
        label_selector,
        multiple_sandbox_found_handler,
    )

    if claim_name is not None:
        return client.get_sandbox(claim_name, namespace=namespace)

    return client.create_sandbox(
        warmpool,
        namespace=namespace,
    )

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


