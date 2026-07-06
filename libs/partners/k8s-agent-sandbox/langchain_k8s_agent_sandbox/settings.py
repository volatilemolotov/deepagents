from dataclasses import (
    dataclass,
)


@dataclass
class K8sAgentSandboxSettings:
    """
    The dataclass that stores sandbox settings. 
    The signature fully matches the `SandboxClient.create_sandbox`.
    """

    warmpool: str
    namespace: str
    sandbox_ready_timeout: int = 180
    labels: dict[str, str] | None = None
    shutdown_after_seconds: int | None = None
    pod_labels: dict[str, str] | None = None
    pod_annotations: dict[str, str] | None = None
