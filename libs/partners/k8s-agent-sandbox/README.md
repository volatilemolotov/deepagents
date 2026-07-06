# langchain-k8s-agent-sandbox

## Quick Install

```bash
uv add langchain-k8s-agent-sandbox
```

```python

from k8s_agent_sandbox.sandbox_client import SandboxClient
from langchain_k8s_agent_sandbox import K8sAgentSandbox


client = SandboxClient()

sandbox = K8sAgentSandbox.from_existing_claim_name(
    client,
    "some-claim",
)
result = sandbox.execute("echo hello")
print(result.output)
```

## 🤔 What is this?

Kubernetes Agent Sandbox integration for Deep Agents.

## 📕 Releases & Versioning

See our [Releases](https://docs.langchain.com/oss/python/release-policy) and [Versioning](https://docs.langchain.com/oss/python/versioning) policies.

## 💁 Contributing

As an open-source project in a rapidly developing field, we are extremely open to contributions, whether it be in the form of a new feature, improved infrastructure, or better documentation.

For detailed information on how to contribute, see the [Contributing Guide](https://docs.langchain.com/oss/python/contributing/overview).

## Resources

- [LangChain Academy](https://academy.langchain.com/) — Comprehensive, free courses on LangChain libraries and products, made by the LangChain team.
- [Code of Conduct](https://github.com/langchain-ai/langchain/?tab=coc-ov-file) — community guidelines and standards
