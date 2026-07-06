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


import pytest

from k8s_agent_sandbox.models import ExecutionResult
from deepagents.backends.protocol import (
    ExecuteResponse,
)
from langchain_k8s_agent_sandbox import (
    K8sAgentSandbox,
)


def test_execute(lifecycle_manager, mock_sandbox):
    backend = K8sAgentSandbox(
        lifecycle_manager,
    )

    mock_sandbox.commands.run.return_value = ExecutionResult(
        exit_code=0,
        stdout="some output",
        stderr="some logs",
    )
    
    result = backend.execute("some-command", timeout=180)

    assert result == ExecuteResponse(
        output='some output\n<stderr>\nsome logs\n</stderr>', 
        exit_code=0, 
        truncated=False
    )









