# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

from airflow.sdk.execution_time.callback_runner import _main
from airflow.sdk.execution_time.supervisor import CallbackSubprocess


def test_callback_main_entry_point(monkeypatch):
    """Test that _main parses the callback payload and executes the request."""
    mock_run = MagicMock()
    monkeypatch.setattr("airflow.sdk.execution_time.callback_runner.run_callback", mock_run)

    payload = json.dumps({
        "ti_key": "test_dag.test_task.run_1",
        "callback_type": "on_failure",
    })

    _main(["--callback-payload", payload])

    assert mock_run.called


@patch("airflow.sdk.execution_time.supervisor.conf.getboolean", return_value=True)
def test_callback_subprocess_uses_exec_when_enabled(mock_getboolean):
    """Verify CallbackSubprocess sets use_exec=True when execute_tasks_new_python_interpreter is True."""
    mock_request = MagicMock()
    mock_request.model_dump.return_value = {"ti_key": "test_dag.test_task.run_1"}

    subproc = CallbackSubprocess(callback_request=mock_request)

    assert subproc.use_exec is True