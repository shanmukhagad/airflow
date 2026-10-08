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
"""Unit tests for ``python_kubernetes_script.py``.

+-----------------------------------------------+--------------------------------------------------+--------------------+
| Test                                          | What / how                                       | Mocked             |
+===============================================+==================================================+====================+
| test_remove_task_decorator [without_parens]   | strip bare @task.kubernetes                      | none               |
| test_remove_task_decorator [parens]           | strip @task.kubernetes()                         | none               |
| test_remove_task_decorator [with_args]        | strip @task.kubernetes(image=...)                | none               |
| test_remove_task_decorator [nested_without_]  | keep @foo/@bar, drop only kubernetes             | none               |
| test_remove_task_decorator [nested_with_]     | same, with @task.kubernetes()                    | none               |
| test_balances_nested_parentheses_in_arguments | dict(limits=dict(...)) does not cut at first )   | none               |
| test_also_strips_setup_and_teardown           | @setup and @teardown are stripped too            | none               |
| test_returns_source_unchanged_when_absent     | no decorator → source unchanged                  | none               |
| test_writes_rendered_script_to_file           | file has import + function + call site           | none (real tmp file)|
| test_respects_pickling_library_from_context   | pickling_library=dill → import dill              | none               |
| test_render_as_native_obj_uses_native_env     | native Jinja env still writes a valid file       | none               |
| test_missing_context_variable_raises          | StrictUndefined raises UndefinedError            | none               |
+-----------------------------------------------+--------------------------------------------------+--------------------+
"""
from __future__ import annotations

from textwrap import dedent

import pytest
from jinja2 import UndefinedError

from airflow.providers.cncf.kubernetes.python_kubernetes_script import (
    remove_task_decorator,
    write_python_script,
)


class TestRemoveTaskDecorator:
    @pytest.mark.parametrize(
        ("decorators", "expected_decorators"),
        [
            (["@task.kubernetes"], []),
            (["@task.kubernetes()"], []),
            (['@task.kubernetes(image="python:3.11")'], []),
            (["@foo", "@task.kubernetes", "@bar"], ["@foo", "@bar"]),
            (["@foo", "@task.kubernetes()", "@bar"], ["@foo", "@bar"]),
        ],
        ids=["without_parens", "parens", "with_args", "nested_without_parens", "nested_with_parens"],
    )
    def test_remove_task_decorator(self, decorators, expected_decorators):
        script = dedent(
            """
            def f():
                import funcsigs
            """
        )
        py_source = "\n".join(decorators) + script
        expected = "\n".join(expected_decorators)
        expected_source = expected + script if expected else script.lstrip()

        res = remove_task_decorator(python_source=py_source, task_decorator_name="@task.kubernetes")
        assert res == expected_source

    def test_balances_nested_parentheses_in_arguments(self):
        py_source = '@task.kubernetes(resources=dict(limits=dict(cpu="1")))\ndef f():\n    pass\n'
        res = remove_task_decorator(py_source, "@task.kubernetes")
        assert res == "def f():\n    pass\n"

    def test_also_strips_setup_and_teardown(self):
        py_source = "@setup\n@task.kubernetes\n@teardown\ndef f():\n    pass\n"
        res = remove_task_decorator(py_source, "@task.kubernetes")
        assert res == "def f():\n    pass\n"

    def test_returns_source_unchanged_when_decorator_absent(self):
        py_source = "def f():\n    pass\n"
        res = remove_task_decorator(py_source, "@task.kubernetes")
        assert res == py_source


class TestWritePythonScript:
    def _context(self, **overrides):
        context = {
            "op_args": [],
            "op_kwargs": {},
            "pickling_library": "pickle",
            "python_callable": "f",
            "python_callable_source": "def f():\n    return 1\n",
        }
        context.update(overrides)
        return context

    def test_writes_rendered_script_to_file(self, tmp_path):
        filename = tmp_path / "script.py"
        write_python_script(self._context(), filename.as_posix())

        rendered = filename.read_text()
        assert "import pickle" in rendered
        assert "def f():\n    return 1" in rendered
        assert "res = f(*arg_dict[\"args\"], **arg_dict[\"kwargs\"])" in rendered

    def test_respects_pickling_library_from_context(self, tmp_path):
        filename = tmp_path / "script.py"
        write_python_script(self._context(pickling_library="dill"), filename.as_posix())

        assert "import dill" in filename.read_text()

    def test_render_as_native_obj_uses_native_environment(self, tmp_path):
        filename = tmp_path / "script.py"
        write_python_script(
            self._context(),
            filename.as_posix(),
            render_template_as_native_obj=True,
        )

        assert filename.exists()
        assert "def f():\n    return 1" in filename.read_text()

    def test_missing_context_variable_raises(self, tmp_path):
        filename = tmp_path / "script.py"
        incomplete = self._context()
        del incomplete["python_callable_source"]

        with pytest.raises(UndefinedError):
            write_python_script(incomplete, filename.as_posix())
