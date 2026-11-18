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
"""
Check that the ``@task.<name>`` stubs match the operators behind the decorators.

The stub ``task-sdk/src/airflow/sdk/definitions/decorators/__init__.pyi`` is written by hand, while the
operators define the real parameters. For every decorator that has a stub entry this test reports:

* parameters the operator accepts but the stub does not list,
* parameters the decorator class adds (for example ``expect_airflow``) but the stub does not list,
* parameters the stub lists but neither the operator, the decorator class nor the base classes accept.

Parameters that the decorator sets itself (``super().__init__(cmds=[...])``) are not reported as missing.

The decorators and their operators are discovered from the ``task-decorators`` section of every
``provider.yaml`` and from the decorator source, so there is no list to maintain. Sources are parsed with
``ast``: nothing from the providers is imported. Parameter names are compared, not types.
"""

from __future__ import annotations

import ast
import glob
from pathlib import Path

import pytest
import yaml

STUB_RELATIVE_PATH = "task-sdk/src/airflow/sdk/definitions/decorators/__init__.pyi"
BASES_RELATIVE_DIR = "task-sdk/src/airflow/sdk/bases"

# Parent classes that are decorator machinery, not the operator the decorator wraps.
WRAPPER_BASES = {"DecoratedOperator", "_PythonDecoratedOperator"}

# Never expected in the stub. ``kwargs_to_upstream`` is internal plumbing of ``DecoratedOperator``.
STRUCTURAL_PARAMS = {
    "self",
    "args",
    "kwargs",
    "task_id",
    "python_callable",
    "op_args",
    "op_kwargs",
    "kwargs_to_upstream",
}
STOP_AT_FOR_OPERATOR = {"BaseOperator", "BaseSensorOperator", "DecoratedOperator"}
STOP_AT_FOR_DECORATOR = {"BaseOperator", "BaseSensorOperator"}

# Known differences that are accepted for now. Every entry needs a reason, and the test
# ``test_known_differences_are_still_needed`` fails when an entry is no longer needed.
KNOWN_DIFFERENCES: dict[str, dict[str, str]] = {
    "docker": {"use_dill": "listed in the stub but not accepted by _DockerDecoratedOperator; to be decided"},
}


# --------------------------------------------------------------------------- parsing
def parse_classes(source: str) -> dict[str, dict]:
    """Map class name -> own ``__init__`` params, whether it takes ``**kwargs``, base names, fixed params."""
    classes: dict[str, dict] = {}
    for node in ast.walk(ast.parse(source)):
        if not isinstance(node, ast.ClassDef):
            continue
        init = next((n for n in node.body if isinstance(n, ast.FunctionDef) and n.name == "__init__"), None)
        info: dict = {
            "params": None,
            "accepts_kwargs": False,
            "bases": [b.id if isinstance(b, ast.Name) else getattr(b, "attr", None) for b in node.bases],
            "fixed": set(),
        }
        if init is not None:
            args = init.args
            own = [a.arg for a in args.posonlyargs + args.args + args.kwonlyargs if a.arg != "self"]
            info["params"], info["accepts_kwargs"] = own, args.kwarg is not None
            info["fixed"] = fixed_by_super_init(init, set(own))
        classes.setdefault(node.name, info)
    return classes


def fixed_by_super_init(init: ast.FunctionDef, own_params: set[str]) -> set[str]:
    """Keywords passed to ``super().__init__()`` that are not simply a forwarded own parameter."""
    fixed = set()
    for node in ast.walk(init):
        if (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and node.func.attr == "__init__"
            and isinstance(node.func.value, ast.Call)
            and getattr(node.func.value.func, "id", None) == "super"
        ):
            for kw in node.keywords:
                forwarded = isinstance(kw.value, ast.Name) and kw.value.id in own_params
                if kw.arg and not forwarded:
                    fixed.add(kw.arg)
    return fixed


def effective_params(classes: dict, name: str, stop_at: set[str], _seen: tuple = ()) -> list[str]:
    """Params of ``name.__init__`` plus its parents' while ``__init__`` forwards ``**kwargs``."""
    if name in stop_at or name not in classes or name in _seen:
        return []
    info = classes[name]
    result = list(info["params"] or [])
    if info["params"] is None or info["accepts_kwargs"]:
        for base in info["bases"]:
            for param in effective_params(classes, base, stop_at, _seen + (name,)) if base else []:
                if param not in result:
                    result.append(param)
    return result


def fixed_params(classes: dict, name: str, _seen: tuple = ()) -> set[str]:
    """Params fixed by ``super().__init__()`` in the decorated class and its decorator-wrapper parents."""
    if name not in classes or name in _seen:
        return set()
    result = set(classes[name]["fixed"])
    for base in classes[name]["bases"]:
        if base in WRAPPER_BASES or (base and base.endswith("DecoratedOperator")):
            result |= fixed_params(classes, base, _seen + (name,))
    return result


def parse_stub(source: str) -> dict[str, set[str]]:
    """Map decorator name -> parameter names listed in the stub (union over its overloads)."""
    stub: dict[str, set[str]] = {}
    for node in ast.walk(ast.parse(source)):
        if isinstance(node, ast.ClassDef) and node.name == "TaskDecoratorCollection":
            for fn in node.body:
                if isinstance(fn, ast.FunctionDef):
                    a = fn.args
                    stub.setdefault(fn.name, set()).update(
                        x.arg for x in a.posonlyargs + a.args + a.kwonlyargs
                    )
    return stub


def compare(classes: dict, decorated_class: str, listed: set[str], base_accepted: set[str]) -> dict:
    """Compare one decorator's stub entry with its operator and decorator class."""
    bases = classes[decorated_class]["bases"]
    operator = next((b for b in bases if b and b not in WRAPPER_BASES and b in classes), None)
    operator_params = (
        {p for p in effective_params(classes, operator, STOP_AT_FOR_OPERATOR) if p not in STRUCTURAL_PARAMS}
        if operator is not None
        else set()
    )
    decorated = {
        p
        for p in effective_params(classes, decorated_class, STOP_AT_FOR_DECORATOR)
        if p not in STRUCTURAL_PARAMS
    }
    decorator_side = decorated - operator_params
    fixed = fixed_params(classes, decorated_class)
    listed = listed - STRUCTURAL_PARAMS
    return {
        "operator": operator,
        "missing": sorted(p for p in operator_params - listed if p not in fixed),
        "set_by_decorator": sorted(p for p in (operator_params | decorator_side) - listed if p in fixed),
        "decorator_missing": sorted(p for p in decorator_side - listed if p not in fixed),
        "extra": sorted(listed - decorated - base_accepted),
    }


# --------------------------------------------------------------------------- repository discovery
def _find_repo_root() -> Path:
    for parent in Path(__file__).resolve().parents:
        if (parent / STUB_RELATIVE_PATH).exists():
            return parent
    pytest.skip(f"{STUB_RELATIVE_PATH} not found above {__file__}", allow_module_level=True)


def _decorated_class_name(func: ast.FunctionDef) -> str | None:
    for node in ast.walk(func):
        if isinstance(node, ast.Call):
            for kw in node.keywords:
                if kw.arg == "decorated_operator_class" and isinstance(kw.value, ast.Name):
                    return kw.value.id
    return None


def _discover_decorators(root: Path) -> list[tuple[str, str | None, Path]]:
    """Return (decorator name, decorated class name, provider src dir) from every provider.yaml."""
    found = []
    for provider_yaml in sorted(glob.glob(str(root / "providers" / "**" / "provider.yaml"), recursive=True)):
        entries = yaml.safe_load(Path(provider_yaml).read_text()).get("task-decorators") or []
        for entry in entries:
            module, func_name = entry["class-name"].rsplit(".", 1)
            relative = module.replace(".", "/") + ".py"
            hits = glob.glob(str(root / "providers" / "**" / "src" / relative), recursive=True)
            decorated = None
            if hits:
                tree = ast.parse(Path(hits[0]).read_text())
                func = next(
                    (n for n in ast.walk(tree) if isinstance(n, ast.FunctionDef) and n.name == func_name),
                    None,
                )
                decorated = _decorated_class_name(func) if func else None
            found.append((entry["name"], decorated, Path(provider_yaml).parent / "src"))
    return found


ROOT = _find_repo_root()
STUB = parse_stub((ROOT / STUB_RELATIVE_PATH).read_text())
DECORATORS = _discover_decorators(ROOT)
DECORATED_CLASS = {name: cls for name, cls, _ in DECORATORS}
STUBBED = sorted(name for name in DECORATED_CLASS if name in STUB)
KNOWN_PAIRS = [(d, p) for d, params in KNOWN_DIFFERENCES.items() for p in params]


@pytest.fixture(scope="module")
def classes() -> dict:
    result: dict = {}
    source_dirs = {src for _, _, src in DECORATORS}
    files = [f for d in source_dirs for f in glob.glob(str(d / "**" / "*.py"), recursive=True)]
    files += glob.glob(str(ROOT / BASES_RELATIVE_DIR / "*.py"))
    for path in files:
        try:
            parsed = parse_classes(Path(path).read_text())
        except (SyntaxError, UnicodeDecodeError):
            continue
        for name, info in parsed.items():
            result.setdefault(name, info)
    return result


@pytest.fixture(scope="module")
def base_accepted(classes) -> set[str]:
    accepted: set[str] = set()
    for name in ("BaseOperator", "BaseSensorOperator"):
        accepted |= set(classes.get(name, {}).get("params") or [])
    return accepted


# --------------------------------------------------------------------------- tests on the repository
def test_decorators_are_discovered():
    assert STUBBED, "no decorator with a stub entry was found; the discovery from provider.yaml is broken"


@pytest.mark.parametrize("decorator", STUBBED)
def test_stub_matches_operator(decorator, classes, base_accepted):
    decorated_class = DECORATED_CLASS[decorator]
    if decorated_class not in classes:
        pytest.fail(f"decorated class {decorated_class!r} for `{decorator}` was not found in the sources")
    result = compare(classes, decorated_class, STUB[decorator], base_accepted)
    known = KNOWN_DIFFERENCES.get(decorator, {})
    labels = {
        "missing": "the operator accepts these but the stub does not list them",
        "decorator_missing": "the decorator class accepts these but the stub does not list them",
        "extra": "the stub lists these but nothing accepts them",
    }
    problems = [
        f"{labels[key]}: {', '.join(names)}"
        for key in labels
        if (names := [p for p in result[key] if p not in known])
    ]
    assert not problems, (
        f"`{decorator}` stub is out of sync with {result['operator']} in {STUB_RELATIVE_PATH}:\n  - "
        + "\n  - ".join(problems)
        + "\nFix the stub, or add a reasoned entry to KNOWN_DIFFERENCES in this test."
    )


@pytest.mark.parametrize(("decorator", "param"), KNOWN_PAIRS)
def test_known_differences_are_still_needed(decorator, param, classes, base_accepted):
    result = compare(classes, DECORATED_CLASS[decorator], STUB[decorator], base_accepted)
    still_different = {p for key in ("missing", "decorator_missing", "extra") for p in result[key]}
    assert param in still_different, f"`{decorator}`: `{param}` no longer differs, remove it from the list"


# --------------------------------------------------------------------------- tests of the comparison itself
class TestCompare:
    """Check that the comparison detects each kind of difference, using small synthetic sources."""

    SOURCE = """
class MyOperator(BaseOperator):
    def __init__(self, *, image, runtime_class_name=None, cmds=None, **kwargs): ...

class _MyDecoratedOperator(DecoratedOperator, MyOperator):
    def __init__(self, *, python_callable, use_dill=False, **kwargs):
        super().__init__(cmds=["placeholder"], **kwargs)
"""

    def _compare(self, listed: set[str]) -> dict:
        return compare(parse_classes(self.SOURCE), "_MyDecoratedOperator", listed, set())

    def test_operator_param_missing_from_stub(self):
        assert self._compare({"image", "use_dill"})["missing"] == ["runtime_class_name"]

    def test_param_set_by_decorator_is_not_reported_as_missing(self):
        result = self._compare({"image", "runtime_class_name", "use_dill"})
        assert result["missing"] == []
        assert result["set_by_decorator"] == ["cmds"]

    def test_decorator_side_param_missing_from_stub(self):
        assert self._compare({"image", "runtime_class_name"})["decorator_missing"] == ["use_dill"]

    def test_extra_param_in_stub(self):
        assert self._compare({"image", "runtime_class_name", "use_dill", "bogus"})["extra"] == ["bogus"]

    def test_complete_stub_has_no_differences(self):
        result = self._compare({"image", "runtime_class_name", "use_dill"})
        assert result["missing"] == result["decorator_missing"] == result["extra"] == []
