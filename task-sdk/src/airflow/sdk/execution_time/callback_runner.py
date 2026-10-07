#
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

import argparse
import inspect
import json
import logging
import sys
from collections.abc import AsyncIterator, Awaitable, Callable
from typing import TYPE_CHECKING, Any, Generic, Protocol, TypeVar, cast

from typing_extensions import ParamSpec

if TYPE_CHECKING:
    from structlog.typing import FilteringBoundLogger as Logger

    from airflow.sdk.types import OutletEventAccessorsProtocol

P = ParamSpec("P")
R = TypeVar("R")


class _ExecutionCallableRunner(Generic[P, R]):
    @staticmethod
    def run(*args: P.args, **kwargs: P.kwargs) -> R: ...  # type: ignore[empty-body]


class _AsyncExecutionCallableRunner(Generic[P, R]):
    @staticmethod
    async def run(*args: P.args, **kwargs: P.kwargs) -> R: ...  # type: ignore[empty-body]


class ExecutionCallableRunner(Protocol):
    def __call__(
        self,
        func: Callable[P, R],
        outlet_events: OutletEventAccessorsProtocol,
        *,
        logger: logging.Logger | Logger,
    ) -> _ExecutionCallableRunner[P, R]: ...


class AsyncExecutionCallableRunner(Protocol):
    def __call__(
        self,
        func: Callable[P, R],
        outlet_events: OutletEventAccessorsProtocol,
        *,
        logger: logging.Logger | Logger,
    ) -> _AsyncExecutionCallableRunner[P, R]: ...


def create_executable_runner(
    func: Callable[P, R],
    outlet_events: OutletEventAccessorsProtocol,
    *,
    logger: logging.Logger | Logger,
) -> _ExecutionCallableRunner[P, R]:
    """Run an execution callable against a task context and given arguments."""

    class _ExecutionCallableRunnerImpl(_ExecutionCallableRunner):
        @staticmethod
        def run(*args: P.args, **kwargs: P.kwargs) -> R:
            from airflow.sdk.definitions.asset.metadata import Metadata

            if not inspect.isgeneratorfunction(func):
                return func(*args, **kwargs)

            result: R

            if isinstance(logger, logging.Logger):

                def _warn_unknown(metadata):
                    logger.warning("Ignoring unknown data of %r received from task", type(metadata))
                    logger.debug("Full yielded value: %r", metadata)
            else:

                def _warn_unknown(metadata):
                    logger.warning("Ignoring unknown type received from task", type=type(metadata))
                    logger.debug("Full yielded value", metadata=metadata)

            def _run():
                nonlocal result
                result = yield from func(*args, **kwargs)

            for metadata in _run():
                if isinstance(metadata, Metadata):
                    outlet_events[metadata.asset].extra.update(metadata.extra)
                    if metadata.alias:
                        outlet_events[metadata.alias].add(metadata.asset, extra=metadata.extra)
                else:
                    _warn_unknown(metadata)

            return result

    return cast("_ExecutionCallableRunner[P, R]", _ExecutionCallableRunnerImpl)


def create_async_executable_runner(
    func: Callable[P, Awaitable[R] | AsyncIterator],
    outlet_events: OutletEventAccessorsProtocol,
    *,
    logger: logging.Logger | Logger,
) -> _AsyncExecutionCallableRunner[P, R]:
    """Run an async execution callable against a task context and given arguments."""

    class _AsyncExecutionCallableRunnerImpl(_AsyncExecutionCallableRunner):
        @staticmethod
        async def run(*args: P.args, **kwargs: P.kwargs) -> R:
            from airflow.sdk.definitions.asset.metadata import Metadata

            if not inspect.isasyncgenfunction(func):
                result = cast("Awaitable[R]", func(*args, **kwargs))
                return await result

            results: list[Any] = []

            async for result in func(*args, **kwargs):
                if isinstance(result, Metadata):
                    outlet_events[result.asset].extra.update(result.extra)
                    if result.alias:
                        outlet_events[result.alias].add(result.asset, extra=result.extra)

                results.append(result)

            return cast("R", results)

    return cast("_AsyncExecutionCallableRunner[P, R]", _AsyncExecutionCallableRunnerImpl)


def run_callback(request: Any) -> None:
    """Execute a callback payload inside the current subprocess."""
    log = logging.getLogger(__name__)
    log.info("Executing callback for task %s", getattr(request, "ti_key", "unknown"))

    if hasattr(request, "execute"):
        request.execute()


def _main(args: list[str] | None = None) -> None:
    """
    Named, importable entry point for callback subprocesses launched via exec.

    Acts as the entry point when `[core] execute_tasks_new_python_interpreter` is enabled.
    """
    parser = argparse.ArgumentParser(description="Airflow Task Callback Runner")
    parser.add_argument(
        "--callback-payload",
        type=str,
        required=False,
        help="JSON-serialized callback payload",
    )

    parsed_args = parser.parse_args(args)

    try:
        raw_payload = json.loads(parsed_args.callback_payload) if parsed_args.callback_payload else {}
        run_callback(raw_payload)
    except Exception as e:
        logging.exception("Failed to execute callback subprocess: %s", e)
        sys.exit(1)


if __name__ == "__main__":
    _main()