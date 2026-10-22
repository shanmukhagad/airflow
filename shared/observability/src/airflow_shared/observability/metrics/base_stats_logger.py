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
import logging
from collections.abc import Callable
from typing import TYPE_CHECKING, Any, Protocol

from .protocols import Timer

if TYPE_CHECKING:
    from .protocols import DeltaType

log = logging.getLogger(__name__)


def read_gauge_callback(stat: str, callback: Callable[[], int | float]) -> int | float | None:
    """
    Return the value from a gauge callback, or None if the callback fails.

    The error is logged and not raised, so a broken callback never crashes the caller.

    :param stat: The name of the stat, used in the log message.
    :param callback: Function with no arguments that returns the current value.
    """
    try:
        return callback()
    except Exception:
        log.exception("Observable gauge callback for '%s' failed.", stat)
        return None

class StatsLogger(Protocol):
    """This class is only used for TypeChecking (for IDEs, mypy, etc)."""

    @classmethod
    def initialize(
        cls,
        *,
        factory: Callable,
    ) -> None:
        """Initialize the StatsLogger instance."""

    @classmethod
    def incr(
        cls,
        stat: str,
        count: int = 1,
        rate: int | float = 1,
        *,
        tags: dict[str, Any] | None = None,
    ) -> None:
        """Increment stat."""

    @classmethod
    def decr(
        cls,
        stat: str,
        count: int = 1,
        rate: int | float = 1,
        *,
        tags: dict[str, Any] | None = None,
    ) -> None:
        """Decrement stat."""

    @classmethod
    def gauge(
        cls,
        stat: str,
        value: float,
        rate: int | float = 1,
        delta: bool = False,
        *,
        tags: dict[str, Any] | None = None,
    ) -> None:
        """Gauge stat."""

    @classmethod
    def timing(
        cls,
        stat: str,
        dt: DeltaType | None,
        *,
        tags: dict[str, Any] | None = None,
    ) -> None:
        """Stats timing."""

    @classmethod
    def observable_gauge(
        cls,
        stat: str,
        callback: Callable[[], int | float],
        *,
        tags: dict[str, Any] | None = None,
   ) -> None:
       """Observable gauge stat."""

    @classmethod
    def timer(cls, *args, **kwargs) -> Timer:
        """Timer metric that can be cancelled."""
        raise NotImplementedError()


class NoStatsLogger:
    """If no StatsLogger is configured, NoStatsLogger is used as a fallback."""

    @classmethod
    def initialize(
        cls,
        *,
        factory: Callable,
    ) -> None:
        """Initialize the NoStatsLogger instance."""

    @classmethod
    def incr(cls, stat: str, count: int = 1, rate: int = 1, *, tags: dict[str, str] | None = None) -> None:
        """Increment stat."""

    @classmethod
    def decr(cls, stat: str, count: int = 1, rate: int = 1, *, tags: dict[str, str] | None = None) -> None:
        """Decrement stat."""

    @classmethod
    def gauge(
        cls,
        stat: str,
        value: float,
        rate: int = 1,
        delta: bool = False,
        *,
        tags: dict[str, str] | None = None,
    ) -> None:
        """Gauge stat."""

    @classmethod
    def timing(cls, stat: str, dt: DeltaType, *, tags: dict[str, str] | None = None) -> None:
        """Stats timing."""

    @classmethod
    def observable_gauge( cls, stat: str, callback: Callable[[], int | float], *, tags: dict[str, str] | None = None) -> None:
           """Observable gauge stat."""

    @classmethod
    def timer(cls, *args, **kwargs) -> Timer:
        """Timer metric that can be cancelled."""
        return Timer()
