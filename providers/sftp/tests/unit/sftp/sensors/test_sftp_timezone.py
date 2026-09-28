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

import datetime
import time
from unittest import mock

import pytest

from airflow.providers.sftp.hooks.sftp import SFTPHook
from airflow.providers.sftp.sensors.sftp import SFTPSensor

MTIME_EPOCH = 1704110400  # 2024-01-01 12:00:00 UTC


def test_sftp_hook_get_mod_time_utc_invariant(monkeypatch):
    """Verify get_mod_time returns UTC formatted time string regardless of system process TZ."""
    monkeypatch.setenv("TZ", "America/New_York")
    if hasattr(time, "tzset"):
        time.tzset()

    hook = SFTPHook.__new__(SFTPHook)
    mock_conn = mock.MagicMock()
    mock_attrs = mock.MagicMock()
    mock_attrs.st_mtime = MTIME_EPOCH
    mock_conn.stat.return_value = mock_attrs
    hook.conn = mock_conn

    # 1704110400 is 12:00:00 UTC. Even if local system TZ is EST, output must be UTC: 20240101120000
    assert hook.get_mod_time("/path/to/file") == "20240101120000"


def test_sftp_sensor_newer_than_with_mismatched_timezones(monkeypatch):
    """Verify SFTPSensor evaluates newer_than correctly when system TZ and default TZ differ."""
    monkeypatch.setenv("TZ", "UTC")
    monkeypatch.setenv("AIRFLOW__CORE__DEFAULT_TIMEZONE", "America/New_York")

    mock_hook = mock.MagicMock()
    # File mod_time is 12:00:00 UTC
    mock_hook.get_mod_time.return_value = "20240101120000"

    # Threshold set to 14:00:00 UTC (file is 12:00, which is OLDER than 14:00 threshold -> should be False)
    threshold_utc = datetime.datetime(2024, 1, 1, 14, 0, 0, tzinfo=datetime.timezone.utc)

    sensor = SFTPSensor(
        task_id="test_tz_mismatch",
        path="/path/to/file",
        newer_than=threshold_utc,
    )
    sensor.hook = mock_hook

    assert sensor.poke({}) is False