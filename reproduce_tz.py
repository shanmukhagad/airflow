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
# /// script
# requires-python = ">=3.10"
# ///
from __future__ import annotations

import asyncio
import calendar
import datetime
import json
import os
import platform
import stat
import time
from importlib.metadata import version
from unittest import mock

import asyncssh
import paramiko

from airflow import settings
from airflow.providers.common.compat.sdk import Connection, timezone
from airflow.providers.sftp.hooks.sftp import SFTPHook, SFTPHookAsync
from airflow.providers.sftp.sensors.sftp import SFTPSensor
from airflow.providers.sftp.triggers.sftp import SFTPTrigger

MTIME = 1704110400


@mock.patch(
    "airflow.providers.sftp.triggers.sftp.asyncio.sleep", autospec=True, side_effect=asyncio.CancelledError
)
@mock.patch.object(SFTPHookAsync, "_get_conn", autospec=True)
async def inspect_async_path(get_conn, sleep, threshold):
    client = mock.AsyncMock(spec=asyncssh.SFTPClient)
    client.stat.return_value = asyncssh.SFTPAttrs(mtime=MTIME)
    sftp_cm = mock.MagicMock()
    sftp_cm.__aenter__ = mock.AsyncMock(return_value=client)
    ssh = mock.MagicMock(spec=asyncssh.SSHClientConnection)
    ssh.__aenter__.return_value = ssh
    ssh.start_sftp_client.return_value = sftp_cm
    get_conn.return_value = ssh
    mod_time = await SFTPHookAsync().get_mod_time("/files/file.txt")
    trigger = SFTPTrigger(path="/files/file.txt", newer_than=threshold)
    generator = trigger.run()
    try:
        try:
            event = await anext(generator)
        except asyncio.CancelledError:
            event = None
        if event is None:
            sleep.assert_awaited_once_with(trigger.poke_interval)
        else:
            if event.payload["status"] != "success":
                raise AssertionError(event)
        if client.stat.await_count != 2 or ssh.__aexit__.await_count != 2:
            raise AssertionError("Expected two stat calls and two closed connection contexts")
        return {"mod_time": mod_time, "accepted": event is not None}
    finally:
        await generator.aclose()


@mock.patch.object(SFTPHook, "get_connection", spec=True)
@mock.patch.object(SFTPHook, "get_managed_conn", autospec=True)
def inspect_sync_paths(managed_conn, get_connection, threshold):
    get_connection.return_value = Connection(conn_id="sftp_default", conn_type="sftp", host="mock.invalid")
    attrs = paramiko.SFTPAttributes()
    attrs.st_mtime = MTIME
    attrs.st_mode = stat.S_IFREG | 0o644
    attrs.filename = "file.txt"
    client = mock.MagicMock(spec=paramiko.SFTPClient)
    client.stat.return_value = attrs
    client.listdir_attr.return_value = [attrs]
    managed_conn.return_value.__enter__.return_value = client
    hook = SFTPHook()
    result = {"mod_time": hook.get_mod_time("/files/file.txt")}
    for pattern in ("", "*.txt"):
        sensor = SFTPSensor(
            task_id="mtime_probe",
            path="/files" if pattern else "/files/file.txt",
            file_pattern=pattern,
            newer_than=threshold,
        )
        sensor.hook = hook
        result["pattern" if pattern else "single_file"] = {
            "get_files": sensor._get_files(),
            "poke": sensor.poke({}),
        }
    return result


def main():
    time.tzset()
    if calendar.timegm((2024, 1, 1, 12, 0, 0)) != MTIME:
        raise AssertionError("Unexpected epoch timestamp")
    packages = [
        "apache-airflow",
        "apache-airflow-core",
        "apache-airflow-task-sdk",
        "apache-airflow-providers-sftp",
        "apache-airflow-providers-ssh",
        "apache-airflow-providers-common-compat",
        "asyncssh",
        "paramiko",
        "pendulum",
        "python-dateutil",
        "pytest",
        "pytest-asyncio",
        "tzdata",
    ]
    result = {
        "python": platform.python_version(),
        "versions": {package: version(package) for package in packages},
        "process_tz": os.environ.get("TZ"),
        "process_time": time.strftime("%Y-%m-%dT%H:%M:%S%z", time.localtime(MTIME)),
        "core_timezone": str(settings.TIMEZONE),
        "provider_timezone": str(timezone.datetime(2024, 1, 1).tzinfo),
        "helper_module": timezone.convert_to_utc.__module__,
        "helper_file": __import__(timezone.convert_to_utc.__module__, fromlist=[""]).__file__,
        "source": SFTPTrigger.run.__code__.co_filename,
        "library_optout": os.environ.get("_AIRFLOW__AS_LIBRARY"),
        "unit_test_mode": os.environ.get("AIRFLOW__CORE__UNIT_TEST_MODE"),
        "process_context": os.environ.get("_AIRFLOW_PROCESS_CONTEXT"),
        "cases": [],
    }
    for hour in (10, 14):
        threshold = datetime.datetime(2024, 1, 1, hour, tzinfo=datetime.timezone.utc)
        result["cases"].append(
            {
                "threshold": threshold.isoformat(),
                "expected": hour == 10,
                "sync": inspect_sync_paths(threshold=threshold),
                "async_single_file": asyncio.run(inspect_async_path(threshold=threshold)),
            }
        )
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()