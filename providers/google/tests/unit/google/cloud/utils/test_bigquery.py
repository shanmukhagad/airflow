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

import pytest

from airflow.providers.google.cloud.utils.bigquery import bq_cast, convert_job_id


@pytest.mark.parametrize("bq_type", ["INTEGER", "FLOAT", "TIMESTAMP", "BOOLEAN", "STRING"])
def test_bq_cast_none_is_none(bq_type):
    assert bq_cast(None, bq_type) is None


@pytest.mark.parametrize(
    ("value", "bq_type", "expected"),
    [
        ("7", "INTEGER", 7),
        ("1.5", "FLOAT", 1.5),
        # BigQuery sends TIMESTAMP as epoch seconds, so it stays a float rather than a datetime.
        ("1600000000.123456", "TIMESTAMP", 1600000000.123456),
        ("true", "BOOLEAN", True),
        ("false", "BOOLEAN", False),
        ("unchanged", "STRING", "unchanged"),
        ("unchanged", "UNKNOWN", "unchanged"),
        # DATE is a real BigQuery type with no branch here, so it falls through as a string.
        ("2024-01-01", "DATE", "2024-01-01"),
    ],
)
def test_bq_cast(value, bq_type, expected):
    assert bq_cast(value, bq_type) == expected
    assert type(bq_cast(value, bq_type)) is type(expected)


def test_bq_cast_boolean_rejects_other_values():
    with pytest.raises(ValueError, match="maybe must have value 'true' or 'false'"):
        bq_cast("maybe", "BOOLEAN")


def test_convert_job_id_single():
    assert convert_job_id("job-1", "my-project", "EU") == "my-project:EU:job-1"


def test_convert_job_id_list():
    assert convert_job_id(["job-1", "job-2"], "my-project", "EU") == [
        "my-project:EU:job-1",
        "my-project:EU:job-2",
    ]


def test_convert_job_id_defaults_location_to_us():
    assert convert_job_id("job-1", "my-project", None) == "my-project:US:job-1"
    assert convert_job_id(["job-1"], "my-project", None) == ["my-project:US:job-1"]
