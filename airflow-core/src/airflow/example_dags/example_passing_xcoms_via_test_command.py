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

"""
Example DAG demonstrating how to pass XCom values of upstream tasks via the test command.

Run:
`airflow tasks test example_passing_xcoms_via_test_command print_message
--xcoms '[{"task_id": "get_message", "value": "test message"}]'`
"""

from __future__ import annotations

import pendulum

from airflow.sdk import DAG, task

with DAG(
    "example_passing_xcoms_via_test_command",
    schedule=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["example"],
) as dag:

    @task
    def get_message():
        return "message from upstream"

    @task
    def print_message(message):
        print(f"Received message: {message}")

    print_message(get_message())
