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
from unittest import mock

import pytest
from google.cloud.bigquery.table import RowIterator

from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.google.cloud.utils.bigquery_get_data import bigquery_get_data


class _StubRowIterator(RowIterator):
    """A RowIterator that can be constructed without a BigQuery client."""

    def __init__(self):
        return


def _row(*values):
    row = mock.Mock()
    row.values.return_value = values
    return row


def test_bigquery_get_data_yields_batches_and_stops_on_empty(caplog):
    hook = mock.create_autospec(BigQueryHook, instance=True)
    selected_fields = ["col_a", "col_b"]
    batch_size = 2
    hook.list_rows.side_effect = [
        [_row(1, "a"), _row(2, "b")],
        [_row(3, "c")],
        [],
    ]
    logger = logging.getLogger("test.bigquery_get_data")

    with caplog.at_level(logging.INFO, logger=logger.name):
        batches = list(
            bigquery_get_data(
                logger=logger,
                dataset_id="dataset",
                table_id="table",
                big_query_hook=hook,
                batch_size=batch_size,
                selected_fields=selected_fields,
            )
        )

    assert batches == [[(1, "a"), (2, "b")], [(3, "c")]]
    hook.list_rows.assert_has_calls(
        [
            mock.call(
                dataset_id="dataset",
                table_id="table",
                max_results=batch_size,
                selected_fields=selected_fields,
                start_index=0,
            ),
            mock.call(
                dataset_id="dataset",
                table_id="table",
                max_results=batch_size,
                selected_fields=selected_fields,
                start_index=batch_size,
            ),
            mock.call(
                dataset_id="dataset",
                table_id="table",
                max_results=batch_size,
                selected_fields=selected_fields,
                start_index=batch_size * 2,
            ),
        ]
    )
    assert [(record.levelno, record.getMessage()) for record in caplog.records] == [
        (logging.INFO, "Fetching Data from:"),
        (logging.INFO, "Dataset: dataset ; Table: table"),
        (logging.INFO, "Total Extracted rows: 2"),
        (logging.INFO, "Total Extracted rows: 3"),
        (logging.INFO, "Job Finished"),
    ]


def test_bigquery_get_data_passes_selected_fields_string_unmodified():
    hook = mock.create_autospec(BigQueryHook, instance=True)
    hook.list_rows.side_effect = [[_row("only")], []]
    selected_fields = "col_a,col_b"

    batches = list(
        bigquery_get_data(
            logger=logging.getLogger("test.bigquery_get_data.fields"),
            dataset_id="dataset",
            table_id="table",
            big_query_hook=hook,
            batch_size=10,
            selected_fields=selected_fields,
        )
    )

    assert batches == [[("only",)]]
    assert hook.list_rows.call_args.kwargs["selected_fields"] is selected_fields
    assert hook.list_rows.call_args.kwargs["max_results"] == 10


def test_bigquery_get_data_rejects_row_iterator():
    hook = mock.create_autospec(BigQueryHook, instance=True)
    hook.list_rows.return_value = _StubRowIterator()

    with pytest.raises(
        TypeError,
        match="BigQueryHook.list_rows\\(\\) returns iterator when return_iterator=False",
    ):
        next(
            bigquery_get_data(
                logger=logging.getLogger("test.bigquery_get_data.iterator"),
                dataset_id="dataset",
                table_id="table",
                big_query_hook=hook,
                batch_size=10,
                selected_fields=None,
            )
        )
