from __future__ import annotations

from datetime import timedelta

import pytest

from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk.definitions.callback import SyncCallback
from airflow.sdk.definitions.deadline import DeadlineAlert, DeadlineReference
from airflow.utils.state import DagRunState, TaskInstanceState


def _cb(**kwargs):
    pass


@pytest.mark.db_test
def test_dagrun_with_no_schedule(dag_maker, session):
    """Verify dagrun lifecycle and update_state succeed when schedule=None and DeadlineAlert is configured."""
    with dag_maker(
        dag_id="deadline_dagtest_repro",
        schedule=None,
        deadline=DeadlineAlert(
            reference=DeadlineReference.DAGRUN_LOGICAL_DATE,
            interval=timedelta(hours=2),
            callback=SyncCallback(_cb),
        ),
        session=session,
    ):
        EmptyOperator(task_id="task")

    # 1. dag_maker serializes the DAG to the DB and creates the DagRun
    dr = dag_maker.create_dagrun()

    # 2. Mark task instance as SUCCESS to trigger the successful completion path in update_state
    for ti in dr.task_instances:
        ti.state = TaskInstanceState.SUCCESS
        session.merge(ti)
    session.flush()

    # 3. Trigger update_state (this is where deadline alert checking and pruning occurs)
    schedulable_tis, callback = dr.update_state(session=session)

    # 4. Assert the DAG run finished successfully without raising errors
    assert dr.state == DagRunState.SUCCESS
    assert callback is None