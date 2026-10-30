import asyncio
import logging
import time
from uuid import uuid4
from unittest.mock import AsyncMock, patch

import pytest
from airflow.sdk.execution_time import task_runner
from airflow.sdk.execution_time.comms import (
    AssetStateStoreResult,
    TaskStateStoreResult,
)
from airflow.sdk.execution_time.context import (
    AssetStateStoreAccessor,
    TaskStateStoreAccessor,
)
from airflow_shared.state import BaseStoreBackend


log = logging.getLogger(__name__)


class TestBackend(BaseStoreBackend):
    def get(self, scope, key, *, session=None):
        return None

    def set(self, scope, key, value, *, expires_at=None, session=None):
        pass

    def delete(self, scope, key, *, session=None):
        pass

    def clear(self, scope, *, all_map_indices=False, session=None):
        pass

    async def aget(self, scope, key, *, session=None):
        return None

    async def aset(self, scope, key, value, *, expires_at=None, session=None):
        pass

    async def adelete(self, scope, key, *, session=None):
        pass

    async def aclear(self, scope, *, all_map_indices=False, session=None):
        pass


# Check that response extraction uses the async asset deserializer.
@pytest.mark.asyncio
async def test_aextract_get_response_uses_async_deserializer():
    accessor = AssetStateStoreAccessor(name="testing_asset")
    response = AssetStateStoreResult(
        value={"__airflow_state_ref__": "fake-ref"}
    )

    with patch(
        "airflow.sdk.execution_time.context._get_worker_state_store_backend"
    ) as mock_backend:
        backend = mock_backend.return_value
        backend.adeserialize_asset_state_store_from_ref = AsyncMock(
            return_value={"status": "success"}
        )

        result = await accessor._aextract_get_response(
            response, "job_id", None
        )

    if result == {"status": "success"}:
        log.debug("Asset state response deserialized successfully")
    else:
        log.debug("Unexpected asset state response: %r", result)

    assert result == {"status": "success"}
    backend.adeserialize_asset_state_store_from_ref.assert_awaited_once_with(
        "fake-ref"
    )


# Check that building a set message uses the async asset serializer.
@pytest.mark.asyncio
async def test_abuild_set_message_uses_async_serializer():
    accessor = AssetStateStoreAccessor(name="test_asset")

    with patch(
        "airflow.sdk.execution_time.context._get_worker_state_store_backend"
    ) as mock_backend:
        backend = mock_backend.return_value
        backend.aserialize_asset_state_store_to_ref = AsyncMock(
            return_value="fake-ref"
        )

        message = await accessor._abuild_set_message(
            "job_id", "spark_001"
        )

    if (
        message.key == "job_id"
        and message.value == {"__airflow_state_ref__": "fake-ref"}
    ):
        log.debug("Asset state set message validation successfully")
    else:
        log.debug("Unexpected asset state set message: %r", message)

    assert message.key == "job_id"
    assert message.value == {"__airflow_state_ref__": "fake-ref"}
    backend.aserialize_asset_state_store_to_ref.assert_awaited_once()


# Check that slow serialization in a thread does not block the event loop.
@pytest.mark.asyncio
async def test_async_serializer_does_not_block_event_loop():
    started = asyncio.Event()
    finished = asyncio.Event()

    class SlowBackend:
        def serialize(self, *, value, key, scope):
            started.set()
            time.sleep(0.5)
            finished.set()
            return "fake-ref"

        async def aserialize(self, *, value, key, scope):
            return await asyncio.to_thread(
                self.serialize,
                value=value,
                key=key,
                scope=scope,
            )

    backend = SlowBackend()
    heartbeat_worked = False

    async def heartbeat():
        nonlocal heartbeat_worked

        await started.wait()
        await asyncio.sleep(0.1)
        heartbeat_worked = not finished.is_set()

    result, _ = await asyncio.gather(
        backend.aserialize(
            value={"status": "success"},
            key="job_id",
            scope=None,
        ),
        heartbeat(),
    )

    if result == "fake-ref" and heartbeat_worked:
        log.debug("serilze completed without blocking the heartbeat as expected ")
    else:
        log.debug(
            "Unexpected serializer result=%r, heartbeat_worked=%s",
            result,
            heartbeat_worked,
        )

    assert result == "fake-ref"
    assert heartbeat_worked


# Check that the backend asynchronously serializes asset state to JSON.
@pytest.mark.asyncio
async def test_base_backend_asset_serializer():
    backend = TestBackend()
    value = {"status": "success"}

    result = await backend.aserialize_asset_state_store_to_ref(
        value=value,
        key="job_id",
        scope=None,
    )

    if result == '{"status": "success"}':
        log.debug("Asset state serialization returned the as expected JSON")
    else:
        log.debug("Unexpected serialized asset state: %r", result)

    assert result == '{"status": "success"}'


# Check that the backend asynchronously converts stored asset JSON to Python.
@pytest.mark.asyncio
async def test_base_backend_asset_deserializer():
    backend = TestBackend()

    result = await backend.adeserialize_asset_state_store_from_ref(
        stored='{"status": "success"}'
    )

    if result == {"status": "success"}:
        log.debug("Asset state deserialization returned the expected value")
    else:
        log.debug("Unexpected deserialized asset state: %r", result)

    assert result == {"status": "success"}


@pytest.mark.asyncio
async def test_base_backend_task_serializer():
    backend = TestBackend()
    value = {"status": "success"}

    result = await backend.aserialize_task_state_store_to_ref(
        value=value,
        key="job_id",
        scope=None,
    )

    if result == '{"status": "success"}':
        log.debug("Task state serialization returned the expected JSON")
    else:
        log.debug("Unexpected serialized task state: %r", result)

    assert result == '{"status": "success"}'


@pytest.mark.asyncio
async def test_base_backend_task_deserializer():
    backend = TestBackend()

    result = await backend.adeserialize_task_state_store_from_ref(
        stored='{"status": "success"}'
    )

    if result == {"status": "success"}:
        log.debug("Task state deserialization returned the expected value")
    else:
        log.debug("Unexpected deserialized task state: %r", result)

    assert result == {"status": "success"}


# Check that aget retrieves task state using async supervisor communication.
@pytest.mark.asyncio
async def test_task_state_store_aget():
    accessor = TaskStateStoreAccessor(ti_id=uuid4(), scope=None)

    with (
        patch.object(
            task_runner, "SUPERVISOR_COMMS", create=True
        ) as mock_comms,
        patch(
            "airflow.sdk.execution_time.context._get_worker_state_store_backend"
        ) as mock_backend,
    ):
        mock_comms.asend = AsyncMock(
            return_value=TaskStateStoreResult(
                value={"__airflow_state_ref__": "fake-task-ref"}
            )
        )
        backend = mock_backend.return_value
        backend.adeserialize_task_state_store_from_ref = AsyncMock(
            return_value={"status": "success"}
        )

        result = await accessor.aget("job_id")

        if result == {"status": "success"}:
            log.debug("Task state retrieved successfully")
        else:
            log.debug("Unexpected task state result: %r", result)

        assert result == {"status": "success"}
        mock_comms.asend.assert_awaited_once()
        backend.adeserialize_task_state_store_from_ref.assert_awaited_once_with(
            "fake-task-ref"
        )


# Check that aset serializes task state and sends it to the supervisor.
@pytest.mark.asyncio
async def test_task_state_store_aset():
    accessor = TaskStateStoreAccessor(ti_id=uuid4(), scope=None)

    with (
        patch.object(
            task_runner, "SUPERVISOR_COMMS", create=True
        ) as mock_comms,
        patch(
            "airflow.sdk.execution_time.context.conf.getint",
            return_value=0,
        ),
        patch(
            "airflow.sdk.execution_time.context._get_worker_state_store_backend"
        ) as mock_backend,
    ):
        mock_comms.asend = AsyncMock()
        backend = mock_backend.return_value
        backend.aserialize_task_state_store_to_ref = AsyncMock(
            return_value="fake-task-ref"
        )

        await accessor.aset("job_id", {"status": "success"})

        mock_comms.asend.assert_awaited_once()
        message = mock_comms.asend.await_args.args[0]

        if (
            message.key == "job_id"
            and message.value == {"__airflow_state_ref__": "fake-task-ref"}
        ):
            log.debug("Task state set message validated successfully")
        else:
            log.debug("Unexpected task state set message: %r", message)

        assert message.key == "job_id"
        assert message.value == {"__airflow_state_ref__": "fake-task-ref"}
        backend.aserialize_task_state_store_to_ref.assert_awaited_once()