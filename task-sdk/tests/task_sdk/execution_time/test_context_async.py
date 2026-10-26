
import asyncio
import time
from unittest.mock import AsyncMock, patch

import pytest

from airflow.sdk.execution_time.comms import AssetStateStoreResult
from airflow.sdk.execution_time.context import AssetStateStoreAccessor
from airflow_shared.state import BaseStoreBackend
from uuid import uuid4
from airflow.sdk.execution_time import task_runner
from airflow.sdk.execution_time.comms import TaskStateStoreResult
from airflow.sdk.execution_time.context import TaskStateStoreAccessor



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


# This test used to check response extraction uses the async asset deserializer.
@pytest.mark.asyncio
async def test_aextract_get_response_uses_async_deserializer():
    # Purpose: Check that 
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

    print("Test 1 - Result:", result)

    if result == {"status": "success"}:
        print("Test 1 passed")
    else:
        print("Test 1 failed")

    assert result == {"status": "success"}
    backend.adeserialize_asset_state_store_from_ref.assert_awaited_once_with(
        "fake-ref"
    )

# Used to check building a set message uses the async asset serializer.
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

    print("Test 2 - Message key:", message.key)
    print("Test 2 - Message value:", message.value)

    if (
        message.key == "job_id"
        and message.value == {"__airflow_state_ref__": "fake-ref"}
    ):
        print("Test 2 passed")
    else:
        print("Test 2 failed")

    assert message.key == "job_id"
    assert message.value == {"__airflow_state_ref__": "fake-ref"}
    backend.aserialize_asset_state_store_to_ref.assert_awaited_once()

# Showind the that running slow serialization in a thread
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

    print("Test 3 - Serializer result:", result)
    print("Test 3 - Heartbeat worked:", heartbeat_worked)

    if result == "fake-ref" and heartbeat_worked:
        print("Test 3 passed")
    else:
        print("Test 3 failed")

    assert result == "fake-ref"
    assert heartbeat_worked

#Check the backend asynchronously serializes asset state to JSON.
@pytest.mark.asyncio
async def test_base_backend_asset_serializer():
    backend = TestBackend()
    value = {"status": "success"}

    result = await backend.aserialize_asset_state_store_to_ref(
        value=value,
        key="job_id",
        scope=None,
    )

    print("Test 4 - Input value:", value)
    print("Test 4 - Serialized result:", result)

    if result == '{"status": "success"}':
        print("Test 4 passed")
    else:
        print("Test 4 failed")

    assert result == '{"status": "success"}'

# check backend asynchronously converts stored asset JSON back to Python data.
@pytest.mark.asyncio
async def test_base_backend_asset_deserializer():
    backend = TestBackend()

    result = await backend.adeserialize_asset_state_store_from_ref(
        stored='{"status": "success"}'
    )

    print("Test 5 - Deserialized result:", result)

    if result == {"status": "success"}:
        print("Test 5 passed")
    else:
        print("Test 5 failed")

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

    print("Test 6 - Input value:", value)
    print("Test 6 - Serialized result:", result)

    if result == '{"status": "success"}':
        print("Test 6 passed")
    else:
        print("Test 6 failed")

    assert result == '{"status": "success"}'


@pytest.mark.asyncio
async def test_base_backend_task_deserializer():
    backend = TestBackend()

    result = await backend.adeserialize_task_state_store_from_ref(
        stored='{"status": "success"}'
    )

    print("Test 7 - Deserialized result:", result)

    if result == {"status": "success"}:
        print("Test 7 passed")
    else:
        print("Test 7 failed")

    assert result == {"status": "success"}


from uuid import uuid4

from airflow.sdk.execution_time.comms import TaskStateStoreResult
from airflow.sdk.execution_time.context import TaskStateStoreAccessor

#used to check that aget retrieves task state using async supervisor communication.
@pytest.mark.asyncio
async def test_task_state_store_aget():
    
    accessor = TaskStateStoreAccessor(ti_id=uuid4(), scope=None)

    mock_comms = patch.object(
        task_runner, "SUPERVISOR_COMMS", create=True
    ).start()
    mock_backend = patch(
        "airflow.sdk.execution_time.context._get_worker_state_store_backend"
    ).start()

    try:
        # Mock supervisor returning an external ref marker
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

        print("Test 8 - Retrieved value:", result)

        if result == {"status": "success"}:
            print("Test 8 passed")
        else:
            print("Test 8 failed")

        assert result == {"status": "success"}
        mock_comms.asend.assert_awaited_once()
        backend.adeserialize_task_state_store_from_ref.assert_awaited_once_with(
            "fake-task-ref"
        )
    finally:
        patch.stopall()



@pytest.mark.asyncio
async def test_task_state_store_aset():
    from uuid import uuid4

    from airflow.sdk.execution_time import task_runner
    from airflow.sdk.execution_time.context import TaskStateStoreAccessor

    accessor = TaskStateStoreAccessor(ti_id=uuid4(), scope=None)

    mock_comms = patch.object(
        task_runner, "SUPERVISOR_COMMS", create=True
    ).start()
    mock_conf = patch(
        "airflow.sdk.execution_time.context.conf.getint", return_value=0
    ).start()
    mock_backend = patch(
        "airflow.sdk.execution_time.context._get_worker_state_store_backend"
    ).start()

    try:
        mock_comms.asend = AsyncMock()
        backend = mock_backend.return_value
        backend.aserialize_task_state_store_to_ref = AsyncMock(
            return_value="fake-task-ref"
        )

        await accessor.aset("job_id", {"status": "success"})

        print("Test 9 - aset completed")
        mock_comms.asend.assert_awaited_once()

        message = mock_comms.asend.await_args.args[0]

        print("Test 9 - Message key:", message.key)
        print("Test 9 - Message value:", message.value)

        if (
            message.key == "job_id"
            and message.value == {"__airflow_state_ref__": "fake-task-ref"}
        ):
            print("Test 9 passed")
        else:
            print("Test 9 failed")

        assert message.key == "job_id"
        assert message.value == {"__airflow_state_ref__": "fake-task-ref"}
        backend.aserialize_task_state_store_to_ref.assert_awaited_once()
    finally:
        patch.stopall()



