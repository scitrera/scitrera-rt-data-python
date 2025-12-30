import pytest
import asyncio
import os
import redis.asyncio as aioredis
from scitrera_rt_data.q.async_redis_q import AsyncRedisTaskQueueManager, TaskStates, TaskNotFound

@pytest.fixture
async def redis_connection():
    redis_host = os.environ.get("REDIS_HOST", "localhost")
    redis_port = int(os.environ.get("REDIS_PORT", "6379"))
    redis_db = int(os.environ.get("REDIS_DB", "0"))
    redis_password = os.environ.get("REDIS_PASSWORD")

    if redis_password:
        redis_url = f"redis://:{redis_password}@{redis_host}:{redis_port}/{redis_db}"
    else:
        redis_url = f"redis://{redis_host}:{redis_port}/{redis_db}"

    connection = aioredis.from_url(url=redis_url, decode_responses=False)
    try:
        await connection.ping()
        yield connection
    finally:
        await connection.flushdb() # Clean up after tests
        await connection.aclose()

@pytest.mark.asyncio
async def test_q_basic_flow(redis_connection):
    manager = AsyncRedisTaskQueueManager(redis_client=redis_connection)
    workspace = "test_workspace"
    
    # Create a new task
    task_id = await manager.new_task(workspace, request_id="req123", meta="data")
    assert task_id is not None
    
    # Get task status
    task = await manager.get_task(workspace, task_id)
    assert task['task_id'] == task_id
    assert task['workspace'] == workspace
    assert task['state'] == TaskStates.PENDING
    assert task['request_id'] == "req123"
    assert task['meta'] == "data"
    
    # Update status to RUNNING
    await manager.status_update(workspace, task_id, new_state=TaskStates.RUNNING)
    task = await manager.get_task(workspace, task_id)
    assert task['state'] == TaskStates.RUNNING
    
    # List tasks
    tasks = await manager.list_tasks(workspace)
    assert len(tasks) == 1
    assert tasks[task_id]['task_id'] == task_id

    # Update status to FINISHED
    await manager.status_update(workspace, task_id, new_state=TaskStates.FINISHED, result="done")
    task = await manager.get_task(workspace, task_id)
    assert task['state'] == TaskStates.FINISHED
    assert task['result'] == "done"

@pytest.mark.asyncio
async def test_q_task_not_found(redis_connection):
    manager = AsyncRedisTaskQueueManager(redis_client=redis_connection)
    # The current implementation returns None instead of raising TaskNotFound
    assert await manager.get_task("any", "non_existent") is None

@pytest.mark.asyncio
async def test_q_delete_task(redis_connection):
    manager = AsyncRedisTaskQueueManager(redis_client=redis_connection)
    workspace = "test_workspace"
    task_id = await manager.new_task(workspace)
    
    await manager.delete_task(workspace, task_id)
    assert await manager.get_task(workspace, task_id) is None

@pytest.mark.asyncio
async def test_q_list_by_state(redis_connection):
    manager = AsyncRedisTaskQueueManager(redis_client=redis_connection)
    workspace = "test_workspace"
    
    t1 = await manager.new_task(workspace)
    t2 = await manager.new_task(workspace)
    await manager.status_update(workspace, t1, new_state=TaskStates.RUNNING)
    
    running_tasks = await manager.list_tasks(workspace, states=[TaskStates.RUNNING])
    assert len(running_tasks) == 1
    assert running_tasks[t1]['task_id'] == t1
    
    queued_tasks = await manager.list_tasks(workspace, states=[TaskStates.PENDING])
    assert len(queued_tasks) == 1
    assert queued_tasks[t2]['task_id'] == t2
