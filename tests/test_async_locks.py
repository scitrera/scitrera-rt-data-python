import asyncio
import os
import pytest
from typing import Tuple, Optional, Any, List
from datetime import datetime

try:
    import redis.asyncio as aioredis
except ImportError:
    raise RuntimeError("redis.asyncio is not installed")

from scitrera_rt_data.locks.in_process import InProcessAsyncLock, InProcessLockManager
from scitrera_rt_data.locks.redis_lock_async2 import AsyncRedisLock2, RedisLockManager, AlreadyLocked


# ===== InProcessAsyncLock Tests =====

@pytest.mark.asyncio
async def test_in_process_lock_acquire_release():
    """Test basic acquire and release functionality of InProcessAsyncLock."""
    lock = InProcessAsyncLock("test_lock")

    # Initially the lock should not be locked
    assert await lock.is_locked() is False

    # Acquire the lock
    await lock.acquire()
    assert await lock.is_locked() is True

    # Release the lock
    await lock.release()
    assert await lock.is_locked() is False


@pytest.mark.asyncio
async def test_in_process_lock_context_manager():
    """Test context manager functionality of InProcessAsyncLock."""
    lock = InProcessAsyncLock("test_lock")

    # Initially, the lock should not be locked
    assert await lock.is_locked() is False

    # Use the lock as a context manager
    async with lock:
        assert await lock.is_locked() is True

    # After exiting the context, the lock should be released
    assert await lock.is_locked() is False


@pytest.mark.asyncio
async def test_in_process_lock_timeout():
    """Test timeout behavior of InProcessAsyncLock."""
    lock1 = InProcessAsyncLock("test_lock")
    lock2 = InProcessAsyncLock("test_lock")

    # Acquire the first lock
    await lock1.acquire()
    assert await lock1.is_locked() is True

    # Try to acquire the second lock with a short timeout
    with pytest.raises(AlreadyLocked):
        await lock2.acquire(timeout=0.1)

    # Release the first lock
    await lock1.release()

    # Now the second lock should be able to acquire
    await lock2.acquire()
    assert await lock2.is_locked() is True
    await lock2.release()


@pytest.mark.asyncio
async def test_in_process_lock_fail_when_locked():
    """Test fail_when_locked behavior of InProcessAsyncLock."""
    lock1 = InProcessAsyncLock("test_lock")
    lock2 = InProcessAsyncLock("test_lock", fail_when_locked=True)

    # Acquire the first lock
    await lock1.acquire()

    # Try to acquire the second lock with fail_when_locked=True
    with pytest.raises(AlreadyLocked):
        await lock2.acquire()

    # Release the first lock
    await lock1.release()


@pytest.mark.asyncio
async def test_in_process_lock_concurrent_access():
    """Test concurrent access to InProcessAsyncLock."""
    lock_id = "test_concurrent_lock"

    async def acquire_and_hold(lock_name: str, hold_time: float) -> Tuple[str, bool]:
        """Acquire a lock, hold it for a time, then release it."""
        lock = InProcessAsyncLock(lock_name)
        try:
            await lock.acquire(timeout=0.1)
            await asyncio.sleep(hold_time)
            return lock_name, True
        except AlreadyLocked:
            return lock_name, False
        except Exception as e:
            return str(e), False  # if something else goes wrong...
        finally:
            await lock.release()

    # Start multiple tasks trying to acquire the same lock
    tasks = [
        asyncio.create_task(acquire_and_hold(lock_id, 0.2)),
        asyncio.create_task(acquire_and_hold(lock_id, 0.2)),
        asyncio.create_task(acquire_and_hold(lock_id, 0.2))
    ]

    # Wait for all tasks to complete
    results = await asyncio.gather(*tasks)

    # Only one task should have successfully acquired the lock
    successful_acquisitions = sum(1 for _, success in results if success)
    assert successful_acquisitions == 1


@pytest.mark.asyncio
async def test_in_process_lock_is_locked_from_different_instance():
    """Test that is_locked returns True when lock is held by a different instance."""
    lock1 = InProcessAsyncLock("test_lock")
    lock2 = InProcessAsyncLock("test_lock")

    # Acquire the first lock
    await lock1.acquire()

    # Check if the second lock instance detects that the lock is held
    assert await lock2.is_locked() is True

    # Release the first lock
    await lock1.release()

    # Now the lock should be released
    assert await lock2.is_locked() is False


# ===== InProcessLockManager Tests =====

@pytest.mark.asyncio
async def test_in_process_lock_manager_create_lock():
    """Test that InProcessLockManager creates locks correctly."""
    manager = InProcessLockManager()
    lock = manager.lock("test_lock")

    assert isinstance(lock, InProcessAsyncLock)
    assert lock.lock_id == "test_lock"

    # Test the lock works
    await lock.acquire()
    assert await lock.is_locked() is True
    await lock.release()
    assert await lock.is_locked() is False


@pytest.mark.asyncio
async def test_in_process_lock_manager_configure():
    """Test that InProcessLockManager configuration works."""
    manager = InProcessLockManager()

    # Configure the manager with custom settings
    manager.configure(
        timeout=5.0,
        check_interval=0.2,
        fail_when_locked=True,
        thread_sleep_time=0.05
    )

    # Create a lock and verify it has the configured settings
    lock = manager.lock("test_lock")
    assert lock.timeout == 5.0
    assert lock.check_interval == 0.2
    assert lock.fail_when_locked is True
    assert lock.thread_sleep_time == 0.05


# ===== Real Redis Tests =====

@pytest.fixture
async def redis_connection():
    """
    Fixture to create a real Redis connection using environment variables.

    Environment variables:
    - REDIS_HOST: Redis server hostname (default: localhost)
    - REDIS_PORT: Redis server port (default: 6379)
    - REDIS_DB: Redis database number (default: 0)
    - REDIS_PASSWORD: Redis password (default: None)
    """
    redis_host = os.environ.get("REDIS_HOST", "localhost")
    redis_port = int(os.environ.get("REDIS_PORT", "6379"))
    redis_db = int(os.environ.get("REDIS_DB", "0"))
    redis_password = os.environ.get("REDIS_PASSWORD")

    # Construct Redis URL
    if redis_password:
        redis_url = f"redis://:{redis_password}@{redis_host}:{redis_port}/{redis_db}"
    else:
        redis_url = f"redis://{redis_host}:{redis_port}/{redis_db}"

    # Create Redis connection
    connection = aioredis.from_url(
        url=redis_url,
        health_check_interval=10,
        decode_responses=False
    )

    try:
        # Test connection
        await connection.ping()
        yield connection
    finally:
        # Close connection
        await connection.aclose()


@pytest.mark.asyncio
async def test_real_redis_lock_acquire_release(redis_connection):
    """Test basic acquire and release functionality of AsyncRedisLock with a real Redis instance."""

    # Create lock with real connection
    lock = AsyncRedisLock2(
        redis_url="redis://localhost:6379/0",  # This URL is not used since we pass the connection
        lock_channel="test_real_lock",
        connection=redis_connection
    )

    # Initially the lock should not be locked
    assert await lock.is_locked() is False

    # Acquire the lock
    await lock.acquire()
    assert await lock.is_locked() is True

    # Release the lock
    await lock.release()
    assert await lock.is_locked() is False


@pytest.mark.asyncio
async def test_real_redis_lock_context_manager(redis_connection):
    """Test context manager functionality of AsyncRedisLock with a real Redis instance."""

    # Create lock with real connection
    lock = AsyncRedisLock2(
        redis_url="redis://localhost:6379/0",  # This URL is not used since we pass the connection
        lock_channel="test_real_lock_cm",
        connection=redis_connection
    )

    # Initially, the lock should not be locked
    assert await lock.is_locked() is False

    # Use the lock as a context manager
    async with lock:
        assert await lock.is_locked() is True

    # After exiting the context, the lock should be released
    assert await lock.is_locked() is False


@pytest.mark.asyncio
async def test_real_redis_lock_is_locked_from_different_instance(redis_connection):
    """Test that is_locked returns True when lock is held by a different instance."""

    # Create two locks with the same channel
    lock1 = AsyncRedisLock2(
        redis_url="redis://localhost:6379/0",
        lock_channel="test_real_lock_multi",
        connection=redis_connection
    )

    lock2 = AsyncRedisLock2(
        redis_url="redis://localhost:6379/0",
        lock_channel="test_real_lock_multi",
        connection=redis_connection
    )

    # Acquire the first lock
    await lock1.acquire()

    # Check if the second lock instance detects that the lock is held
    assert await lock2.is_locked() is True

    # Release the first lock
    await lock1.release()

    # Now the lock should be released
    assert await lock2.is_locked() is False


@pytest.mark.asyncio
async def test_real_redis_lock_timeout(redis_connection):
    """Test timeout behavior of AsyncRedisLock with a real Redis instance."""

    # Create two locks with the same channel
    lock1 = AsyncRedisLock2(
        redis_url="redis://localhost:6379/0",
        lock_channel="test_real_lock_timeout",
        connection=redis_connection
    )

    lock2 = AsyncRedisLock2(
        redis_url="redis://localhost:6379/0",
        lock_channel="test_real_lock_timeout",
        connection=redis_connection
    )

    # Acquire the first lock
    await lock1.acquire()
    assert await lock1.is_locked() is True

    # Try to acquire the second lock with a short timeout
    with pytest.raises(AlreadyLocked):
        await lock2.acquire(timeout=0.1)

    # Release the first lock
    await lock1.release()

    # Now the second lock should be able to acquire
    await lock2.acquire()
    assert await lock2.is_locked() is True
    await lock2.release()


@pytest.mark.asyncio
async def test_real_redis_lock_concurrent_access(redis_connection):
    """Test concurrent access to AsyncRedisLock with a real Redis instance."""
    lock_id = "test_real_concurrent_lock"
    hold_time = 0.2

    async def acquire_and_hold(lock_name: str, hold_time: float, conn) -> Tuple[bool, Optional[float], Optional[float], Any]:
        """Acquire a lock, hold it for a time, then release it."""
        lock = AsyncRedisLock2(
            redis_url="redis://localhost:6379/0",
            lock_channel=lock_name,
            connection=conn
        )
        # noinspection PyBroadException
        try:
            # We use a longer timeout to allow multiple sequential acquisitions in this test
            start_time = asyncio.get_running_loop().time()
            await lock.acquire(timeout=2.0)
            acquired_at = asyncio.get_running_loop().time()
            # print(f'acquired {lock_name} at {acquired_at}')
            await asyncio.sleep(hold_time)
            released_at = asyncio.get_running_loop().time()
            return True, acquired_at, released_at, conn
        except AlreadyLocked:
            # print(f'NOT acquired {lock_name} at {asyncio.get_running_loop().time()}')
            return False, None, None, conn
        except Exception as e:
            # print(f'Exception for {lock_name}: {e}')
            return False, None, None, conn
        finally:
            await lock.release()

    # Start multiple tasks trying to acquire the same lock
    # Use different connection for each task to better simulate independent processes/clients
    tasks = []
    # delete the lock key first to ensure a clean state
    await redis_connection.delete(f"lock:{lock_id}")
    await asyncio.sleep(0.1)  # wait a bit

    num_tasks = 10
    for _ in range(num_tasks):
        conn = aioredis.Redis(host="localhost", port=6379, db=0)
        tasks.append(asyncio.create_task(acquire_and_hold(lock_id, hold_time, conn)))

    # Wait for all tasks to complete
    results = await asyncio.gather(*tasks)

    # Close connections
    for _, _, _, conn in results:
        await conn.aclose()

    # Filter successful acquisitions and sort them by acquisition time
    acquisitions = sorted(
        [(acquired_at, released_at) for success, acquired_at, released_at, _ in results if success],
        key=lambda x: x[0]
    )

    print(f"\nSuccessful acquisitions: {len(acquisitions)}")
    for i, (acq, rel) in enumerate(acquisitions):
        print(f"  {i}: Acquired at {acq:.3f}, Released at {rel:.3f}, Duration: {rel-acq:.3f}")

    # Ensure at least some tasks succeeded (should be most of them given 2.0s timeout and 0.2s hold)
    assert len(acquisitions) >= 1

    # Verify that the gap between successful acquisitions is always >= hold_time
    # or more accurately, that they don't overlap.
    for i in range(len(acquisitions) - 1):
        prev_release = acquisitions[i][1]
        next_acquire = acquisitions[i+1][0]
        # The gap should be positive (no overlap)
        assert next_acquire >= prev_release, f"Overlap detected between acquisition {i} and {i+1}"
        # Given how the retry loop works, the gap might be small, but it must be non-negative.


@pytest.mark.asyncio
async def test_real_redis_lock_manager():
    """Test RedisLockManager with real Redis connection from environment variables."""

    # Create a lock manager using environment variables
    redis_host = os.environ.get("REDIS_HOST", "localhost")
    redis_port = int(os.environ.get("REDIS_PORT", "6379"))
    redis_db = int(os.environ.get("REDIS_DB", "0"))
    redis_password = os.environ.get("REDIS_PASSWORD")

    manager = RedisLockManager(
        redis_host=redis_host,
        redis_port=redis_port,
        redis_db=redis_db,
        password=redis_password
    )

    # Create a lock
    lock = manager.lock("test_real_manager_lock")

    # Test the lock works
    await lock.acquire()
    assert await lock.is_locked() is True
    await lock.release()
    assert await lock.is_locked() is False


@pytest.mark.asyncio
async def test_real_redis_lock_manager_with_existing_connection(redis_connection):
    """Test RedisLockManager with an existing Redis connection."""
    # Create a lock manager with default settings
    manager = RedisLockManager()

    # Create a lock with the existing connection
    lock_id = "test_real_manager_existing_conn_lock"
    lock = AsyncRedisLock2(
        redis_url='provided-as-connection-object',
        lock_channel=f"lock:{lock_id}",
        connection=redis_connection
    )

    # Test the lock works
    await lock.acquire()
    assert await lock.is_locked() is True
    await lock.release()
    assert await lock.is_locked() is False


@pytest.mark.asyncio
async def test_real_redis_lock_manager_configure():
    """Test RedisLockManager configuration with real Redis connection."""
    # Create a lock manager with default settings
    manager = RedisLockManager()

    # Configure with environment variables
    redis_host = os.environ.get("REDIS_HOST", "localhost")
    redis_port = int(os.environ.get("REDIS_PORT", "6379"))
    redis_db = int(os.environ.get("REDIS_DB", "0"))
    redis_password = os.environ.get("REDIS_PASSWORD")

    manager.configure(
        redis_host=redis_host,
        redis_port=redis_port,
        redis_db=redis_db,
        redis_password=redis_password,
        timeout=5.0,
        check_interval=0.2,
        fail_when_locked=True
    )

    # Create a lock
    lock = manager.lock("test_real_manager_configure_lock")

    # Test the lock works with the new configuration
    await lock.acquire()
    assert await lock.is_locked() is True
    await lock.release()
    assert await lock.is_locked() is False
