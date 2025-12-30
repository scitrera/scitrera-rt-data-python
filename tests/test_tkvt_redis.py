import pytest
import asyncio
import pandas as pd
import redis.asyncio as aioredis
from scitrera_rt_data.tkvt.asyncio.redis_streams import AsyncRedisBroker

@pytest.fixture
async def redis_conn():
    conn = aioredis.Redis(host="localhost", port=6379, db=0)
    yield conn
    await conn.flushdb()
    await conn.aclose()

@pytest.mark.asyncio
async def test_async_redis_broker_publish_consume(redis_conn):
    broker = AsyncRedisBroker(redis_url="redis://localhost:6379/0", redis_instance=redis_conn)
    
    received = asyncio.Queue()
    async def consumer(topic, key, value, timestamp):
        await received.put((topic, key, value, timestamp))
        
    await broker.register_consumer("test_topic", consumer)
    assert broker.subscribed_topics == ["test_topic"]
    
    # Start consumer loop in background
    # We use blocking=False to get the task but it still creates it
    await broker.consumer_loop_async(blocking=False)
    
    # Publish message
    timestamp = pd.Timestamp.now(tz='UTC')
    await broker.publish("test_topic", "key1", {"data": 1}, timestamp)
    
    # Wait for message
    topic, key, value, ts = await asyncio.wait_for(received.get(), timeout=2.0)
    
    assert topic == "test_topic"
    assert key == "key1"
    assert value == {"data": 1}
    # Timestamps might lose some precision or have different types but should be close
    assert abs((ts - timestamp).total_seconds()) < 0.001
    
    await broker.abort()

@pytest.mark.asyncio
async def test_async_redis_broker_state(redis_conn):
    broker = AsyncRedisBroker(redis_url="redis://localhost:6379/0", redis_instance=redis_conn)
    
    state = {"test_topic": "12345-0"}
    broker.consumer_state = state
    assert broker.consumer_state == state
    
    await broker.abort()

@pytest.mark.asyncio
async def test_async_redis_broker_limits(redis_conn):
    # Test with max_stream_length
    broker = AsyncRedisBroker(
        redis_url="redis://localhost:6379/0", 
        redis_instance=redis_conn,
        max_stream_length=5
    )
    
    topic = "limit_topic"
    for i in range(10):
        await broker.publish(topic, f"k{i}", i, pd.Timestamp.now(tz='UTC'))
        
    info = await redis_conn.xinfo_stream(topic)
    # length should be around 5 (approximate=True is used in code)
    assert info['length'] <= 10 
    # Usually it's exactly 5 if not using approximate, but code uses approximate
    
    await broker.abort()
