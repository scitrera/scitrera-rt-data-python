import pytest
import asyncio
from unittest.mock import AsyncMock, MagicMock
from scitrera_rt_data.tkvt.asyncio.in_process import AsyncInProcessLightTKVTBroker, AsyncInProcessFakeTKVTBroker
from scitrera_rt_data.tkvt.sync.in_process import InProcessLightTKVTBroker, InProcessFakeTKVTBroker

@pytest.mark.asyncio
async def test_async_in_process_light_broker():
    broker = AsyncInProcessLightTKVTBroker(topic_prefix="test")
    assert broker.prefix == "test."
    
    received = []
    async def consumer(topic, key, value, timestamp):
        received.append((topic, key, value, timestamp))
        
    await broker.register_consumer("topic1", consumer)
    assert broker.subscribed_topics == ["test.topic1"]
    
    await broker.publish("topic1", "k1", "v1", 12345)
    assert received == [("topic1", "k1", "v1", 12345)]
    
    # Test state
    broker.consumer_state = {"offset": 10}
    assert broker.consumer_state == {"offset": 10}
    
    # Test reset
    broker.reset()
    assert broker.subscribed_topics == []
    assert broker.consumer_state == {}

@pytest.mark.asyncio
async def test_async_in_process_fake_broker():
    broker = AsyncInProcessFakeTKVTBroker()
    
    received = []
    async def consumer(topic, key, value, timestamp):
        received.append(value)
        
    await broker.register_consumer("topic1", consumer)
    
    # Fake broker serializes/deserializes
    data = {"complex": [1, 2, 3]}
    await broker.publish("topic1", "k1", data, 12345)
    assert received == [data]

def test_sync_in_process_light_broker():
    broker = InProcessLightTKVTBroker(topic_prefix="test")
    assert broker.prefix == "test."
    
    received = []
    def consumer(topic, key, value, timestamp):
        received.append((topic, key, value, timestamp))
        
    broker.register_consumer("topic1", consumer)
    
    broker.publish("topic1", "k1", "v1", 12345)
    assert received == [("topic1", "k1", "v1", 12345)]

def test_sync_in_process_fake_broker():
    broker = InProcessFakeTKVTBroker()
    
    received = []
    def consumer(topic, key, value, timestamp):
        received.append(value)
        
    broker.register_consumer("topic1", consumer)
    
    data = {"complex": [1, 2, 3]}
    broker.publish("topic1", "k1", data, 12345)
    assert received == [data]
