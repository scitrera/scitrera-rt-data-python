import pytest
import asyncio
import pandas as pd
import time
from time import sleep
from threading import Thread

from scitrera_rt_data.tkvt.asyncio.rmq_streams import AsyncRabbitMQStreamsBroker
from scitrera_rt_data.tkvt.sync.rmq_streams import RabbitMQStreamsBroker
from scitrera_rt_data.rmq import rmq_kwargs
from scitrera_rt_data.dt import Timestamp, now

# TODO: resolve RMQ_HOST conundrum (in practice, we may have complex overrides scheme...
#       which is why RMQ_HOST is separate from other parameters in rmq_kwargs(...))
RMQ_HOST = "localhost"
RMQ_PORT = 5552


@pytest.mark.asyncio
async def test_async_rmq_broker_publish_consume():
    broker = AsyncRabbitMQStreamsBroker(RMQ_HOST, rmq_port=RMQ_PORT, mtls=False)

    topic = "test_async_rmq_topic"
    received = asyncio.Queue()

    async def consumer(t, k, v, ts):
        await received.put((t, k, v, ts))

    await broker.register_consumer(topic, consumer)

    # Start consumer loop
    loop_task = asyncio.create_task(broker.consumer_loop_async(blocking=True))

    # Wait for consumer to be ready (it needs to subscribe)
    await asyncio.sleep(1.0)

    # Publish message
    now = pd.Timestamp.now(tz='UTC')
    timestamp = Timestamp(now.value)
    await broker.publish(topic, "key1", {"data": "async_rmq"}, timestamp)

    # Wait for message
    t, k, v, ts = await asyncio.wait_for(received.get(), timeout=5.0)

    assert t == topic
    assert k == "key1"
    assert v == {"data": "async_rmq"}

    await broker.abort()
    try:
        await asyncio.wait_for(loop_task, timeout=1.0)
    except (asyncio.TimeoutError, asyncio.CancelledError):
        pass


def test_sync_rmq_broker_publish_consume():
    broker = RabbitMQStreamsBroker(RMQ_HOST, rmq_port=RMQ_PORT, mtls=False)

    topic = "test_sync_rmq_topic"
    received = []

    def consumer(t, k, v, ts):
        received.append((t, k, v, ts))

    broker.register_consumer(topic, consumer)

    # Run consumer loop (do this in a separate thread because otherwise it'll prevent the test from finishing)
    loop_thread = Thread(target=broker.consumer_loop_sync, daemon=True)
    loop_thread.start()

    # Wait for consumer to be ready
    sleep(2.0)

    # Publish message
    timestamp = now()
    broker.publish(topic, "key1", {"data": "sync_rmq"}, timestamp)

    # Wait in main thread and allow some time for message consumption
    sleep(5.0)

    assert len(received) == 1
    t, k, v, ts = received[0]
    assert t == topic
    assert k == "key1"
    assert v == {"data": "sync_rmq"}

    broker.abort()
