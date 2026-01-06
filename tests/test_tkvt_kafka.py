from threading import Thread
from time import sleep

import pytest
import asyncio
import pandas as pd
import time
from scitrera_rt_data.tkvt.asyncio.kafka import AsyncKafkaBroker
from scitrera_rt_data.tkvt.sync.kafka import KafkaBroker
from scitrera_rt_data.dt import Timestamp, now

KAFKA_BOOTSTRAP = "localhost:9092"


@pytest.mark.asyncio
async def test_async_kafka_broker_publish_consume():
    broker = AsyncKafkaBroker(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        consumer_group_name="test_async_group",
        start_at_end=True
    )

    topic = "test_async_kafka_topic"
    received = asyncio.Queue()

    async def consumer(t, k, v, ts):
        await received.put((t, k, v, ts))

    await broker.register_consumer(topic, consumer)

    # Start consumer loop
    await broker.consumer_loop_async(blocking=False)

    # Wait for consumer to be ready
    await asyncio.sleep(5.0)

    # Publish message
    timestamp = now()
    await broker.publish(topic, "key1", {"data": "async_kafka"}, timestamp)

    # Wait for message
    t, k, v, ts = await asyncio.wait_for(received.get(), timeout=10.0)

    assert t == topic
    assert k == "key1"
    assert v == {"data": "async_kafka"}

    await broker.abort()


def test_sync_kafka_broker_publish_consume():
    broker = KafkaBroker(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        consumer_group_name="test_sync_group",
        start_at_end=True
    )

    topic = "test_sync_kafka_topic"
    received = []

    def consumer(t, k, v, ts):
        received.append((t, k, v, ts))
        broker.abort()  # Stop the loop

    broker.register_consumer(topic, consumer)

    # Run consumer loop (do this in a separate thread because otherwise it'll prevent the test from finishing)
    loop_thread = Thread(target=broker.consumer_loop_sync, daemon=True)
    loop_thread.start()

    # Wait for consumer to be ready
    sleep(2.0)

    # Publish message
    timestamp = now()
    broker.publish(topic, "key1", {"data": "sync_kafka"}, timestamp)
    broker.flush()

    # Wait in main thread and allow some time for message consumption
    sleep(5.0)

    assert len(received) >= 1
    t, k, v, ts = received[0]
    assert t == topic
    assert k == "key1"
    assert v == {"data": "sync_kafka"}

    broker.abort()
