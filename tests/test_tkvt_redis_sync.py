import pytest
import pandas as pd
import redis
from threading import Thread
from time import sleep

from scitrera_rt_data.tkvt.sync.redis_streams import RedisBroker
from scitrera_rt_data.dt import Timestamp, now


@pytest.fixture
def redis_conn():
    conn = redis.Redis(host="localhost", port=6379, db=0, decode_responses=False)
    yield conn
    conn.flushdb()
    conn.close()


def test_sync_redis_broker_publish_consume(redis_conn):
    broker = RedisBroker(redis_url="redis://localhost:6379/0", redis_instance=redis_conn)

    topic = "test_sync_redis_topic"
    received = []

    def consumer(t, k, v, ts):
        received.append((t, k, v, ts))

    broker.register_consumer(topic, consumer)
    assert broker.subscribed_topics == [topic]

    # Run consumer loop (do this in a separate thread because otherwise it'll prevent the test from finishing)
    loop_thread = Thread(target=broker.consumer_loop_sync, daemon=True)
    loop_thread.start()

    # Wait for consumer to be ready
    sleep(2.0)

    # Publish message
    timestamp = now()
    broker.publish(topic, "key1", {"data": "sync_redis"}, timestamp)

    # Wait in main thread and allow some time for message consumption
    sleep(5.0)

    assert len(received) == 1
    t, k, v, ts = received[0]
    assert t == topic
    assert k == "key1"
    assert v == {"data": "sync_redis"}

    broker.abort()


def test_sync_redis_broker_state(redis_conn):
    broker = RedisBroker(redis_url="redis://localhost:6379/0", redis_instance=redis_conn)

    state = {"test_topic": "12345-0"}
    broker.consumer_state = state
    assert broker.consumer_state == state

    broker.abort()
