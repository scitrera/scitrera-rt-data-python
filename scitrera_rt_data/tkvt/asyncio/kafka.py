import asyncio
import logging
from collections import defaultdict
from time import sleep
from typing import Any

import pandas as pd
from vpd import is_iterable

from .._base import AsyncTKVTBroker
from ...serialization.msgpack import msgpack_serialize, msgpack_deserialize
from ...dt import UTC, dt_to_unix_ms, dt_from_unix_ms

try:
    from confluent_kafka import Producer, Consumer, TopicPartition, KafkaError, KafkaException
    from confluent_kafka.admin import AdminClient, NewTopic
except ImportError:
    raise RuntimeError('Kafka asyncio broker requires confluent-kafka')

DEFAULT_SERIALIZER = msgpack_serialize
DEFAULT_DESERIALIZER = msgpack_deserialize


def kafka_cfg_disable_offsets(kafka_cfg: dict) -> dict:
    kafka_cfg['auto.offset.reset'] = 'latest'
    kafka_cfg['enable.auto.commit'] = False
    return kafka_cfg


class AsyncKafkaBroker(AsyncTKVTBroker):
    _ac = None  # admin client
    _pr = None  # producer
    _active_consumer = None
    _more_topics = None
    logger = None

    _not_abort = True

    def __init__(self, bootstrap_servers, consumer_group_name,
                 topic_prefix=None, start_at_end=True,
                 logger=None, ooo_warning=True,
                 **custom_config):
        self._topic_prefix = f'{topic_prefix}.' if topic_prefix else ''
        self._kafka_config = kafka_cfg = {
            'bootstrap.servers': bootstrap_servers,
            'group.id': consumer_group_name,
        }
        self._consumer_topics = defaultdict(list)
        self._more_topics = []
        if start_at_end:
            kafka_cfg_disable_offsets(kafka_cfg)

        kafka_cfg.update(custom_config)

        if logger is None:
            logger = logging.getLogger('async_kafka_broker')
        self.logger = logger

        self._ooo_warning = ooo_warning
        self._consumer_task = None

    @property
    def prefix(self):
        return self._topic_prefix

    def _producer_config(self):
        config = self._kafka_config.copy()
        config.pop('group.id', None)
        config.pop('enable.auto.commit', None)
        config.pop('auto.offset.reset', None)
        config.pop('compression.codec', None)
        return config

    async def ensure_topics_exist(self, topics, partitions=1):
        if self._ac is None:
            self._ac = AdminClient(self._producer_config())
        ac = self._ac
        
        if not is_iterable(topics):
            topics = [topics]

        self.logger.info('Ensuring topics exist: %s', topics)

        loop = asyncio.get_running_loop()
        fs = ac.create_topics([NewTopic(t, partitions) for t in topics], operation_timeout=0.5)
        
        for t, f in fs.items():
            try:
                # confluent_kafka futures are not asyncio futures, we need to wait for them
                while not f.done():
                    await asyncio.sleep(0.01)
                f.result()
            except KafkaException as e:
                if e.args[0].code() != KafkaError.TOPIC_ALREADY_EXISTS:
                    self.logger.warning('error creating topic %s: %s', t, e)
                    raise
        return

    async def register_consumer(self, topic, fn_tkvt, **kwargs):
        topic = f'{self._topic_prefix}{topic}'
        if self._active_consumer and topic not in self._consumer_topics:
            self._more_topics.append(topic)
        
        self._consumer_topics[topic].append(fn_tkvt)
        return

    async def consumer_loop_async(self, blocking=True, poll_timeout=0.003, ooo_grace_period_ms=350):
        if self._consumer_task is not None:
            raise RuntimeError('Consumer loop already running')
        
        self._not_abort = True
        self._consumer_task = asyncio.create_task(self._run_consumer_loop(poll_timeout, ooo_grace_period_ms))
        
        if blocking:
            return await self._consumer_task
        return None

    async def _run_consumer_loop(self, poll_timeout, ooo_grace_period_ms):
        while self._not_abort:
            if not await self._inner_loop(poll_timeout, ooo_grace_period_ms):
                break
        self._consumer_task = None

    async def _inner_loop(self, timeout, ooo_grace_period_ms):
        ct = self._consumer_topics
        mt = self._more_topics
        logger = self.logger
        
        topics = list(ct.keys())
        if not topics:
            await asyncio.sleep(0.1)
            return self._not_abort

        await self.ensure_topics_exist(topics)
        
        kafka_cfg = dict(self._kafka_config)
        def error_cb(kafka_error):
            logger.warning('kafka error: %s', kafka_error)
        kafka_cfg['error_cb'] = error_cb
        
        c = Consumer(kafka_cfg)
        c.subscribe(topics)
        self._active_consumer = c
        
        latest_dt = pd.Timestamp(0, tz=UTC)
        grace_period = pd.Timedelta(ooo_grace_period_ms, unit='ms')
        deserialize = DEFAULT_DESERIALIZER
        
        try:
            logger.info('starting async consumer loop')
            while self._not_abort:
                # poll is blocking, so we run it in a thread to not block the event loop
                msg = await asyncio.get_running_loop().run_in_executor(None, c.poll, timeout)
                
                if msg is None:
                    if mt:
                        mt.clear()
                        return self._not_abort
                    await asyncio.sleep(0) # yield
                    continue
                
                if msg.error():
                    error = msg.error()
                    logger.warning(error)
                    if error.code() == KafkaError.UNKNOWN_TOPIC_OR_PART:
                        break
                    continue
                
                topic = msg.topic()
                key = msg.key().decode('utf-8') if msg.key() else ''
                value = deserialize(msg.value())
                timestamp = dt_from_unix_ms(msg.timestamp()[1])
                
                for fn in ct[topic]:
                    if asyncio.iscoroutinefunction(fn):
                        await fn(topic, key, value, timestamp)
                    else:
                        fn(topic, key, value, timestamp)
                
                if timestamp >= latest_dt:
                    latest_dt = timestamp
                elif self._ooo_warning and latest_dt - timestamp > grace_period:
                    logger.warning('OOO WARNING: topic "%s" saw message from %s, latest_dt=%s', topic, timestamp, latest_dt)
                
                if mt:
                    mt.clear()
                    return self._not_abort
        finally:
            self._active_consumer = None
            c.close()
            logger.info('closed async consumer loop')
        
        return self._not_abort

    async def publish(self, topic: str, key: str, value: Any, timestamp: pd.Timestamp, **kwargs):
        if self._pr is None:
            self._pr = Producer(self._producer_config())
        
        p = self._pr
        topic = f'{self._topic_prefix}{topic}'
        key_bytes = key.encode('utf-8') if key else b''
        value_bytes = msgpack_serialize(value)
        ts_ms = dt_to_unix_ms(timestamp)
        
        # produce is non-blocking (it queues the message)
        p.produce(topic, value=value_bytes, key=key_bytes, timestamp=ts_ms)
        p.poll(0)
        return

    async def flush(self):
        if self._pr is not None:
            await asyncio.get_running_loop().run_in_executor(None, self._pr.flush)

    async def abort(self, failure=False):
        self.logger.info('abort called')
        self._not_abort = False
        await self.flush()
        if self._consumer_task:
            await self._consumer_task
