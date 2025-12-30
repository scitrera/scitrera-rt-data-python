import logging
from collections import defaultdict
from typing import Any, Optional

from .._base import AsyncTKVTBroker


class AsyncInProcessLightTKVTBroker(AsyncTKVTBroker):
    _consumers = None
    _consumer_state = {}

    def __init__(self, topic_prefix=''):
        self._prefix = prefix = topic_prefix or ''
        if prefix:
            self._prefix = f"{prefix}."
        self.reset()
        self.logger = logging.getLogger(self.__class__.__name__)

    @property
    def prefix(self):
        return self._prefix

    async def register_consumer(self, topic, fn_tkvt, **kwargs):
        c = self._consumers[f'{self._prefix}{topic}']
        if fn_tkvt not in c:  # memory ref equality check, even though shouldn't be necessary for our testing harness...
            c.append(fn_tkvt)
            # TODO: add option to prioritize topic order for easier, equivalent single-threading

    def reset(self):
        self._consumers = defaultdict(list)
        self._consumer_state = {}

    @property
    def subscribed_topics(self):
        return sorted(self._consumers.keys())

    async def consumer_loop_async(self, blocking=True, **kwargs):
        # no-op because publishing is direct
        return

    async def publish(self, topic, key, value, timestamp, **kwargs):
        self.logger.info(f'TKVT push: topic=%s, key=%s, timestamp=%s, value=%s',
                         topic, key, value, timestamp)
        for fn in self._consumers[f'{self._prefix}{topic}']:
            await fn(topic, key, value, timestamp)  # TODO: option for debug/trace logging of calls?

    async def flush(self):
        # no-op
        pass

    async def abort(self, failure=False):
        # for in-process broker, we can stop all activities by clearing the consumers
        self._consumers = defaultdict(list)

    @property
    def consumer_state(self) -> dict[str, Any]:
        return self._consumer_state

    @consumer_state.setter
    def consumer_state(self, value: Optional[dict[str, Any]]):
        self._consumer_state = value or {}


class AsyncInProcessFakeTKVTBroker(AsyncInProcessLightTKVTBroker):
    async def publish(self, topic, key, value, timestamp, **kwargs):
        already_serialized = kwargs.pop('already_serialized', False)
        from ...serialization.msgpack import msgpack_serialize, msgpack_deserialize

        # serialize w/ msgpack since then we get a better representative test of TKVT behavior (unless already serialized)
        serialized_value = value if already_serialized else msgpack_serialize(value)

        # deserialize before printing and running consumer functions regardless
        deserialized_value = msgpack_deserialize(serialized_value)
        await super().publish(topic, key, deserialized_value, timestamp, **kwargs)
