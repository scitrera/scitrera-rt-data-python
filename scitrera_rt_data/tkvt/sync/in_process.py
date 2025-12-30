from collections import defaultdict

from ...tkvt._base import TKVTBroker


class InProcessLightTKVTBroker(TKVTBroker):
    _consumers = None

    def __init__(self, topic_prefix=''):
        self._prefix = prefix = topic_prefix or ''
        if prefix:
            self._prefix = f"{prefix}."
        self.reset()

    @property
    def prefix(self):
        return self._prefix

    def register_consumer(self, topic, fn_tkvt, **kwargs):
        c = self._consumers[f'{self._prefix}{topic}']
        if fn_tkvt not in c:  # memory ref equality check, even though shouldn't be necessary for our testing harness...
            c.append(fn_tkvt)
            # TODO: add option to prioritize topic order for easier, equivalent single-threading

    def reset(self):
        self._consumers = defaultdict(list)

    def consumer_loop_sync(self, **kwargs):
        # no-op because publishing is direct
        return

    def publish(self, topic, key, value, timestamp, **kwargs):
        for fn in self._consumers[f'{self._prefix}{topic}']:
            fn(topic, key, value, timestamp)  # TODO: option for debug/trace logging of calls?

    def flush(self):
        # no-op
        pass

    def abort(self):
        # for in-process broker, we can stop all activities by clearing the consumers
        self._consumers = defaultdict(list)


class InProcessFakeTKVTBroker(InProcessLightTKVTBroker):
    def publish(self, topic, key, value, timestamp, **kwargs):
        from ...serialization.msgpack import msgpack_serialize, msgpack_deserialize
        # serialize w/ msgpack since then we get a better representative test of TKVT behavior
        serialized_value = msgpack_serialize(value)
        deserialized_value = msgpack_deserialize(serialized_value)
        super().publish(topic, key, deserialized_value, timestamp, **kwargs)
