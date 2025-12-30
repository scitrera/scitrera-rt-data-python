import time
import logging
from typing import Any, Optional, Iterable

from scitrera_rt_data.tkvt._base import TKVTBroker, TKVT_FN, Timestamp
from scitrera_rt_data.serialization.msgpack import msgpack_serialize, msgpack_deserialize
from scitrera_rt_data.dt import dt_from_unix_ns

try:
    import redis
except ImportError:
    raise RuntimeError("redis is not installed")

DEFAULT_POLL_TIMEOUT_MS = 500
DEFAULT_BATCH_SIZE = 3


class RedisBroker(TKVTBroker):
    def __init__(self, redis_url: str, logger: logging.Logger = None, redis_instance=None,
                 max_stream_days: int = None, max_stream_length: int = None):
        self._redis_url = redis_url
        self._redis = redis.Redis.from_url(redis_url, decode_responses=False) if redis_instance is None else redis_instance
        self._shutdown = False
        self._failure = False
        self._consumers: dict[str, list[TKVT_FN]] = {}
        self.logger = logging.getLogger('RedisBroker') if logger is None else logger
        self._last_ids = None  # type: Optional[dict[str, str]]

        # housekeeping/gc stuff # TODO: only let one of days or length be set?
        self._max_stream_days_adjustment = None if max_stream_days is None else int(86_400_000 * max_stream_days)
        self._max_stream_length = max_stream_length

    @property
    def subscribed_topics(self):
        return sorted(self._consumers.keys())

    def consumer_loop_sync(
            self,
            poll_timeout_ms=DEFAULT_POLL_TIMEOUT_MS,
            message_batch_size=DEFAULT_BATCH_SIZE,
            **kwargs):
        self.logger.debug('ping redis connection')
        self._redis.ping()

        self._shutdown = False
        return self._consume(poll_timeout_ms, message_batch_size)

    def _consume(self, poll_timeout_ms=DEFAULT_POLL_TIMEOUT_MS, message_batch_size=DEFAULT_BATCH_SIZE):
        c = self._consumers
        xread = self._redis.xread
        if self._last_ids is None:
            self._last_ids = last_ids = {topic: "0" for topic in c}
        else:
            last_ids = self._last_ids

        self.logger.debug('consumer task init: %s', last_ids)
        while not self._shutdown:
            try:
                response = xread(last_ids, block=poll_timeout_ms, count=message_batch_size)
                for topic_bytes, messages in response:
                    topic = topic_bytes.decode('utf-8')
                    if topic not in c:
                        continue
                    functions = c[topic]
                    for message_id, data in messages:
                        key = data[b'key'].decode('utf-8')
                        timestamp = dt_from_unix_ns(int(data[b'timestamp']))
                        value = msgpack_deserialize(data[b'value'])
                        for fn in functions:
                            fn(topic, key, value, timestamp)
                        last_ids[topic] = message_id
            except Exception as e:
                self.logger.error(f"Error consuming from streams {list(c.keys())}: {e}")
                time.sleep(0.25)

        if isinstance(self._failure, bool):
            return not self._failure
        return self._failure

    def register_consumer(self, topic: str, fn_tkvt: TKVT_FN, **kwargs) -> None:
        self.logger.debug('register_consumer(%s, fn_tkvt=%s)', topic, fn_tkvt)
        c = self._consumers
        if topic in c:
            c[topic].append(fn_tkvt)
        else:
            c[topic] = [fn_tkvt]

    def publish(self, topic: str, key: str, value: Any, timestamp: Timestamp, **kwargs) -> None:
        already_serialized = kwargs.pop('already_serialized', False)
        payload = {
            'key': key,
            'value': value if already_serialized else msgpack_serialize(value),
            'timestamp': timestamp.value,  # int ns
        }
        xadd_kwargs: dict[str, Any] = {
            'approximate': True,
        }
        if self._max_stream_days_adjustment is not None:
            xadd_kwargs['minid'] = int((timestamp.value // 1_000_000) - self._max_stream_days_adjustment)
        elif self._max_stream_length is not None:
            xadd_kwargs['maxlen'] = self._max_stream_length

        result = self._redis.xadd(topic, payload, **xadd_kwargs)
        return result

    def flush(self) -> None:
        self.logger.debug('flush')
        return

    def abort(self, failure=False) -> None:
        self.logger.debug('abort')
        self._failure = failure
        self._shutdown = True
        self.logger.debug('closing redis connection')
        self._redis.close()
        return

    @property
    def consumer_state(self) -> dict[str, Any]:
        return self._last_ids

    @consumer_state.setter
    def consumer_state(self, value: Optional[dict[str, Any]]):
        self._last_ids = value
