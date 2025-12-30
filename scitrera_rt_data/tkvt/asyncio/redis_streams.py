import asyncio
import logging

from asyncio import create_task
from typing import Any, Optional

from scitrera_rt_data.tkvt._base import AsyncTKVTBroker, ASYNC_TKVT_FN, Timestamp
from scitrera_rt_data.serialization.msgpack import msgpack_serialize, msgpack_deserialize
from scitrera_rt_data.dt import dt_from_unix_ns

try:
    import redis.asyncio as redis
except ImportError:
    raise RuntimeError("redis.asyncio is not installed")

DEFAULT_POLL_TIMEOUT_MS = 500
DEFAULT_BATCH_SIZE = 3


class AsyncRedisBroker(AsyncTKVTBroker):
    def __init__(self, redis_url: str, logger: logging.Logger = None, redis_instance=None,
                 max_stream_days: int = None, max_stream_length: int = None):
        self._redis_url = redis_url
        self._redis = redis.Redis.from_url(redis_url, decode_responses=False) if redis_instance is None else redis_instance
        self._shutdown = False
        self._failure = False
        self._consumers: dict[str, list[ASYNC_TKVT_FN]] = {}
        self._consumer_task: Optional[asyncio.Task] = None
        self.logger = logging.getLogger('AsyncRedisBroker') if logger is None else logger
        self._last_ids = None  # type: Optional[dict[str, str]]

        # housekeeping/gc stuff # TODO: only let one of days or length be set?
        self._max_stream_days_adjustment = None if max_stream_days is None else int(86_400_000 * max_stream_days)
        self._max_stream_length = max_stream_length

    @property
    def subscribed_topics(self):
        return sorted(self._consumers.keys())

    async def consumer_loop_async(
            self,
            blocking=True,
            poll_timeout_ms=DEFAULT_POLL_TIMEOUT_MS,
            message_batch_size=DEFAULT_BATCH_SIZE,
            **kwargs):
        self.logger.debug('ping redis connection')
        await self._redis.ping()

        # if there is an existing consumer task, then we should cancel it (or for now, we just raise error)
        if self._consumer_task is not None:
            raise RuntimeError('current implementation does not support starting consumer loop twice')

        if self._consumer_task is None and self._consumers:
            self.logger.debug('creating asyncio consumer task')
            self._shutdown = False  # ensure that shutdown is false before creating loop task
            # TODO: review/consider if we should directly return await self._consume() here instead of creating a task
            self._consumer_task = create_task(self._consume(poll_timeout_ms, message_batch_size))  # create consumption loop task

        if blocking:
            self.logger.debug('asyncio.wait_for(consumer_task)')
            return await asyncio.wait_for(self._consumer_task, timeout=None)
        return None

    async def _consume(self, poll_timeout_ms=DEFAULT_POLL_TIMEOUT_MS, message_batch_size=DEFAULT_BATCH_SIZE):
        c = self._consumers
        xread = self._redis.xread  # TODO: confirm that if we call this again, it'll try to reconnect or do as needed!
        if self._last_ids is None:
            self._last_ids = last_ids = {topic: "0" for topic in c}  # TODO: configurable starting point, don't always start from zero?
        else:
            last_ids = self._last_ids

        self.logger.debug('consumer task init: %s', last_ids)
        # half_timeout_ms = poll_timeout_ms / 2.0
        # half_timeout_s = half_timeout_ms / 1000.0
        while not self._shutdown:
            try:
                # t_pre = time()
                response = await xread(last_ids, block=poll_timeout_ms, count=message_batch_size)
                # t_post = time()
                # # if not response and time was < half_timeout_ms then sleep half_timeout_ms before the next loop?
                # # (this was meant to counteract that upstash redis *says* it does not support blocking XREAD)
                # if not response and (t_post - t_pre) < half_timeout_s:
                #     await asyncio.sleep(half_timeout_s)
                #     continue
                for topic, messages in response:
                    topic = topic.decode('utf-8')
                    if topic not in c:
                        # TODO: log warning; this should not happen, right?
                        continue
                    functions = c[topic]
                    for message_id, data in messages:
                        key = data[b'key'].decode('utf-8')
                        timestamp = dt_from_unix_ns(int(data[b'timestamp']))
                        value = msgpack_deserialize(data[b'value'])
                        for fn in functions:
                            # noinspection PyTypeChecker
                            create_task(fn(topic, key, value, timestamp))
                        last_ids[topic] = message_id
            except asyncio.CancelledError:  # TODO: confirm that there won't be other reasons for getting asyncio.CancelledError!
                self.logger.debug('asyncio.CancelledError caught in AsyncRedisBroker._consume(...)')
                if isinstance(self._failure, bool):
                    return not self._failure
                return self._failure  # we assume that failure is an exception -- in which case we return it directly
                # break
            except Exception as e:
                print(f"Error consuming from streams {list(c.keys())}: {e}")
                await asyncio.sleep(0.25)

        if isinstance(self._failure, bool):  # bool values get swapped since we return True for success.
            return not self._failure
        return self._failure  # we assume that failure is an exception -- in which case we return it directly

    async def register_consumer(self, topic: str, fn_tkvt: ASYNC_TKVT_FN, **kwargs) -> None:
        self.logger.debug('register_consumer(%s, fn_tkvt=%s)', topic, fn_tkvt)
        c = self._consumers
        if topic in c:  # TODO: consider if need to support multiple consumer functions per topic... maybe a lot of overhead for no gain
            c[topic].append(fn_tkvt)
        else:
            c[topic] = [fn_tkvt]

        return

    async def publish(self, topic: str, key: str, value: Any, timestamp: Timestamp, **kwargs) -> None:
        # self.logger.debug('CALL publish(topic=%s, key=%s, value=%s, timestamp=%s)', topic, key, value, timestamp)
        # self.logger.debug('ping redis connection')
        # await self._redis.ping()
        already_serialized = kwargs.pop('already_serialized', False)
        payload = {
            'key': key,
            'value': value if already_serialized else msgpack_serialize(value),
            'timestamp': timestamp.value,  # int ns
        }
        # enforce stream limits (by either days or count)
        xadd_kwargs: dict[str, Any] = {
            'approximate': True,  # doesn't do anything without other settings, but we want to use approx
        }
        if self._max_stream_days_adjustment is not None:
            # note minid is timestamp int in ms
            xadd_kwargs['minid'] = int((timestamp.value // 1_000_000) - self._max_stream_days_adjustment)
        elif self._max_stream_length is not None:
            xadd_kwargs['maxlen'] = self._max_stream_length

        # self.logger.debug('PRE publish(topic=%s, key=%s, value=%s, timestamp=%s)', topic, key, value, timestamp)
        result = await self._redis.xadd(topic, payload, **xadd_kwargs)
        # result = create_task(self._redis.xadd(topic, payload, **xadd_kwargs))
        # self.logger.debug('POST publish(topic=%s, key=%s, value=%s, timestamp=%s)', topic, key, value, timestamp)
        return result

    async def flush(self) -> None:
        self.logger.debug('flush')
        return

    async def abort(self, failure=False) -> None:
        self.logger.debug('abort')
        self._failure = failure
        self._shutdown = True
        if self._consumer_task is not None:
            self.logger.debug('canceling consumer task')
            self._consumer_task.cancel()
            # TODO: wait for cancel completion?
            self._consumer_task = None
        self.logger.debug('closing redis connection')
        await self._redis.aclose()  # try to close redis
        return

    @property
    def consumer_state(self) -> dict[str, Any]:
        return self._last_ids

    @consumer_state.setter
    def consumer_state(self, value: Optional[dict[str, Any]]):
        self._last_ids = value
