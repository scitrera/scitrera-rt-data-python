from __future__ import annotations

import asyncio
import logging
from typing import Optional, Dict, Any, ClassVar
from uuid import uuid4

from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from ..dt import now, dt_to_unix_s
from ..serialization.msgpack import msgpack_serialize, msgpack_deserialize
from ..redis_ import construct_redis_uri, construct_aioredis_from_url
from ._base import AsyncLock, AlreadyLocked, LockManager

try:
    import redis.asyncio as aioredis
except ImportError:
    raise RuntimeError("redis.asyncio is not installed. Run pip install redis[hiredis]")

DEFAULT_LOCK_LEASE_TTL = 5.0  # increased to 5 s because 2.5 s would sometimes miss
DEFAULT_LOCK_LEASE_RENEWAL = 1.0
FIRST_LOCK_TIME_FACTOR = 5
DEFAULT_LOCK_ACQ_TIMEOUT = 15.0  # should be > (FIRST_LOCK_TIME_FACTOR * DEFAULT_LOCK_LEASE_TTL)
DEFAULT_LOCK_ACQ_INTERVAL = 1.0


class AsyncRedisLock2(AsyncLock):
    """
    Async Redis Lock Mechanism based on storing and renewing TTL for "string"/bytes keys.
    This implementation is compatible with redis, valkey, upstash, and dragonfly.
    """

    def __init__(self,
                 redis_url: str,
                 lock_channel: str,
                 connection: 'aioredis.Redis | None' = None,
                 metadata: dict = None,
                 fail_when_locked: bool = False,
                 redis_kwargs: Optional[dict] = None,
                 lease_time=DEFAULT_LOCK_LEASE_TTL, lease_renewal=DEFAULT_LOCK_LEASE_RENEWAL,
                 acq_timeout=DEFAULT_LOCK_ACQ_TIMEOUT, acq_interval=DEFAULT_LOCK_ACQ_INTERVAL,
                 logger: logging.Logger = None):
        if logger is None:
            logger = logging.getLogger('AsyncRedisLock2')
        self.logger = logger

        self._redis_url = redis_url
        self._redis_kwargs = redis_kwargs = redis_kwargs or {}
        self._redis: 'aioredis.Redis' = (construct_aioredis_from_url(redis_url, **redis_kwargs)
                                         if redis_url and connection is None else connection)

        self._name = name = lock_channel  # lock name is used for lookups, so we want that to be "regulated" (and not random)
        self._unique_id: str = uuid4().hex  # unique ID is used for checks to ensure we can't remove someone else's lock
        self._metadata: dict = metadata or {}  # include metadata in what we store attached to the lock key

        self._lease_time_ms: int = int(lease_time * 1000)  # store in ms
        self._lease_renewal_s: float = lease_renewal  # store in seconds
        self._acq_timeout_s: float = acq_timeout  # store in seconds
        self._acq_interval_s: float = acq_interval  # store in seconds
        self._fail_when_locked: bool = fail_when_locked

        self._key: bytes = b'lock:' + name.encode()
        self._lock_held: bool = False
        self._renewal_future: asyncio.TimerHandle | None = None

    def _conn(self):
        if (r := self._redis) is None:
            self._redis = r = aioredis.Redis.from_url(self._redis_url, **self._redis_kwargs)
        return r

    async def _acquire(self, nx=True) -> bool:
        r = self._conn()
        value = msgpack_serialize({
            'unique_id': self._unique_id,
            'acquired_at': dt_to_unix_s(now()),
            **self._metadata,
        })
        self.logger.debug("[%s] Trying to acquire lock: %d", self._name, self._lease_time_ms)
        # nx indicates to only set the key if it does not exist!
        response = await r.set(self._key, value, px=FIRST_LOCK_TIME_FACTOR * self._lease_time_ms, nx=nx, get=False)
        # self.logger.debug("[%s] Acquire response: %s", self._name, response)
        if response:
            self._lock_held = True
            self.logger.info('[%s] Acquired Lock', self._name)
            await self._schedule_renewal()
            return True

        return False

    def _inner_schedule_renewal(self):
        # self.logger.debug("[%s] Scheduling (Inner) Renewal", self._name)
        asyncio.create_task(self._renew())

    async def _schedule_renewal(self):
        # self.logger.debug("[%s] Scheduling Renewal", self._name)
        # Cancel any existing renewal future
        if self._renewal_future is not None:
            self.logger.debug("[%s] Cancel existing renewal future", self._name)
            self._renewal_future.cancel()

        # Schedule a new renewal
        loop = asyncio.get_running_loop()
        # noinspection PyTypeChecker
        self._renewal_future = loop.call_later(
            self._lease_renewal_s,
            self._inner_schedule_renewal,
        )
        return

    # TODO: potentially this class should take a callback for lock loss? (to allow it operate across thread & asyncio loop lines...)
    @retry(retry=retry_if_exception_type((ConnectionError, TimeoutError)), stop=stop_after_attempt(5), wait=wait_exponential())
    async def _renew(self, and_reschedule: bool = True):
        r = self._conn()
        # self.logger.debug("[%s] Renewing for %d", self._name, self._lease_time_ms)
        updated = await r.pexpire(self._key, self._lease_time_ms, xx=True) == 1  # PEXPIRE responds w/ int 1 if timeout was set
        if not updated:  # if we fail to update pexpire(xx) then we might've lost the lock
            self.logger.warning('Failed to update lock key timeout; we likely lost our lock!; trying to reacquire')
            reacquire = await self._acquire()  # so try to reacquire  # TODO: verify lock unique ID / ensure that we haven't been replaced
            if not reacquire:  # and if we can't, then we fail!
                self.logger.error('Failed to reacquire lock!!!')
                raise AlreadyLocked("unable to reacquire lock")
            else:
                self.logger.debug('Lock reacquisition successful')
        # else:
        #     self.logger.debug('Lock key timeout update successful')
        if and_reschedule:
            # _renew is only called from our perpetual renewal context, so this is to avoid trying to cancel it while it is rescheduling
            self._renewal_future = None
            await self._schedule_renewal()

    async def acquire(self, timeout: float | None = None, check_interval: float | None = None,
                      fail_when_locked: bool | None = None) -> AsyncLock:
        fail_when_locked = self._fail_when_locked if fail_when_locked is None else fail_when_locked
        timeout = self._acq_timeout_s if timeout is None else timeout
        check_interval = self._acq_interval_s if check_interval is None else check_interval

        # Try to acquire the lock immediately
        if await self._acquire():
            return self

        # If we should fail immediately when locked
        if fail_when_locked:
            raise AlreadyLocked(f"Lock {self._name} is already held")

        # Calculate deadline for timeout
        deadline = asyncio.get_running_loop().time() + timeout

        # Retry loop
        while asyncio.get_running_loop().time() < deadline:
            # Wait before retrying
            await asyncio.sleep(check_interval)

            # Try to acquire again
            if await self._acquire():
                return self

        # If we get here, we couldn't acquire the lock within the timeout
        raise AlreadyLocked(f"Lock {self._name} could not be acquired within {timeout} seconds")

    async def release(self) -> None:
        r = self._conn()
        # Cancel the renewal future if it exists (if we don't hold lock, no consequence; if we do, then as intended)
        if self._renewal_future is not None:
            self._renewal_future.cancel()
            self._renewal_future = None

        # TODO: should the delete call be implemented as lua script to run on redis side? [leveraging cmsgpack]
        # Only try to delete the key if we believe we hold the lock
        if self._lock_held:
            # Get the current lock data to check if we're still the owner
            lock_data = await r.get(self._key)
            if lock_data:
                try:
                    data = msgpack_deserialize(lock_data)
                    # Only delete if we're the lock owner
                    if isinstance(data, dict) and data.get('unique_id') == self._unique_id:
                        await r.delete(self._key)
                        self.logger.debug('[%s] Released Lock', self._name)
                except Exception:
                    # If we can't deserialize or there's any other error, don't delete the key
                    pass

            # Reset lock held flag
            self._lock_held = False

    async def is_locked(self) -> bool:
        r = self._conn()
        # check if the lock key exists
        return (await r.exists(self._key)) == 1

    async def __aenter__(self) -> AsyncLock:
        # Acquire lock and return self (or fail and raise AlreadyLocked exception)
        return await self.acquire()

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        # Release lock
        await self.release()


class RedisLockManager(LockManager):
    """
    A Redis-based implementation of LockManager that creates AsyncRedisLock instances.

    This class provides a factory for AsyncRedisLock objects that can be used for
    distributed locking using Redis.
    """

    DEFAULT_REDIS_KWARGS: ClassVar[dict[str, Any]] = dict(
        # health_check_interval=10,
        decode_responses=False,
    )

    def __init__(
            self,
            redis_host: str = 'localhost',
            redis_port: int = 6379,
            redis_db: int = 0,
            password: Optional[str] = None,
            ssl: bool = False,
            redis_kwargs: Optional[Dict[str, Any]] = None,
            logger: Optional[logging.Logger] = None
    ) -> None:
        """
        Initialize the RedisLockManager with Redis connection parameters.

        Args:
            redis_host: The Redis server hostname or IP address.
            redis_port: The Redis server port.
            redis_db: The Redis database number.
            password: The Redis password, if required.
            redis_kwargs: Additional Redis connection parameters.
            logger: Optional logger instance.
        """
        self.redis_host = redis_host
        self.redis_port = redis_port
        self.redis_db = redis_db
        self.redis_password = password
        self.redis_ssl = ssl
        self.redis_kwargs = rk = dict(self.DEFAULT_REDIS_KWARGS)
        if redis_kwargs:
            rk.update(redis_kwargs)
        self.logger = logger or logging.getLogger('RedisLockManager')

        self._redis_uri = construct_redis_uri(self.redis_host, self.redis_port, self.redis_db, self.redis_password, self.redis_ssl)
        self._redis_inst = None  # self._construct_redis()

    def _construct_redis(self) -> 'aioredis.Redis':
        return aioredis.Redis(host=self.redis_host, port=self.redis_port, db=self.redis_db,
                              password=self.redis_password, ssl=self.redis_ssl, **self.redis_kwargs)

    def lock(self, lock_id: str, metadata: Optional[dict] = None, **kwargs) -> AsyncRedisLock2:
        """
        Create and return an AsyncRedisLock for the given lock ID.

        Args:
            lock_id: A unique identifier for the lock.
            metadata: Optional metadata for the lock.

        Returns:
            An AsyncRedisLock instance.
        """
        # if self._redis_inst is None:
        #     self._redis_inst = self._construct_redis()
        # TODO: make it configurable whether we share or create redis instance (for now opt for separate for threading flexibility)
        redis_inst = None  # self._construct_redis()  # setting to None just means that we defer to letting Lock instance do it w/ URI info
        return AsyncRedisLock2(
            self._redis_uri,
            lock_id,
            connection=redis_inst,
            metadata=metadata,
            redis_kwargs=self.redis_kwargs,
            logger=self.logger,
        )

    # TODO: move active_locks up to LockManager interface
    async def active_locks(self) -> Dict[str, Dict]:
        """Get all active locks and their associated data."""
        if self._redis_inst is None:
            self._redis_inst = self._construct_redis()
        pattern = b'lock:*'
        all_locks = {}
        for key in await self._redis_inst.keys(pattern):
            value = await self._redis_inst.get(key)
            if value:
                try:
                    data = msgpack_deserialize(value)
                    if isinstance(data, dict):
                        all_locks[key.decode()[5:]] = data  # Remove 'lock:' prefix
                except Exception:
                    pass
        return all_locks

    def configure(self, **kwargs) -> None:
        """
        Configure the lock manager with the given parameters.

        Args:
            **kwargs: Configuration parameters for the Redis connection.
                Supported parameters:
                - redis_host: The Redis server hostname or IP address.
                - redis_port: The Redis server port.
                - redis_db: The Redis database number.
                - redis_password: The Redis password, if required.
                - redis_kwargs: Additional Redis connection parameters.
        """
        if 'redis_host' in kwargs:
            self.redis_host = kwargs['redis_host']
        if 'redis_port' in kwargs:
            self.redis_port = kwargs['redis_port']
        if 'redis_db' in kwargs:
            self.redis_db = kwargs['redis_db']

        if 'redis_password' in kwargs:
            self.redis_password = kwargs['redis_password']
        elif 'password' in kwargs:
            self.redis_password = kwargs['password']

        if 'ssl' in kwargs:
            self.redis_ssl = kwargs['ssl']
        if 'redis_kwargs' in kwargs:
            self.redis_kwargs = kwargs['redis_kwargs']

        self._redis_uri = construct_redis_uri(self.redis_host, self.redis_port, self.redis_db, self.redis_password, self.redis_ssl)
        self._redis_inst = self._construct_redis()
        return
