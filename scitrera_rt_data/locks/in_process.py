from __future__ import annotations

import asyncio
import logging
import random
import weakref
from typing import Dict, Optional, Any, Set

from ._base import LockManager, AlreadyLocked, AsyncLock

# Global lock registry to track locks across all instances
_GLOBAL_LOCKS: Dict[str, Set[weakref.ref]] = {}
DEFAULT_THREAD_SLEEP_TIME = 0.1
DEFAULT_UNAVAILABLE_TIMEOUT = 1


class InProcessAsyncLock(AsyncLock):
    """
    An in-process asynchronous lock implementation.
    
    This lock uses asyncio primitives to provide locking functionality
    similar to AsyncRedisLock but without requiring Redis.
    """

    def __init__(
            self,
            lock_id: str,
            timeout: float | None = None,
            check_interval: float | None = None,
            fail_when_locked: bool | None = False,
            thread_sleep_time: float = DEFAULT_THREAD_SLEEP_TIME,
            unavailable_timeout: float = DEFAULT_UNAVAILABLE_TIMEOUT,
            logger: logging.Logger | None = None,
    ) -> None:
        """
        Initialize an in-process async lock.
        
        Args:
            lock_id: A unique identifier for the lock.
            timeout: Maximum time to wait for the lock. If None, use the default timeout.
            check_interval: Time between lock acquisition attempts. If None, use the default interval.
            fail_when_locked: If True, raise an exception if the lock is already held.
            thread_sleep_time: Time to sleep between lock acquisition attempts.
            unavailable_timeout: Timeout for checking if a lock is unavailable.
            logger: Logger instance to use for logging.
        """
        self.lock_id = lock_id
        self.thread_sleep_time = thread_sleep_time
        self.unavailable_timeout = unavailable_timeout
        self.logger = logging.getLogger('InProcessAsyncLock') if logger is None else logger
        self._lock_acquired = False
        self._lock = asyncio.Lock()

        self.timeout = timeout
        self.check_interval = check_interval
        self.fail_when_locked = fail_when_locked

        # Register this lock in the global registry
        if lock_id not in _GLOBAL_LOCKS:
            _GLOBAL_LOCKS[lock_id] = set()
        _GLOBAL_LOCKS[lock_id].add(weakref.ref(self, self._cleanup_lock_ref))

    @staticmethod
    def _cleanup_lock_ref(weak_ref):
        """Remove the weak reference from all lock sets when the lock is garbage collected."""
        for lock_set in _GLOBAL_LOCKS.values():
            if weak_ref in lock_set:
                lock_set.remove(weak_ref)

        # Clean up empty sets
        empty_keys = [k for k, v in _GLOBAL_LOCKS.items() if not v]
        for k in empty_keys:
            del _GLOBAL_LOCKS[k]

    async def _timeout_generator(
            self, timeout: float | None, check_interval: float | None
    ) -> asyncio.Future:
        """
        Generate timeouts for lock acquisition attempts.
        
        Args:
            timeout: Maximum time to wait for the lock.
            check_interval: Time between lock acquisition attempts.
            
        Yields:
            A future that completes after the check interval.
        """
        if timeout is None:
            timeout = 0.0
        if check_interval is None:
            check_interval = self.thread_sleep_time

        deadline = asyncio.get_running_loop().time() + timeout
        first = True

        while first or asyncio.get_running_loop().time() < deadline:
            first = False
            effective_interval = (
                check_interval if check_interval > 0 else self.thread_sleep_time
            )
            sleep_time = effective_interval * (0.5 + random.random())
            await asyncio.sleep(sleep_time)
            yield 0

    async def acquire(
            self,
            timeout: float | None = None,
            check_interval: float | None = None,
            fail_when_locked: bool | None = None,
    ) -> InProcessAsyncLock:
        """
        Acquire the lock.
        
        Args:
            timeout: Maximum time to wait for the lock. If None, use the default timeout.
            check_interval: Time between lock acquisition attempts. If None, use the default interval.
            fail_when_locked: If True, raise an exception if the lock is already held.
                             If None, use the default behavior.
                             
        Returns:
            The lock object itself to allow for method chaining.
            
        Raises:
            AlreadyLocked: If the lock is already held and fail_when_locked is True.
        """
        timeout = timeout or self.timeout or 0.0
        check_interval = check_interval or self.check_interval or 0.0
        fail_when_locked = fail_when_locked or self.fail_when_locked

        # If we already have the lock, return immediately
        if self._lock_acquired:
            return self

        # noinspection PyTypeChecker
        async for _ in self._timeout_generator(timeout, check_interval):
            # Try to acquire the lock
            if await self._try_acquire():
                return self

            if fail_when_locked:
                raise AlreadyLocked()

        # If we get here, we couldn't acquire the lock within the timeout
        raise AlreadyLocked()

    async def _try_acquire(self) -> bool:
        """
        Try to acquire the lock.
        
        Returns:
            True if the lock was acquired, False otherwise.
        """
        # Check if any other locks with the same ID are active
        if self.lock_id in _GLOBAL_LOCKS:
            active_locks = [ref() for ref in _GLOBAL_LOCKS[self.lock_id] if ref() is not None and ref() is not self]
            if any(lock._lock_acquired for lock in active_locks):
                return False

        # Try to acquire the asyncio lock
        if not self._lock.locked():
            async with self._lock:
                # Double-check that no other locks were acquired while we were waiting
                if self.lock_id in _GLOBAL_LOCKS:
                    active_locks = [ref() for ref in _GLOBAL_LOCKS[self.lock_id] if ref() is not None and ref() is not self]
                    if any(lock._lock_acquired for lock in active_locks):
                        return False

                self._lock_acquired = True
                return True

        return False

    async def release(self) -> None:
        """
        Release the lock.
        """
        self._lock_acquired = False

    async def is_locked(self) -> bool:
        """
        Check if the lock is currently held.
        
        Returns:
            True if the lock is held, False otherwise.
        """
        if self.lock_id in _GLOBAL_LOCKS:
            active_locks = [ref() for ref in _GLOBAL_LOCKS[self.lock_id] if ref() is not None]
            return any(lock._lock_acquired for lock in active_locks)
        return False

    async def __aenter__(self) -> InProcessAsyncLock:
        """
        Acquire the lock when entering the async context manager.
        
        Returns:
            The lock object itself.
        """
        await self.acquire()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        """
        Release the lock when exiting the async context manager.
        """
        await self.release()


class InProcessLockManager(LockManager):
    """
    An in-process implementation of LockManager that creates InProcessAsyncLock instances.
    
    This class provides a factory for InProcessAsyncLock objects that can be used for
    in-process locking without requiring Redis.
    """

    def __init__(self, logger: Optional[logging.Logger] = None) -> None:
        """
        Initialize the InProcessLockManager.
        
        Args:
            logger: Optional logger instance.
        """
        self.logger = logger or logging.getLogger('InProcessLockManager')
        self.lock_kwargs = {}

    def lock(self, lock_id: str, **kwargs) -> InProcessAsyncLock:
        """
        Create and return an InProcessAsyncLock for the given lock ID.
        
        Args:
            lock_id: A unique identifier for the lock.
            
        Returns:
            An InProcessAsyncLock instance.
        """
        return InProcessAsyncLock(
            lock_id=lock_id,
            logger=self.logger,
            **self.lock_kwargs
        )

    def configure(self, **kwargs) -> None:
        """
        Configure the lock manager with the given parameters.
        
        Args:
            **kwargs: Configuration parameters for the locks.
                Supported parameters:
                - timeout: Default timeout for lock acquisition.
                - check_interval: Default interval between lock acquisition attempts.
                - fail_when_locked: Default behavior when a lock is already held.
                - thread_sleep_time: Default sleep time between lock acquisition attempts.
                - unavailable_timeout: Default timeout for checking if a lock is unavailable.
        """
        self.lock_kwargs = kwargs
