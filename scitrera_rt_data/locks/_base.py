from __future__ import annotations

import abc
from typing import Any  # , Dict, Optional, TypeVar


# # Define a generic type for lock objects
# L = TypeVar('L')

class LockException(Exception):
    pass


class AlreadyLocked(LockException):
    pass


class AsyncLock(abc.ABC):
    """
    Abstract base class for asynchronous locks.

    This class defines the interface that all asynchronous lock implementations must follow.
    It provides methods for acquiring and releasing locks, as well as support for the
    asynchronous context manager protocol.
    """

    @abc.abstractmethod
    async def acquire(
            self,
            timeout: float | None = None,
            check_interval: float | None = None,
            fail_when_locked: bool | None = None,
    ) -> Any:
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
            AlreadyLocked: If the lock is already held.
        """
        pass

    @abc.abstractmethod
    async def release(self) -> None:
        """
        Release the lock.

        This method should be idempotent - calling it multiple times should not raise an error.
        """
        pass

    @abc.abstractmethod
    async def is_locked(self) -> bool:
        """
        Check if the lock is currently held.

        Returns:
            True if the lock is held, False otherwise.
        """
        pass

    @abc.abstractmethod
    async def __aenter__(self) -> AsyncLock:
        """
        Acquire the lock when entering the async context manager.

        Returns:
            The lock object itself.
        """
        pass

    @abc.abstractmethod
    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        """
        Release the lock when exiting the async context manager.
        """
        pass


class LockManager(abc.ABC):
    """
    Base class for lock managers that can provide/manage lock objects.
    
    This class serves as an interface for creating lock objects that can be used
    for distributed locking mechanisms. Implementations of this class should
    provide a way to create lock objects for specific lock IDs.
    """

    @abc.abstractmethod
    def lock(self, lock_id: str, **kwargs) -> AsyncLock:
        """
        Create and return a lock object for the given lock ID.
        
        Args:
            lock_id: A unique identifier for the lock.
            
        Returns:
            A lock object that can be used for locking operations.
        """
        pass

    @abc.abstractmethod
    def configure(self, **kwargs) -> None:
        """
        Configure the lock manager with the given parameters.
        
        Args:
            **kwargs: Configuration parameters specific to the lock manager implementation.
        """
        pass
