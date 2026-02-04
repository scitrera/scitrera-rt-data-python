"""
Async sideloading utilities for background resource initialization.

This module provides a two-stage asynchronous preloading pattern for expensive
resources (e.g., ML models, database connections, large imports) that need to be
initialized in the background while the application continues startup.

**Stage 1 (sync):** Runs in a thread pool — suitable for blocking operations
like importing heavy modules or performing synchronous I/O.

**Stage 2 (async):** Runs on the event loop — suitable for async setup such as
establishing network connections or performing async I/O using the result of
stage 1.

Usage via subclassing::

    class MyLoader(AsyncSideloadModel):
        def _preload_stage1_sync(self):
            import heavy_module
            return heavy_module

        async def _preload_stage2_async(self, s1_result):
            return await s1_result.connect()

    loader = MyLoader()
    await loader.load()          # kick off background loading
    connection = await loader.get()  # block until ready, return result

Usage via factory function::

    loader = async_sideload(
        stage1_sync_fn=lambda: __import__('heavy_module'),
        stage2_async_fn=lambda mod: mod.connect(),
    )
    await loader.load()
    connection = await loader.get()
"""

from __future__ import annotations

import asyncio
import logging
from asyncio import to_thread, create_task, wait_for
from logging import Logger
from typing import Any, Callable, Awaitable, TypeVar

from scitrera_app_framework.api import NOT_SET

T = TypeVar('T')


async def _identity(x: T) -> T:
    """Async identity function used as the default stage 2 pass-through.

    :param x: value to return unchanged
    :return: the same value
    """
    return x


class AsyncSideloadModel:
    """Two-stage asynchronous preloader for expensive resources.

    Orchestrates a synchronous stage (run in a thread pool) followed by an
    asynchronous stage (run on the event loop).  Results are cached after the
    first successful load so that subsequent calls to :meth:`get` return
    immediately.

    Subclasses must implement :meth:`_preload_stage1_sync` and
    :meth:`_preload_stage2_async`.  Alternatively, use the :func:`async_sideload`
    factory for a functional style.
    """

    def __init__(self, logger: Logger | None = None) -> None:
        """Initialize the sideload model.

        :param logger: optional logger for debug-level progress messages
        """
        self._preload_task: asyncio.Task[None] | None = None
        self._result: Any = NOT_SET
        self.logger: Logger | None = logger

    # ------------------------------------------------------------------
    # Stages to be implemented by subclasses
    # ------------------------------------------------------------------

    def _preload_stage1_sync(self) -> Any:
        """Execute synchronous initialization (runs in a thread pool).

        Override this method to perform blocking work such as importing heavy
        modules or reading files from disk.

        :return: an intermediate result passed to :meth:`_preload_stage2_async`
        :raises NotImplementedError: if the subclass does not override this method
        """
        raise NotImplementedError('Implement initial required sync activities (including slow, novel imports!)')

    async def _preload_stage2_async(self, s1_result: Any) -> Any:
        """Execute asynchronous initialization using the stage 1 result.

        Override this method to perform async work such as establishing network
        connections or performing async I/O.

        :param s1_result: the return value of :meth:`_preload_stage1_sync`
        :return: the final loaded resource
        :raises NotImplementedError: if the subclass does not override this method
        """
        raise NotImplementedError('Implement async activities that require initial sync to be complete; '
                                  'this should return an object of interest')

    # ------------------------------------------------------------------
    # Internal wrappers (error handling)
    # ------------------------------------------------------------------

    def _wrapped_stage1_sync(self) -> Any:
        """Wrap :meth:`_preload_stage1_sync` with error capture.

        Exceptions are caught and returned as values so that the orchestrator
        can store them for later re-raising via :meth:`get`.

        :return: the stage 1 result, or the caught :class:`Exception`
        """
        try:
            return self._preload_stage1_sync()
        except Exception as e:
            if self.logger is not None and self.logger.isEnabledFor(logging.DEBUG):
                self.logger.exception('async preload stage 1 failed', exc_info=e)
                import traceback
                traceback.print_exc()
            return e

    async def _wrapped_stage2_async(self, s1_result: Any) -> Any:
        """Wrap :meth:`_preload_stage2_async` with error capture.

        Exceptions are caught and returned as values so that the orchestrator
        can store them for later re-raising via :meth:`get`.

        :param s1_result: the return value of stage 1
        :return: the stage 2 result, or the caught :class:`Exception`
        """
        try:
            return await self._preload_stage2_async(s1_result)
        except Exception as e:
            if self.logger is not None and self.logger.isEnabledFor(logging.DEBUG):
                self.logger.exception('async preload stage 2 failed', exc_info=e)
                import traceback
                traceback.print_exc()
            return e

    # ------------------------------------------------------------------
    # Core orchestration
    # ------------------------------------------------------------------

    async def _preload(self) -> None:
        """Run both stages sequentially and store the final result.

        Stage 1 is executed in a thread pool via :func:`asyncio.to_thread`.
        If it fails, the exception is stored and stage 2 is skipped.
        """
        # load sync steps first in a separate thread
        s1_result = await to_thread(self._wrapped_stage1_sync)

        # do not proceed if stage 1 failed
        if isinstance(s1_result, Exception):
            self._result = s1_result
            return

        # await stage 2 activities and populate the result
        self._result = await self._wrapped_stage2_async(s1_result)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def load(self, block: bool = False) -> bool:
        """Start the background preload, optionally waiting for completion.

        Calling :meth:`load` multiple times is safe — only the first call
        creates the background task.

        :param block: if ``True``, await the preload task before returning
        :return: ``True`` if a new task was created, ``False`` if one was
                 already running
        """
        result = False
        if (task := self._preload_task) is None:
            self._preload_task = task = create_task(self._preload())
            result = True
            if self.logger is not None:
                self.logger.debug('load(block=%s) -- created task', block)
        elif self.logger is not None:
            self.logger.debug('load(block=%s) -- already running', block)
        if block:
            await wait_for(task, timeout=None)
        return result

    async def get(self) -> Any:
        """Return the loaded result, triggering a blocking load if necessary.

        If the preload has already completed successfully, the cached result is
        returned immediately without additional async overhead.  If the preload
        resulted in an exception, it is re-raised.

        :return: the result of stage 2 (or stage 1 if using the identity default)
        :raises Exception: re-raises any exception captured during preloading
        """
        # immediately return the result if populated -- avoid extra async overhead
        result = self._result
        if isinstance(result, Exception):
            raise result
        elif result is not NOT_SET:
            return result

        # trigger preload if not already running
        if self.logger is not None:
            self.logger.debug('get() -- calling load with block=True')
        await self.load(block=True)

        # return result
        if isinstance(result := self._result, Exception):
            raise result
        return result


def async_sideload(
    stage1_sync_fn: Callable[[], Any],
    stage2_async_fn: Callable[[Any], Awaitable[Any]] = _identity,
    logger: Logger | None = None,
) -> AsyncSideloadModel:
    """Create an :class:`AsyncSideloadModel` from plain functions.

    This is a convenience factory that avoids the need to write a subclass
    when the stage logic can be expressed as standalone callables.

    :param stage1_sync_fn: a synchronous callable executed in a thread pool
    :param stage2_async_fn: an async callable that receives the stage 1 result;
                            defaults to :func:`_identity` (pass-through)
    :param logger: optional logger for debug-level progress messages
    :return: an instantiated :class:`AsyncSideloadModel` ready to :meth:`~AsyncSideloadModel.load`
    """
    class LocalASM(AsyncSideloadModel):
        def _preload_stage1_sync(self) -> Any:
            return stage1_sync_fn()

        async def _preload_stage2_async(self, s1_result: Any) -> Any:
            return await stage2_async_fn(s1_result)

    return LocalASM(logger=logger)
