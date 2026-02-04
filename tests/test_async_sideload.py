"""Tests for scitrera_rt_data.async_sideload."""

import asyncio
import logging
import logging.handlers
import time

import pytest

from scitrera_rt_data.async_sideload import AsyncSideloadModel, async_sideload


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

class SimpleLoader(AsyncSideloadModel):
    """Minimal subclass: stage 1 returns a value, stage 2 doubles it."""

    def _preload_stage1_sync(self):
        return 21

    async def _preload_stage2_async(self, s1_result):
        return s1_result * 2


class Stage1OnlyLoader(AsyncSideloadModel):
    """Subclass that only uses stage 1 (stage 2 is identity-like)."""

    def _preload_stage1_sync(self):
        return {"ready": True}

    async def _preload_stage2_async(self, s1_result):
        return s1_result


class SlowLoader(AsyncSideloadModel):
    """Loader with a deliberate delay in stage 1 to test background loading."""

    def _preload_stage1_sync(self):
        time.sleep(0.1)
        return "slow_value"

    async def _preload_stage2_async(self, s1_result):
        return s1_result.upper()


class Stage1FailLoader(AsyncSideloadModel):
    """Loader whose stage 1 always raises."""

    def _preload_stage1_sync(self):
        raise ValueError("stage1 boom")

    async def _preload_stage2_async(self, s1_result):
        return s1_result


class Stage2FailLoader(AsyncSideloadModel):
    """Loader whose stage 2 always raises."""

    def _preload_stage1_sync(self):
        return "ok"

    async def _preload_stage2_async(self, s1_result):
        raise RuntimeError("stage2 boom")


class NoneResultLoader(AsyncSideloadModel):
    """Loader that legitimately returns None as its result."""

    def _preload_stage1_sync(self):
        return "intermediate"

    async def _preload_stage2_async(self, s1_result):
        return None


# ===== Subclass-based Tests =====


@pytest.mark.asyncio
async def test_subclass_basic_load_and_get():
    """Test that a simple subclass loads and returns the expected value."""
    loader = SimpleLoader()
    await loader.load(block=True)
    result = await loader.get()
    assert result == 42


@pytest.mark.asyncio
async def test_subclass_get_triggers_load():
    """Test that get() implicitly triggers load if not yet started."""
    loader = SimpleLoader()
    result = await loader.get()
    assert result == 42


@pytest.mark.asyncio
async def test_subclass_cached_result():
    """Test that get() returns the same cached result on repeated calls."""
    loader = SimpleLoader()
    r1 = await loader.get()
    r2 = await loader.get()
    assert r1 == r2 == 42


@pytest.mark.asyncio
async def test_subclass_stage1_only():
    """Test a loader that effectively only uses stage 1."""
    loader = Stage1OnlyLoader()
    result = await loader.get()
    assert result == {"ready": True}


@pytest.mark.asyncio
async def test_subclass_none_result():
    """Test that None is a valid cached result and doesn't re-trigger load."""
    loader = NoneResultLoader()
    result = await loader.get()
    assert result is None
    # calling get() again should still return None without error
    result2 = await loader.get()
    assert result2 is None


# ===== Factory Function Tests =====


@pytest.mark.asyncio
async def test_factory_basic():
    """Test the async_sideload factory with both stages."""
    loader = async_sideload(
        stage1_sync_fn=lambda: 10,
        stage2_async_fn=_async_triple,
    )
    result = await loader.get()
    assert result == 30


async def _async_triple(x):
    return x * 3


@pytest.mark.asyncio
async def test_factory_stage1_only():
    """Test the factory with only stage 1 (default identity stage 2)."""
    loader = async_sideload(stage1_sync_fn=lambda: "hello")
    result = await loader.get()
    assert result == "hello"


@pytest.mark.asyncio
async def test_factory_returns_async_sideload_model():
    """Test that the factory returns an AsyncSideloadModel instance."""
    loader = async_sideload(stage1_sync_fn=lambda: 1)
    assert isinstance(loader, AsyncSideloadModel)


# ===== load() Behavior Tests =====


@pytest.mark.asyncio
async def test_load_returns_true_on_first_call():
    """Test that load() returns True on the first call (task created)."""
    loader = SimpleLoader()
    created = await loader.load()
    assert created is True
    await loader.load(block=True)  # ensure cleanup


@pytest.mark.asyncio
async def test_load_returns_false_on_subsequent_calls():
    """Test that load() returns False on subsequent calls (task already exists)."""
    loader = SimpleLoader()
    first = await loader.load()
    second = await loader.load()
    assert first is True
    assert second is False
    await loader.load(block=True)  # ensure cleanup


@pytest.mark.asyncio
async def test_load_nonblocking():
    """Test that load(block=False) returns before completion."""
    loader = SlowLoader()
    created = await loader.load(block=False)
    assert created is True
    # result should not yet be available (stage 1 sleeps 0.1s)
    # wait for completion, then verify
    await loader.load(block=True)
    result = await loader.get()
    assert result == "SLOW_VALUE"


@pytest.mark.asyncio
async def test_load_blocking():
    """Test that load(block=True) waits for completion."""
    loader = SimpleLoader()
    await loader.load(block=True)
    result = await loader.get()
    assert result == 42


# ===== Error Propagation Tests =====


@pytest.mark.asyncio
async def test_stage1_failure_propagates_via_get():
    """Test that a stage 1 exception is re-raised by get()."""
    loader = Stage1FailLoader()
    with pytest.raises(ValueError, match="stage1 boom"):
        await loader.get()


@pytest.mark.asyncio
async def test_stage2_failure_propagates_via_get():
    """Test that a stage 2 exception is re-raised by get()."""
    loader = Stage2FailLoader()
    with pytest.raises(RuntimeError, match="stage2 boom"):
        await loader.get()


@pytest.mark.asyncio
async def test_stage1_failure_skips_stage2():
    """Test that stage 2 is never called when stage 1 fails."""
    stage2_called = False

    class CheckSkip(AsyncSideloadModel):
        def _preload_stage1_sync(self):
            raise ValueError("fail")

        async def _preload_stage2_async(self, s1_result):
            nonlocal stage2_called
            stage2_called = True
            return s1_result

    loader = CheckSkip()
    with pytest.raises(ValueError):
        await loader.get()
    assert stage2_called is False


@pytest.mark.asyncio
async def test_repeated_get_after_failure_raises_again():
    """Test that calling get() after a failure re-raises the same exception."""
    loader = Stage1FailLoader()
    with pytest.raises(ValueError):
        await loader.get()
    # second call should also raise
    with pytest.raises(ValueError, match="stage1 boom"):
        await loader.get()


# ===== Concurrency Tests =====


@pytest.mark.asyncio
async def test_concurrent_gets():
    """Test that multiple concurrent get() calls all receive the result."""
    loader = SlowLoader()
    results = await asyncio.gather(
        loader.get(),
        loader.get(),
        loader.get(),
    )
    assert all(r == "SLOW_VALUE" for r in results)


@pytest.mark.asyncio
async def test_concurrent_loads_create_single_task():
    """Test that concurrent load() calls only create one task."""
    loader = SlowLoader()
    created_flags = await asyncio.gather(
        loader.load(),
        loader.load(),
        loader.load(),
    )
    # exactly one should be True (the one that created the task)
    assert sum(created_flags) == 1
    await loader.load(block=True)  # ensure cleanup


# ===== Logger Tests =====


@pytest.mark.asyncio
async def test_logger_receives_debug_messages():
    """Test that the logger receives debug messages during load/get."""
    logger = logging.getLogger("test_async_sideload")
    logger.setLevel(logging.DEBUG)
    handler = logging.handlers.MemoryHandler(capacity=100)
    logger.addHandler(handler)

    loader = SimpleLoader(logger=logger)
    await loader.get()

    # flush and check that at least one record was emitted
    handler.flush()
    assert len(handler.buffer) > 0
    messages = [r.getMessage() for r in handler.buffer]
    # should see a 'created task' message
    assert any("created task" in m for m in messages)

    logger.removeHandler(handler)


@pytest.mark.asyncio
async def test_logger_on_stage1_failure():
    """Test that a stage 1 failure logs at DEBUG level when logger is set."""
    logger = logging.getLogger("test_async_sideload_fail")
    logger.setLevel(logging.DEBUG)
    handler = logging.handlers.MemoryHandler(capacity=100)
    logger.addHandler(handler)

    loader = Stage1FailLoader(logger=logger)
    with pytest.raises(ValueError):
        await loader.get()

    handler.flush()
    messages = [r.getMessage() for r in handler.buffer]
    assert any("stage 1 failed" in m for m in messages)

    logger.removeHandler(handler)


@pytest.mark.asyncio
async def test_no_logger_does_not_raise():
    """Test that operations work fine without a logger."""
    loader = SimpleLoader(logger=None)
    result = await loader.get()
    assert result == 42


# ===== Edge Cases =====


@pytest.mark.asyncio
async def test_base_class_raises_not_implemented():
    """Test that the base class raises NotImplementedError for both stages."""
    loader = AsyncSideloadModel()
    with pytest.raises(NotImplementedError):
        await loader.get()


@pytest.mark.asyncio
async def test_factory_with_logger():
    """Test that the factory correctly passes the logger through."""
    logger = logging.getLogger("test_factory_logger")
    loader = async_sideload(stage1_sync_fn=lambda: 5, logger=logger)
    assert loader.logger is logger
    result = await loader.get()
    assert result == 5
