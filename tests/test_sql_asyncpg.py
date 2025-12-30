import pytest
import asyncio
from unittest.mock import AsyncMock, MagicMock, patch
import asyncpg
from scitrera_rt_data.sql.asyncpg_ import AsyncPGEntityClass

@pytest.mark.asyncio
async def test_asyncpg_entity_retry_success():
    mock_pool = AsyncMock(spec=asyncpg.Pool)
    entity = AsyncPGEntityClass(pool=mock_pool, max_retries=3, initial_delay=0.01)
    
    activity_fn = AsyncMock(return_value="success")
    result = await entity.retry_wrapper(activity_fn)
    
    assert result == "success"
    assert activity_fn.call_count == 1

@pytest.mark.asyncio
async def test_asyncpg_entity_retry_on_connection_error():
    mock_pool = AsyncMock(spec=asyncpg.Pool)
    entity = AsyncPGEntityClass(pool=mock_pool, max_retries=2, initial_delay=0.01)
    
    # Simulate a transient connection error
    activity_fn = AsyncMock(side_effect=[
        asyncpg.exceptions.ConnectionFailureError("transient"),
        "success"
    ])
    
    with patch("asyncio.sleep", return_value=None):
        result = await entity.retry_wrapper(activity_fn)
    
    assert result == "success"
    assert activity_fn.call_count == 2

@pytest.mark.asyncio
async def test_asyncpg_entity_retry_exhausted():
    mock_pool = AsyncMock(spec=asyncpg.Pool)
    entity = AsyncPGEntityClass(pool=mock_pool, max_retries=2, initial_delay=0.01)
    
    activity_fn = AsyncMock(side_effect=asyncpg.exceptions.ConnectionFailureError("transient"))
    
    with patch("asyncio.sleep", return_value=None):
        with pytest.raises(ConnectionError, match="Failed to execute query after 3 attempts"):
            await entity.retry_wrapper(activity_fn)
    
    assert activity_fn.call_count == 3

@pytest.mark.asyncio
async def test_asyncpg_entity_transaction_wrapper_pool():
    mock_pool = AsyncMock(spec=asyncpg.Pool)
    mock_conn = AsyncMock(spec=asyncpg.Connection)
    
    # Setup pool.acquire() context manager
    mock_pool.acquire.return_value.__aenter__.return_value = mock_conn
    # Setup conn.transaction() context manager
    mock_conn.transaction.return_value.__aenter__.return_value = MagicMock()
    
    entity = AsyncPGEntityClass(pool=mock_pool)
    
    activity_fn = AsyncMock()
    await entity.transaction_wrapper(activity_fn)
    
    assert activity_fn.call_count == 1
    activity_fn.assert_called_with(mock_conn)
    assert mock_conn.transaction.call_count == 1

@pytest.mark.asyncio
async def test_asyncpg_entity_invalid_cached_statement_retry():
    mock_pool = AsyncMock(spec=asyncpg.Pool)
    mock_conn = AsyncMock(spec=asyncpg.Connection)
    mock_pool.acquire.return_value.__aenter__.return_value = mock_conn
    
    entity = AsyncPGEntityClass(pool=mock_pool, max_retries=2)
    
    # First call fails with InvalidCachedStatementError, second succeeds
    activity_fn = AsyncMock(side_effect=[
        asyncpg.exceptions.InvalidCachedStatementError("stale"),
        "success"
    ])
    
    # Mock _get_conn to return our mock_conn
    with patch.object(AsyncPGEntityClass, "_get_conn", return_value=mock_conn):
        result = await entity.retry_wrapper(activity_fn)
    
    assert result == "success"
    assert activity_fn.call_count == 2
    # Should have executed DISCARD ALL
    mock_conn.execute.assert_called_with('DISCARD ALL')
