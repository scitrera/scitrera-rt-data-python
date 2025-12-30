from __future__ import annotations

import asyncio
import logging
import random

from pathlib import Path
from typing import Coroutine, Optional, Any, Callable

try:
    import asyncpg
except ImportError:
    raise RuntimeError("asyncpg is not installed")


async def apply_sql_files(directory: str | Path, conn: asyncpg.Connection, logger: logging.Logger,
                          schema_name: str = None) -> None:
    """Apply all .sql files in the given directory in sorted order using asyncpg."""
    if schema_name is None:
        sql_files = sorted(Path(directory).glob("*.sql"))
    else:
        sql_files = sorted(Path(directory).glob(f"{schema_name}.sql"))

    for file_path in sql_files:
        logger.info('Applying %s', file_path.name)
        try:
            sql = file_path.read_text(encoding='utf-8')
            await conn.execute(sql)
        except Exception as e:
            logger.error('Error applying %s: %s: %s', file_path.name, e.__class__.__name__, e)
            raise  # Re-raise to stop on first failure


class AsyncPGEntityClass:

    def __init__(
            self,
            pool: asyncpg.Pool = None,
            logger: logging.Logger = None,
            # --- Retry Configuration ---
            max_retries: int = 3,  # Max number of retry attempts
            initial_delay: float = 0.05,  # Initial delay in seconds
            backoff_factor: float = 2.0,  # Multiplier for delay (e.g., 2 = double delay each time)
            max_delay: float = 2.0,  # Maximum delay between retries
            jitter_fraction: float = 0.1,  # Percentage for jitter (+/-)
    ):
        self._pool = pool
        if logger is None:
            logger = logging.getLogger(self.__class__.__name__)
        self.logger = logger

        # Store retry parameters
        self.max_retries = max_retries
        self.initial_delay = initial_delay
        self.backoff_factor = backoff_factor
        self.max_delay = max_delay
        self.jitter_fraction = jitter_fraction

    def _get_conn(self, conn: asyncpg.Connection = None) -> asyncpg.Connection:
        if conn is None:
            conn = self._pool.acquire()
        return conn

    async def transaction_wrapper(self, activity_fn: Callable[[asyncpg.Connection], Coroutine], conn: asyncpg.Connection = None):
        effective_conn = conn or self._pool

        if isinstance(effective_conn, asyncpg.Connection):
            try:
                async with effective_conn.transaction():
                    # All operations here are part of the same transaction
                    await activity_fn(effective_conn)
                # The transaction is committed automatically on successful exit
            except Exception as e:
                # Transaction is rolled back automatically on exception
                self.logger.error("Transaction failed: %s: %s", e.__class__.__name__, e)
        elif isinstance(effective_conn, asyncpg.Pool):
            async with effective_conn.acquire() as inner_conn:
                try:
                    async with inner_conn.transaction():
                        await activity_fn(inner_conn)
                    # Transaction is committed on successful exit
                except Exception as e:
                    # Rolled back automatically
                    self.logger.error("Transaction failed: %s: %s", e.__class__.__name__, e)
        else:
            raise ValueError('expected Connection or Pool instance')

        pass

    async def retry_wrapper(self, activity_fn: Callable[[asyncpg.Connection], Coroutine],
                            conn: asyncpg.Connection = None, wrap_transaction: bool = False):
        if debug_mode := self.logger.isEnabledFor(logging.DEBUG):
            task_id = id(asyncio.current_task())
        else:
            task_id = None
            conn_id = None

        last_exc = None
        for attempt in range(self.max_retries + 1):  # +1 to include the initial attempt
            try:
                effective_conn = conn or self._pool
                if debug_mode:
                    conn_id = id(effective_conn)

                # self.logger.debug('SQL get attempt: task_id=%s, conn_id=%s', task_id, conn_id)
                if wrap_transaction:
                    result = await self.transaction_wrapper(activity_fn, effective_conn)
                else:
                    result = await activity_fn(effective_conn)

                # self.logger.debug('SQL get complete: task_id=%s, conn_id=%s', task_id, conn_id)

                return result

            # --- Catch specific transient errors suitable for retry ---
            except (
                    asyncpg.exceptions.ConnectionDoesNotExistError,
                    asyncpg.exceptions.ConnectionFailureError,
                    asyncpg.exceptions.CannotConnectNowError,
                    TimeoutError,  # Can get timeout error on get new connection from pool, but we should retry that...
                    # Add other potentially transient errors if observed, but be cautious
            ) as e:
                last_exc = e
                if attempt >= self.max_retries:
                    self.logger.error('Query failed after %d retries. Error: %s', self.max_retries, e)
                    break  # Exit loop, will raise last_exception below

                # Calculate delay with exponential backoff and jitter
                delay = min(self.max_delay, self.initial_delay * (self.backoff_factor ** attempt))
                jitter = delay * self.jitter_fraction
                actual_delay = max(0.0, delay + random.uniform(-jitter, jitter))  # Ensure non-negative

                self.logger.info("Attempt %d failed with %s. Retrying in %.2f seconds...",
                                 attempt + 1, e.__class__.__name__, actual_delay)
                await asyncio.sleep(actual_delay)
                continue  # Go to the next attempt

            except asyncpg.exceptions.InvalidCachedStatementError as e:
                self.logger.info("Attempt %d failed with %s. Attempting to clear caches and retry",
                                 attempt + 1, e.__class__.__name__, )
                conn = self._get_conn(conn)
                await conn.execute('DISCARD ALL')
                await asyncio.sleep(0.01)
                continue  # try again after discarding cached data

            # --- Catch other non-retryable Postgres errors ---
            except asyncpg.exceptions.PostgresError as e:
                # e.g., UniqueViolation, ForeignKeyViolation, SyntaxError etc.
                self.logger.error('database error (no retry): %s', e)
                raise  # Re-raise immediately, retrying won't help

            # --- Catch potential logical errors ---
            except Exception as e:
                self.logger.error('unexpected error during DB operation (no retry): %s', e)
                raise  # Re-raise unexpected errors

        # If loop finishes without success, raise the last caught retryable exception
        raise ConnectionError(f"Failed to execute query after {self.max_retries + 1} attempts. Last error: {last_exc}") from last_exc
