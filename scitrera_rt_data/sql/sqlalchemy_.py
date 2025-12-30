import logging
import random
import time
from typing import Optional, Any, Callable, Dict

try:
    from sqlalchemy import create_engine, text
    from sqlalchemy.engine import Engine, Connection
    from sqlalchemy.exc import SQLAlchemyError, OperationalError, IntegrityError
except ImportError:
    raise RuntimeError("sqlalchemy is not installed")


class SQLAlchemyEntityClass:
    """
    Base class for entities using SQLAlchemy with psycopg2.
    Provides connection management and retry functionality.
    """

    def __init__(
            self,
            engine: Engine = None,
            logger: logging.Logger = None,
            # --- Retry Configuration ---
            max_retries: int = 3,  # Max number of retry attempts
            initial_delay: float = 0.1,  # Initial delay in seconds
            backoff_factor: float = 2.0,  # Multiplier for delay (e.g., 2 = double delay each time)
            max_delay: float = 2.0,  # Maximum delay between retries
            jitter_fraction: float = 0.1,  # Percentage for jitter (+/-)
    ):
        self._engine = engine
        if logger is None:
            logger = logging.getLogger(self.__class__.__name__)
        self.logger = logger

        # Store retry parameters
        self.max_retries = max_retries
        self.initial_delay = initial_delay
        self.backoff_factor = backoff_factor
        self.max_delay = max_delay
        self.jitter_fraction = jitter_fraction

    def _get_conn(self, conn: Connection = None) -> Connection:
        if conn is None:
            conn = self._engine.connect()
        return conn

    def retry_wrapper(self, activity_fn: Callable, conn: Connection = None):
        """
        Executes the given function with retry logic for transient database errors.
        
        Args:
            activity_fn: Function to execute
            conn: Optional connection to use (if None, a new connection will be created)
            
        Returns:
            The result of the activity_fn
        """
        last_exc = None
        for attempt in range(self.max_retries + 1):  # +1 to include the initial attempt
            try:
                # Pass the connection to the activity function
                return activity_fn(conn)

            # --- Catch specific transient errors suitable for retry ---
            except OperationalError as e:
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
                time.sleep(actual_delay)
                continue  # Go to the next attempt

            # --- Catch other non-retryable SQLAlchemy errors ---
            except IntegrityError as e:
                # e.g., UniqueViolation, ForeignKeyViolation, etc.
                self.logger.error('database error (no retry): %s', e)
                raise  # Re-raise immediately, retrying won't help

            # --- Catch potential logical errors ---
            except Exception as e:
                self.logger.error('unexpected error during DB operation (no retry): %s', e)
                raise  # Re-raise unexpected errors

        # If loop finishes without success, raise the last caught retryable exception
        raise ConnectionError(f"Failed to execute query after {self.max_retries + 1} attempts. Last error: {last_exc}") from last_exc
