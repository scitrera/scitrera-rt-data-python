import asyncio
import logging
import time
import uuid
from typing import Dict, Any, Optional, Iterable, List, TypeAlias

from ..serialization.msgpack import msgpack_serialize, msgpack_deserialize

try:
    import redis.asyncio as aioredis
except ImportError:
    raise RuntimeError("aioredis is required for async Redis task queue operations. Please install it with 'pip install redis[hiredis]'.")

# Type alias for task information dictionaries
TaskInfoDict: TypeAlias = Dict[str, Any]


# --- Constants ---
class TaskStates:
    PENDING = "pending"
    RUNNING = "running"
    STALLED = "stalled"
    FAILED = "failed"
    FINISHED = "finished"
    ALL = [PENDING, RUNNING, STALLED, FAILED, FINISHED]


# TODO: migrate to these and centralize instead of using from ai runtime rt_schema
STATE_PENDING = TaskStates.PENDING
STATE_RUNNING = TaskStates.RUNNING
STATE_FAILED = TaskStates.FAILED
STATE_FINISHED = TaskStates.FINISHED
STATE_STALLED = TaskStates.STALLED

DEFAULT_EXPIRE_COMPLETED_SECONDS = 3 * 24 * 3600  # 3 days
DEFAULT_STALLED_TIMEOUT_SECONDS = 300  # 5 min


# --- Custom Exceptions ---
class TaskQueueException(Exception):
    """Base exception for task queue errors."""
    pass


class TaskNotFound(TaskQueueException):
    """Raised when a task ID is not found."""
    pass


class WorkspaceMismatchError(TaskQueueException):
    """Raised when a task's workspace does not match the expected one."""
    pass


class InvalidStateError(TaskQueueException):
    """Raised when an invalid task state is provided."""
    pass


class SerializationError(TaskQueueException):
    """Raised for msgpack serialization/deserialization errors."""
    pass


class AsyncRedisTaskQueueManager:
    """
    Manages asynchronous task queuing using Redis.
    Tasks are grouped by workspace and can have various states.
    """

    def __init__(
            self,
            redis_client: aioredis.Redis,
            expire_completed_after_seconds: int = DEFAULT_EXPIRE_COMPLETED_SECONDS,
            stalled_timeout_seconds: int = DEFAULT_STALLED_TIMEOUT_SECONDS,
            task_prefix='task',
            logger: logging.Logger = None,
    ):
        """
        Initializes the AsyncTaskQueueManager.

        Args:
            redis_client: An initialized async Redis client instance.
            expire_completed_after_seconds: Time in seconds after which 'failed' or 'finished' tasks are cleared.
            stalled_timeout_seconds: Time in seconds after which a 'running' task without updates is considered 'stalled'.
        """
        self.redis: aioredis.Redis = redis_client
        self.expire_completed_after_seconds: int = expire_completed_after_seconds
        self.stalled_timeout_seconds: int = stalled_timeout_seconds
        self._task_prefix = task_prefix
        if logger is None:
            logger = logging.getLogger('AsyncRedisTaskQueueManager')
        self.logger = logger

        # Use the provided serialization functions
        self._serialize = msgpack_serialize
        self._deserialize = msgpack_deserialize

    def _task_key(self, task_id: str) -> str:
        """Generates the Redis key for a task's data."""
        return f"{self._task_prefix}:tasks:{task_id}"

    # TODO: memoize this fn [only 1 value]
    def _running_tasks_heartbeat_key(self) -> str:
        """Redis key for the sorted set tracking running task heartbeats."""
        return f"{self._task_prefix}:running_tasks_heartbeat"

    # TODO: memoize this fn [only {len(states)} values]
    def _workspace_state_key(self, workspace: str, state: str) -> str:
        """Generates the Redis key for a workspace's state set."""
        if state not in TaskStates.ALL:
            raise InvalidStateError(f"Unknown state: {state}")
        return f"{self._task_prefix}:workspaces:{workspace}:{state}"

    async def _get_task_data_internal(self, task_id: str) -> Optional[TaskInfoDict]:
        """Internal helper to fetch and deserialize task data without workspace check."""
        serialized_data = await self.redis.get(self._task_key(task_id))
        if not serialized_data:
            return None
        try:
            return self._deserialize(serialized_data)
        except Exception as e:
            # Consider logging this error
            print(f"Error deserializing task {task_id}: {e}")
            raise SerializationError(f"Failed to deserialize task {task_id}: {e}") from e

    async def new_task(
            self,
            workspace: str,
            request_id: Optional[str] = None,
            **task_metadata: Any,
    ) -> str:
        """
        Creates a new task in the 'pending' state.

        Args:
            workspace: The workspace this task belongs to.
            request_id: An optional client-provided request identifier.
            **task_metadata: Additional metadata to store with the task.

        Returns:
            The unique ID (UUID string) of the newly created task.
        """
        task_id = str(uuid.uuid4())
        current_time = time.time()  # time (s) since unix epoch

        task_data: TaskInfoDict = {
            "task_id": task_id,
            "workspace": workspace,
            "request_id": request_id,
            "state": TaskStates.PENDING,
            # "status_data": {},  # For data provided during status updates
            "created_at": current_time,
            "updated_at": current_time,
            **(task_metadata or {}),
        }
        self.logger.debug('task [%s]: NEW=%s', task_id, task_data)
        try:
            serialized_task = self._serialize(task_data)
        except Exception as e:
            raise SerializationError(f"Failed to serialize new task data: {e}") from e

        async with self.redis.pipeline(transaction=True) as pipe:
            await pipe.set(self._task_key(task_id), serialized_task)
            await pipe.sadd(self._workspace_state_key(workspace, TaskStates.PENDING), task_id)
            await pipe.execute()

        return task_id

    async def status_update(
            self,
            workspace: str,
            task_id: str,
            new_state: Optional[str] = None,
            **other_status_data: Any,
    ) -> None:
        """
        Updates the status of an existing task.

        Args:
            workspace: The expected workspace of the task (for confirmation).
            task_id: The ID of the task to update.
            new_state: The new state for the task (e.g., "running", "failed").
            **other_status_data: Additional data to store with this status update.

        Raises:
            TaskNotFound: If the task_id does not exist.
            WorkspaceMismatchError: If the task's workspace doesn't match the provided one.
            InvalidStateError: If new_state is not a valid state.
            SerializationError: If task data cannot be processed.
        """
        if new_state and new_state not in TaskStates.ALL:
            raise InvalidStateError(f"Invalid target state: {new_state}")

        task_data = await self._get_task_data_internal(task_id)
        if not task_data:
            raise TaskNotFound(f"Task with ID '{task_id}' not found.")

        if task_data.get("workspace") != workspace:
            raise WorkspaceMismatchError(
                f"Task '{task_id}' belongs to workspace '{task_data.get('workspace')}', "
                f"not '{workspace}'."
            )

        # prepare for state changes (or not)
        old_state = task_data["state"]
        if new_state:
            task_data["state"] = new_state

        # if the task was previously marked as stalled, and we're providing a new update without an explicit state change,
        # then we should assume that the task is now running again
        elif old_state == TaskStates.STALLED:
            task_data["state"] = new_state = TaskStates.RUNNING
            task_data['stall_reason'] = None  # clear the previous stall reason if we're now running again

        # otherwise we just update new_state so that it's defined for the rest of the function
        else:
            new_state = old_state  # if no new_state given, then we assume state has not changed

        # update time and other given properties
        current_time = time.time()
        task_data["updated_at"] = current_time
        if other_status_data:
            task_data.update(other_status_data)

        self.logger.debug('task [%s]: UPDATED=%s', task_id, task_data)
        try:
            serialized_task_updated = self._serialize(task_data)
        except Exception as e:
            raise SerializationError(f"Failed to serialize updated task data for {task_id}: {e}") from e

        async with self.redis.pipeline(transaction=True) as pipe:
            await pipe.set(self._task_key(task_id), serialized_task_updated)

            if old_state != new_state:
                await pipe.srem(self._workspace_state_key(workspace, old_state), task_id)
                await pipe.sadd(self._workspace_state_key(workspace, new_state), task_id)

            # Manage heartbeat for running tasks
            if new_state == TaskStates.RUNNING:
                await pipe.zadd(self._running_tasks_heartbeat_key(), {task_id: current_time})
            elif old_state == TaskStates.RUNNING and new_state != TaskStates.RUNNING:
                # Task is no longer running (e.g., stalled, failed, finished)
                await pipe.zrem(self._running_tasks_heartbeat_key(), task_id)

            # Set expiration for failed or finished tasks
            if new_state in [TaskStates.FAILED, TaskStates.FINISHED]:
                await pipe.expire(self._task_key(task_id), self.expire_completed_after_seconds)

            await pipe.execute()

    async def get_task(self, workspace: str, task_id: str) -> TaskInfoDict:
        """
        Retrieves information about a specific task.

        Args:
            workspace: The expected workspace of the task.
            task_id: The ID of the task to retrieve.

        Returns:
            A dictionary containing the task's information.

        Raises:
            TaskNotFound: If the task_id does not exist.
            WorkspaceMismatchError: If the task's workspace doesn't match.
            SerializationError: If task data cannot be deserialized.
        """
        task_data = await self._get_task_data_internal(task_id)
        if not task_data:
            return None
            # raise TaskNotFound(f"Task with ID '{task_id}' not found.")

        if task_data.get("workspace") != workspace:
            raise WorkspaceMismatchError(
                f"Task '{task_id}' belongs to workspace '{task_data.get('workspace')}', "
                f"not '{workspace}'."
            )
        return task_data

    async def list_tasks(
            self,
            workspace: str,
            states: Optional[Iterable[str]] = None,
    ) -> Dict[str, TaskInfoDict]:
        """
        Lists tasks for a given workspace, optionally filtered by states.

        Args:
            workspace: The workspace to list tasks from.
            states: An optional iterable of state strings to filter by.
                    If None, tasks from all states are considered.

        Returns:
            A dictionary where keys are task IDs and values are task info dictionaries.
        """
        states_to_query = states if states is not None else TaskStates.ALL
        if isinstance(states_to_query, str):  # Handle single string case
            states_to_query = [states_to_query]

        for state_name in states_to_query:
            if state_name not in TaskStates.ALL:
                raise InvalidStateError(f"Invalid state provided in filter: {state_name}")

        task_ids_from_sets: set[str] = set()
        for state_name in states_to_query:
            key = self._workspace_state_key(workspace, state_name)
            # smembers returns set of bytes
            members_bytes = await self.redis.smembers(key)
            task_ids_from_sets.update(m.decode('utf-8') for m in members_bytes)

        results: Dict[str, TaskInfoDict] = {}

        # Concurrently fetch task data
        # We define a small helper here to fetch and validate tasks for listing
        async def _fetch_and_validate_for_list(task_id_str: str) -> Optional[TaskInfoDict]:
            try:
                # Use internal getter to avoid repeated workspace checks if task data is malformed
                # or if we want to be more lenient in listing.
                task_info = await self._get_task_data_internal(task_id_str)
                if not task_info:  # Task key might have expired and been deleted
                    return None
                if task_info.get("workspace") != workspace:
                    # Log this: task ID in workspace set but data says different workspace
                    print(f"Warning: Task {task_id_str} found in set for workspace {workspace} "
                          f"but its data indicates workspace {task_info.get('workspace')}.")
                    return None
                # Ensure the task's current state is one of those requested (if filter applied)
                if states is not None and task_info.get("state") not in states:
                    return None
                return task_info
            except SerializationError:
                print(f"Warning: Could not deserialize task {task_id_str} during list_tasks.")
                return None  # Skip corrupted tasks
            except Exception as e:
                print(f"Warning: Unexpected error fetching task {task_id_str} for list: {e}")
                return None

        fetch_coroutines = [_fetch_and_validate_for_list(tid) for tid in task_ids_from_sets]

        if fetch_coroutines:
            potential_task_infos = await asyncio.gather(*fetch_coroutines)
            for task_info in potential_task_infos:
                if task_info:  # If not None (i.e., valid, exists, and matches criteria)
                    results[task_info["task_id"]] = task_info

        return results

    async def delete_task(self, workspace: str, task_id: str) -> bool:
        """
        Forcefully deletes a task.

        Args:
            workspace: The expected workspace of the task.
            task_id: The ID of the task to delete.

        Returns:
            True if the task was found and deleted, False otherwise.

        Raises:
            WorkspaceMismatchError: If the task exists but belongs to a different workspace.
            SerializationError: If task data cannot be deserialized to check workspace/state.
        """
        task_data = await self._get_task_data_internal(task_id)
        if not task_data:
            return False  # Task already gone or never existed

        if task_data.get("workspace") != workspace:
            raise WorkspaceMismatchError(
                f"Cannot delete task '{task_id}'. It belongs to workspace "
                f"'{task_data.get('workspace')}', not '{workspace}'."
            )

        current_state = task_data["state"]

        self.logger.debug('task [%s]: DELETE', task_id)
        async with self.redis.pipeline(transaction=True) as pipe:
            await pipe.delete(self._task_key(task_id))
            await pipe.srem(self._workspace_state_key(workspace, current_state), task_id)
            if current_state == TaskStates.RUNNING:
                await pipe.zrem(self._running_tasks_heartbeat_key(), task_id)
            await pipe.execute()
        return True

    async def check_and_update_stalled_tasks(self) -> List[str]:
        """
        Identifies running tasks that haven't received updates and marks them as 'stalled'.
        This method would typically be called periodically by a background process.

        Returns:
            A list of task IDs that were marked as stalled.
        """
        stale_threshold_time = time.time() - self.stalled_timeout_seconds

        # TODO: if lock key is registered for task, then... check if lock active w/ same UUID, if so... then do not consider task stalled!

        # Get task IDs from the sorted set whose score (last heartbeat) is older than the threshold
        # zrangebyscore returns list of bytes
        stalled_task_ids_bytes: List[bytes] = await self.redis.zrangebyscore(
            self._running_tasks_heartbeat_key(), 0, stale_threshold_time
        )

        stalled_ids_marked: List[str] = []

        for task_id_bytes in stalled_task_ids_bytes:
            task_id = task_id_bytes.decode('utf-8')
            try:
                # Fetch task data to get its workspace and confirm it's still 'running'
                # before marking it as stalled (it might have been completed/failed already)
                task_data = await self._get_task_data_internal(task_id)
                if task_data and task_data["state"] == TaskStates.RUNNING:
                    task_workspace = task_data["workspace"]
                    await self.status_update(
                        task_workspace,
                        task_id,
                        TaskStates.STALLED,
                        stall_reason="heartbeat_timeout"
                    )
                    stalled_ids_marked.append(task_id)
                    print(f"Task {task_id} in workspace {task_workspace} marked as STALLED.")
            except TaskNotFound:
                # Task might have been deleted between zrangebyscore and fetching data.
                # Or its key expired. Remove from heartbeat just in case.
                await self.redis.zrem(self._running_tasks_heartbeat_key(), task_id)
                print(f"Task {task_id} not found during stall check, removed from heartbeats.")
            except WorkspaceMismatchError:
                # This shouldn't happen if data is consistent. Log it.
                print(f"Error: Workspace mismatch for task {task_id} during stall check.")
            except InvalidStateError as e:
                print(f"Error: Invalid state encountered for task {task_id} during stall check: {e}")
            except SerializationError as e:
                print(f"Error: Serialization issue for task {task_id} during stall check: {e}")
            except Exception as e:
                # Catch-all for other unexpected errors during update of a single task
                print(f"Unexpected error processing task {task_id} for stalling: {e}")

        return stalled_ids_marked
