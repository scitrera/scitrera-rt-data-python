

from typing import Callable, Any, Optional, Awaitable, Iterable
from abc import ABCMeta, abstractmethod
from concurrent.futures import ThreadPoolExecutor

from ..dt import Timestamp, dt_from_unix_s

# Type definitions
TKVT_FN = Callable[[str, str, Any, Timestamp], None]
ASYNC_TKVT_FN = Callable[[str, str, Any, Timestamp], Awaitable[None]]


class TKVTBroker(object, metaclass=ABCMeta):  # TODO: should starting loop be part of ABC? (also related to sync vs async considerations)
    @abstractmethod
    def register_consumer(self, topic: str, fn_tkvt: TKVT_FN, **kwargs) -> None:
        """
        Register a consumer TKVT function that will handle incoming messages on the given topic.

        :param topic: topic for which to respond
        :param fn_tkvt: the function that will receive (topic, key, value, timestamp) and react appropriately.
        :param kwargs: additional kwargs that may be relevant in implementation-specific use cases but ideally should not be required
        """
        pass

    def ensure_topic_exists(self, topic: str, **kwargs):
        """
        Ensure that the given topic exists in the upstream broker. This may be a no-op depending on the implementation details.
        kwargs are defined in the ABC to facilitate implementation-specific configuration that may be needed.

        :param topic: string topic to initialize (if needed)
        :param kwargs: additional kwargs that may be relevant in implementation-specific use cases but ideally should not be required
        """
        pass

    def ensure_topics_exist(self, topics: Iterable[str], **kwargs):
        """
        Ensure that the given topics exist in the upstream broker. This may be a no-op depending on the implementation details.
        kwargs are defined in the ABC to facilitate implementation-specific configuration that may be needed. The default implementation
        just calls `ensure_topic_exists` in a loop; implementing classes can replace this with a higher performance batch implementation.

        :param topics: iterable of string topics to initialize (if needed)
        :param kwargs: additional kwargs that may be relevant in implementation-specific use cases but ideally should not be required
        """
        for topic in topics:
            self.ensure_topic_exists(topic, **kwargs)

    @abstractmethod
    def publish(self, topic: str, key: str, value: Any, timestamp: Timestamp, **kwargs) -> None:
        """
        Publish on topic & key the given value marked with the given timestamp. This method should not block.

        :param topic: string topic to publish on
        :param key: string key
        :param value: value (not serialized but preferably suitable for serialization)
        :param timestamp: pandas timestamp
        :param kwargs: additional kwargs that may be relevant in implementation-specific use cases but ideally should not be required
        """
        pass

    @abstractmethod
    def consumer_loop_sync(self, **kwargs):
        """
        Start synchronous consumer loop on the current thread. This will block!

        Keyword arguments are allowed--and these are implementation specific. Defaults
        should be sane given that they are optional.
        """
        pass

    @property
    def prefix(self) -> Optional[str]:
        """
        An optional prefix used by internals of this broker. Typically, this would be an internal prefix applied to all
        publish/subscribe activities for this broker object; however, it is an implementation-specific detail.
        """
        return None

    @abstractmethod
    def flush(self) -> None:
        """
        Force any pending messages to be sent. Details depend on the implementation, but should always be called whenever shutting down
        the application, loop, thread-pool, etc. to be safe across implementations. There is no async equivalent.
        """
        pass

    @abstractmethod
    def abort(self) -> None:
        """
        Stop polling for new messages / shutdown consumer loop. Details depend on the implementation, but should always be called whenever
        shutting down the application, loop, thread-pool, etc. to be safe across implementations. There is no async equivalent.
        """
        pass

    @property
    def consumer_state(self) -> dict[str, Any]:
        """
        Get a serializable dict containing necessary information to resume this TKVT consumer. Although this can vary with implementation,
        the intention is that getting this property should return a snapshot of the correct state information on demand,
        even while the consumer loop is running.
        """
        raise NotImplementedError('consumer_state not implemented')

    @consumer_state.setter
    def consumer_state(self, value: Optional[dict[str, Any]]):
        """
        Configure state of consumer based on previously saved state. This can be used to resume consumption. Although
        this can vary with implementation, the intention is that setting the consumer_state must be done before starting
        the consumer loop in order to take effect.

        :param value: existing consumer state data. None should be the same as reset / clean slate.
        """
        raise NotImplementedError('consumer_state not implemented')


class AsyncTKVTBroker(object, metaclass=ABCMeta):

    @abstractmethod
    async def register_consumer(self, topic: str, fn_tkvt: ASYNC_TKVT_FN, **kwargs) -> None:
        """
        Register an async consumer TKVT function that will handle incoming messages on the given topic.

        :param topic: topic for which to respond
        :param fn_tkvt: the function that will receive (topic, key, value, timestamp) and react appropriately.
        :param kwargs: additional kwargs that may be relevant in implementation-specific use cases but ideally should not be required
        """
        pass

    async def unsubscribe_topic(self, topic: str, **kwargs) -> None:
        raise NotImplementedError('this implementation of AsyncTKVTBroker does not implement unsubscribe_topic')

    @property
    def subscribed_topics(self):
        raise NotImplementedError('this implementation of AsyncTKVTBroker does not implement subscribed_topics')

    @abstractmethod
    async def publish(self, topic: str, key: str, value: Any, timestamp: Timestamp, **kwargs) -> None:
        """
        Publish on the given topic and key the given value marked with the given timestamp.

        :param topic: string topic to publish on
        :param key: string key
        :param value: value (not serialized but preferably suitable for serialization)
        :param timestamp: pandas timestamp
        :param kwargs: additional kwargs that may be relevant in implementation-specific use cases but ideally should not be required
        """
        pass

    async def ensure_topic_exists(self, topic: str, **kwargs):
        """
        Ensure that the given topic exists in the upstream broker. This may be a no-op depending on the implementation details.
        kwargs are defined in the ABC to facilitate implementation-specific configuration that may be needed.

        :param topic: string topic to initialize (if needed)
        :param kwargs: additional kwargs that may be relevant in implementation-specific use cases but ideally should not be required
        """
        pass

    async def ensure_topics_exist(self, topics: Iterable[str], **kwargs):
        """
        Ensure that the given topics exist in the upstream broker. This may be a no-op depending on the implementation details.
        kwargs are defined in the ABC to facilitate implementation-specific configuration that may be needed. The default implementation
        just calls `ensure_topic_exists` in a loop; implementing classes can replace this with a higher performance batch implementation.

        :param topics: iterable of string topics to initialize (if needed)
        :param kwargs: additional kwargs that may be relevant in implementation-specific use cases but ideally should not be required
        """
        for topic in topics:
            await self.ensure_topic_exists(topic, **kwargs)

    @abstractmethod
    async def consumer_loop_async(self, blocking=True, **kwargs):
        """
        Start an asynchronous consumer loop. Keyword arguments are allowed--but these are implementation-specific! Defaults
        should be sane given that they are optional.
        """
        pass

    # def consumer_loop_sync(self, **kwargs):
    #     """
    #     Start synchronous consumer loop on the current thread. This will block! This implementation is a default
    #     convenience stub that will run the async version either BLOCKING in the current loop or start a new loop if not within one.
    #
    #     Keyword arguments are allowed--and these are implementation specific. Defaults
    #     should be sane given that they are optional.
    #
    #     """
    #     return sync_await(self.consumer_loop_async(**kwargs))

    @property
    def prefix(self) -> Optional[str]:
        """
        An optional prefix used by internals of this broker. Typically, this would be an internal prefix applied to all
        publish/subscribe activities for this broker object; however, it is an implementation-specific detail.
        """
        return None

    @abstractmethod
    async def flush(self) -> None:
        """
        Force any pending messages to be sent. Details depend on the implementation, but should always be called whenever shutting down
        the application, loop, thread-pool, etc. to be safe across implementations. There is no async equivalent.
        """
        pass

    @abstractmethod
    async def abort(self, failure=False) -> None:
        """
        Stop polling for new messages / shutdown consumer loop. Details depend on the implementation, but should always be called whenever
        shutting down the application, loop, thread-pool, etc. to be safe across implementations. There is no async equivalent.
        """
        pass

    @property
    def consumer_state(self) -> dict[str, Any]:
        """
        Get a serializable dict containing necessary information to resume this TKVT consumer. Although this can vary with implementation,
        the intention is that getting this property should return a snapshot of the correct state information on demand,
        even while the consumer loop is running.
        """
        raise NotImplementedError('consumer_state not implemented')

    @consumer_state.setter
    def consumer_state(self, value: Optional[dict[str, Any]]):
        """
        Configure state of consumer based on previously saved state. This can be used to resume consumption. Although
        this can vary with implementation, the intention is that setting the consumer_state must be done before starting
        the consumer loop in order to take effect.

        :param value: existing consumer state data. None should be the same as reset / clean slate.
        """
        raise NotImplementedError('consumer_state not implemented')


# noinspection PyUnusedLocal
def tpe_wrapper(tktv_fn: TKVT_FN, tpe: Optional[ThreadPoolExecutor] = None, max_workers: int = 1, **tpe_options) -> TKVT_FN:
    """
    Function wrapper to make function run in a dedicated (or given) ThreadPoolExecutor.

    Intended to facilitate "thread per topic" for kafka broker.

    :param tktv_fn: a TKVT Function (takes standard topic,key,value,timestamp arguments) that will be called by the TPE.
    :param tpe: an optional TPE to use (if None, a new one will be created using the given options).
    :param max_workers: maximum workers for new TPE (only applies when TPE is None).
    :param tpe_options: optional kwargs to be used when initializing a TPE (only applies when TPE is None).
    :return: wrapper function that will treat all calls to this function (using standard TKVT arguments) as
                submitting to either the given TPE or the TPE instantiated from this method call.
    """
    if tpe is None:
        tpe = ThreadPoolExecutor(**dict(max_workers=max_workers, **tpe_options))

    def inner_fn(topic: str, key: str, value: Any, timestamp: Timestamp):
        return tpe.submit(tktv_fn, topic, key, value, timestamp)

    # add a reference to the TPE on the function so that GC for TPE and function are tied together
    inner_fn.__dict__['_tpe'] = tpe

    return inner_fn

