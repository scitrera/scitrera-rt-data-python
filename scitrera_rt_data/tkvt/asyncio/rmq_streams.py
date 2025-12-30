from __future__ import annotations

import logging

from asyncio import create_task, sleep as asleep, Task, CancelledError, wait_for
from typing import Any, Optional, Iterable

from ...dt import Timestamp, dt_from_unix_ns
from ...serialization.msgpack import msgpack_serialize, msgpack_deserialize
from ...tkvt._base import AsyncTKVTBroker, ASYNC_TKVT_FN
from ...tls import mtls_client_ssl_context

try:
    from rstream.exceptions import StreamAlreadyExists
    from rstream import (
        Producer, Consumer, SlasMechanism, StreamDoesNotExist,
        AMQPMessage, amqp_decoder,
        MessageContext, OnClosedErrorInfo,
        ConsumerOffsetSpecification, OffsetType,
    )
except ImportError:
    raise RuntimeError("rstream is not installed. `pip install rstream` to use AsyncRabbitMQStreamsBroker.")

# default connections information
DEFAULT_RMQ_STREAMS_PORT = 5551
DEFAULT_RMQ_VHOST = '/'
DEFAULT_RMQ_USER = 'guest'
DEFAULT_RMQ_PASS = 'guest'
DEFAULT_RMQ_LOAD_BALANCER_MODE = True
DEFAULT_MTLS_MODE = True

# default stream configuration
DEFAULT_RMQ_STREAM_MAX_AGE: str = '7d'  # default is to keep up to 7 days of messages
DEFAULT_RMQ_STREAM_MAX_BYTES: Optional[int] = None  # with no particular limit on the total size
DEFAULT_RMQ_STREAM_MAX_SEGMENT_BYTES: Optional[int] = 50_000_000  # ~50MB  # but limit each segment to 50MB given low load expectation


# TODO: constants or enum for offset specification???

class AsyncRabbitMQStreamsBroker(AsyncTKVTBroker):
    _producer: Optional[Producer] = None
    _consumer: Optional[Consumer] = None

    def __init__(
            self,
            # login/targeting information
            rmq_host: str,
            rmq_port: Optional[int] = None,
            vhost: Optional[str] = None,
            username: Optional[str] = None,
            password: Optional[str] = None,
            direct: Optional[bool] = None,  # ~load_balancer_mode (if direct is True, then we DO want to go DIRECT to brokers)
            # mTLS configuration
            mtls: Optional[bool] = None,
            ca_cert_path: Optional[str] = None,
            ca_data: Optional[bytes] = None,
            client_cert_path: Optional[str] = None,
            client_cert_data: Optional[bytes] = None,
            client_key_path: Optional[str] = None,
            client_key_data: Optional[bytes] = None,
            client_key_password: Optional[str] = None,
            # misc configuration (logger +...)
            logger: logging.Logger = None,
            # default stream configuration options
            max_stream_days: Optional[int] = None,
            max_stream_bytes: Optional[int] = None,
            max_segment_bytes: Optional[int] = None,
            undefined_offset_value: Optional[int] = None,
            default_identity: Optional[str] = None,
    ):

        # ensure that we create a local instance copy of default kwargs; generate in constructor to open the door
        # for global default configuration by changing module-level DEFAULT_* constants
        rmq_kwargs: dict[str, Any] = {
            'host': rmq_host,
            'port': rmq_port or DEFAULT_RMQ_STREAMS_PORT,
            'vhost': vhost or DEFAULT_RMQ_VHOST,
            'username': username or DEFAULT_RMQ_USER,
            'password': password or DEFAULT_RMQ_PASS,
            'load_balancer_mode': DEFAULT_RMQ_LOAD_BALANCER_MODE if direct is None else (not direct),
        }
        self._rmq_kwargs = rmq_kwargs
        mtls = DEFAULT_MTLS_MODE if mtls is None else mtls
        if mtls:
            rmq_kwargs['ssl_context'] = mtls_client_ssl_context(
                ca_cert_path=ca_cert_path,
                ca_data=ca_data,
                client_cert_path=client_cert_path,
                client_cert_data=client_cert_data,
                client_key_path=client_key_path,
                client_key_data=client_key_data,
                client_key_password=client_key_password
            )
            rmq_kwargs['sasl_configuration_mechanism'] = SlasMechanism.MechanismExternal
        # TODO: consider alternative ssl context configuration if mTLS is False but CA information given!??!

        self._shutdown = False
        self._failure: bool | Exception = False
        self._consumer_fns: dict[str, list[ASYNC_TKVT_FN]] = {}
        self._consumer_refs: dict[str, str | int] = {}
        self._pending_subs: set[str] = set()
        self._consumer_task: Optional[Task] = None
        self.logger = logging.getLogger('AsyncRabbitMQStreamsBroker') if logger is None else logger
        self._last_ids = {}  # type: Optional[dict[str, Any]]

        self._stream_kwargs = stream_kwargs = {}

        # handle max age
        if max_stream_days is not None:
            stream_kwargs['max-age'] = f"{max_stream_days}d"
        elif DEFAULT_RMQ_STREAM_MAX_AGE is not None:
            stream_kwargs['max-age'] = DEFAULT_RMQ_STREAM_MAX_AGE

        # handle max bytes
        if max_stream_bytes is not None:
            stream_kwargs['max-length-bytes'] = max_stream_bytes
        elif DEFAULT_RMQ_STREAM_MAX_BYTES is not None:
            stream_kwargs['max-length-bytes'] = DEFAULT_RMQ_STREAM_MAX_BYTES

        # handle segment bytes
        if max_segment_bytes is not None:
            stream_kwargs['stream-max-segment-size-bytes'] = max_segment_bytes
        elif DEFAULT_RMQ_STREAM_MAX_SEGMENT_BYTES is not None:
            stream_kwargs['stream-max-segment-size-bytes'] = DEFAULT_RMQ_STREAM_MAX_SEGMENT_BYTES

        # undefined offset handling
        self._undefined_offset_value: Optional[int] = undefined_offset_value

        # connection name/identity handling
        self._default_identity = default_identity

        pass  # end of __init__

    async def consumer_loop_async(self, blocking=True, **kwargs):
        # if there is an existing consumer task, then we should cancel it (or for now, we just raise error)
        if self._consumer_task is not None:
            raise RuntimeError('current implementation does not support starting consumer loop twice')

        if self._consumer_task is None and self._consumer_fns:
            self.logger.debug('creating asyncio consumer task')
            self._shutdown = False  # ensure that shutdown is false before creating the loop task
            self._consumer_task = create_task(self._consume(**kwargs))  # create the consumption loop task

        if blocking:
            self.logger.debug('asyncio.wait_for(consumer_task)')
            return await wait_for(self._consumer_task, timeout=None)
        return None

    async def _on_message(self, msg: AMQPMessage, ctx: MessageContext):
        c = self._consumer_fns

        # extract TKVT
        try:
            topic = ctx.stream
        except AttributeError:
            # older rstream versions
            topic = ctx.consumer.get_stream(ctx.subscriber_name)
        key = msg.application_properties[b'key'].decode()
        value = msgpack_deserialize(msg.body)
        timestamp = dt_from_unix_ns(msg.application_properties[b'timestamp'])

        # update local offset tracking
        offset = ctx.offset
        # previous_offset = self._last_ids.get(topic, None)
        self._last_ids[topic] = offset  # regardless of the value, we should capture it...

        # get per-topic handling functions and call them with the messages we've received
        functions = c.get(topic, None)
        if not functions:
            return
        for fn in functions:
            # noinspection PyTypeChecker
            create_task(fn(topic, key, value, timestamp))

        self.logger.debug('%s|%s@[%s]: %s (%s)', offset, key, topic, value, timestamp)
        return

    async def _setup_consumer(self, **kwargs):
        self.logger.debug('setup consumer')
        c = self._get_consumer(**kwargs)
        cf = self._consumer_fns
        cr = self._consumer_refs
        pending = self._pending_subs
        last_ids = self._last_ids
        # self.logger.debug('consumer task init: %s', last_ids)

        # start consumer (init)
        if not cr:  # if no existing consumer refs, then we should call start
            self.logger.debug('call to consumer start() [only should happen 1x]')
            await c.start()

        # the actual code for creating a subscription (including interaction w/ pending set)
        # noinspection PyShadowingNames
        async def inner_sub(topic, offset_spec):
            try:
                pending.remove(topic)
                self.logger.debug('[inner] registering (actual) subscription for "%s"', topic)
                cr[topic] = await c.subscribe(
                    topic, callback=self._on_message, decoder=amqp_decoder,
                    offset_specification=offset_spec,
                    # TODO: subscriber_name !!! (and save offsets on server also?)
                    subscriber_name=None,
                    consumer_update_listener=None,
                )
            except KeyError:
                self.logger.debug('[inner] NOT registering sub for %s, no pending sub reference', topic)

        for topic in cf.keys():  # TODO: maybe just iterate through copy of "pending" in the first place?
            if topic in cr and topic not in pending:
                self.logger.debug('skipping topic %s due to existing ref', topic)
                continue

            # ensure stream defined
            await self.ensure_topic_exists(topic)

            # determine starting offset
            saved_offset: int | None = last_ids.get(topic, self._undefined_offset_value)

            # if None or otherwise invalid, then we start from the *NEXT* received value
            if saved_offset is None or isinstance(saved_offset, (bytes, str)):  # ignore old checkpoints data! (redis didn't use ints)
                offset_spec = ConsumerOffsetSpecification(offset_type=OffsetType.NEXT)
                last_ids.pop(topic, None)  # if we are using next (possibly to disregard old data, then we should apply it internally!)
            elif saved_offset == -1:  # -1 means take from the last message
                offset_spec = ConsumerOffsetSpecification(offset_type=OffsetType.LAST)
            elif saved_offset <= -2:  # -2 means take from the beginning of the stream
                offset_spec = ConsumerOffsetSpecification(offset_type=OffsetType.FIRST)
            else:  # otherwise we want the message after our last checkpoint/saved_offset
                # if we're using a given offset, then we'll use that (+1) to start from the message AFTER the last one we checkpointed!
                offset_spec = ConsumerOffsetSpecification(offset_type=OffsetType.OFFSET, offset=saved_offset + 1)

            # allow the subscription change to happen in the background
            create_task(inner_sub(topic, offset_spec))

        return c

    async def _consume(self, **kwargs):
        c = await self._setup_consumer(**kwargs)
        cf = self._consumer_fns

        self.logger.info('TKVT Consumer Started: %s', self._last_ids)
        self.logger.debug('start event-wait loop')
        while not self._shutdown:
            try:
                await c.run()
                # await sleep(0.1)  # just busy-wait loop (switch to event?)
                # await asyncio.wait_for(c.run(), timeout=None)
            except CancelledError:  # TODO: confirm that there won't be other reasons for getting asyncio.CancelledError!
                self.logger.debug('asyncio.CancelledError caught in AsyncRedisBroker._consume(...)')
                if isinstance(self._failure, bool):
                    return not self._failure
                return self._failure  # we assume that failure is an exception -- in which case we return it directly
            except Exception as e:
                # TODO: depending on the issue, maybe we should just reconfigure the consumer
                self.logger.warning("Error consuming from streams %s: %s", list(cf.keys()), e)
                await asleep(0.25)

        if isinstance(self._failure, bool):  # bool values get swapped since we return True for success.
            return not self._failure
        return self._failure  # we assume that failure is an exception -- in which case we return it directly

    async def register_consumer(self, topic: str, fn_tkvt: ASYNC_TKVT_FN, **kwargs) -> None:
        # TODO: store information for subscription if not running, otherwise add subscription in real-time??

        self.logger.debug('register_consumer(%s, fn_tkvt=%s)', topic, fn_tkvt)
        c = self._consumer_fns
        if topic in c:  # TODO: consider if need to support multiple consumer functions per topic... maybe a lot of overhead for no gain
            c[topic].append(fn_tkvt)
        else:
            c[topic] = [fn_tkvt]

        # if this topic is not registered in our consumer refs, then we add it to pending
        if topic not in self._consumer_refs:
            # self.logger.debug('adding pending subscription for topic: %s', topic)
            self._pending_subs.add(topic)

            if self._consumer is not None:
                self.logger.debug('live adding subscription from topic: %s', topic)
                await self._setup_consumer()  # re-use existing code for managing offsets ahead of subscription

        return

    async def unsubscribe_topic(self, topic: str, **kwargs) -> None:
        self.logger.debug('unsubscribing from topic: %s', topic)
        # clear from pending subs if we intend to unsubscribe
        self._pending_subs.discard(topic)

        # pop ref from consumer refs if it exists
        ref = self._consumer_refs.pop(topic, None)  # this will also prevent subscription if the inner_sub task hasn't started yet
        # self.logger.debug('consumer ref pop for topic %s = %s', topic, ref)

        # if ref and consumer is defined, then we're doing a live change in subscription!
        if ref and (c := self._consumer) is not None:
            self.logger.debug('calling upstream unsubscribe from topic: %s', topic)
            await c.unsubscribe(ref)  # ref will be a str if it is defined and not PENDING_SUB

        # regardless if consumer defined, we clear consumer functions since we don't intend to subscribe to this
        self._consumer_fns.pop(topic, None)
        # self.logger.debug('consumer fn pop for topic %s', topic)

        return

    @property
    def subscribed_topics(self):
        return sorted(self._consumer_refs.keys())

    async def ensure_topic_exists(self, topic: str, **kwargs):
        cleanup_cp = False
        # if either consumer or producer exist, then we should use that rather than init more stuff
        if self._consumer is not None:
            cp = self._consumer
        elif self._producer is not None:
            cp = self._producer
        else:
            # we'll create a temporary producer instance and then discard it!
            cp = self._make_producer(**kwargs)
            cleanup_cp = True

        try:
            client = await cp.default_client  # default client because otherwise rmq tries to look up a producer/consumer by stream name
            # create topics with default settings
            try:
                return await client.create_stream(topic, self._stream_kwargs)
            except StreamAlreadyExists:  # no exception if the stream already exists
                pass
        finally:
            # if we created a temporary producer, then we should make sure that we close it!
            if cleanup_cp:
                await cp.close()

    async def ensure_topics_exist(self, topics: Iterable[str], **kwargs):
        cleanup_cp = False
        # if either consumer or producer exist, then we should use that rather than init more stuff
        if self._consumer is not None:
            cp = self._consumer
        elif self._producer is not None:
            cp = self._producer
        else:
            # we'll create a temporary producer instance and then discard it!
            cp = self._make_producer(**kwargs)
            cleanup_cp = True

        try:
            for topic in topics:
                # create topics with default settings with exist_ok=True!
                await cp.create_stream(topic, self._stream_kwargs, exists_ok=True)
        except StreamDoesNotExist:
            self.logger.warning('stream does not exist exception while trying to create stream!')
            import traceback
            self.logger.debug(traceback.format_exc())

        finally:
            # if we created a temporary producer, then we should make sure that we close it!
            if cleanup_cp:
                await cp.close()

    def _make_producer(self, **kwargs) -> Producer:
        p = Producer(
            connection_name=kwargs.get('identity', self._default_identity),
            **self._rmq_kwargs
        )
        return p

    def _get_producer(self, **kwargs):
        if self._producer is None:
            self._producer = p = self._make_producer(**kwargs)
            return p

        # noinspection PyUnreachableCode
        if identity := kwargs.get('identity', self._default_identity):
            # TODO: try to work out only using public APIs; and this might require more work if pool clients already initialized
            self._producer._connection_name = identity
        return self._producer

    def _get_consumer(self, **kwargs):
        # TODO: review and refine on_close_handler
        if self._consumer is None:
            consumer: Optional[Consumer] = None

            # metadata and disconnection events for consumers
            async def on_close_connection(on_closed_info: OnClosedErrorInfo) -> None:
                nonlocal consumer
                self.logger.debug('on_close_connection(): on_closed_info=%s, consumer=%s', on_closed_info, consumer)

                await asleep(0.050)
                # reconnect just if the partition exists
                for stream in on_closed_info.streams:
                    backoff = 0.5
                    while True:
                        try:
                            self.logger.info("reconnecting stream: %s", stream)
                            if consumer is not None:
                                await consumer.maybe_restart_subscriber('wrapper-reconnect', stream)
                            break
                        except Exception as ex:
                            if backoff > 4:
                                # failed to found the leader
                                self.logger.warning("reconnecting stream: %s", stream)
                                break
                            backoff = backoff * 2
                            self.logger.debug("exception reconnecting, waiting: %s", ex)
                            await asleep(backoff)
                            continue

            connection_name = kwargs.get('identity', self._default_identity)
            consumer = Consumer(
                on_close_handler=on_close_connection,
                connection_name=connection_name,
                **self._rmq_kwargs
            )
            self._consumer = consumer
            return consumer

        return self._consumer

    async def publish(self, topic: str, key: str, value: Any, timestamp: Timestamp, **kwargs) -> None:
        already_serialized = kwargs.pop('already_serialized', False)
        body = value if already_serialized else msgpack_serialize(value)
        msg = AMQPMessage(
            body=body,
            application_properties={
                'key': key,
                'timestamp': timestamp.value,  # int ns
            })

        try:
            p = self._get_producer(**kwargs)
            # using send_batch since that is synchronous,
            # (required to make exception catching for non-existent streams functional)
            result = await p.send_batch(topic, [msg])
        except (StreamDoesNotExist,):  # create stream and try again
            p = self._get_producer(**kwargs)
            await self.ensure_topic_exists(topic)
            result = await p.send_batch(topic, [msg])
        except (ConnectionResetError,):  # just try again
            p = self._get_producer(**kwargs)
            result = await p.send_batch(topic, [msg])
        return

    async def flush(self) -> None:
        if self._producer is not None:
            # close producer as a means to force flush... reset producer reference to None,
            # so that we create a new instance if we continue
            try:
                await self._producer.close()
                self._producer = None
            except Exception as e:
                self.logger.warning('Exception during flush', e)

        return

    async def abort(self, failure=False) -> None:
        self.logger.debug('abort')
        self._failure = failure
        self._shutdown = True

        self.logger.debug('flush and close producer')
        await self.flush()  # try to close producer

        if self._consumer_task is not None:  # this is a hint that consumer exists...
            # call close consumer first before calling cancel
            if self._consumer is not None:
                self._consumer.stop()
                self.logger.debug('async create task to close consumer ')
                await self._consumer.close()
                self._consumer_refs = {}  # reset consumer_refs on consumer close!

            self.logger.debug('canceling consumer task')
            self._consumer_task.cancel()
            self._consumer_task = None
        return

    @property
    def consumer_state(self) -> dict[str, Any]:
        return self._last_ids

    @consumer_state.setter
    def consumer_state(self, value: Optional[dict[str, Any]]):
        self._last_ids = value
