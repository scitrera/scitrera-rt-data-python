from __future__ import annotations

import logging
import time
import asyncio
import threading
from typing import Any, Optional, Iterable

from ...dt import Timestamp, dt_from_unix_ns
from ...serialization.msgpack import msgpack_serialize, msgpack_deserialize
from ...tkvt._base import TKVTBroker, TKVT_FN
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
    raise RuntimeError("rstream is not installed. `pip install rstream` to use RabbitMQStreamsBroker.")

# default connections information
DEFAULT_RMQ_STREAMS_PORT = 5551
DEFAULT_RMQ_VHOST = '/'
DEFAULT_RMQ_USER = 'guest'
DEFAULT_RMQ_PASS = 'guest'
DEFAULT_RMQ_LOAD_BALANCER_MODE = True
DEFAULT_MTLS_MODE = True

# default stream configuration
DEFAULT_RMQ_STREAM_MAX_AGE: str = '7d'
DEFAULT_RMQ_STREAM_MAX_BYTES: Optional[int] = None
DEFAULT_RMQ_STREAM_MAX_SEGMENT_BYTES: Optional[int] = 50_000_000


class RabbitMQStreamsBroker(TKVTBroker):
    _producer: Optional[Producer] = None
    _consumer: Optional[Consumer] = None

    def __init__(
            self,
            rmq_host: str,
            rmq_port: Optional[int] = None,
            vhost: Optional[str] = None,
            username: Optional[str] = None,
            password: Optional[str] = None,
            direct: Optional[bool] = None,
            mtls: Optional[bool] = None,
            ca_cert_path: Optional[str] = None,
            ca_data: Optional[bytes] = None,
            client_cert_path: Optional[str] = None,
            client_cert_data: Optional[bytes] = None,
            client_key_path: Optional[str] = None,
            client_key_data: Optional[bytes] = None,
            client_key_password: Optional[str] = None,
            logger: logging.Logger = None,
            max_stream_days: Optional[int] = None,
            max_stream_bytes: Optional[int] = None,
            max_segment_bytes: Optional[int] = None,
            undefined_offset_value: Optional[int] = None,
            default_identity: Optional[str] = None,
    ):
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

        self._shutdown = False
        self._failure: bool | Exception = False
        self._consumer_fns: dict[str, list[TKVT_FN]] = {}
        self._consumer_refs: dict[str, str | int] = {}
        self._pending_subs: set[str] = set()
        self.logger = logging.getLogger('RabbitMQStreamsBroker') if logger is None else logger
        self._last_ids = {}  # type: Optional[dict[str, Any]]

        self._stream_kwargs = stream_kwargs = {}
        if max_stream_days is not None:
            stream_kwargs['max-age'] = f"{max_stream_days}d"
        elif DEFAULT_RMQ_STREAM_MAX_AGE is not None:
            stream_kwargs['max-age'] = DEFAULT_RMQ_STREAM_MAX_AGE

        if max_stream_bytes is not None:
            stream_kwargs['max-length-bytes'] = max_stream_bytes
        elif DEFAULT_RMQ_STREAM_MAX_BYTES is not None:
            stream_kwargs['max-length-bytes'] = DEFAULT_RMQ_STREAM_MAX_BYTES

        if max_segment_bytes is not None:
            stream_kwargs['stream-max-segment-size-bytes'] = max_segment_bytes
        elif DEFAULT_RMQ_STREAM_MAX_SEGMENT_BYTES is not None:
            stream_kwargs['stream-max-segment-size-bytes'] = DEFAULT_RMQ_STREAM_MAX_SEGMENT_BYTES

        self._undefined_offset_value: Optional[int] = undefined_offset_value
        self._default_identity = default_identity

        self._loop = asyncio.new_event_loop()
        self._loop_thread = threading.Thread(target=self._run_loop, daemon=True)
        self._loop_thread.start()

    def _run_loop(self):
        asyncio.set_event_loop(self._loop)
        self._loop.run_forever()

    def _run_sync(self, coro):
        if not self._loop.is_running():
            self.logger.warning("_run_sync called but loop is not running")
            return None
        future = asyncio.run_coroutine_threadsafe(coro, self._loop)
        return future.result()

    def consumer_loop_sync(self, **kwargs):
        self._shutdown = False
        return self._consume(**kwargs)

    def _on_message(self, msg: AMQPMessage, ctx: MessageContext):
        c = self._consumer_fns
        try:
            topic = ctx.stream
        except AttributeError:
            topic = ctx.consumer.get_stream(ctx.subscriber_name)
        key = msg.application_properties[b'key'].decode()
        value = msgpack_deserialize(msg.body)
        timestamp = dt_from_unix_ns(msg.application_properties[b'timestamp'])

        offset = ctx.offset
        self._last_ids[topic] = offset

        functions = c.get(topic, None)
        if not functions:
            return
        for fn in functions:
            fn(topic, key, value, timestamp)

        self.logger.debug('%s|%s@[%s]: %s (%s)', offset, key, topic, value, timestamp)


    def _setup_consumer(self, **kwargs):
        self.logger.debug('setup consumer')
        c = self._get_consumer(**kwargs)
        cf = self._consumer_fns
        cr = self._consumer_refs
        pending = self._pending_subs
        last_ids = self._last_ids

        if not cr:
            self.logger.debug('call to consumer start()')
            self._run_sync(c.start())

        def inner_sub(topic, offset_spec):
            try:
                pending.remove(topic)
                self.logger.debug('[inner] registering (actual) subscription for "%s"', topic)
                cr[topic] = self._run_sync(c.subscribe(
                    topic, callback=self._on_message, decoder=amqp_decoder,
                    offset_specification=offset_spec,
                    subscriber_name=None,
                    consumer_update_listener=None,
                ))
            except KeyError:
                self.logger.debug('[inner] NOT registering sub for %s, no pending sub reference', topic)

        for topic in list(cf.keys()):
            if topic in cr and topic not in pending:
                continue

            self.ensure_topic_exists(topic)
            saved_offset: int | None = last_ids.get(topic, self._undefined_offset_value)

            if saved_offset is None or isinstance(saved_offset, (bytes, str)):
                offset_spec = ConsumerOffsetSpecification(offset_type=OffsetType.NEXT)
                last_ids.pop(topic, None)
            elif saved_offset == -1:
                offset_spec = ConsumerOffsetSpecification(offset_type=OffsetType.LAST)
            elif saved_offset <= -2:
                offset_spec = ConsumerOffsetSpecification(offset_type=OffsetType.FIRST)
            else:
                offset_spec = ConsumerOffsetSpecification(offset_type=OffsetType.OFFSET, offset=saved_offset + 1)

            inner_sub(topic, offset_spec)

        return c

    def _consume(self, **kwargs):
        c = self._setup_consumer(**kwargs)
        cf = self._consumer_fns

        self.logger.info('TKVT Consumer Started: %s', self._last_ids)
        while not self._shutdown:
            try:
                self._run_sync(c.run())
            except Exception as e:
                self.logger.warning("Error consuming from streams %s: %s", list(cf.keys()), e)
                time.sleep(0.25)

        if isinstance(self._failure, bool):
            return not self._failure
        return self._failure

    def register_consumer(self, topic: str, fn_tkvt: TKVT_FN, **kwargs) -> None:
        self.logger.debug('register_consumer(%s, fn_tkvt=%s)', topic, fn_tkvt)
        c = self._consumer_fns
        if topic in c:
            c[topic].append(fn_tkvt)
        else:
            c[topic] = [fn_tkvt]

        if topic not in self._consumer_refs:
            self._pending_subs.add(topic)
            if self._consumer is not None:
                self._setup_consumer()

    def unsubscribe_topic(self, topic: str, **kwargs) -> None:
        self.logger.debug('unsubscribing from topic: %s', topic)
        self._pending_subs.discard(topic)
        ref = self._consumer_refs.pop(topic, None)
        if ref and (c := self._consumer) is not None:
            self._run_sync(c.unsubscribe(ref))
        self._consumer_fns.pop(topic, None)

    @property
    def subscribed_topics(self):
        return sorted(self._consumer_refs.keys())

    def ensure_topic_exists(self, topic: str, **kwargs):
        cleanup_cp = False
        if self._consumer is not None:
            cp = self._consumer
        elif self._producer is not None:
            cp = self._producer
        else:
            cp = self._make_producer(**kwargs)
            cleanup_cp = True

        try:
            client = self._run_sync(cp.default_client)
            try:
                return self._run_sync(client.create_stream(topic, self._stream_kwargs))
            except StreamAlreadyExists:
                pass
        finally:
            if cleanup_cp:
                self._run_sync(cp.close())

    def ensure_topics_exist(self, topics: Iterable[str], **kwargs):
        cleanup_cp = False
        if self._consumer is not None:
            cp = self._consumer
        elif self._producer is not None:
            cp = self._producer
        else:
            cp = self._make_producer(**kwargs)
            cleanup_cp = True

        try:
            for topic in topics:
                self._run_sync(cp.create_stream(topic, self._stream_kwargs, exists_ok=True))
        except StreamDoesNotExist:
            self.logger.warning('stream does not exist exception while trying to create stream!')
        finally:
            if cleanup_cp:
                self._run_sync(cp.close())

    def _make_producer(self, **kwargs) -> Producer:
        return Producer(
            connection_name=kwargs.get('identity', self._default_identity),
            **self._rmq_kwargs
        )

    def _get_producer(self, **kwargs):
        if self._producer is None:
            self._producer = self._make_producer(**kwargs)
        elif identity := kwargs.get('identity', self._default_identity):
            self._producer._connection_name = identity
        return self._producer

    def _get_consumer(self, **kwargs):
        if self._consumer is None:
            def on_close_connection(on_closed_info: OnClosedErrorInfo) -> None:
                self.logger.debug('on_close_connection(): on_closed_info=%s', on_closed_info)
                time.sleep(0.050)
                for stream in on_closed_info.streams:
                    backoff = 0.5
                    while True:
                        try:
                            self.logger.info("reconnecting stream: %s", stream)
                            if self._consumer is not None:
                                self._consumer.maybe_restart_subscriber('wrapper-reconnect', stream)
                            break
                        except Exception as ex:
                            if backoff > 4:
                                break
                            backoff = backoff * 2
                            time.sleep(backoff)
                            continue

            connection_name = kwargs.get('identity', self._default_identity)
            self._consumer = Consumer(
                on_close_handler=on_close_connection,
                connection_name=connection_name,
                **self._rmq_kwargs
            )
        return self._consumer

    def publish(self, topic: str, key: str, value: Any, timestamp: Timestamp, **kwargs) -> None:
        already_serialized = kwargs.pop('already_serialized', False)
        body = value if already_serialized else msgpack_serialize(value)
        msg = AMQPMessage(
            body=body,
            application_properties={
                'key': key,
                'timestamp': timestamp.value,
            })

        # ensure topic exists first to avoid rstream bugs with non-existent streams
        self.ensure_topic_exists(topic)

        try:
            p = self._get_producer(**kwargs)
            self._run_sync(p.send_batch(topic, [msg]))
        except (ConnectionResetError,):
            p = self._get_producer(**kwargs)
            self._run_sync(p.send_batch(topic, [msg]))
        return

    def flush(self) -> None:
        if self._producer is not None:
            try:
                coro = self._producer.close()
                if threading.current_thread() is self._loop_thread:
                    self._loop.create_task(coro)
                else:
                    self._run_sync(coro)
                self._producer = None
            except Exception as e:
                self.logger.warning('Exception during flush: %s', e)

    def abort(self, failure=False) -> None:
        self.logger.debug('abort(failure=%s)', failure)
        self._failure = failure
        self._shutdown = True
        self.flush()
        if self._consumer is not None:
            self.logger.debug('stopping consumer')
            self._consumer.stop()
            self.logger.debug('closing consumer')
            coro = self._consumer.close()
            if threading.current_thread() is self._loop_thread:
                self._loop.create_task(coro)
            else:
                self._run_sync(coro)
            self._consumer_refs = {}
            self._consumer = None
        self.logger.debug('stopping loop')
        self._loop.call_soon_threadsafe(self._loop.stop)
        if threading.current_thread() is not self._loop_thread:
            self.logger.debug('joining loop thread')
            self._loop_thread.join(timeout=2.0)
        self.logger.debug('abort finished')

    @property
    def consumer_state(self) -> dict[str, Any]:
        return self._last_ids

    @consumer_state.setter
    def consumer_state(self, value: Optional[dict[str, Any]]):
        self._last_ids = value
