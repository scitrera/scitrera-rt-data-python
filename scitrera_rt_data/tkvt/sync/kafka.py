import logging
import concurrent.futures
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from time import sleep

import pandas as pd
from vpd import is_iterable

from .._base import TKVTBroker, tpe_wrapper
from ...serialization.msgpack import msgpack_serialize, msgpack_deserialize
from ...dt import UTC, dt_to_unix_ms, dt_from_unix_ms

try:
    from confluent_kafka import Producer, Consumer, TopicPartition, KafkaError, KafkaException
    from confluent_kafka.admin import AdminClient, NewTopic
except ImportError:
    raise RuntimeError('Kafka sync broker requires confluent-kafka')

DEFAULT_SERIALIZER = msgpack_serialize
DEFAULT_DESERIALIZER = msgpack_deserialize


def kafka_cfg_disable_offsets(kafka_cfg: dict) -> dict:
    """
    This is kind of a 'pointless' method to just add/modify two parameters to kafka_cfg dict,
    but it basically serves as a reminder of the details.

    :param kafka_cfg: kafka_cfg
    :return: kafka_cfg w/ 'auto.offset.reset' and 'enable.auto.commit' set
    """
    # modify kafka config so that we always start from the latest clock data

    # default policy = latest # applies when no committed offset exists for consumer group
    kafka_cfg['auto.offset.reset'] = 'latest'

    # don't automatically commit out last position for this consumer (so that the default policy will always apply...)
    kafka_cfg['enable.auto.commit'] = False

    return kafka_cfg


# TODO: add option to count messages published and periodically return feedback/reset the counter
def kafka_producer_callback(producer_cfg, log_delivery_as_info=False, log_outgoing_as_info=False, serialize=DEFAULT_SERIALIZER,
                            logger=None, **kwargs):
    """
    Return a synchronous on_msg and flush function that can be used as part of a streaming / async producer (data source --> kafka)

    :param producer_cfg:
    :param log_delivery_as_info:
    :param log_outgoing_as_info:
    :param serialize:
    :param logger: [optional] logging instance
    :return:
    """
    p = Producer(producer_cfg)
    silent_mode = kwargs.pop('silent_mode', False)
    # serialize = json.dumps

    if not silent_mode and logger is None:
        logger = logging.getLogger('kafka_producer')
    del_log_level = logging.INFO if log_delivery_as_info else logging.DEBUG
    inc_log_level = logging.INFO if log_outgoing_as_info else logging.DEBUG

    def report(err, msg):
        if err is not None:
            logger.warning('Message delivery failed!: %s', err)
        else:
            logger.log(del_log_level, 'Message key "%s" delivered to %s [%s]; latency=%ds',
                       msg.key().decode('utf-8'), msg.topic(), msg.partition(), msg.latency())
        return

    # TODO: alternate version of publish that does not poll; have kwarg to select fn variant
    # noinspection PyShadowingNames
    def kf_publish(topic, key, value, timestamp, serialize=serialize):
        p.poll(0)  # TODO: poll should be called less frequently? (and/or after call to produce?)
        logger.log(inc_log_level, '[%s][%s], key=%s, value=%s', timestamp, topic, key, value)
        timestamp = dt_to_unix_ms(timestamp)
        p.produce(topic, value=serialize(value), key=key.encode('utf-8'), timestamp=timestamp, callback=report)

    # noinspection PyShadowingNames
    def kf_publish_silent(topic, key, value, timestamp, serialize=serialize):
        timestamp = dt_to_unix_ms(timestamp)
        p.produce(topic, value=serialize(value), key=key.encode('utf-8'), timestamp=timestamp)
        p.poll(0)  # TODO: consider how calls to poll should be handled...

    def kf_flush(timeout=None):
        if isinstance(timeout, (int, float)):
            return p.flush(timeout)
        return p.flush()

    if silent_mode:
        return kf_publish_silent, kf_flush

    logger.info('Kafka producer init')
    return kf_publish, kf_flush


class KafkaBroker(TKVTBroker):
    _ac = None  # admin client
    _pr = None  # producer
    _active_consumer = False
    _more_topics = None
    _tpe_pool = None
    logger = None

    _not_abort = True

    def __init__(self, bootstrap_servers, consumer_group_name,
                 topic_prefix=None, start_at_end=True,
                 logger=None, thread_per_topic=False,
                 ooo_warning=True,
                 **custom_config):
        self._topic_prefix = f'{topic_prefix}.' if topic_prefix else ''
        # TODO: validate bootstrap servers, etc.
        self._kafka_config = kafka_cfg = {
            'bootstrap.servers': bootstrap_servers,
            'group.id': consumer_group_name,
            # 'compression.codec': 'lz4',  # LZ4 compression by default?
        }
        self._consumer_topics = defaultdict(list)
        self._more_topics = []
        if start_at_end:
            kafka_cfg_disable_offsets(kafka_cfg)

        kafka_cfg.update(custom_config)

        if logger is None:
            logger = logging.getLogger('kafka_broker')
        self.logger = logger

        if thread_per_topic:  # thread per topic on consumption
            self._tpe_pool = {}

        self._ooo_warning = ooo_warning

        pass

    @property
    def prefix(self):
        return self._topic_prefix

    def get_bootstrap_hosts(self):
        # TODO: handle if iterable?
        return self._kafka_config['bootstrap.servers']

    def __getstate__(self):
        state = self.__dict__.copy()
        # Remove admin client, producer, and consumer from pickling
        state.pop('_ac', None)
        state.pop('_pr', None)
        state.pop('_active_consumer', None)
        # remove threading and function references because some functions/threading stuff can't be pickled...
        state.pop('_tpe_pool', None)
        state.pop('_consumer_topics', None)
        return state

    def _get_tpe(self, topic):
        tpe_pool = self._tpe_pool
        if tpe_pool is None:
            tpe = None
        elif topic not in tpe_pool:
            tpe_pool[topic] = tpe = ThreadPoolExecutor(max_workers=1)  # TODO: options config (add to __init__?) ...
        else:
            tpe = tpe_pool[topic]
        return tpe

    def ensure_topics_exist(self, topics, partitions=1):
        ac = self._ac
        logger = self.logger

        if ac is None:
            self._ac = ac = AdminClient(self._producer_config())
        if not is_iterable(topics):
            topics = [topics]

        logger.info('Ensuring topics exist: %s', topics, extra={
            'topics': topics,
            'partitions': partitions,
        })

        r = ac.create_topics([NewTopic(t, partitions) for t in topics],
                             operation_timeout=0.5, )  # type: dict[str, concurrent.futures.Future]
        # delay until we confirm that futures are done...
        for (t, f) in r.items():  # type: (str, concurrent.futures.Future)
            try:
                logger.debug('requesting new topic: %s', t, extra={'topic': t, })
                f.result()
            except KafkaException as e:
                # raise on any error that is not 'topic already exists', we allow existing topics...
                if e.args[0].code() != KafkaError.TOPIC_ALREADY_EXISTS:
                    logger.warning('error: %s', e, )
                    raise
                else:
                    logger.debug('topic already exists for %s', t, extra={'topic': t, })

        return

    def register_consumer(self, topic, fn_tkvt, **kwargs):  # TODO: do we want to consider adding key filter function at this level?
        topic = f'{self._topic_prefix}{topic}'
        ct = self._consumer_topics
        # mechanism to add to an active consumer...
        if self._active_consumer and topic not in ct:
            self._more_topics.append(topic)

        # wrap with thread pool executor if configured to do so...
        tpe = self._get_tpe(topic)
        if tpe is not None:
            fn_tkvt = tpe_wrapper(fn_tkvt, tpe=tpe)

        # append function
        ct[topic].append(fn_tkvt)

        return

    def consumer_loop_sync(self, poll_timeout=0.003, ooo_grace_period_ms=350):
        ct = self._consumer_topics
        mt = self._more_topics
        logger = self.logger
        ensure_topics = self.ensure_topics_exist

        # localize a bunch of variables
        latest_dt = [pd.Timestamp(0, tz=UTC)]
        ooo_warning = self._ooo_warning
        grace_period = pd.Timedelta(ooo_grace_period_ms, unit='ms')
        kafka_cfg = dict(self._kafka_config)
        deserialize = DEFAULT_DESERIALIZER

        def error_cb(kafka_error):
            self.logger.warning('kafka error: %s', kafka_error)
            raise IOError(kafka_error)  # try raising on all errors!
            # kafka_error.code == KafkaError._ALL_BROKERS_DOWN
            # # noinspection PyProtectedMember
            # if kafka_error.code == KafkaError._ALL_BROKERS_DOWN:
            #     raise IOError('kafka unreachable')
            # _RESOLVE comes up when DNS fails after kafka broker is taken down...
            # return

        # assign error callback
        kafka_cfg['error_cb'] = error_cb

        # convenience function for subscriptions...
        def subscribe(c, new_topics):
            c.subscribe(new_topics)
            group_id = kafka_cfg['group.id']
            logger.info("Subscription set to %s; group=%s", new_topics, group_id, extra={
                'topics': new_topics,
                'group_id': group_id,
            })

        def inner_loop(timeout=0.003):
            # ensure topics exist before loop
            topics = list(ct.keys())
            ensure_topics(topics)

            # init consumer instance
            c = Consumer(kafka_cfg)

            # configure subscription
            subscribe(c, topics)

            self._active_consumer = c
            self._not_abort = True
            try:
                logger.info('starting consumer loop')
                while self._not_abort:
                    msg = c.poll(timeout=timeout)
                    if msg is None:
                        continue
                    elif msg is not None and msg.error():
                        error = msg.error()
                        logger.warning(error, extra={
                            'error': error,
                            'topic': msg.topic(),
                        })
                        if error.code() == KafkaError.UNKNOWN_TOPIC_OR_PART:
                            c.close()
                            sleep(0.05)
                            # we've had resubscribing fall short in previous code,
                            # so we'll try to reinit the consumer connection after a short delay
                            return self._not_abort  # always True unless user aborted
                        continue
                    else:
                        topic, key, value, timestamp = (
                            msg.topic(),
                            msg.key().decode('utf-8') if msg.key() else '',
                            deserialize(msg.value()),
                            dt_from_unix_ms(msg.timestamp()[1])
                        )
                        for fn in ct[topic]:  # TODO: standardize TPE behavior?
                            # TODO: error handling for consumer functions?
                            fn(topic, key, value, timestamp)
                        # TODO: if topic not in ct then we try cached wildcard search? (otherwise wildcards don't work..)

                        # look for OoO messages...
                        if timestamp >= latest_dt[0]:
                            latest_dt[0] = timestamp
                        elif ooo_warning and latest_dt[0] - timestamp > grace_period:
                            logger.warning(
                                'OOO WARNING: topic "%s" saw message from %s, latest_dt=%s, key=%s',
                                topic, timestamp, latest_dt[0], key,
                                extra={
                                    'topic': topic,
                                    'dt_in': timestamp,
                                    'latest_dt': latest_dt[0],
                                    'key': key,
                                })

                        # add subscription and clear more_topics if we get late additions to what we're consuming
                        if mt:
                            mt.clear()  # clear mt since we got the message and then return True to re-init loop
                            return self._not_abort  # always True unless user aborted
            finally:
                self._active_consumer = None
                if c is not None:
                    c.close()
                logger.info('closed consumer loop')
            return self._not_abort  # always True unless user aborted

        # loop will return True if we intend to try again and False if not, so this should be enough
        # to maintain the logic without increasing stack depth!
        while inner_loop(poll_timeout):
            pass

        return

    def abort(self):
        self.logger.info('abort called')
        # if (c := self._active_consumer) is not None:
        #     # TODO: take an active action to abort consumer??
        #     pass
        self._not_abort = False  # results in (passive) consumer shutdown
        self.flush()  # flush on abort (producer shutdown)

    def _producer_config(self):
        config = self._kafka_config.copy()
        # pop these configuration entries to reduce unnecessary warnings
        config.pop('group.id', None)
        config.pop('enable.auto.commit', None)
        config.pop('auto.offset.reset', None)
        config.pop('compression.codec', None)
        return config

    def _establish_publisher(self):
        p = self._pr
        if p is None:
            self._pr = p = kafka_producer_callback(self._producer_config(), silent_mode=True, serialize=DEFAULT_SERIALIZER)
        return p

    def publish(self, topic, key, value, timestamp, **kwargs):  # TODO: ability to switch serializer for specific topics/calls?
        p = self._pr
        if p is None:
            p = self._establish_publisher()
        topic = f'{self._topic_prefix}{topic}'  # TODO: topic does not exist handling?
        if key is None:
            key = ''
        publish, flush = p
        publish(topic, key, value, timestamp, **kwargs)

    def flush(self):
        p = self._pr
        if p is None:  # if producer not set up, then there will not be anything to flush...
            return
        publish, flush = p
        flush()
