import logging

from .._base import TKVTBroker
from ...dt import dt_from_unix_s


class TKVTLoggingHandler(logging.Handler):
    _broker = None
    _topic = None

    def __init__(self, broker: TKVTBroker, topic: str, level=logging.NOTSET) -> None:
        super().__init__(level)
        self._broker = broker
        self._topic = topic

    def emit(self, record: logging.LogRecord) -> None:
        value = record.__dict__.copy()
        key = value.pop('name')  # remove name and use as key

        # remove 'msecs' and set timestamp
        timestamp = dt_from_unix_s(value.pop('created'), utc=False)
        value.pop('msecs', None)  # remove the msecs field (just truncated milliseconds...)

        # standardize level information to 'lvl'
        value['lvl'] = value.pop('levelname')
        value.pop('levelno', None)

        # apply string arguments in message
        value['msg'] = record.getMessage()

        # TODO: how do we want to handle exceptions information
        # TODO: what other items should we filter/manage before transmission

        # finally publish
        self._broker.publish(self._topic, key, value, timestamp)

    def flush(self) -> None:
        self._broker.flush()

    pass


def attach_tktv_logging_handler(broker: TKVTBroker, topic: str, logger: Logger = logging.root, level=logging.NOTSET):
    """
    Attach a TKVT logging handler to the specified logger.

    :param broker: The TKVT broker instance to use for logging.
    :param topic: The topic to publish log records to.
    :param logger: The logger to attach the handler to. Defaults to the root logger.
    :param level: The logging level for the handler. Defaults to NOTSET.
    :return: The attached TKVTLoggingHandler instance.
    """
    handler = TKVTLoggingHandler(broker, topic, level)
    logger.addHandler(handler)
    return handler
