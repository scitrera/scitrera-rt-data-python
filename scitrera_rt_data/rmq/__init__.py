from typing import Optional

from scitrera_app_framework import Variables, ext_parse_bool, get_variables

try:
    from rstream import (Consumer, Producer, AMQPMessage, ConsumerOffsetSpecification, SlasMechanism)
except ImportError:
    raise RuntimeError("rstream is not installed. `pip install rstream` to use rmq functionality.")


def rmq_kwargs(v: Variables = None):
    v = get_variables(v)  # use global variables if none provided
    return {
        # 'rmq_host': v.environ('RABBITMQ_HOST', default='localhost'),
        'rmq_port': v.environ('RABBITMQ_PORT', default=5551, type_fn=int),
        'direct': v.environ('RABBITMQ_DIRECT', default=False, type_fn=ext_parse_bool),
        'mtls': v.environ('RABBITMQ_MTLS', default=True, type_fn=ext_parse_bool),
        'ca_data': v.get('RABBITMQ_CA_CERT', default=None),
        'client_cert_data': v.get('RABBITMQ_CLIENT_CERT', default=None),
        'client_key_data': v.get('RABBITMQ_CLIENT_KEY', default=None),
        'username': v.environ('RABBITMQ_USERNAME', default='guest') or None,
        'password': v.environ('RABBITMQ_PASSWORD', default='guest') or None,
        'vhost': v.environ('RABBITMQ_VHOST', default=None) or None,
        'max_stream_days': v.environ('RABBITMQ_MAX_STREAM_DAYS', default=7, type_fn=int),
        # TODO: max stream bytes and max segment bytes?
        # TODO: offset behavior
    }
