import pytest
from unittest.mock import MagicMock
from scitrera_app_framework import Variables
from scitrera_rt_data.redis_ import construct_redis_uri, redis_fn_kwargs

def test_construct_redis_uri():
    assert construct_redis_uri() == "redis://localhost:6379/0"
    assert construct_redis_uri(redis_host="myhost", redis_port=1234, redis_db=2) == "redis://myhost:1234/2"
    assert construct_redis_uri(password="pass") == "redis://:pass@localhost:6379/0"
    assert construct_redis_uri(ssl=True) == "rediss://localhost:6379/0"
    assert construct_redis_uri(password="pass", ssl=True) == "rediss://:pass@localhost:6379/0"

def test_redis_fn_kwargs():
    v = Variables()
    # Mock environment variables
    v.environ = MagicMock(side_effect=lambda key, default=None, type_fn=None: {
        'REDIS_HOST': 'host1',
        'REDIS_PORT': '1234',
        'REDIS_DB': '5',
        'REDIS_PASSWORD': 'pw',
        'REDIS_SSL': 'True'
    }.get(key, default))
    
    # We need a better mock because environ is called multiple times
    mock_env = {
        'REDIS_HOST': 'host1',
        'REDIS_PORT': 1234,
        'REDIS_DB': 5,
        'REDIS_PASSWORD': 'pw',
        'REDIS_SSL': True
    }
    def mock_environ(key, default=None, type_fn=None):
        val = mock_env.get(key, default)
        if type_fn and val is not None:
             # Just call the type_fn if provided, like the real v.environ does
             return type_fn(val) if not isinstance(val, (int, bool)) else val
        return val
    
    v.environ = MagicMock(side_effect=mock_environ)
    
    kwargs = redis_fn_kwargs(v)
    assert kwargs['redis_host'] == 'host1'
    assert kwargs['redis_port'] == 1234
    assert kwargs['redis_db'] == 5
    assert kwargs['password'] == 'pw'
    assert kwargs['ssl'] is True
