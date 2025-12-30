from typing import Optional

from scitrera_app_framework import Variables, ext_parse_bool


def construct_redis_uri(redis_host: str = 'localhost', redis_port: int = 6379, redis_db: int = 0,
                        password: Optional[str] = None, ssl: bool = False) -> str:
    """
    Construct a Redis URI from the connection parameters.

    Returns:
        A Redis URI string.
    """
    ssl_modifier = 's' if ssl else ''
    if password:
        return f"redis{ssl_modifier}://:{password}@{redis_host}:{redis_port}/{redis_db}"
    else:
        return f"redis{ssl_modifier}://{redis_host}:{redis_port}/{redis_db}"


def redis_fn_kwargs(v: Variables):
    return {
        'redis_host': v.environ('REDIS_HOST', default='localhost'),
        'redis_port': v.environ('REDIS_PORT', default=6379, type_fn=int),
        'redis_db': v.environ('REDIS_DB', default='0', type_fn=int),
        'password': v.environ('REDIS_PASSWORD', default=None) or None,
        'ssl': v.environ('REDIS_SSL', default=False, type_fn=ext_parse_bool),
    }


def construct_aioredis_from_url(url: str, **kwargs):
    try:
        import redis.asyncio as aioredis
    except ImportError:
        raise RuntimeError("redis.asyncio is not installed. `pip install redis[hiredis]` to use this feature.")
    return aioredis.from_url(url, **kwargs)


def construct_aioredis(v: Variables, **kwargs):
    return construct_aioredis_from_url(construct_redis_uri(**redis_fn_kwargs(v)), **kwargs)
