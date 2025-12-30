from pickle import loads as pickle_loads, dumps as pickle_dumps
from ..redis_raw_ttl import RedisRawTTLKVDict


class RedisPickleTTLKVDict(RedisRawTTLKVDict):

    def __setitem__(self, key, value, ttl=None):
        """Set a value in Redis with optional TTL. Uses default TTL if none is provided."""
        return super().__setitem__(key, pickle_dumps(value), ttl=ttl)

    put = __setitem__

    def __getitem__(self, key):
        """Retrieve a value from Redis."""
        return pickle_loads(super().__getitem__(key))

    def __repr__(self):
        """Return a string representation of the Redis dictionary."""
        return f"RedisPickleTTLKVDict({self.items()}, default_ttl={self.default_ttl})"
