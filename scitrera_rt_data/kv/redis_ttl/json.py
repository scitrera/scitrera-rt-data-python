from ...serialization.json import (json_serialize, json_deserialize)
from ..redis_raw_ttl import RedisRawTTLKVDict


class RedisJsonTTLKVDict(RedisRawTTLKVDict):

    def __setitem__(self, key, value, ttl=None):
        """Set a value in Redis with optional TTL. Uses default TTL if none is provided."""
        return super().__setitem__(key, json_serialize(value), ttl=ttl)

    put = __setitem__

    def __getitem__(self, key):
        """Retrieve a value from Redis."""
        return json_deserialize(super().__getitem__(key))

    def __repr__(self):
        """Return a string representation of the Redis dictionary."""
        return f"RedisJsonTTLKVDict({self.items()}, default_ttl={self.default_ttl})"
