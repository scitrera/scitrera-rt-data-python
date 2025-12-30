from ._base import KVStore


class RedisRawTTLKVDict(KVStore):
    _REDIS_KWARGS = {
        'decode_responses': False,
    }

    def __init__(self, redis_host='localhost', redis_port=6379, redis_db=0, namespace="rdict", password=None, ssl: bool = False,
                 default_ttl=None):
        """
        Initialize a Redis-backed dictionary-like object with optional TTL support.

        Note that the function parameters except for namespace and default_ttl are the same as those output from
        `scitrera_rt_data.redis_.redis_fn_kwargs` to enable instantiation from scitrera-app-framework variables instance /
        environment variables.

        :param redis_host: Redis server hostname
        :param redis_port: Redis server port
        :param redis_db: Redis database index
        :param namespace: Prefix for Redis keys to avoid collisions
        :param password: Redis password
        :param default_ttl: Default TTL (in seconds) for new keys if not specified, or None for no default TTL
        """
        try:
            import redis
        except ImportError:
            raise RuntimeError("redis[hiredis] is not installed")

        self.client = redis.StrictRedis(host=redis_host, port=redis_port, db=redis_db, password=password, ssl=ssl, **self._REDIS_KWARGS)
        self.namespace = namespace  # Prefix for keys
        self.default_ttl = default_ttl  # Default TTL (if set)

    def _full_key(self, key):
        """Generate a namespaced key."""
        return f"{self.namespace}:{key}"

    def __setitem__(self, key, value, ttl=None):
        """Set a value in Redis with the default TTL (if defined)."""
        ttl = ttl if ttl is not None else self.default_ttl
        if ttl is not None:
            self.client.setex(self._full_key(key), ttl, value)
        else:
            self.client.set(self._full_key(key), value)

    put = __setitem__

    def refresh(self, key, ttl=None):
        """
        Refresh the TTL of a key.

        :param key: The key to refresh.
        :param ttl: TTL override (if provided). Defaults to the instance's default_ttl.
        :raises KeyError: If the key does not exist.
        """
        if not self.__contains__(key):
            raise KeyError(f"Key {key} not found")

        ttl = ttl if ttl is not None else self.default_ttl
        if ttl is not None:
            self.client.expire(self._full_key(key), ttl)

    def __getitem__(self, key):
        """Retrieve a value from Redis."""
        value = self.client.get(self._full_key(key))
        if value is None:
            raise KeyError(f"Key {key} not found")
        return value

    def __delitem__(self, key):
        """Delete a key from Redis."""
        if not self.client.delete(self._full_key(key)):
            raise KeyError(f"Key {key} not found")

    def __contains__(self, key):
        """Check if a key exists in Redis."""
        return self.client.exists(self._full_key(key)) > 0

    def get(self, key, default=None):
        """Retrieve a value or return a default if not found."""
        try:
            return self.__getitem__(key)
        except KeyError:
            return default

    def keys(self, prefix: str = None, remove_prefix: bool = False):
        """Return a list of keys in the Redis namespace."""
        if not prefix:
            prefix = ''

        return [
            (
                key.decode().removeprefix(self._full_key('')).removeprefix(prefix)
                if prefix and remove_prefix else
                key.decode().removeprefix(self._full_key(''))
            ) for key in self.client.keys(f"{self.namespace}:{prefix}*")]

    def values(self, prefix: str = None, remove_prefix: bool = False):
        """Return a list of values in the Redis namespace."""
        return [self.get(k) for k in self.keys(prefix, remove_prefix=False)]

    def items(self, prefix: str = None, remove_prefix: bool = False):
        """Return a list of (key, value) tuples in the Redis namespace."""
        return [(k.removeprefix(prefix) if prefix and remove_prefix else k,
                 self.get(k)) for k in self.keys(prefix, remove_prefix=False)]

    def clear(self):
        """Remove all keys in the Redis namespace."""
        keys = self.client.keys(f"{self.namespace}:*")
        if keys:
            self.client.delete(*keys)

    def __len__(self):
        """Return the number of keys in the Redis namespace."""
        return len(self.client.keys(f"{self.namespace}:*"))

    def __iter__(self):
        """Iterate over keys in the Redis namespace."""
        return iter(self.keys())

    def ttl(self, key):
        """Get the time-to-live (TTL) of a key in seconds. Returns None if no TTL is set."""
        ttl = self.client.ttl(self._full_key(key))
        return ttl if ttl != -1 else None

    def __repr__(self):
        """Return a string representation of the Redis dictionary."""
        return f"RedisRawTTLKVDict({self.items()}, default_ttl={self.default_ttl})"
