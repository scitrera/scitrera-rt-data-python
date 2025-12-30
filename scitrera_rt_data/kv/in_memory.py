from ._base import KVStore


class DictKVStore(KVStore):
    """
    A simple in-memory key-value store implementation backed by a Python dictionary.
    This implementation does not support TTL functionality.
    """

    def __init__(self, namespace: str = None, store: dict = None):
        """
        Initialize an in-memory dictionary-backed key-value store.

        :param namespace: (not really needed for this implementation...) arbitrary namespace
        """
        if store is None:
            store = {}
        self._store = store  # Internal dictionary to store key-value pairs
        self.namespace = namespace

    def __getitem__(self, key):
        """
        Retrieve a value by key.
        
        :param key: The key to retrieve
        :raises KeyError: If the key does not exist
        :return: The value associated with the key
        """
        if key not in self._store:
            raise KeyError(f"Key {key} not found")
        return self._store[key]

    def __setitem__(self, key, value):
        """
        Set a value for a key.
        
        :param key: The key to set
        :param value: The value to store
        """
        self._store[key] = value

    def set(self, key, value):
        """
        Set a value for a key (alias for __setitem__).
        
        :param key: The key to set
        :param value: The value to store
        """
        self.__setitem__(key, value)

    def __delitem__(self, key):
        """
        Delete a key-value pair.
        
        :param key: The key to delete
        :raises KeyError: If the key does not exist
        """
        if key not in self._store:
            raise KeyError(f"Key {key} not found")
        del self._store[key]

    def __contains__(self, key):
        """
        Check if a key exists.
        
        :param key: The key to check
        :return: True if the key exists, False otherwise
        """
        return key in self._store

    def get(self, key, default=None):
        """
        Get a value by key, or return a default value if the key doesn't exist.
        
        :param key: The key to retrieve
        :param default: The default value to return if the key doesn't exist
        :return: The value associated with the key, or the default value
        """
        try:
            return self.__getitem__(key)
        except KeyError:
            return default

    def keys(self, prefix: str = None, remove_prefix: bool = False):
        """
        Get all keys in the store.
        
        :return: A list of all keys
        """
        return [
            k.removeprefix(prefix) if prefix and remove_prefix else k
            for k in self._store.keys()
            if (not prefix or k.startswith(prefix))
        ]

    def values(self, prefix: str = None, remove_prefix: bool = False):
        """
        Get all values in the store.
        
        :return: A list of all values
        """
        return [self.get(k) for k in self.keys(prefix, remove_prefix=False)]

    def items(self, prefix: str = None, remove_prefix: bool = False):
        """
        Get all key-value pairs in the store.
        
        :return: A list of (key, value) tuples
        """
        return [(k.removeprefix(prefix) if prefix and remove_prefix else k,
                 self.get(k)) for k in self.keys(prefix, remove_prefix=False)]

    def clear(self):
        """
        Remove all key-value pairs from the store.
        """
        self._store.clear()

    def __len__(self):
        """
        Get the number of key-value pairs in the store.
        
        :return: The number of key-value pairs
        """
        return len(self._store)

    def __iter__(self):
        """
        Get an iterator over the keys in the store.
        
        :return: An iterator over the keys
        """
        return iter(self.keys())

    def __repr__(self):
        """
        Return a string representation of the dictionary.
        
        :return: A string representation
        """
        return f"DictKVStore({self.items()}, namespace={self.namespace})"
