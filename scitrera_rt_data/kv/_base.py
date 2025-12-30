from abc import abstractmethod


class KVStore:
    """
    Abstract base class for key-value stores.
    Implementations should provide dictionary-like behavior.
    """

    @abstractmethod
    def __getitem__(self, key):
        """
        Retrieve a value by key.
        
        :param key: The key to retrieve
        :raises KeyError: If the key does not exist
        :return: The value associated with the key
        """
        pass

    @abstractmethod
    def __setitem__(self, key, value):
        """
        Set a value for a key.
        
        :param key: The key to set
        :param value: The value to store
        """
        pass

    @abstractmethod
    def __delitem__(self, key):
        """
        Delete a key-value pair.
        
        :param key: The key to delete
        :raises KeyError: If the key does not exist
        """
        pass

    @abstractmethod
    def __contains__(self, key):
        """
        Check if a key exists.
        
        :param key: The key to check
        :return: True if the key exists, False otherwise
        """
        pass

    @abstractmethod
    def get(self, key, default=None):
        """
        Get a value by key, or return a default value if the key doesn't exist.
        
        :param key: The key to retrieve
        :param default: The default value to return if the key doesn't exist
        :return: The value associated with the key, or the default value
        """
        pass

    def set(self, key, value):
        """
        Set a value for a key.

        :param key: The key to set
        :param value: The value to store
        """
        return self.__setitem__(key, value)

    @abstractmethod
    def keys(self, prefix: str = None, remove_prefix: bool = False):
        """
        Get all keys in the store.
        
        :return: A list of all keys
        """
        pass

    @abstractmethod
    def values(self, prefix: str = None, remove_prefix: bool = False):
        """
        Get all values in the store.
        
        :return: A list of all values
        """
        pass

    @abstractmethod
    def items(self, prefix: str = None, remove_prefix: bool = False):
        """
        Get all key-value pairs in the store.
        
        :return: A list of (key, value) tuples
        """
        pass

    @abstractmethod
    def clear(self):
        """
        Remove all key-value pairs from the store.
        """
        pass

    @abstractmethod
    def __len__(self):
        """
        Get the number of key-value pairs in the store.
        
        :return: The number of key-value pairs
        """
        pass

    @abstractmethod
    def __iter__(self):
        """
        Get an iterator over the keys in the store.
        
        :return: An iterator over the keys
        """
        pass
