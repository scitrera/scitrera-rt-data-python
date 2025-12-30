import unittest
from scitrera_rt_data.kv import KVStore
from scitrera_rt_data.kv.in_memory import DictKVStore


class TestKVStores(unittest.TestCase):
    """Test cases for KV store implementations."""

    def test_dict_kv_store(self):
        """Test the DictKVStore implementation."""
        # Create a new DictKVStore
        store = DictKVStore(namespace="test_dict")

        # Test basic operations
        self._test_basic_operations(store)

        # Verify the namespace
        self.assertEqual(store.namespace, "test_dict")

    def _test_basic_operations(self, store):
        """Test basic operations on a KV store."""
        # Verify it's a KVStore
        self.assertIsInstance(store, KVStore)

        # Test setting and getting values
        store["key1"] = "value1"
        store["key2"] = "value2"
        self.assertEqual(store["key1"], "value1")
        self.assertEqual(store["key2"], "value2")

        # Test contains
        self.assertTrue("key1" in store)
        self.assertFalse("key3" in store)

        # Test get with default
        self.assertEqual(store.get("key1"), "value1")
        self.assertEqual(store.get("key3", "default"), "default")

        # Test keys, values, items
        self.assertEqual(set(store.keys()), {"key1", "key2"})
        self.assertEqual(set(store.values()), {"value1", "value2"})
        self.assertEqual(set(store.items()), {("key1", "value1"), ("key2", "value2")})

        # Test length and iteration
        self.assertEqual(len(store), 2)
        keys = set()
        for key in store:
            keys.add(key)
        self.assertEqual(keys, {"key1", "key2"})

        # Test deletion
        del store["key1"]
        self.assertFalse("key1" in store)
        self.assertTrue("key2" in store)

        # Test clear
        store.clear()
        self.assertEqual(len(store), 0)
        self.assertFalse("key2" in store)


if __name__ == "__main__":
    # Run only the DictKVStore tests by default
    # RedisRawTTLKVDict tests would require a Redis server
    unittest.main()
