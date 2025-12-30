import pytest
import os
import redis
from scitrera_rt_data.kv.redis_raw_ttl import RedisRawTTLKVDict
from scitrera_rt_data.kv.in_memory import DictKVStore

@pytest.fixture
def redis_params():
    return {
        "redis_host": os.environ.get("REDIS_HOST", "localhost"),
        "redis_port": int(os.environ.get("REDIS_PORT", "6379")),
        "redis_db": int(os.environ.get("REDIS_DB", "0")),
        "password": os.environ.get("REDIS_PASSWORD"),
    }

def test_dict_kv_store_extended():
    store = DictKVStore(namespace="test")
    store["a"] = 1
    store["b/1"] = 2
    store["b/2"] = 3
    
    assert set(store.keys(prefix="b/")) == {"b/1", "b/2"}
    assert set(store.keys(prefix="b/", remove_prefix=True)) == {"1", "2"}
    assert set(store.values(prefix="b/")) == {2, 3}
    assert set(store.items(prefix="b/", remove_prefix=True)) == {("1", 2), ("2", 3)}
    
    assert str(store).startswith("DictKVStore")

def test_redis_raw_ttl_kv_dict(redis_params):
    store = RedisRawTTLKVDict(**redis_params, namespace="test_kv", default_ttl=10)
    store.clear()
    
    try:
        store["key1"] = b"val1"
        assert store["key1"] == b"val1"
        assert "key1" in store
        
        # Test TTL
        ttl = store.ttl("key1")
        assert ttl is not None and 0 < ttl <= 10
        
        # Test Refresh
        store.refresh("key1", ttl=20)
        assert 10 < store.ttl("key1") <= 20
        
        # Test keys with prefix
        store["pref:1"] = b"p1"
        store["pref:2"] = b"p2"
        assert set(store.keys(prefix="pref:")) == {"pref:1", "pref:2"}
        assert set(store.keys(prefix="pref:", remove_prefix=True)) == {"1", "2"}
        
        # Test delete
        del store["key1"]
        assert "key1" not in store
        with pytest.raises(KeyError):
            _ = store["key1"]
            
        store.clear()
        assert len(store) == 0
        
    finally:
        store.clear()

def test_redis_raw_ttl_no_ttl(redis_params):
    store = RedisRawTTLKVDict(**redis_params, namespace="test_no_ttl")
    store.clear()
    try:
        store["key"] = b"val"
        assert store.ttl("key") is None
    finally:
        store.clear()
