import pytest
from scitrera_rt_data.kv.redis_ttl.json import RedisJsonTTLKVDict
from scitrera_rt_data.kv.redis_ttl.msgpack import RedisMsgpackTTLKVDict
from scitrera_rt_data.kv.redis_ttl.pickle import RedisPickleTTLKVDict

def test_redis_json_ttl_kv_dict():
    kv = RedisJsonTTLKVDict(namespace="test_json:")
    kv.clear()
    data = {"a": 1, "b": [1, 2, 3]}
    kv.put("key1", data)
    assert kv["key1"] == data
    
    # Test __setitem__ via put
    kv.put("key2", {"x": "y"}, ttl=10)
    assert kv["key2"] == {"x": "y"}
    
    # Test repr
    assert "RedisJsonTTLKVDict" in repr(kv)
    kv.clear()

def test_redis_msgpack_ttl_kv_dict():
    kv = RedisMsgpackTTLKVDict(namespace="test_msgpack:")
    kv.clear()
    data = {"a": 1, "b": [1, 2, 3]}
    kv.put("key1", data)
    assert kv["key1"] == data
    
    # Test repr
    assert "RedisMsgpackTTLKVDict" in repr(kv)
    kv.clear()

def test_redis_pickle_ttl_kv_dict():
    kv = RedisPickleTTLKVDict(namespace="test_pickle:")
    kv.clear()
    data = {"a": 1, "b": [1, 2, 3]}
    kv.put("key1", data)
    assert kv["key1"] == data
    
    # Test repr
    assert "RedisPickleTTLKVDict" in repr(kv)
    kv.clear()
