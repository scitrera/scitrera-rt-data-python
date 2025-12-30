import pytest
import numpy as np
import pandas as pd
from scitrera_rt_data.serialization.json import json_serialize, json_deserialize
from scitrera_rt_data.serialization.msgpack import msgpack_serialize, msgpack_deserialize
from scitrera_rt_data.serialization._common import default_encoder

def test_json_serialization():
    data = {
        "int": 1,
        "float": 1.5,
        "str": "hello",
        "list": [1, 2, 3],
        "dict": {"a": 1},
        "np_int": np.int64(10),
        "np_float": np.float64(10.5),
        "ts": pd.Timestamp("2023-01-01", tz="UTC"),
        "nat": pd.NA
    }
    
    serialized = json_serialize(data)
    deserialized = json_deserialize(serialized)
    
    assert deserialized["int"] == 1
    assert deserialized["float"] == 1.5
    assert deserialized["str"] == "hello"
    assert deserialized["np_int"] == 10
    assert deserialized["np_float"] == 10.5
    assert deserialized["ts"] == data["ts"].value
    assert np.isnan(deserialized["nat"])

def test_msgpack_serialization():
    data = {
        "int": 1,
        "float": 1.5,
        "np_int": np.int32(5),
        "ts": pd.Timestamp("2023-01-01", tz="UTC")
    }
    
    serialized = msgpack_serialize(data)
    deserialized = msgpack_deserialize(serialized)
    
    assert deserialized["int"] == 1
    assert deserialized["float"] == 1.5
    assert deserialized["np_int"] == 5
    assert deserialized["ts"] == data["ts"].value

def test_default_encoder_unsupported():
    class Unserializable:
        pass
    
    with pytest.raises(TypeError, match="object type not supported for encoding"):
        default_encoder(Unserializable())

def test_pydantic_serialization():
    try:
        from pydantic import BaseModel
    except ImportError:
        pytest.skip("pydantic not installed")
        
    class MyModel(BaseModel):
        name: str
        value: int
        
    model = MyModel(name="test", value=123)
    encoded = default_encoder(model)
    assert encoded == {"name": "test", "value": 123}
