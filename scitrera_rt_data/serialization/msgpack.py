from typing import Any
from ._common import default_encoder

try:
    from msgpack import packb, unpackb
except ImportError:
    raise RuntimeError("msgpack is not installed")


def msgpack_serialize(content: Any) -> bytes:
    return packb(content, default=default_encoder)


def msgpack_deserialize(content: bytes) -> Any:
    return unpackb(content)
