from json import dumps, loads
from typing import Any

from ._common import default_encoder

JSON_ENCODING = 'ascii'


def json_serialize(content: Any, bytes_encoding=JSON_ENCODING) -> bytes:
    return dumps(content, default=default_encoder).encode(bytes_encoding)


def json_deserialize(content: str | bytes | bytearray, bytes_encoding=JSON_ENCODING) -> Any:
    return loads(content.decode(bytes_encoding))
