from stash.serializers.serializer import Serializer
from typing import Any

try:
    from cbor2 import dumps, loads
except ImportError:
    pass


class CBORSerializer(Serializer):
    def deserialize(self, data: bytes | str) -> Any:
        return loads(data)

    def serialize(self, data: Any) -> bytes | str:
        return dumps(data)
