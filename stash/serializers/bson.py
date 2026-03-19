from stash.serializers.serializer import Serializer
from typing import Any

try:
    from bson import decode, encode
except ImportError:
    pass


class BSONSerializer(Serializer):
    def deserialize(self, data: bytes | str) -> Any:
        return decode(data)

    def serialize(self, data: Any) -> bytes | str:
        return encode(data)
