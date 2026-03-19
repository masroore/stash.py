from stash.serializers.serializer import Serializer
from typing import Any

try:
    from orjson import loads, dumps
except ImportError:
    pass


class OrJSONSerializer(Serializer):
    def deserialize(self, data: bytes | str) -> Any:
        return loads(data)

    def serialize(self, data: Any) -> bytes | str:
        return dumps(data)
