from stash.serializers.serializer import Serializer
from typing import Any

try:
    from ujson import loads, dumps
except ImportError:
    pass


class UltraJSONSerializer(Serializer):
    def deserialize(self, data: bytes | str) -> Any:
        return loads(data)

    def serialize(self, data: Any) -> bytes | str:
        return dumps(data)
