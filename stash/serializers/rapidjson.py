from stash.serializers.serializer import Serializer
from typing import Any

try:
    from rapidjson import loads, dumps
except ImportError:
    pass


class RapidJSONSerializer(Serializer):
    def deserialize(self, data: bytes | str) -> Any:
        return loads(data)

    def serialize(self, data: Any) -> bytes | str:
        return dumps(data)
