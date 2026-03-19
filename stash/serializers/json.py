from stash.serializers.serializer import Serializer
from typing import Any

try:
    from json import loads, dumps
except ImportError:
    pass


class JSONSerializer(Serializer):
    def deserialize(self, data: bytes | str) -> Any:
        return loads(data)

    def serialize(self, data: Any) -> bytes | str:
        return dumps(data)
