from stash.serializers.serializer import Serializer
import pickle
from typing import Any


class DefaultSerializer(Serializer):
    def deserialize(self, data: bytes | str) -> Any:
        if isinstance(data, str):
            data = data.encode("utf-8")
        return pickle.loads(data)

    def serialize(self, data: Any) -> bytes | str:
        return pickle.dumps(data)
