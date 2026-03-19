from abc import ABC, abstractmethod
from typing import Any


class Serializer(ABC):
    @abstractmethod
    def serialize(self, data: Any) -> bytes | str:
        raise NotImplementedError()

    @abstractmethod
    def deserialize(self, data: bytes | str) -> Any:
        raise NotImplementedError()
