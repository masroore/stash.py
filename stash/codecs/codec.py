from abc import ABC, abstractmethod


class Codec(ABC):
    @abstractmethod
    def encode(self, data: bytes) -> bytes:
        raise NotImplementedError()

    @abstractmethod
    def decode(self, data: bytes) -> bytes:
        raise NotImplementedError()
