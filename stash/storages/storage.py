from abc import ABC, abstractmethod
from typing import Optional

from stash.options import StashOptions


class Storage(ABC):
    def __init__(self, options: StashOptions):
        self.options = options

    @abstractmethod
    def exists(self, key: str) -> bool:
        raise NotImplementedError()

    @abstractmethod
    def purge(self, cutoff: int) -> None:
        raise NotImplementedError()

    @abstractmethod
    def clear(self) -> None:
        raise NotImplementedError()

    @abstractmethod
    def close(self) -> None:
        raise NotImplementedError()

    @abstractmethod
    def write(self, key: str, content: bytes) -> None:
        raise NotImplementedError()

    @abstractmethod
    def read(self, key: str) -> Optional[bytes]:
        raise NotImplementedError()

    @abstractmethod
    def rm(self, key: str) -> None:
        raise NotImplementedError()
