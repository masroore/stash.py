from stash.options import StashOptions
from stash.storages.storage import Storage
from typing import Optional


class MemoryStorage(Storage):
    def __init__(self, options: StashOptions):
        super().__init__(options)
        self.__dict: dict[str, bytes] = {}

    def exists(self, key: str) -> bool:
        return key in self.__dict

    def purge(self, cutoff: int) -> None:
        pass

    def clear(self) -> None:
        self.__dict.clear()

    def close(self) -> None:
        self.clear()

    def write(self, key: str, content: bytes) -> None:
        self.__dict[key] = content

    def read(self, key: str) -> Optional[bytes]:
        return self.__dict.get(key)

    def rm(self, key: str) -> None:
        self.__dict.pop(key, None)
