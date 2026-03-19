from stash.options import StashOptions
from stash.storages.storage import Storage


class NullStorage(Storage):
    def __init__(self, options: StashOptions):
        super().__init__(options)

    def exists(self, key: str) -> bool:
        return False

    def purge(self, cutoff: int) -> None:
        pass

    def clear(self) -> None:
        pass

    def close(self) -> None:
        pass

    def write(self, key: str, content: bytes) -> None:
        pass

    def read(self, key: str) -> None:
        pass

    def rm(self, key: str) -> None:
        pass
