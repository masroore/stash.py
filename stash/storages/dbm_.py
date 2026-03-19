import os
from time import time
import dbm
from typing import Optional
from stash.options import StashOptions
from stash.storages.storage import Storage


class DbmStorage(Storage):
    def __init__(self, options: StashOptions):
        super().__init__(options)
        os.makedirs(self.options.fs_cache_dir, exist_ok=True)
        dbpath = os.path.join(self.options.fs_cache_dir, options.dbm_filename)
        self.__db = dbm.open(dbpath, "c")

    def _data_key(self, key: str) -> bytes:
        return f"{key.strip()}^@d".encode("utf-8")

    def _meta_key(self, key: str) -> bytes:
        return f"{key.strip()}^@m".encode("utf-8")

    def exists(self, key: str) -> bool:
        return self._data_key(key) in self.__db

    def purge(self, cutoff: int) -> None:
        pass

    def clear(self) -> None:
        self.__db.clear()

    def close(self) -> None:
        self.__db.close()

    def write(self, key: str, content: bytes) -> None:
        self.__db[self._data_key(key)] = content
        self.__db[self._meta_key(key)] = str(time())

    def read(self, key: str) -> Optional[bytes]:
        return self.__db.get(self._data_key(key))

    def rm(self, key: str) -> None:
        self.__db.pop(self._data_key(key), None)
        self.__db.pop(self._meta_key(key), None)
