import os

import dbm
from stash.options import StashOptions
from stash.storages.storage import Storage


class DbmStorage(Storage):
    def __init__(self, options: StashOptions):
        super().__init__(options)
        filepath = os.path.join(self.options.fs_cache_dir, options.dbm_filename)
        self.__db = dbm.open(filepath, "c")

    def exists(self, key: str) -> bool:
        return key in self.__db.keys()

    def purge(self, cutoff: int):
        pass

    def clear(self):
        self.__db.clear()

    def close(self):
        self.__db.close()

    def write(self, key: str, content):
        self.__db[key] = content

    def read(self, key: str):
        return self.__db.get(key)

    def rm(self, key: str):
        self.__db.pop(key)
