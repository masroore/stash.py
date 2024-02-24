from __future__ import annotations

import os.path

from stash.utils.checksum import to_bytes

try:
    from leveldb import LevelDB, LevelDBException
except ImportError:
    pass

from stash.options import StashOptions
from stash.storages.storage import Storage


class LeveldbStorage(Storage):
    def __init__(self, options: StashOptions):
        super().__init__(options)
        self._cache_max_age = self.options.cache_max_age
        self._logger = self.options.logger
        leveldb_path = os.path.abspath(self.options.fs_cache_dir)
        self._db: LevelDB | None = LevelDB(
            path=leveldb_path,
            create_if_missing=True,
            block_size=options.leveldb_block_size,
            lru_cache_size=options.leveldb_lru_cache_size,
            write_buffer_size=options.leveldb_write_buffer_size,
        )

    @staticmethod
    def _encode_str(s: str) -> bytes:
        return to_bytes(s.strip())

    def close(self):
        self._db.close(compact=True)
        self._db = None

    def exists(self, key: str) -> bool:
        return self._encode_str(key) in self._db

    def purge(self, cutoff: int):
        pass

    def clear(self):
        for key in self._db.keys():
            self._db.delete(key)

    def write(self, key: str, content):
        self._db.put(self._encode_str(key), to_bytes(content))

    def read(self, key: str) -> bytes:
        return self._db.get(self._encode_str(key))

    def rm(self, key: str):
        self._db.delete(self._encode_str(key))
