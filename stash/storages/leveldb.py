from __future__ import annotations

import os.path
from typing import Optional

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

    def close(self) -> None:
        if self._db is not None:
            self._db.close(compact=True)
            self._db = None

    def exists(self, key: str) -> bool:
        if self._db is None:
            return False
        try:
            self._db.get(self._encode_str(key))
            return True
        except LevelDBException:
            return False

    def purge(self, cutoff: int) -> None:
        pass

    def clear(self) -> None:
        if self._db is None:
            return
        for key in self._db.keys():
            self._db.delete(key)

    def write(self, key: str, content: bytes) -> None:
        if self._db is None:
            return
        self._db.put(self._encode_str(key), to_bytes(content))

    def read(self, key: str) -> Optional[bytes]:
        if self._db is None:
            return None
        try:
            return self._db.get(self._encode_str(key))
        except LevelDBException:
            return None

    def rm(self, key: str) -> None:
        if self._db is None:
            return
        self._db.delete(self._encode_str(key))
