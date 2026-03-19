import pytest

from stash.options import StashOptions


@pytest.mark.optional_backend
def test_lmdb_missing_key_returns_none_when_dependency_present(tmp_path):
    pytest.importorskip("lmdb")
    from stash.storages.lmdb import LmdbStorage

    options = StashOptions({"algo": "md5", "fs_cache_dir": str(tmp_path / "lmdb")})
    storage = LmdbStorage(options)

    assert storage.read("missing") is None
    storage.close()


@pytest.mark.optional_backend
def test_leveldb_missing_key_returns_none_when_dependency_present(tmp_path):
    pytest.importorskip("leveldb")
    from stash.storages.leveldb import LeveldbStorage

    options = StashOptions({"algo": "md5", "fs_cache_dir": str(tmp_path / "leveldb")})
    storage = LeveldbStorage(options)

    assert storage.read("missing") is None
    storage.close()
