import pytest

from stash.options import StashOptions
from stash.storages.dbm_ import DbmStorage
from stash.storages.filesystem import FileSystemStorage
from stash.storages.memory import MemoryStorage


BASE_OPTIONS = {"algo": "md5", "cache_min_size": 0}


def _create_storage(kind: str, tmp_path):
    cache_dir = tmp_path / kind
    options_data = {**BASE_OPTIONS, "fs_cache_dir": str(cache_dir)}

    if kind == "memory":
        return MemoryStorage(StashOptions(options_data))

    if kind == "filesystem":
        return FileSystemStorage(StashOptions(options_data))

    if kind == "dbm":
        options_data["dbm_filename"] = "contract_cache.dbm"
        return DbmStorage(StashOptions(options_data))

    raise ValueError(f"Unsupported storage kind: {kind}")


@pytest.mark.parametrize("kind", ["memory", "filesystem", "dbm"])
def test_storage_contract_write_read_exists_rm(kind: str, tmp_path):
    storage = _create_storage(kind, tmp_path)
    key = "abc"
    content = b"payload"

    assert storage.exists(key) is False

    storage.write(key, content)

    assert storage.exists(key) is True
    assert storage.read(key) == content

    storage.rm(key)

    assert storage.exists(key) is False
    storage.close()


@pytest.mark.parametrize("kind", ["memory", "filesystem", "dbm"])
def test_storage_contract_clear(kind: str, tmp_path):
    storage = _create_storage(kind, tmp_path)

    storage.write("k1", b"a")
    storage.write("k2", b"b")

    assert storage.exists("k1") is True
    assert storage.exists("k2") is True

    storage.clear()

    assert storage.exists("k1") is False
    assert storage.exists("k2") is False
    storage.close()
