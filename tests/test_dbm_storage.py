from stash.options import StashOptions
from stash.storages.dbm_ import DbmStorage


def _options(tmp_path):
    return StashOptions(
        {
            "algo": "md5",
            "fs_cache_dir": str(tmp_path),
            "dbm_filename": "test_cache.dbm",
        }
    )


def test_dbm_rm_removes_data_and_meta(tmp_path):
    storage = DbmStorage(_options(tmp_path))

    storage.write("k", b"v")
    assert storage.exists("k")

    storage.rm("k")

    assert not storage.exists("k")
    assert storage.read("k") is None
    storage.close()
