import stash._helpers as helpers
from stash.options import StashOptions


def _filesystem_options(tmp_path):
    return StashOptions(
        {
            "algo": "md5",
            "cache_min_size": 0,
            "fs_cache_dir": str(tmp_path / "cache"),
        }
    )


def test_get_stash_supports_memory_backend():
    stash = helpers.get_stash("memory", StashOptions({"algo": "md5"}), "passthru")

    stash.write("hello", b"world")

    assert stash.read("hello") == b"world"


def test_compatibility_wrapper_still_builds_filesystem_stash(tmp_path):
    stash = helpers.get_fs_stash(_filesystem_options(tmp_path))

    stash.write("greeting", {"message": "hello"})

    assert stash.read("greeting") == {"message": "hello"}


def test_get_stash_rejects_unknown_storage():
    try:
        helpers.get_stash("missing-backend", StashOptions({"algo": "md5"}))
    except ValueError as exc:
        assert str(exc) == "Unknown storage backend: missing-backend"
    else:
        raise AssertionError("Expected ValueError for unknown storage backend")


def test_stashify_uses_kwargs_in_cache_key():
    calls = {"count": 0}
    stash = helpers.get_stash("memory", StashOptions({"algo": "md5"}), "passthru")

    @helpers.stashify(stash=stash)
    def add(a, b=0):
        calls["count"] += 1
        return a + b

    assert add(a=1, b=2) == 3
    assert add(b=2, a=1) == 3
    assert calls["count"] == 1

    assert add(a=1, b=3) == 4
    assert calls["count"] == 2
