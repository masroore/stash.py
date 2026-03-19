from stash.codecs.passthru import PassthruCodec
from stash.manager import StashManager
from stash.options import StashOptions
from stash.serializers.serializer import Serializer
from stash.storages.memory import MemoryStorage
from typing import Any


class TrackingSerializer(Serializer):
    def __init__(self):
        self.serialize_calls = 0
        self.deserialize_calls = 0

    def serialize(self, data: Any) -> bytes | str:
        self.serialize_calls += 1
        return f"serialized:{data}".encode("utf-8")

    def deserialize(self, data: bytes | str) -> Any:
        self.deserialize_calls += 1
        if isinstance(data, str):
            data = data.encode("utf-8")
        return data.decode("utf-8").replace("serialized:", "", 1)


def _options():
    return StashOptions({"algo": "md5"})


def test_manager_applies_serializer_during_write_and_read():
    serializer = TrackingSerializer()
    manager = StashManager(
        storage=MemoryStorage(_options()),
        codec=PassthruCodec(),
        options=_options(),
        serializer=serializer,
    )

    manager.write("k", "v")

    assert serializer.serialize_calls == 1
    assert manager.read("k") == "v"
    assert serializer.deserialize_calls == 1


def test_manager_read_missing_returns_none():
    manager = StashManager(
        storage=MemoryStorage(_options()),
        codec=PassthruCodec(),
        options=_options(),
    )

    assert manager.read("missing") is None


class CloseTrackingStorage(MemoryStorage):
    def __init__(self, options):
        super().__init__(options)
        self.closed = False

    def close(self):
        self.closed = True


def test_manager_context_manager_closes_storage():
    storage = CloseTrackingStorage(_options())

    with StashManager(storage=storage, codec=PassthruCodec(), options=_options()) as manager:
        manager.write("k", "v")

    assert storage.closed is True
