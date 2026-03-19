from stash.codecs.passthru import PassthruCodec
from stash.manager import StashManager
from stash.options import StashOptions
from stash.storages.memory import MemoryStorage


class TrackingSerializer:
    def __init__(self):
        self.serialize_calls = 0
        self.deserialize_calls = 0

    def serialize(self, data):
        self.serialize_calls += 1
        return f"serialized:{data}".encode("utf-8")

    def deserialize(self, data):
        self.deserialize_calls += 1
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
