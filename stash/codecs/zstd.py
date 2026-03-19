from stash.codecs.codec import Codec

try:
    import zstd
except ImportError:
    pass


class ZstdCodec(Codec):
    def encode(self, data: bytes) -> bytes:
        return zstd.compress(data, 10)

    def decode(self, data: bytes) -> bytes:
        return zstd.decompress(data)
