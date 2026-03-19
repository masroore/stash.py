from stash.codecs.codec import Codec

try:
    import brotli
except ImportError:
    pass


class BrotliCodec(Codec):
    def encode(self, data: bytes) -> bytes:
        return brotli.compress(data)

    def decode(self, data: bytes) -> bytes:
        return brotli.decompress(data)
