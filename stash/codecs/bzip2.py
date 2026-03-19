from stash.codecs.codec import Codec

try:
    import bz2
except ImportError:
    pass


class BZip2Codec(Codec):
    def encode(self, data: bytes) -> bytes:
        return bz2.compress(data)

    def decode(self, data: bytes) -> bytes:
        return bz2.decompress(data)
