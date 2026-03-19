import zlib
from stash.codecs.codec import Codec


class ZlibCodec(Codec):
    def encode(self, data: bytes) -> bytes:
        return zlib.compress(data)

    def decode(self, data: bytes) -> bytes:
        return zlib.decompress(data)
