from stash.codecs.codec import Codec

try:
    from zopfli.zlib import compress
    from zlib import decompress
except ImportError:
    pass


class ZopfliCodec(Codec):
    def encode(self, data: bytes) -> bytes:
        return compress(data)

    def decode(self, data: bytes) -> bytes:
        return decompress(data)
