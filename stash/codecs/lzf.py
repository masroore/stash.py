from stash.codecs.codec import Codec

try:
    import lzf
except ImportError:
    pass


class LzfCodec(Codec):
    def encode(self, data: bytes) -> bytes:
        return lzf.compress(data)

    def decode(self, data: bytes) -> bytes:
        return lzf.decompress(data)
