from stash.codecs.codec import Codec

try:
    import snappy
except ImportError:
    pass


class SnappyCodec(Codec):
    def encode(self, data: bytes) -> bytes:
        return snappy.compress(data)

    def decode(self, data: bytes) -> bytes:
        return snappy.uncompress(data)
