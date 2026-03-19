from stash.codecs.codec import Codec

try:
    import gzip
except ImportError:
    pass


class GZipCodec(Codec):
    def encode(self, data: bytes) -> bytes:
        return gzip.compress(data)

    def decode(self, data: bytes) -> bytes:
        return gzip.decompress(data)
