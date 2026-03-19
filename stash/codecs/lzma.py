from stash.codecs.codec import Codec

try:
    import lzma as _lzma
except ImportError:
    try:
        import backports.lzma as _backports_lzma

        _lzma = _backports_lzma
    except ImportError:
        pass


class LzmaCodec(Codec):
    def encode(self, data: bytes) -> bytes:
        return _lzma.compress(data)

    def decode(self, data: bytes) -> bytes:
        return _lzma.decompress(data)
