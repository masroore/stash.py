from stash.codecs.codec import Codec


class PassthruCodec(Codec):
    def encode(self, data: bytes) -> bytes:
        return data

    def decode(self, data: bytes) -> bytes:
        return data
