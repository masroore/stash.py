import hashlib

from stash.consts import *

try:
    import xxhash as _xxhash
except ImportError:
    _xxhash = None

try:
    import murmurhash.mrmr as _murmurhash
except ImportError:
    _murmurhash = None


def calcsum(payload: str, algo: str) -> str:
    if not algo:
        return payload

    algo = algo.strip().lower()
    if algo == CHECKSUM_ALGO_XXH32:
        return calc_xxh32(payload)
    elif algo == CHECKSUM_ALGO_XXH64:
        return calc_xxh64(payload)
    elif algo == CHECKSUM_ALGO_MD5:
        return calc_md5(payload)
    elif algo == CHECKSUM_ALGO_SHA1:
        return calc_sha1(payload)
    elif algo == CHECKSUM_ALGO_MURMUR:
        return calc_murmur(payload)

    raise ValueError("Unknown checksum algorithm: {}".format(algo))


def to_bytes(data) -> bytes:
    if isinstance(data, str):
        data = data.encode("utf-8")
    return data


def calc_md5(data: str) -> str:
    return hashlib.md5(to_bytes(data)).hexdigest().lower()


def calc_sha1(data: str) -> str:
    return hashlib.sha1(to_bytes(data)).hexdigest().lower()


def calc_xxh32(data: str) -> str:
    if _xxhash is None:
        raise RuntimeError("xxhash dependency is required for xxh32 algorithm")
    return _xxhash.xxh32(data).hexdigest().lower()


def calc_xxh64(data: str) -> str:
    if _xxhash is None:
        raise RuntimeError("xxhash dependency is required for xxh64 algorithm")
    return _xxhash.xxh64(data).hexdigest().lower()


def calc_xxh3_64(data: str) -> str:
    if _xxhash is None:
        raise RuntimeError("xxhash dependency is required for xxh3_64 algorithm")
    return _xxhash.xxh3_64(data).hexdigest().lower()


def calc_xxh3_128(data: str) -> str:
    if _xxhash is None:
        raise RuntimeError("xxhash dependency is required for xxh3_128 algorithm")
    return _xxhash.xxh3_128(data).hexdigest().lower()


def calc_murmur(data: str) -> str:
    if _murmurhash is None:
        raise RuntimeError("murmurhash dependency is required for murmur algorithm")
    return str(_murmurhash.hash(data)).lower()
