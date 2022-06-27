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


def calcsum(algo: str, payload: str) -> str:
    if not algo:
        return payload

    algo = algo.strip().lower()
    if algo == CACHE_ALGO_XXH32:
        return calc_xxh32(payload)
    elif algo == CACHE_ALGO_XXH64:
        return calc_xxh64(payload)
    elif algo == CACHE_ALGO_MD5:
        return calc_md5(payload)
    elif algo == CACHE_ALGO_SHA1:
        return calc_sha1(payload)
    elif algo == CACHE_ALGO_MURMUR:
        return calc_murmur(payload)


def calc_md5(data: str) -> str:
    return hashlib.md5(data.encode("utf-8")).hexdigest().lower()


def calc_sha1(data: str) -> str:
    return hashlib.sha1(data.encode("utf-8")).hexdigest().lower()


def calc_xxh32(data: str) -> str:
    return _xxhash.xxh32(data).hexdigest().lower()


def calc_xxh64(data: str) -> str:
    return _xxhash.xxh64(data).hexdigest().lower()


def calc_xxh3_64(data: str) -> str:
    return _xxhash.xxh3_64(data).hexdigest().lower()


def calc_xxh3_128(data: str) -> str:
    return _xxhash.xxh3_128(data).hexdigest().lower()


def calc_murmur(data: str) -> str:
    return _murmurhash.hash(data).lower()
