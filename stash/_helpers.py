from stash.cache import CacheManager
from stash.codecs.passthru import PassthruCodec
from stash.options import CacheOptions


def _init_cache(storage, codec, options: CacheOptions) -> CacheManager:
    cache_man = CacheManager(storage=storage, codec=codec, options=options)
    return cache_man


def _init_fs_cache(codec, options: CacheOptions) -> CacheManager:
    from .storages.filesystem import FilesystemStorage

    return _init_cache(
        storage=FilesystemStorage(options=options), codec=codec, options=options
    )


def get_fs_zl_cache(options: CacheOptions) -> CacheManager:
    from .codecs.zlib import ZlibCodec

    return _init_fs_cache(ZlibCodec(), options=options)


def get_fs_br_cache(options: CacheOptions) -> CacheManager:
    from .codecs.brotli import BrotliCodec

    return _init_fs_cache(BrotliCodec(), options=options)


def get_fs_zs_cache(options: CacheOptions) -> CacheManager:
    from .codecs.zstd import ZstdCodec

    return _init_fs_cache(ZstdCodec(), options=options)


def get_mongo_zl_cache(options: CacheOptions) -> CacheManager:
    from .codecs.zlib import ZlibCodec
    from .storages.mongodb import MongoDbStorage

    storage = MongoDbStorage(options=options)
    return _init_cache(storage, ZlibCodec(), options=options)


def get_lmdb_zl_cache(options: CacheOptions) -> CacheManager:
    from .codecs.zlib import ZlibCodec
    from .storages.lm_db import LmdbStorage

    storage = LmdbStorage(options=options)
    return _init_cache(storage, ZlibCodec(), options=options)


def get_lmdb_zs_cache(options: CacheOptions) -> CacheManager:
    from .codecs.zstd import ZstdCodec
    from .storages.lm_db import LmdbStorage

    storage = LmdbStorage(options=options)
    return _init_cache(storage, ZstdCodec(), options=options)


def get_fs_cache(options: CacheOptions) -> CacheManager:
    return _init_fs_cache(codec=None, options=options)


def get_null_cache() -> CacheManager:
    from .storages.null import NullStorage

    options = CacheOptions()
    return _init_cache(
        storage=NullStorage(options=options), codec=PassthruCodec(), options=options
    )
