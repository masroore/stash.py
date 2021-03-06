from stash.manager import StashManager
from stash.options import StashOptions


def _init_cache(storage, codec, options: StashOptions) -> StashManager:
    cache_man = StashManager(storage=storage, codec=codec, options=options)
    return cache_man


def _init_fs_cache(codec, options: StashOptions) -> StashManager:
    from .storages.filesystem import FileSystemStorage

    return _init_cache(
        storage=FileSystemStorage(options=options), codec=codec, options=options
    )


def get_fs_zlib_stash(options: StashOptions) -> StashManager:
    from .codecs.zlib import ZlibCodec

    return _init_fs_cache(ZlibCodec(), options=options)


def get_fs_brotli_stash(options: StashOptions) -> StashManager:
    from .codecs.brotli import BrotliCodec

    return _init_fs_cache(BrotliCodec(), options=options)


def get_fs_zstd_stash(options: StashOptions) -> StashManager:
    from .codecs.zstd import ZstdCodec

    return _init_fs_cache(ZstdCodec(), options=options)


def get_mongo_zlib_stash(options: StashOptions) -> StashManager:
    from .codecs.zlib import ZlibCodec
    from .storages.mongodb import MongoDbStorage

    storage = MongoDbStorage(options=options)
    return _init_cache(storage, ZlibCodec(), options=options)


def get_lmdb_zl_stash(options: StashOptions) -> StashManager:
    from .codecs.zlib import ZlibCodec
    from .storages.lmdb import LmdbStorage

    storage = LmdbStorage(options=options)
    return _init_cache(storage, ZlibCodec(), options=options)


def get_lmdb_zstd_stash(options: StashOptions) -> StashManager:
    from .codecs.zstd import ZstdCodec
    from .storages.lmdb import LmdbStorage

    storage = LmdbStorage(options=options)
    return _init_cache(storage, ZstdCodec(), options=options)


def get_fs_stash(options: StashOptions) -> StashManager:
    return _init_fs_cache(codec=None, options=options)


def get_null_stash() -> StashManager:
    from .storages.null import NullStorage
    from .codecs.passthru import PassthruCodec

    options = StashOptions()
    return _init_cache(
        storage=NullStorage(options=options), codec=PassthruCodec(), options=options
    )


def get_dbm_zstd_stash(options: StashOptions) -> StashManager:
    from .codecs.zstd import ZstdCodec
    from .storages.dbm import DbmStorage

    storage = DbmStorage(options=options)
    return _init_cache(storage, ZstdCodec(), options=options)


def get_dbm_zlib_stash(options: StashOptions) -> StashManager:
    from .codecs.zlib import ZlibCodec
    from .storages.dbm import DbmStorage

    storage = DbmStorage(options=options)
    return _init_cache(storage, ZlibCodec(), options=options)


def get_dbm_stash(options: StashOptions) -> StashManager:
    from .storages.dbm import DbmStorage
    from .codecs.passthru import PassthruCodec

    storage = DbmStorage(options=options)
    return _init_cache(storage, codec=PassthruCodec(), options=options)
