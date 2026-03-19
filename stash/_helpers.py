import functools
import hashlib
from importlib import import_module
from typing import Callable, Dict, Optional, Protocol, Tuple, TypeVar, cast

from stash.codecs.codec import Codec
from stash.consts import SIZE_KB, SIZE_MB, SIZE_GB
from stash.manager import StashManager
from stash.options import StashOptions
from stash.serializers.serializer import Serializer
from stash.storages.storage import Storage


StorageSpec = Tuple[str, str]
CodecSpec = Tuple[str, str]
SerializerSpec = Tuple[str, str]
CompatHelperSpec = Tuple[str, Optional[str], bool]

StorageT = TypeVar("StorageT", bound=Storage, covariant=True)
CodecT = TypeVar("CodecT", bound=Codec, covariant=True)
SerializerT = TypeVar("SerializerT", bound=Serializer, covariant=True)


class StorageConstructor(Protocol[StorageT]):
    def __call__(self, *, options: StashOptions) -> StorageT: ...


class ComponentConstructor(Protocol[CodecT]):
    def __call__(self) -> CodecT: ...


class SerializerConstructor(Protocol[SerializerT]):
    def __call__(self) -> SerializerT: ...

_STORAGE_REGISTRY: Dict[str, StorageSpec] = {
    "dbm": ("stash.storages.dbm_", "DbmStorage"),
    "filesystem": ("stash.storages.filesystem", "FileSystemStorage"),
    "leveldb": ("stash.storages.leveldb", "LeveldbStorage"),
    "lmdb": ("stash.storages.lmdb", "LmdbStorage"),
    "lsmdb": ("stash.storages.lsmdb", "LsmDbStorage"),
    "memory": ("stash.storages.memory", "MemoryStorage"),
    "mongodb": ("stash.storages.mongodb", "MongoDbStorage"),
    "null": ("stash.storages.null", "NullStorage"),
    "redis": ("stash.storages.redis", "RedisStorage"),
}

_STORAGE_ALIASES = {
    "fs": "filesystem",
    "mongo": "mongodb",
}

_CODEC_REGISTRY: Dict[str, CodecSpec] = {
    "brotli": ("stash.codecs.brotli", "BrotliCodec"),
    "lzma": ("stash.codecs.lzma", "LzmaCodec"),
    "passthru": ("stash.codecs.passthru", "PassthruCodec"),
    "zlib": ("stash.codecs.zlib", "ZlibCodec"),
    "zstd": ("stash.codecs.zstd", "ZstdCodec"),
}

_CODEC_ALIASES = {
    "passthrough": "passthru",
}

_SERIALIZER_REGISTRY: Dict[str, SerializerSpec] = {
    "bson": ("stash.serializers.bson", "BSONSerializer"),
    "cbor": ("stash.serializers.cbor", "CBORSerializer"),
    "default": ("stash.serializers.default", "DefaultSerializer"),
    "json": ("stash.serializers.json", "JSONSerializer"),
    "msgpack": ("stash.serializers.msgpack", "MsgPackSerializer"),
    "orjson": ("stash.serializers.orjson", "OrJSONSerializer"),
    "rapidjson": ("stash.serializers.rapidjson", "RapidJSONSerializer"),
    "simplejson": ("stash.serializers.simplejson", "SimpleJSONSerializer"),
    "ujson": ("stash.serializers.ujson", "UltraJSONSerializer"),
}

_SERIALIZER_ALIASES = {
    "pickle": "default",
}

_COMPAT_HELPERS: Dict[str, CompatHelperSpec] = {
    "get_dbm_brotli_stash": ("dbm", "brotli", True),
    "get_dbm_lzma_stash": ("dbm", "lzma", True),
    "get_dbm_stash": ("dbm", "passthru", True),
    "get_dbm_zlib_stash": ("dbm", "zlib", True),
    "get_dbm_zstd_stash": ("dbm", "zstd", True),
    "get_fs_brotli_stash": ("filesystem", "brotli", True),
    "get_fs_lzma_stash": ("filesystem", "lzma", True),
    "get_fs_stash": ("filesystem", None, True),
    "get_fs_zlib_stash": ("filesystem", "zlib", True),
    "get_fs_zstd_stash": ("filesystem", "zstd", True),
    "get_leveldb_brotli_stash": ("leveldb", "brotli", True),
    "get_leveldb_lzma_stash": ("leveldb", "lzma", True),
    "get_leveldb_stash": ("leveldb", "passthru", True),
    "get_leveldb_zlib_stash": ("leveldb", "zlib", True),
    "get_leveldb_zstd_stash": ("leveldb", "zstd", True),
    "get_lmdb_brotli_stash": ("lmdb", "brotli", True),
    "get_lmdb_lzma_stash": ("lmdb", "lzma", True),
    "get_lmdb_stash": ("lmdb", "passthru", True),
    "get_lmdb_zlib_stash": ("lmdb", "zlib", True),
    "get_lmdb_zstd_stash": ("lmdb", "zstd", True),
    "get_lsmdb_brotli_stash": ("lsmdb", "brotli", True),
    "get_lsmdb_lzma_stash": ("lsmdb", "lzma", True),
    "get_lsmdb_stash": ("lsmdb", "passthru", True),
    "get_lsmdb_zlib_stash": ("lsmdb", "zlib", True),
    "get_lsmdb_zstd_stash": ("lsmdb", "zstd", True),
    "get_mongo_zlib_stash": ("mongodb", "zlib", True),
    "get_null_stash": ("null", "passthru", False),
}


def size_kb(n: int) -> int:
    return SIZE_KB * n


def size_mb(n: int) -> int:
    return SIZE_MB * n


def size_gb(n: int) -> int:
    return SIZE_GB * n


def _load_storage_constructor(spec: StorageSpec) -> StorageConstructor[Storage]:
    module_name, class_name = spec
    module = import_module(module_name)
    return cast(StorageConstructor[Storage], getattr(module, class_name))


def _load_codec_constructor(spec: CodecSpec) -> ComponentConstructor[Codec]:
    module_name, class_name = spec
    module = import_module(module_name)
    return cast(ComponentConstructor[Codec], getattr(module, class_name))


def _load_serializer_constructor(
    spec: SerializerSpec,
) -> SerializerConstructor[Serializer]:
    module_name, class_name = spec
    module = import_module(module_name)
    return cast(SerializerConstructor[Serializer], getattr(module, class_name))


def _normalize_storage_name(storage_name: str) -> str:
    normalized = storage_name.strip().lower()
    return _STORAGE_ALIASES.get(normalized, normalized)


def _normalize_codec_name(codec_name: Optional[str]) -> Optional[str]:
    if codec_name is None:
        return None

    normalized = codec_name.strip().lower()
    if normalized == "none":
        return None
    return _CODEC_ALIASES.get(normalized, normalized)


def _normalize_serializer_name(serializer_name: Optional[str]) -> Optional[str]:
    if serializer_name is None:
        return None

    normalized = serializer_name.strip().lower()
    if normalized == "none":
        return None
    return _SERIALIZER_ALIASES.get(normalized, normalized)


def _init_cache(
    storage: Storage,
    codec: Optional[Codec],
    options: StashOptions,
    serializer: Optional[Serializer] = None,
) -> StashManager:
    manager_codec = codec or _create_codec("passthru")
    cache_man = StashManager(
        storage=storage,
        codec=manager_codec,
        options=options,
        serializer=serializer,
    )
    return cache_man


def _create_storage(storage_name: str, options: StashOptions) -> Storage:
    normalized_name = _normalize_storage_name(storage_name)
    spec = _STORAGE_REGISTRY.get(normalized_name)
    if spec is None:
        raise ValueError("Unknown storage backend: {}".format(storage_name))

    storage_class = _load_storage_constructor(spec)
    return storage_class(options=options)


def _create_codec(codec_name: Optional[str]) -> Optional[Codec]:
    normalized_name = _normalize_codec_name(codec_name)
    if normalized_name is None:
        return None

    spec = _CODEC_REGISTRY.get(normalized_name)
    if spec is None:
        raise ValueError("Unknown codec: {}".format(codec_name))

    codec_class = _load_codec_constructor(spec)
    return codec_class()


def _create_serializer(serializer_name: Optional[str]) -> Optional[Serializer]:
    normalized_name = _normalize_serializer_name(serializer_name)
    if normalized_name is None:
        return None

    spec = _SERIALIZER_REGISTRY.get(normalized_name)
    if spec is None:
        raise ValueError("Unknown serializer: {}".format(serializer_name))

    serializer_class = _load_serializer_constructor(spec)
    return serializer_class()


def get_stash(
    storage_name: str,
    options: Optional[StashOptions] = None,
    codec_name: Optional[str] = None,
    serializer_name: Optional[str] = None,
) -> StashManager:
    options = options or StashOptions()
    storage = _create_storage(storage_name, options)
    codec = _create_codec(codec_name)
    serializer = _create_serializer(serializer_name)
    return _init_cache(
        storage=storage,
        codec=codec,
        options=options,
        serializer=serializer,
    )


def _make_compat_helper(
    storage_name: str,
    codec_name: Optional[str],
    expects_options: bool,
) -> Callable[..., StashManager]:
    if expects_options:

        def helper_with_options(options: StashOptions) -> StashManager:
            return get_stash(
                storage_name=storage_name,
                options=options,
                codec_name=codec_name,
            )

        return helper_with_options

    def helper_without_options() -> StashManager:
        return get_stash(
            storage_name=storage_name,
            options=StashOptions(),
            codec_name=codec_name,
        )

    return helper_without_options


def _make_stashify_key(
    function: Callable[..., object],
    args: tuple[object, ...],
    kwargs: dict[str, object],
) -> str:
    kwargs_items = tuple(sorted(kwargs.items()))
    payload = (function.__module__, function.__qualname__, args, kwargs_items)
    return hashlib.sha256(repr(payload).encode("utf-8")).hexdigest()


def stashify(stash: Optional[StashManager] = None):
    stash_ = stash

    def decorator(function):
        stash = stash_
        if stash is None:
            stash = get_stash("filesystem", StashOptions())

        @functools.wraps(function)
        def func(*args, **kwargs):
            key = _make_stashify_key(function, args, kwargs)
            if not stash.exists(key):
                content = function(*args, **kwargs)
                stash.write(key=key, content=content)
                return content

            return stash.read(key)

        return func

    return decorator


get_dbm_brotli_stash = _make_compat_helper("dbm", "brotli", True)
get_dbm_lzma_stash = _make_compat_helper("dbm", "lzma", True)
get_dbm_stash = _make_compat_helper("dbm", "passthru", True)
get_dbm_zlib_stash = _make_compat_helper("dbm", "zlib", True)
get_dbm_zstd_stash = _make_compat_helper("dbm", "zstd", True)
get_fs_brotli_stash = _make_compat_helper("filesystem", "brotli", True)
get_fs_lzma_stash = _make_compat_helper("filesystem", "lzma", True)
get_fs_stash = _make_compat_helper("filesystem", None, True)
get_fs_zlib_stash = _make_compat_helper("filesystem", "zlib", True)
get_fs_zstd_stash = _make_compat_helper("filesystem", "zstd", True)
get_leveldb_brotli_stash = _make_compat_helper("leveldb", "brotli", True)
get_leveldb_lzma_stash = _make_compat_helper("leveldb", "lzma", True)
get_leveldb_stash = _make_compat_helper("leveldb", "passthru", True)
get_leveldb_zlib_stash = _make_compat_helper("leveldb", "zlib", True)
get_leveldb_zstd_stash = _make_compat_helper("leveldb", "zstd", True)
get_lmdb_brotli_stash = _make_compat_helper("lmdb", "brotli", True)
get_lmdb_lzma_stash = _make_compat_helper("lmdb", "lzma", True)
get_lmdb_stash = _make_compat_helper("lmdb", "passthru", True)
get_lmdb_zlib_stash = _make_compat_helper("lmdb", "zlib", True)
get_lmdb_zstd_stash = _make_compat_helper("lmdb", "zstd", True)
get_lsmdb_brotli_stash = _make_compat_helper("lsmdb", "brotli", True)
get_lsmdb_lzma_stash = _make_compat_helper("lsmdb", "lzma", True)
get_lsmdb_stash = _make_compat_helper("lsmdb", "passthru", True)
get_lsmdb_zlib_stash = _make_compat_helper("lsmdb", "zlib", True)
get_lsmdb_zstd_stash = _make_compat_helper("lsmdb", "zstd", True)
get_mongo_zlib_stash = _make_compat_helper("mongodb", "zlib", True)
get_null_stash = _make_compat_helper("null", "passthru", False)


__all__ = [
    "get_stash",
    "size_gb",
    "size_kb",
    "size_mb",
    "stashify",
    *_COMPAT_HELPERS.keys(),
]
