import functools
import hashlib
from enum import Enum
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


class StorageName(str, Enum):
    DBM = "dbm"
    FILESYSTEM = "filesystem"
    LEVELDB = "leveldb"
    LMDB = "lmdb"
    LSMDB = "lsmdb"
    MEMORY = "memory"
    MONGODB = "mongodb"
    NULL = "null"
    REDIS = "redis"


class CodecName(str, Enum):
    BROTLI = "brotli"
    LZMA = "lzma"
    PASSTHRU = "passthru"
    ZLIB = "zlib"
    ZSTD = "zstd"


class SerializerName(str, Enum):
    BSON = "bson"
    CBOR = "cbor"
    DEFAULT = "default"
    JSON = "json"
    MSGPACK = "msgpack"
    ORJSON = "orjson"
    RAPIDJSON = "rapidjson"
    SIMPLEJSON = "simplejson"
    UJSON = "ujson"


StorageNameInput = StorageName | str
CodecNameInput = CodecName | str
SerializerNameInput = SerializerName | str
CompatHelperSpec = Tuple[StorageName, Optional[CodecName], bool]

StorageT = TypeVar("StorageT", bound=Storage, covariant=True)
CodecT = TypeVar("CodecT", bound=Codec, covariant=True)
SerializerT = TypeVar("SerializerT", bound=Serializer, covariant=True)


class StorageConstructor(Protocol[StorageT]):
    def __call__(self, *, options: StashOptions) -> StorageT: ...


class ComponentConstructor(Protocol[CodecT]):
    def __call__(self) -> CodecT: ...


class SerializerConstructor(Protocol[SerializerT]):
    def __call__(self) -> SerializerT: ...

_STORAGE_REGISTRY: Dict[StorageName, StorageSpec] = {
    StorageName.DBM: ("stash.storages.dbm_", "DbmStorage"),
    StorageName.FILESYSTEM: ("stash.storages.filesystem", "FileSystemStorage"),
    StorageName.LEVELDB: ("stash.storages.leveldb", "LeveldbStorage"),
    StorageName.LMDB: ("stash.storages.lmdb", "LmdbStorage"),
    StorageName.LSMDB: ("stash.storages.lsmdb", "LsmDbStorage"),
    StorageName.MEMORY: ("stash.storages.memory", "MemoryStorage"),
    StorageName.MONGODB: ("stash.storages.mongodb", "MongoDbStorage"),
    StorageName.NULL: ("stash.storages.null", "NullStorage"),
    StorageName.REDIS: ("stash.storages.redis", "RedisStorage"),
}

_STORAGE_ALIASES: Dict[str, StorageName] = {
    "fs": StorageName.FILESYSTEM,
    "mongo": StorageName.MONGODB,
}

_CODEC_REGISTRY: Dict[CodecName, CodecSpec] = {
    CodecName.BROTLI: ("stash.codecs.brotli", "BrotliCodec"),
    CodecName.LZMA: ("stash.codecs.lzma", "LzmaCodec"),
    CodecName.PASSTHRU: ("stash.codecs.passthru", "PassthruCodec"),
    CodecName.ZLIB: ("stash.codecs.zlib", "ZlibCodec"),
    CodecName.ZSTD: ("stash.codecs.zstd", "ZstdCodec"),
}

_CODEC_ALIASES: Dict[str, CodecName] = {
    "passthrough": CodecName.PASSTHRU,
}

_SERIALIZER_REGISTRY: Dict[SerializerName, SerializerSpec] = {
    SerializerName.BSON: ("stash.serializers.bson", "BSONSerializer"),
    SerializerName.CBOR: ("stash.serializers.cbor", "CBORSerializer"),
    SerializerName.DEFAULT: ("stash.serializers.default", "DefaultSerializer"),
    SerializerName.JSON: ("stash.serializers.json", "JSONSerializer"),
    SerializerName.MSGPACK: ("stash.serializers.msgpack", "MsgPackSerializer"),
    SerializerName.ORJSON: ("stash.serializers.orjson", "OrJSONSerializer"),
    SerializerName.RAPIDJSON: ("stash.serializers.rapidjson", "RapidJSONSerializer"),
    SerializerName.SIMPLEJSON: ("stash.serializers.simplejson", "SimpleJSONSerializer"),
    SerializerName.UJSON: ("stash.serializers.ujson", "UltraJSONSerializer"),
}

_SERIALIZER_ALIASES: Dict[str, SerializerName] = {
    "pickle": SerializerName.DEFAULT,
}

_COMPAT_HELPERS: Dict[str, CompatHelperSpec] = {
    "get_dbm_brotli_stash": (StorageName.DBM, CodecName.BROTLI, True),
    "get_dbm_lzma_stash": (StorageName.DBM, CodecName.LZMA, True),
    "get_dbm_stash": (StorageName.DBM, CodecName.PASSTHRU, True),
    "get_dbm_zlib_stash": (StorageName.DBM, CodecName.ZLIB, True),
    "get_dbm_zstd_stash": (StorageName.DBM, CodecName.ZSTD, True),
    "get_fs_brotli_stash": (StorageName.FILESYSTEM, CodecName.BROTLI, True),
    "get_fs_lzma_stash": (StorageName.FILESYSTEM, CodecName.LZMA, True),
    "get_fs_stash": (StorageName.FILESYSTEM, None, True),
    "get_fs_zlib_stash": (StorageName.FILESYSTEM, CodecName.ZLIB, True),
    "get_fs_zstd_stash": (StorageName.FILESYSTEM, CodecName.ZSTD, True),
    "get_leveldb_brotli_stash": (StorageName.LEVELDB, CodecName.BROTLI, True),
    "get_leveldb_lzma_stash": (StorageName.LEVELDB, CodecName.LZMA, True),
    "get_leveldb_stash": (StorageName.LEVELDB, CodecName.PASSTHRU, True),
    "get_leveldb_zlib_stash": (StorageName.LEVELDB, CodecName.ZLIB, True),
    "get_leveldb_zstd_stash": (StorageName.LEVELDB, CodecName.ZSTD, True),
    "get_lmdb_brotli_stash": (StorageName.LMDB, CodecName.BROTLI, True),
    "get_lmdb_lzma_stash": (StorageName.LMDB, CodecName.LZMA, True),
    "get_lmdb_stash": (StorageName.LMDB, CodecName.PASSTHRU, True),
    "get_lmdb_zlib_stash": (StorageName.LMDB, CodecName.ZLIB, True),
    "get_lmdb_zstd_stash": (StorageName.LMDB, CodecName.ZSTD, True),
    "get_lsmdb_brotli_stash": (StorageName.LSMDB, CodecName.BROTLI, True),
    "get_lsmdb_lzma_stash": (StorageName.LSMDB, CodecName.LZMA, True),
    "get_lsmdb_stash": (StorageName.LSMDB, CodecName.PASSTHRU, True),
    "get_lsmdb_zlib_stash": (StorageName.LSMDB, CodecName.ZLIB, True),
    "get_lsmdb_zstd_stash": (StorageName.LSMDB, CodecName.ZSTD, True),
    "get_mongo_zlib_stash": (StorageName.MONGODB, CodecName.ZLIB, True),
    "get_null_stash": (StorageName.NULL, CodecName.PASSTHRU, False),
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


def _normalize_storage_name(storage_name: StorageNameInput) -> StorageName:
    if isinstance(storage_name, StorageName):
        return storage_name

    normalized = storage_name.strip().lower()
    if normalized in _STORAGE_ALIASES:
        return _STORAGE_ALIASES[normalized]

    try:
        return StorageName(normalized)
    except ValueError as exc:
        raise ValueError("Unknown storage backend: {}".format(storage_name)) from exc


def _normalize_codec_name(codec_name: Optional[CodecNameInput]) -> Optional[CodecName]:
    if codec_name is None:
        return None

    if isinstance(codec_name, CodecName):
        return codec_name

    normalized = codec_name.strip().lower()
    if normalized == "none":
        return None
    if normalized in _CODEC_ALIASES:
        return _CODEC_ALIASES[normalized]

    try:
        return CodecName(normalized)
    except ValueError as exc:
        raise ValueError("Unknown codec: {}".format(codec_name)) from exc


def _normalize_serializer_name(
    serializer_name: Optional[SerializerNameInput],
) -> Optional[SerializerName]:
    if serializer_name is None:
        return None

    if isinstance(serializer_name, SerializerName):
        return serializer_name

    normalized = serializer_name.strip().lower()
    if normalized == "none":
        return None
    if normalized in _SERIALIZER_ALIASES:
        return _SERIALIZER_ALIASES[normalized]

    try:
        return SerializerName(normalized)
    except ValueError as exc:
        raise ValueError("Unknown serializer: {}".format(serializer_name)) from exc


def _init_cache(
    storage: Storage,
    codec: Optional[Codec],
    options: StashOptions,
    serializer: Optional[Serializer] = None,
) -> StashManager:
    manager_codec = codec or _create_codec(CodecName.PASSTHRU)
    cache_man = StashManager(
        storage=storage,
        codec=manager_codec,
        options=options,
        serializer=serializer,
    )
    return cache_man


def _create_storage(storage_name: StorageNameInput, options: StashOptions) -> Storage:
    normalized_name = _normalize_storage_name(storage_name)
    spec = _STORAGE_REGISTRY[normalized_name]
    storage_class = _load_storage_constructor(spec)
    return storage_class(options=options)


def _create_codec(codec_name: Optional[CodecNameInput]) -> Optional[Codec]:
    normalized_name = _normalize_codec_name(codec_name)
    if normalized_name is None:
        return None

    spec = _CODEC_REGISTRY[normalized_name]
    codec_class = _load_codec_constructor(spec)
    return codec_class()


def _create_serializer(
    serializer_name: Optional[SerializerNameInput],
) -> Optional[Serializer]:
    normalized_name = _normalize_serializer_name(serializer_name)
    if normalized_name is None:
        return None

    spec = _SERIALIZER_REGISTRY[normalized_name]
    serializer_class = _load_serializer_constructor(spec)
    return serializer_class()


def get_stash(
    storage_name: StorageNameInput,
    options: Optional[StashOptions] = None,
    codec_name: Optional[CodecNameInput] = None,
    serializer_name: Optional[SerializerNameInput] = None,
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
    storage_name: StorageName,
    codec_name: Optional[CodecName],
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
            stash = get_stash(StorageName.FILESYSTEM, StashOptions())

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


get_dbm_brotli_stash = _make_compat_helper(StorageName.DBM, CodecName.BROTLI, True)
get_dbm_lzma_stash = _make_compat_helper(StorageName.DBM, CodecName.LZMA, True)
get_dbm_stash = _make_compat_helper(StorageName.DBM, CodecName.PASSTHRU, True)
get_dbm_zlib_stash = _make_compat_helper(StorageName.DBM, CodecName.ZLIB, True)
get_dbm_zstd_stash = _make_compat_helper(StorageName.DBM, CodecName.ZSTD, True)
get_fs_brotli_stash = _make_compat_helper(StorageName.FILESYSTEM, CodecName.BROTLI, True)
get_fs_lzma_stash = _make_compat_helper(StorageName.FILESYSTEM, CodecName.LZMA, True)
get_fs_stash = _make_compat_helper(StorageName.FILESYSTEM, None, True)
get_fs_zlib_stash = _make_compat_helper(StorageName.FILESYSTEM, CodecName.ZLIB, True)
get_fs_zstd_stash = _make_compat_helper(StorageName.FILESYSTEM, CodecName.ZSTD, True)
get_leveldb_brotli_stash = _make_compat_helper(StorageName.LEVELDB, CodecName.BROTLI, True)
get_leveldb_lzma_stash = _make_compat_helper(StorageName.LEVELDB, CodecName.LZMA, True)
get_leveldb_stash = _make_compat_helper(StorageName.LEVELDB, CodecName.PASSTHRU, True)
get_leveldb_zlib_stash = _make_compat_helper(StorageName.LEVELDB, CodecName.ZLIB, True)
get_leveldb_zstd_stash = _make_compat_helper(StorageName.LEVELDB, CodecName.ZSTD, True)
get_lmdb_brotli_stash = _make_compat_helper(StorageName.LMDB, CodecName.BROTLI, True)
get_lmdb_lzma_stash = _make_compat_helper(StorageName.LMDB, CodecName.LZMA, True)
get_lmdb_stash = _make_compat_helper(StorageName.LMDB, CodecName.PASSTHRU, True)
get_lmdb_zlib_stash = _make_compat_helper(StorageName.LMDB, CodecName.ZLIB, True)
get_lmdb_zstd_stash = _make_compat_helper(StorageName.LMDB, CodecName.ZSTD, True)
get_lsmdb_brotli_stash = _make_compat_helper(StorageName.LSMDB, CodecName.BROTLI, True)
get_lsmdb_lzma_stash = _make_compat_helper(StorageName.LSMDB, CodecName.LZMA, True)
get_lsmdb_stash = _make_compat_helper(StorageName.LSMDB, CodecName.PASSTHRU, True)
get_lsmdb_zlib_stash = _make_compat_helper(StorageName.LSMDB, CodecName.ZLIB, True)
get_lsmdb_zstd_stash = _make_compat_helper(StorageName.LSMDB, CodecName.ZSTD, True)
get_mongo_zlib_stash = _make_compat_helper(StorageName.MONGODB, CodecName.ZLIB, True)
get_null_stash = _make_compat_helper(StorageName.NULL, CodecName.PASSTHRU, False)


__all__ = [
    "CodecName",
    "SerializerName",
    "StorageName",
    "get_stash",
    "size_gb",
    "size_kb",
    "size_mb",
    "stashify",
    *_COMPAT_HELPERS.keys(),
]
