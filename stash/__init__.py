from typing import List

import stash.consts
from stash._helpers import *
from stash.codecs.brotli import BrotliCodec
from stash.codecs.bzip2 import BZip2Codec
from stash.codecs.codec import Codec
from stash.codecs.gzip import GZipCodec
from stash.codecs.lz4 import Lz4Codec
from stash.codecs.lzf import LzfCodec
from stash.codecs.lzma import LzmaCodec
from stash.codecs.lzo import LzoCodec
from stash.codecs.passthru import PassthruCodec
from stash.codecs.snappy import SnappyCodec
from stash.codecs.zlib import ZlibCodec
from stash.codecs.zopfli import ZopfliCodec
from stash.codecs.zstd import ZstdCodec
from stash.manager import StashManager
from stash.serializers.bson import BSONSerializer
from stash.serializers.cbor import CBORSerializer
from stash.serializers.default import DefaultSerializer
from stash.serializers.json import JSONSerializer
from stash.serializers.msgpack import MsgPackSerializer
from stash.serializers.orjson import OrJSONSerializer
from stash.serializers.rapidjson import RapidJSONSerializer
from stash.serializers.serializer import Serializer
from stash.serializers.serializer import Serializer
from stash.serializers.simplejson import SimpleJSONSerializer
from stash.serializers.ujson import UltraJSONSerializer
from stash.storages.dbm import DbmStorage
from stash.storages.filesystem import FilesystemStorage
from stash.storages.leveldb import LeveldbStorage
from stash.storages.lm_db import LmdbStorage
from stash.storages.memory import MemoryStorage
from stash.storages.mongodb import MongoDbStorage
from stash.storages.null import NullStorage
from stash.storages.redis import RedisStorage
from stash.storages.storage import Storage

__name__: str = consts.PROJECT
__author__: str = "Masroor Ehsan"
__email__: str = "masroore@gmail.com"
__version__: str = ".".join(map(str, consts.VERSION))

__all__: List[str] = [
    "StashManager",
    # serializers
    "Serializer",
    "DefaultSerializer",
    "BSONSerializer",
    "CBORSerializer",
    "JSONSerializer",
    "MsgPackSerializer",
    "OrJSONSerializer",
    "RapidJSONSerializer",
    "SimpleJSONSerializer",
    "UltraJSONSerializer",
    # codecs
    "Codec",
    "PassthruCodec",
    "BrotliCodec",
    "BZip2Codec",
    "GZipCodec",
    "Lz4Codec",
    "LzfCodec",
    "LzmaCodec",
    "LzoCodec",
    "SnappyCodec",
    "ZlibCodec",
    "ZopfliCodec",
    "ZstdCodec",
    # storages
    "Storage",
    "FilesystemStorage",
    "LmdbStorage",
    "LeveldbStorage",
    "MemoryStorage",
    "NullStorage",
    "MongoDbStorage",
    "RedisStorage",
    "DbmStorage",
]
