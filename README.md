# stashlib

stash is an extensible and lightweight cache library for python.

## Installation

Install the core package:

```bash
python -m pip install stashlib
```

Install optional backends, codecs, serializers, and checksum helpers as needed:

```bash
python -m pip install "stashlib[lmdb,zstd]"
python -m pip install "stashlib[redis,orjson]"
python -m pip install "stashlib[all]"
```

Available extras:

- Storage backends: `lmdb`, `leveldb`, `lsmdb`, `mongodb`, `redis`
- Codecs: `brotli`, `lz4`, `lzf`, `lzo`, `snappy`, `zopfli`, `zstd`
- Serializers: `bson`, `cbor`, `msgpack`, `orjson`, `rapidjson`, `simplejson`, `ujson`
- Checksum helpers: `checksum`
- Development tooling: `dev`

## Quick Start

Use the default filesystem-backed stash:

```python
from stash import StashOptions, get_fs_stash

stash = get_fs_stash(StashOptions({"algo": "md5"}))
stash.write("greeting", {"message": "hello"})

assert stash.read("greeting") == {"message": "hello"}
```

Use a context manager to ensure resources are closed for storages that keep handles open:

```python
from stash import StashManager, StashOptions
from stash.codecs.passthru import PassthruCodec
from stash.storages.memory import MemoryStorage

with StashManager(
    storage=MemoryStorage(StashOptions({"algo": "md5"})),
    codec=PassthruCodec(),
    options=StashOptions({"algo": "md5"}),
) as stash:
    stash.write("key", [1, 2, 3])
    assert stash.read("key") == [1, 2, 3]
```

## Architecture

stash composes three layers:

1. Storage backends store bytes.
2. Codecs compress or transform bytes.
3. Serializers convert Python objects to and from bytes.

The `StashManager` pipeline is:

1. Hash cache key.
2. Serialize content.
3. Encode serialized bytes with the configured codec.
4. Persist the result in the configured storage backend.

Reads reverse that process.

## Development

Install development tooling and run tests:

```bash
python -m pip install -e ".[dev]"
pytest -q
```
