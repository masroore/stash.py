from typing import Tuple

PROJECT: str = "stash"
VERSION: Tuple[int, int, int] = (0, 0, 6)

CHECKSUM_ALGO_MD5 = "md5"
CHECKSUM_ALGO_SHA1 = "sha1"
CHECKSUM_ALGO_XXH32 = "xxh32"
CHECKSUM_ALGO_XXH64 = "xxh64"
CHECKSUM_ALGO_MURMUR = "murmur"

SIZE_KB = 1024
SIZE_MB = SIZE_KB * 1024
SIZE_GB = SIZE_MB * 1024

SECONDS_IN_DAY = 86400
SECONDS_IN_HOUR = 3600
SECONDS_IN_MINUTE = 60
