import pytest

from stash.options import StashOptions
from stash.utils.checksum import calcsum


def test_calcsum_raises_on_unknown_algorithm():
    with pytest.raises(ValueError):
        calcsum("payload", "unknown")


def test_options_user_values_override_defaults():
    options = StashOptions({"algo": "md5", "cache_max_age": 99})

    assert options.algo == "md5"
    assert options.cache_max_age == 99
