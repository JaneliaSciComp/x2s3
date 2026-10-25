"""parse_range_header contract: None means ignore the header, ValueError means 416."""

import pytest

from x2s3.utils import parse_range_header


@pytest.mark.parametrize("header, expected", [
    ("bytes=0-3", (0, 3)),
    ("bytes=5-", (5, 9)),
    ("bytes=-4", (6, 9)),
    ("bytes=0-99", (0, 9)),
])
def test_satisfiable(header, expected):
    assert parse_range_header(header, 10) == expected


@pytest.mark.parametrize("header", ["", None, "items=0-3", "bytes=abc", "bytes=3", "bytes=0-1,3-4", "bytes=-"])
def test_absent_malformed_and_multi_range_are_none(header):
    assert parse_range_header(header, 10) is None


@pytest.mark.parametrize("header", ["bytes=10-", "bytes=99-200", "bytes=5-2"])
def test_unsatisfiable_raises(header):
    with pytest.raises(ValueError):
        parse_range_header(header, 10)


def test_still_importable_from_client_file():
    from x2s3.client_file import parse_range_header as legacy
    assert legacy is parse_range_header
