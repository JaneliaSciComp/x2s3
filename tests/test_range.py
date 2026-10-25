"""parse_range_header contract: None means ignore the header, ValueError means 416."""

import asyncio

import pytest

from x2s3 import client_file
from x2s3.utils import parse_range_header


@pytest.mark.parametrize("header, expected", [
    ("bytes=0-3", (0, 3)),
    ("bytes=5-", (5, 9)),
    ("bytes=-4", (6, 9)),
    ("bytes=0-99", (0, 9)),
])
def test_satisfiable(header, expected):
    assert parse_range_header(header, 10) == expected


@pytest.mark.parametrize("header", ["", None, "items=0-3", "bytes=abc", "bytes=3", "bytes=0-1,3-4", "bytes=-", "bytes=5-2", "bytes=--3", "bytes=+1-3"])
def test_absent_malformed_and_multi_range_are_none(header):
    assert parse_range_header(header, 10) is None


@pytest.mark.parametrize("header", ["bytes=10-", "bytes=99-200"])
def test_unsatisfiable_raises(header):
    with pytest.raises(ValueError):
        parse_range_header(header, 10)


def test_legacy_client_file_parser_keeps_the_1_6_contract():
    """x2s3.client_file.parse_range_header returned None for unsatisfiable ranges; callers written against it must not see a raise."""
    assert client_file.parse_range_header("bytes=0-3", 10) == (0, 3)
    assert client_file.parse_range_header("bytes=10-", 10) is None
    assert client_file.parse_range_header("bytes=0-1,3-4", 10) is None


@pytest.mark.parametrize("header, status", [
    ("bytes=0-3", 206),
    ("bytes=0-1,3-4", 416),
    ("bytes=abc", 416),
    ("bytes=99-", 416),
    ("bytes=5-2", 416),
])
def test_file_proxy_client_range_behaviour_is_unchanged(tmp_path, header, status):
    """FileProxyClient answered 416 for every range it could not serve in 1.6.0; keep that."""
    (tmp_path / "f.txt").write_bytes(b"0123456789")
    client = client_file.FileProxyClient({"target_name": "t"}, path=str(tmp_path))
    result = asyncio.run(client.open_object("f.txt", header))
    if status == 206:
        assert result.headers["Content-Range"] == "bytes 0-3/10"
        result.close()
    else:
        assert result.status_code == 416
        assert result.headers["Content-Range"] == "bytes */10"
