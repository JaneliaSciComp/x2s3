from email.utils import parsedate_to_datetime

from x2s3.utils import (
    CACHE_CONTROL_PUBLIC,
    check_not_modified,
    format_http_date,
    make_file_etag,
)

ETAG = make_file_etag(1755172800.0, 1234)
LAST_MODIFIED = format_http_date(1755172800.0)
RESPONSE_HEADERS = {
    "ETag": ETAG,
    "Last-Modified": LAST_MODIFIED,
    "Cache-Control": CACHE_CONTROL_PUBLIC,
    "Content-Length": "1234",
    "Content-Type": "application/octet-stream",
}


def test_format_http_date_is_rfc7231():
    # Browsers and shared caches can only use Last-Modified if it parses as an
    # HTTP-date; the S3 ISO format does not.
    assert LAST_MODIFIED.endswith("GMT")
    assert parsedate_to_datetime(LAST_MODIFIED) is not None


def test_make_file_etag_is_quoted_and_varies():
    assert ETAG.startswith('"') and ETAG.endswith('"')
    assert make_file_etag(1755172800.0, 1234) != make_file_etag(1755172800.0, 1235)
    assert make_file_etag(1755172800.0, 1234) != make_file_etag(1755172801.0, 1234)


def test_no_validators_means_no_304():
    assert check_not_modified({}, RESPONSE_HEADERS) is None


def test_matching_if_none_match_returns_304():
    response = check_not_modified({"if-none-match": ETAG}, RESPONSE_HEADERS)
    assert response is not None
    assert response.status_code == 304


def test_304_carries_validators_but_no_content_headers():
    response = check_not_modified({"if-none-match": ETAG}, RESPONSE_HEADERS)
    assert response.headers["etag"] == ETAG
    assert response.headers["last-modified"] == LAST_MODIFIED
    assert response.headers["cache-control"] == CACHE_CONTROL_PUBLIC
    assert "content-length" not in response.headers
    assert "content-type" not in response.headers


def test_stale_if_none_match_returns_none():
    assert check_not_modified({"if-none-match": '"nope-1"'}, RESPONSE_HEADERS) is None


def test_star_if_none_match_returns_304():
    response = check_not_modified({"if-none-match": "*"}, RESPONSE_HEADERS)
    assert response is not None


def test_weak_and_list_if_none_match_match():
    response = check_not_modified({"if-none-match": f'"other", W/{ETAG}'}, RESPONSE_HEADERS)
    assert response is not None


def test_if_none_match_takes_precedence_over_if_modified_since():
    # A non-matching ETag means not-modified is false, even though the
    # If-Modified-Since alone would have produced a 304.
    headers = {"if-none-match": '"nope-1"', "if-modified-since": LAST_MODIFIED}
    assert check_not_modified(headers, RESPONSE_HEADERS) is None


def test_if_modified_since_at_or_after_mtime_returns_304():
    assert check_not_modified({"if-modified-since": LAST_MODIFIED}, RESPONSE_HEADERS) is not None
    later = format_http_date(1755172800.0 + 60)
    assert check_not_modified({"if-modified-since": later}, RESPONSE_HEADERS) is not None


def test_if_modified_since_before_mtime_returns_none():
    earlier = format_http_date(1755172800.0 - 60)
    assert check_not_modified({"if-modified-since": earlier}, RESPONSE_HEADERS) is None


def test_unparseable_if_modified_since_returns_none():
    assert check_not_modified({"if-modified-since": "not a date"}, RESPONSE_HEADERS) is None


def test_response_header_lookup_is_case_insensitive():
    # Fileglancer passes a plain dict whose keys came from x2s3, so the helper
    # cannot rely on Starlette's case-insensitive Headers mapping.
    lowered = {k.lower(): v for k, v in RESPONSE_HEADERS.items()}
    assert check_not_modified({"if-none-match": ETAG}, lowered) is not None


def test_missing_etag_never_matches():
    assert check_not_modified({"if-none-match": ETAG}, {"Cache-Control": CACHE_CONTROL_PUBLIC}) is None
