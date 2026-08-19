from email.utils import parsedate_to_datetime

from x2s3.utils import (
    CACHE_CONTROL_PUBLIC,
    check_not_modified,
    format_http_date,
    if_range_matches,
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


def test_naive_if_modified_since_does_not_crash():
    # parsedate_to_datetime returns a naive datetime for a zoneless date, and
    # comparing it to our timezone-aware one raises TypeError.
    assert check_not_modified({"if-modified-since": "Thu, 14 Aug 2026 12:00:00"},
                              RESPONSE_HEADERS) is None


def test_make_file_etag_wire_format_is_pinned():
    # Fileglancer's make_etag must produce this byte-for-byte for the same file.
    assert make_file_etag(1755172800.0, 1234) == '"1755172800.000000-1234"'


def test_if_range_matches_etag():
    assert if_range_matches(ETAG, RESPONSE_HEADERS) is True


def test_if_range_matches_last_modified():
    assert if_range_matches(LAST_MODIFIED, RESPONSE_HEADERS) is True


def test_if_range_stale_validator_does_not_match():
    assert if_range_matches('"stale-etag"', RESPONSE_HEADERS) is False


def test_if_range_weak_etag_never_matches():
    # RFC 9110 13.1.5: a weak validator is never valid in If-Range, even if
    # its underlying tag matches -- so this is a plain string comparison,
    # not the weak-stripping If-None-Match does.
    assert if_range_matches(f"W/{ETAG}", RESPONSE_HEADERS) is False


def test_star_if_none_match_matches_even_without_etag():
    # RFC 9110 13.1.2: '*' matches any existing representation, so it must
    # yield a 304 even when the response carries no ETag — the default for
    # S3 targets, which ship with proxy_etag=False.
    response = check_not_modified({"if-none-match": "*"},
                                  {"Last-Modified": LAST_MODIFIED})
    assert response is not None and response.status_code == 304
