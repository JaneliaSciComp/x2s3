import urllib.parse

import pytest
from fastapi.testclient import TestClient
from pydantic import HttpUrl

from x2s3.app import create_app
from x2s3.settings import Target, Settings
from x2s3.utils import parse_xml

@pytest.fixture
def get_settings():
    settings = Settings()
    settings.base_url = HttpUrl('http://testserver')
    settings.targets = [
        Target(
            name='local-files',
            client='file',
            options={'path':'.'}
        ),
        Target(
            name='hidden-files',
            browseable=False,
            client='file',
            options={'path':'.'}
        )
    ]
    return settings


@pytest.fixture
def app(get_settings):
    return create_app(get_settings)

    
def test_get_html_root(app):
    with TestClient(app) as client:
        response = client.get("/", headers={"Accept": "text/html"})
        assert response.status_code == 200
        assert response.headers['content-type'].startswith("text/html")
        for target in app.settings.targets:
            if target.browseable:
                assert target.name in response.text
            else:
                assert target.name not in response.text


def test_vary_accept_header(app):
    # HTML vs XML is negotiated on Accept, so caches must key on it
    with TestClient(app) as client:
        for path in ["/", "/local-files/"]:
            for accept in ["text/html", "application/xml"]:
                response = client.get(path, headers={"Accept": accept})
                assert response.status_code == 200
                assert response.headers['vary'] == "Accept"
                assert response.headers['content-type'].startswith(accept)


def test_list_objects(app):
    with TestClient(app) as client:
        bucket_name = 'local-files'
        max_keys = 5
        response = client.get(f"/{bucket_name}?list-type=2&prefix=tests/&max-keys={max_keys}")
        assert response.status_code == 200
        root = parse_xml(response.text)
        assert root.tag == "ListBucketResult"
        assert root.find('Name').text == bucket_name
        contents = root.findall('Contents')
        assert len(contents) == max_keys
        for content in contents:
            etag = content.find('ETag').text
            assert etag.startswith('"')
            # mtime-size, not a constant: see make_file_etag
            assert etag != '"11111111111111111111111111111111"'


def test_list_objects_delimiter(app):
    with TestClient(app) as client:
        bucket_name = 'local-files'
        response = client.get(f"/{bucket_name}?list-type=2&delimiter=/")
        assert response.status_code == 200
        root = parse_xml(response.text)
        assert root.tag == "ListBucketResult"
        assert root.find('Name').text == bucket_name
        assert root.find('Delimiter').text == '/'
        assert len(root.findall('CommonPrefixes')) > 1
        assert root.find('IsTruncated').text == "false"


def test_head_object(app):
    with TestClient(app) as client:
        response = client.head("/local-files/README.md")
        assert response.status_code == 200
        response = client.head("/local-files/x2s3/")
        assert response.status_code == 404


def test_head_root_index(app):
    # The root index exists for GET, so HEAD must not report NoSuchBucket
    with TestClient(app) as client:
        for accept in ["text/html", "application/xml"]:
            head_response = client.head("/", headers={"Accept": accept})
            assert head_response.status_code == 200
            assert head_response.headers['content-type'].startswith(accept)
            assert head_response.headers['vary'] == "Accept"
            # RFC 9110: HEAD sends the header fields GET would have sent
            get_response = client.get("/", headers={"Accept": accept})
            assert get_response.status_code == head_response.status_code
            assert get_response.headers['content-type'] == head_response.headers['content-type']


def test_head_bucket_root_index(app):
    # GET /{bucket}/ negotiates browse HTML vs XML listing; HEAD must match
    with TestClient(app) as client:
        for accept in ["text/html", "application/xml"]:
            head_response = client.head("/local-files/", headers={"Accept": accept})
            assert head_response.status_code == 200
            assert head_response.headers['content-type'].startswith(accept)
            assert head_response.headers['vary'] == "Accept"
            get_response = client.get("/local-files/", headers={"Accept": accept})
            assert get_response.status_code == head_response.status_code
            assert get_response.headers['content-type'] == head_response.headers['content-type']


def test_get_object(app):
    with TestClient(app) as client:
        response = client.get("/local-files/README.md")
        assert response.status_code == 200
        assert response.headers['content-type'].startswith("text/markdown")
        assert 'x2s3' in response.text


def test_get_object_missing(app):
    with TestClient(app) as client:
        response = client.get("/local-files/missing")
        assert response.status_code == 404
        assert response.headers['content-type'] == "application/xml"
        root = parse_xml(response.text)
        assert root.find('Code').text == 'NoSuchKey'


def _assert_valid_request_id(response):
    request_id = response.headers.get('x-amz-request-id')
    assert request_id, "missing x-amz-request-id header"
    assert len(request_id) == 16
    assert request_id.isalnum() and request_id.upper() == request_id


def test_request_id_header_present(app):
    """Every response should carry an S3-style x-amz-request-id header."""
    with TestClient(app) as client:
        _assert_valid_request_id(client.get("/", headers={"Accept": "text/html"}))
        _assert_valid_request_id(client.get("/local-files?list-type=2&prefix=tests/"))
        _assert_valid_request_id(client.head("/local-files/README.md"))
        _assert_valid_request_id(client.get("/local-files/README.md"))
        # Also present on error responses
        _assert_valid_request_id(client.get("/local-files/missing"))


def test_request_id_header_unique(app):
    """Each request should get a distinct x-amz-request-id."""
    with TestClient(app) as client:
        first = client.get("/local-files/README.md").headers['x-amz-request-id']
        second = client.get("/local-files/README.md").headers['x-amz-request-id']
        assert first != second


def test_unbrowseable_list_denied(app):
    with TestClient(app) as client:
        response = client.get("/hidden-files?list-type=2")
        assert response.status_code == 403
        assert response.headers['content-type'] == "application/xml"
        root = parse_xml(response.text)
        assert root.find('Code').text == 'AccessDenied'
        # Deny takes precedence over list-type validation
        response = client.get("/hidden-files?list-type=1")
        assert response.status_code == 403


def test_unbrowseable_hidden_from_xml_root(app):
    with TestClient(app) as client:
        response = client.get("/")
        assert response.status_code == 200
        assert response.headers['content-type'].startswith("application/xml")
        assert 'local-files' in response.text
        assert 'hidden-files' not in response.text


def test_unbrowseable_browse_denied(app):
    with TestClient(app) as client:
        # HTML browse UI on bucket root and on a subdirectory
        response = client.get("/hidden-files/", headers={"Accept": "text/html"})
        assert response.status_code == 403
        response = client.get("/hidden-files/tests/", headers={"Accept": "text/html"})
        assert response.status_code == 403
        # Trailing-slash XML listing (no HTML preference)
        response = client.get("/hidden-files/tests/")
        assert response.status_code == 403
        root = parse_xml(response.text)
        assert root.find('Code').text == 'AccessDenied'


def test_unbrowseable_acl_denied(app):
    with TestClient(app) as client:
        # GetBucketAcl
        response = client.get("/hidden-files?acl")
        assert response.status_code == 403
        # GetObjectAcl
        response = client.get("/hidden-files/README.md?acl")
        assert response.status_code == 403


def test_unbrowseable_get_object_allowed(app):
    with TestClient(app) as client:
        response = client.get("/hidden-files/README.md")
        assert response.status_code == 200
        assert 'x2s3' in response.text
        # GetObject takes precedence over list-type when a key is present
        response = client.get("/hidden-files/README.md?list-type=2")
        assert response.status_code == 200


def test_unbrowseable_get_missing_masked(app):
    with TestClient(app) as client:
        # Missing key returns 403 (not 404) so key existence can't be probed,
        # matching real S3 behavior when s3:ListBucket is denied
        response = client.get("/hidden-files/missing")
        assert response.status_code == 403
        root = parse_xml(response.text)
        assert root.find('Code').text == 'AccessDenied'
        # Same masking on the list-type+key (GetObject precedence) path
        response = client.get("/hidden-files/missing?list-type=2")
        assert response.status_code == 403


def test_unbrowseable_head(app):
    with TestClient(app) as client:
        # Existing key: works
        response = client.head("/hidden-files/README.md")
        assert response.status_code == 200
        # Missing key: masked as 403
        response = client.head("/hidden-files/missing")
        assert response.status_code == 403
        # HeadBucket: real S3 requires s3:ListBucket, so deny
        response = client.head("/hidden-files")
        assert response.status_code == 403
        # Browseable bucket unaffected
        response = client.head("/local-files")
        assert response.status_code == 200
        response = client.head("/local-files/missing")
        assert response.status_code == 404


def test_file_get_has_caching_headers(app):
    with TestClient(app) as client:
        response = client.get("/local-files/README.md")
        assert response.status_code == 200
        assert response.headers['cache-control'] == "public, max-age=3600"
        assert response.headers['etag'].startswith('"')


def test_file_head_has_caching_headers(app):
    with TestClient(app) as client:
        response = client.head("/local-files/README.md")
        assert response.status_code == 200
        assert response.headers['cache-control'] == "public, max-age=3600"
        assert response.headers['etag'] == client.get("/local-files/README.md").headers['etag']


def test_file_last_modified_is_http_date(app):
    # The S3 ISO format is for listing XML; a header must be an HTTP-date or
    # browsers and shared caches cannot use it.
    from email.utils import parsedate_to_datetime
    with TestClient(app) as client:
        response = client.head("/local-files/README.md")
        assert response.headers['last-modified'].endswith("GMT")
        assert parsedate_to_datetime(response.headers['last-modified']) is not None


def test_file_ranged_response_has_caching_headers(app):
    with TestClient(app) as client:
        response = client.get("/local-files/README.md", headers={"Range": "bytes=0-9"})
        assert response.status_code == 206
        assert response.headers['cache-control'] == "public, max-age=3600"
        assert response.headers['etag'].startswith('"')


def test_listing_keeps_s3_iso_timestamps(app):
    # Only the header format changes; the XML body still speaks S3.
    with TestClient(app) as client:
        response = client.get("/local-files?list-type=2&prefix=tests/&max-keys=1")
        root = parse_xml(response.text)
        last_modified = root.find('Contents').find('LastModified').text
        assert last_modified.endswith("Z")
        assert "GMT" not in last_modified


def test_get_returns_304_for_matching_etag(app):
    with TestClient(app) as client:
        first = client.get("/local-files/README.md")
        assert first.status_code == 200
        second = client.get("/local-files/README.md",
                            headers={"If-None-Match": first.headers['etag']})
        assert second.status_code == 304
        assert second.content == b""
        assert second.headers['cache-control'] == "public, max-age=3600"


def test_get_returns_200_for_stale_etag(app):
    with TestClient(app) as client:
        response = client.get("/local-files/README.md",
                              headers={"If-None-Match": '"stale-1"'})
        assert response.status_code == 200
        assert response.content


def test_get_returns_304_for_if_modified_since(app):
    with TestClient(app) as client:
        first = client.get("/local-files/README.md")
        second = client.get("/local-files/README.md",
                            headers={"If-Modified-Since": first.headers['last-modified']})
        assert second.status_code == 304


def test_if_none_match_beats_range(app):
    # RFC 9110 13.1.3: a matching If-None-Match wins over Range, so this is a
    # 304 rather than a 206.
    with TestClient(app) as client:
        first = client.get("/local-files/README.md")
        second = client.get("/local-files/README.md",
                            headers={"If-None-Match": first.headers['etag'],
                                     "Range": "bytes=0-9"})
        assert second.status_code == 304


def test_head_returns_304_for_matching_etag(app):
    with TestClient(app) as client:
        first = client.head("/local-files/README.md")
        second = client.head("/local-files/README.md",
                             headers={"If-None-Match": first.headers['etag']})
        assert second.status_code == 304


def test_304_does_not_leak_file_handles(app, monkeypatch):
    # The handle is opened before the validator check, so the 304 path has to
    # close it explicitly. Neither a ResourceWarning trap nor an open-fd count
    # can observe this reliably: CPython's refcounting deallocates the
    # underlying file object (running its own closing finalizer) the instant
    # the local `handle` variable in get_object_or_denied goes out of scope,
    # before either detector gets a chance to look. So this spies directly on
    # FileObjectHandle.close() to confirm the 304 path actually calls it.
    from x2s3.client_file import FileObjectHandle

    calls = []
    original_close = FileObjectHandle.close

    def spy_close(self):
        calls.append(self)
        original_close(self)

    monkeypatch.setattr(FileObjectHandle, "close", spy_close)

    with TestClient(app) as client:
        # The initial 200 GET streams its own handle closed when the response
        # finishes, which also calls close() — so only count closes seen
        # during the 304 loop below, not this warm-up call.
        etag = client.get("/local-files/README.md").headers['etag']
        before = len(calls)
        for _ in range(3):
            assert client.get("/local-files/README.md",
                              headers={"If-None-Match": etag}).status_code == 304

    assert len(calls) - before == 3


def test_304_not_returned_for_missing_key(app):
    with TestClient(app) as client:
        response = client.get("/local-files/does-not-exist.txt",
                              headers={"If-None-Match": "*"})
        assert response.status_code == 404


def test_416_response_is_not_cacheable(app):
    # An explicitly cacheable 416 could be replayed by a shared cache for a
    # later plain GET (caches key on URI+method, not Range), making the
    # object look permanently broken.
    with TestClient(app) as client:
        response = client.get("/local-files/README.md",
                              headers={"Range": "bytes=99999999-100000000"})
        assert response.status_code == 416
        assert 'cache-control' not in response.headers
        assert 'etag' not in response.headers


def test_if_none_match_beats_unsatisfiable_range(app):
    # RFC 9110 13.2.2 evaluates If-None-Match before Range, so a matching
    # validator yields 304 even when the Range is unsatisfiable.
    with TestClient(app) as client:
        first = client.get("/local-files/README.md")
        second = client.get("/local-files/README.md",
                            headers={"If-None-Match": first.headers['etag'],
                                     "Range": "bytes=99999999-100000000"})
        assert second.status_code == 304


def test_if_range_stale_with_unsatisfiable_range_returns_full_body(app):
    # RFC 9110 13.1.5: a stale If-Range means the Range is ignored entirely,
    # including one that would otherwise be unsatisfiable.
    with TestClient(app) as client:
        full = client.get("/local-files/README.md")
        response = client.get("/local-files/README.md",
                              headers={"Range": "bytes=99999999-100000000",
                                       "If-Range": '"stale-etag"'})
        assert response.status_code == 200
        assert response.content == full.content


def test_unsatisfiable_range_with_fresh_if_range_still_416(app):
    with TestClient(app) as client:
        full = client.get("/local-files/README.md")
        response = client.get("/local-files/README.md",
                              headers={"Range": "bytes=99999999-100000000",
                                       "If-Range": full.headers['etag']})
        assert response.status_code == 416


def test_unsatisfiable_range_with_stale_if_none_match_still_416(app):
    with TestClient(app) as client:
        response = client.get("/local-files/README.md",
                              headers={"Range": "bytes=99999999-100000000",
                                       "If-None-Match": '"stale-1"'})
        assert response.status_code == 416


def test_if_range_matching_etag_returns_ranged_body(app):
    with TestClient(app) as client:
        full = client.get("/local-files/README.md")
        response = client.get("/local-files/README.md",
                              headers={"Range": "bytes=0-9",
                                       "If-Range": full.headers['etag']})
        assert response.status_code == 206
        assert response.content == full.content[:10]


def test_if_range_stale_etag_returns_full_body(app):
    with TestClient(app) as client:
        full = client.get("/local-files/README.md")
        response = client.get("/local-files/README.md",
                              headers={"Range": "bytes=0-9",
                                       "If-Range": '"stale-etag"'})
        assert response.status_code == 200
        assert response.content == full.content


def test_if_range_without_range_is_ignored(app):
    with TestClient(app) as client:
        full = client.get("/local-files/README.md")
        response = client.get("/local-files/README.md",
                              headers={"If-Range": '"stale-etag"'})
        assert response.status_code == 200
        assert response.content == full.content


def test_if_range_matching_last_modified_returns_ranged_body(app):
    with TestClient(app) as client:
        full = client.get("/local-files/README.md")
        response = client.get("/local-files/README.md",
                              headers={"Range": "bytes=0-9",
                                       "If-Range": full.headers['last-modified']})
        assert response.status_code == 206
        assert response.content == full.content[:10]


def test_head_and_get_agree_on_caching_headers(app):
    # The validators are built separately in head_object and open_object. If
    # they ever drift, a client revalidating against the pair gets a full body
    # forever, and nothing else in the suite would notice.
    with TestClient(app) as client:
        head = client.head("/local-files/README.md")
        get = client.get("/local-files/README.md")
        for header in ("etag", "last-modified", "cache-control"):
            assert head.headers[header] == get.headers[header], header


def test_listing_etag_matches_get_etag(app):
    # A client that caches an object it found in a listing must be able to
    # revalidate with that listing's ETag. A constant ETag guaranteed a miss.
    with TestClient(app) as client:
        listing = client.get("/local-files?list-type=2&prefix=tests/&max-keys=1")
        entry = parse_xml(listing.text).find('Contents')
        key, listed = entry.find('Key').text, entry.find('ETag').text

        assert client.get(f"/local-files/{key}").headers['etag'] == listed
        assert client.get(f"/local-files/{key}",
                          headers={"If-None-Match": listed}).status_code == 304
