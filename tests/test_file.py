import urllib.parse

import pytest
from fastapi.testclient import TestClient
from pydantic import HttpUrl

from x2s3.app import create_app
from x2s3.client_file import STATIC_ETAG
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
            name='local-files-with-etags',
            client='file',
            options={
                'path':'.',
                'calculate_etags':'true'
            }
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
            assert etag==STATIC_ETAG


def test_list_objects_with_etags(app):
    with TestClient(app) as client:
        bucket_name = 'local-files-with-etags'
        response = client.get(f"/{bucket_name}?list-type=2&prefix=tests/")
        assert response.status_code == 200
        root = parse_xml(response.text)
        assert root.tag == "ListBucketResult"
        assert root.find('Name').text == bucket_name
        for content in root.findall('Contents'):
            etag = content.find('ETag').text
            assert etag.startswith('"')
            assert etag!=STATIC_ETAG


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
