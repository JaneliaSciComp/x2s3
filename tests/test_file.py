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
            name='mounted-files',
            client='file',
            options={
                'path':'.',
                'virtual_prefix':'abc123/my-data/'
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


VIRTUAL_PREFIX = 'abc123/my-data/'


def test_virtual_prefix_list(app):
    # Keys are reported under the virtual prefix, which is not on disk
    with TestClient(app) as client:
        response = client.get(f"/mounted-files?list-type=2&prefix={VIRTUAL_PREFIX}tests/&delimiter=/")
        assert response.status_code == 200
        root = parse_xml(response.text)
        assert root.find('Prefix').text == f"{VIRTUAL_PREFIX}tests/"
        keys = [c.find('Key').text for c in root.findall('Contents')]
        prefixes = [cp.find('Prefix').text for cp in root.findall('CommonPrefixes')]
        assert keys and prefixes
        assert f"{VIRTUAL_PREFIX}tests/test_file.py" in keys
        assert f"{VIRTUAL_PREFIX}tests/java/" in prefixes
        assert all(k.startswith(VIRTUAL_PREFIX) for k in keys + prefixes)


def test_virtual_prefix_list_within_prefix(app):
    # A prefix that stops inside the virtual prefix sees only the next segment
    with TestClient(app) as client:
        for prefix, expected in [('', 'abc123/'),
                                 ('abc', 'abc123/'),
                                 ('abc123/', VIRTUAL_PREFIX),
                                 ('abc123/my-', VIRTUAL_PREFIX)]:
            response = client.get(f"/mounted-files?list-type=2&prefix={prefix}&delimiter=/")
            assert response.status_code == 200
            root = parse_xml(response.text)
            assert [cp.find('Prefix').text for cp in root.findall('CommonPrefixes')] == [expected]
            assert root.findall('Contents') == []
            assert root.find('KeyCount').text == '1'


def test_virtual_prefix_list_no_match(app):
    # Anything outside the virtual prefix matches nothing
    with TestClient(app) as client:
        for prefix in ['zzz/', 'abc123/other/', 'tests/']:
            response = client.get(f"/mounted-files?list-type=2&prefix={prefix}&delimiter=/")
            assert response.status_code == 200
            root = parse_xml(response.text)
            assert root.findall('Contents') == []
            assert root.findall('CommonPrefixes') == []
            assert root.find('KeyCount').text == '0'


def test_virtual_prefix_list_recursive(app):
    # Without a delimiter, a prefix inside the virtual prefix lists everything
    with TestClient(app) as client:
        response = client.get("/mounted-files?list-type=2&prefix=abc123/&max-keys=5")
        assert response.status_code == 200
        root = parse_xml(response.text)
        keys = [c.find('Key').text for c in root.findall('Contents')]
        assert len(keys) == 5
        assert all(k.startswith(VIRTUAL_PREFIX) for k in keys)


def test_virtual_prefix_objects(app):
    with TestClient(app) as client:
        response = client.get(f"/mounted-files/{VIRTUAL_PREFIX}README.md")
        assert response.status_code == 200
        assert 'x2s3' in response.text
        assert client.head(f"/mounted-files/{VIRTUAL_PREFIX}README.md").status_code == 200
        # The real path, without the virtual prefix, is not addressable
        response = client.get("/mounted-files/README.md")
        assert response.status_code == 404
        assert parse_xml(response.text).find('Code').text == 'NoSuchKey'
        assert client.head("/mounted-files/README.md").status_code == 404


def test_virtual_prefix_continuation(app):
    # Continuation tokens are reported and accepted in the client's key space
    with TestClient(app) as client:
        url = f"/mounted-files?list-type=2&prefix={VIRTUAL_PREFIX}x2s3/&delimiter=/"
        root = parse_xml(client.get(url).text)
        all_keys = [c.find('Key').text for c in root.findall('Contents')]
        assert len(all_keys) > 3

        paged, token = [], None
        while True:
            page_url = url + "&max-keys=2" + (f"&continuation-token={token}" if token else "")
            root = parse_xml(client.get(page_url).text)
            paged += [c.find('Key').text for c in root.findall('Contents')]
            if root.find('IsTruncated').text != 'true':
                break
            token = root.find('NextContinuationToken').text
            assert token.startswith(VIRTUAL_PREFIX)
        assert paged == all_keys


def test_list_objects_encoding_type_url(app):
    # Clients such as Neuroglancer always ask for encoding-type=url, which used
    # to raise a TypeError because the flag shadowed the encoding function
    with TestClient(app) as client:
        for bucket, prefix in [('local-files', 'tests/'),
                               ('mounted-files', f'{VIRTUAL_PREFIX}tests/')]:
            response = client.get(
                f"/{bucket}?list-type=2&prefix={prefix}&delimiter=/&encoding-type=url")
            assert response.status_code == 200
            root = parse_xml(response.text)
            assert root.find('Prefix').text == prefix
            assert root.find('EncodingType').text == 'url'
            assert f"{prefix}test_file.py" in [c.find('Key').text for c in root.findall('Contents')]
            assert f"{prefix}java/" in [cp.find('Prefix').text for cp in root.findall('CommonPrefixes')]


@pytest.fixture
def spaced_app(tmp_path):
    """An app over a tree whose names need encoding."""
    (tmp_path / "sub dir").mkdir()
    (tmp_path / "my file.txt").write_text("spaced")
    settings = Settings()
    settings.base_url = HttpUrl('http://testserver')
    settings.targets = [
        Target(name='spaced-files', client='file', options={'path': str(tmp_path)})
    ]
    return create_app(settings)


def test_list_objects_encodes_spaces(spaced_app):
    # A space is %20, not '+': a client that reads a key out of a listing and
    # asks for it back has to reach the same file
    with TestClient(spaced_app) as client:
        response = client.get("/spaced-files?list-type=2&delimiter=/&encoding-type=url")
        assert response.status_code == 200
        root = parse_xml(response.text)
        assert [c.find('Key').text for c in root.findall('Contents')] == ['my%20file.txt']
        assert [cp.find('Prefix').text for cp in root.findall('CommonPrefixes')] == ['sub%20dir/']

        response = client.get("/spaced-files/my%20file.txt")
        assert response.status_code == 200
        assert response.text == "spaced"
