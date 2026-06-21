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
