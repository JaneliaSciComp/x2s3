import urllib.parse

import pytest
from fastapi.testclient import TestClient
from pydantic import HttpUrl
from loguru import logger

from xml.etree.ElementTree import Element
from x2s3.app import create_app
from x2s3.settings import Target, Settings
from x2s3.utils import parse_xml

@pytest.fixture
def get_settings():
    settings = Settings()
    settings.base_url = HttpUrl('http://testserver')
    settings.virtual_buckets = True
    settings.targets = [
        Target(
            name='local-files',
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
        response = client.get("/")
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
        response = client.get(f"/{bucket_name}?list-type=2&max-keys={max_keys}")
        assert response.status_code == 200
        root = parse_xml(response.text)
        assert root.tag == "ListBucketResult"
        assert root.find('Name').text == bucket_name


def test_list_objects_delimiter(app):
    with TestClient(app) as client:
        bucket_name = 'local-files'
        max_keys = 1000
        response = client.get(f"/{bucket_name}?list-type=2&delimiter=/&max-keys={max_keys}")
        assert response.status_code == 200
        root = parse_xml(response.text)
        assert root.tag == "ListBucketResult"
        assert root.find('Name').text == bucket_name
        assert root.find('Delimiter').text == '/'
        assert len(root.findall('CommonPrefixes')) > 1
        assert root.find('IsTruncated').text == "false"


def test_head_object(app):
    with TestClient(app) as client:
        response = client.head("/local-files/requirements.txt")
        assert response.status_code == 200
        response = client.head("/local-files/x2s3/")
        assert response.status_code == 404


def test_get_object(app):
    with TestClient(app) as client:
        response = client.get("/local-files/requirements.txt")
        assert response.status_code == 200
        assert response.headers['content-type'].startswith("text/plain")
        assert 'aiobotocore' in response.text

def test_get_object_missing(app):
    with TestClient(app) as client:
        response = client.get("/local-files/missing")
        assert response.status_code == 404
        assert response.headers['content-type'] == "application/xml"
        root = parse_xml(response.text)
        assert root.find('Code').text == 'NoSuchKey'

def test_prefixed_list_objects(app):
    with TestClient(app) as client:
        bucket_name = 'with-prefix'
        response = client.get(f"/{bucket_name}?list-type=2&delimiter=/")
        assert response.status_code == 200
        root = parse_xml(response.text)
        assert root.tag == "ListBucketResult"
        assert root.find('Name').text == bucket_name
        assert root.find('Delimiter').text == '/'
        assert len(root.findall('CommonPrefixes')) == 2
        assert root.find('IsTruncated').text == "false"