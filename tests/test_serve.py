from typing import Annotated
import urllib.parse

from fastapi import FastAPI, Header, HTTPException
from fastapi.testclient import TestClient
from pydantic import BaseModel
from pydantic import HttpUrl
from loguru import logger

from xml.etree.ElementTree import Element
from jproxy.serve import app
from jproxy.settings import S3LikeTarget, get_settings
from jproxy.utils import parse_xml

settings = get_settings()
settings.base_url = HttpUrl('http://testserver')
settings.targets = [
    S3LikeTarget(
        name='janelia-data-examples',
        bucket='janelia-data-examples'
    ),
    S3LikeTarget(
        name='with-prefix',
        bucket='janelia-data-examples',
        prefix='jrc_mus_lung_covid.n5/'
    ),
    S3LikeTarget(
        name='hidden-with-endpoint',
        endpoint='https://s3.amazonaws.com',
        bucket='janelia-data-examples',
        hidden=True
    )
]
settings.target_map = {t.name.lower(): t for t in settings.targets}


def test_acl():
    with TestClient(app) as client:
        response = client.get(f"/janelia-data-examples?acl")
        assert response.status_code == 200
        assert response.headers['content-type'] == "application/xml"


def test_get_html_root():
    with TestClient(app) as client:
        response = client.get("/")
        assert response.status_code == 200
        assert response.headers['content-type'].startswith("text/html")
        for target in settings.targets:
            if target.hidden:
                assert target.name not in response.text
            else:
                assert target.name in response.text


def test_get_html_listing():
    with TestClient(app) as client:
        response = client.get("/janelia-data-examples/jrc_mus_lung_covid.n5/")
        assert response.status_code == 200
        assert response.headers['content-type'].startswith("text/html")
        assert '<html>' in response.text
        assert '<head>' in response.text
        assert 'attributes.json' in response.text


def test_list_objects():
    with TestClient(app) as client:
        bucket_name = 'janelia-data-examples'
        max_keys = 7
        response = client.get(f"/{bucket_name}?list-type=2&max-keys={max_keys}")
        assert response.status_code == 200
        root = parse_xml(response.text)
        assert root.tag == "ListBucketResult"
        assert root.find('Name').text == bucket_name
        assert len(root.findall('CommonPrefixes')) == 1

        contents = root.findall('Contents')
        assert len(contents) <= max_keys
    
        if root.find('NextContinuationToken') is not None:
            assert root.find('IsTruncated').text == "true"

        assert isinstance(contents[0].find('Key'), Element)
        assert isinstance(contents[0].find('Size'), Element)
        assert isinstance(contents[0].find('LastModified'), Element)


def test_list_objects_delimiter():
    with TestClient(app) as client:
        bucket_name = 'janelia-data-examples'
        max_keys = 9
        response = client.get(f"/{bucket_name}?list-type=2&delimiter=/&max-keys={max_keys}")
        assert response.status_code == 200
        root = parse_xml(response.text)
        assert root.tag == "ListBucketResult"
        assert root.find('Name').text == bucket_name
        assert root.find('Delimiter').text == '/'
        assert len(root.findall('CommonPrefixes')) == 1
        assert root.find('IsTruncated').text == "false"
    

def test_list_objects_continuation():
    with TestClient(app) as client:
        bucket_name = 'janelia-data-examples'
        max_keys = 4
        uri = f"/{bucket_name}?list-type=2&max-keys={max_keys}&prefix=jrc_mus_lung_covid.n5/render/v1_acquire_align___20210609_224836/s0/0/0"
        
        token_param = ''
        total = 0
        c = 0
        while True:
            url = f"{uri}{token_param}"
            print(f"Fetching {url}")
            response = client.get(url)
            assert response.status_code == 200
            root = parse_xml(response.text)

            assert root.tag == "ListBucketResult"
            assert root.find('Name').text == bucket_name
            assert root.find('MaxKeys').text == str(max_keys)
            assert len(root.findall('CommonPrefixes')) == 1

            contents = root.findall('Contents')
            print(f"Got {len(contents)} results")
            total += len(contents)
            assert len(contents) <= max_keys

            next_token_elem = root.find('NextContinuationToken')
            if next_token_elem is not None:
                assert root.find('IsTruncated').text == "true"
            else:
                break

            safe_token = urllib.parse.quote_plus(next_token_elem.text)
            token_param = f"&continuation-token={safe_token}"
            
            c += 1
            if c>10:
                assert False

        assert total == 6


def test_head_object():
    with TestClient(app) as client:
        response = client.head("/janelia-data-examples/jrc_mus_lung_covid.n5/attributes.json")
        assert response.status_code == 200
        response = client.head("/janelia-data-examples/jrc_mus_lung_covid.n5/")
        assert response.status_code == 404


def test_prefixed_head_object():
    with TestClient(app) as client:
        response = client.head("/with-prefix/attributes.json")
        assert response.status_code == 200
        response = client.head("/with-prefix/render/attributes.json")
        assert response.status_code == 200
        response = client.head("/with-prefix/render/")
        assert response.status_code == 404


def test_get_object():
    with TestClient(app) as client:
        response = client.get("/janelia-data-examples/jrc_mus_lung_covid.n5/attributes.json")
        assert response.status_code == 200
        json_obj = response.json()
        assert 'n5' in json_obj


def test_prefixed_get_object():
    with TestClient(app) as client:
        response = client.get("/with-prefix/attributes.json")
        assert response.status_code == 200
        json_obj = response.json()
        assert 'n5' in json_obj


def test_virtual_host_get_object():
    with TestClient(app) as client:
        response = client.get("/jrc_mus_lung_covid.n5/attributes.json",
            headers={'Host':'janelia-data-examples.testserver'})
        assert response.status_code == 200
        json_obj = response.json()
        assert 'n5' in json_obj


def test_prefixed_list_objects():
    with TestClient(app) as client:
        bucket_name = 'with-prefix'
        response = client.get(f"/{bucket_name}?list-type=2&delimiter=/")
        assert response.status_code == 200
        root = parse_xml(response.text)
        assert root.tag == "ListBucketResult"
        assert root.find('Name').text == bucket_name
        assert root.find('Delimiter').text == '/'
        assert len(root.findall('CommonPrefixes')) == 1
        assert root.find('IsTruncated').text == "false"


def test_get_object_hidden():
    with TestClient(app) as client:
        response = client.get("/hidden-with-endpoint/jrc_mus_lung_covid.n5/attributes.json")
        assert response.status_code == 200
        json_obj = response.json()
        assert 'n5' in json_obj


def test_get_object_missing():
    with TestClient(app) as client:
        response = client.get("/janelia-data-examples/missing")
        assert response.status_code == 404
        assert response.headers['content-type'] == "application/xml"


def test_bucket_missing():
    with TestClient(app) as client:
        response = client.get("/missing/attributes.json")
        assert response.status_code == 404
        assert response.headers['content-type'] == "application/xml"
        root = parse_xml(response.text)
        assert root.find('Code').text == 'NoSuchBucket'
        

def test_list_objects_error():
    with TestClient(app) as client:
        response = client.get(f"/janelia-data-examples?list-type=2&max-keys=aaa")
        assert response.status_code == 400
        assert response.headers['content-type'] == "application/json"


def test_get_object_precedence():
    with TestClient(app) as client:
        response = client.get(f"/janelia-data-examples/jrc_mus_lung_covid.n5/attributes.json?list-type=2")
        assert response.status_code == 200
        json_obj = response.json()
        assert 'n5' in json_obj
