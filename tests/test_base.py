import urllib.parse

import pytest
from fastapi.testclient import TestClient
from pydantic import HttpUrl

from xml.etree.ElementTree import Element
from x2s3.app import create_app
from x2s3.settings import Target, Settings
from x2s3.utils import parse_xml


@pytest.fixture
def get_settings():
    settings = Settings()
    settings.targets = [
    ]
    return settings


@pytest.fixture
def app(get_settings):
    return create_app(get_settings)

    
@pytest.fixture
def client(app):
    return TestClient(app)


def test_get_favicon(client):
    response = client.get("/favicon.ico")
    assert response.status_code == 200
    assert response.headers['content-type'].startswith("image")


def test_get_robotstxt(client):
    response = client.get("/robots.txt")
    assert response.status_code == 200
    assert response.headers['content-type'].startswith("text/plain")
    assert response.text == "User-agent: *\nDisallow: /"