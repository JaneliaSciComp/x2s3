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


def test_pna_preflight_grants_private_network(client):
    """A CORS preflight carrying Access-Control-Request-Private-Network must be
    answered with Access-Control-Allow-Private-Network: true so Chromium permits
    public-origin pages to load data from an internal-network host."""
    response = client.options(
        "/some-target/some.zarr/.zattrs",
        headers={
            "Origin": "https://example.com",
            "Access-Control-Request-Method": "GET",
            "Access-Control-Request-Private-Network": "true",
        },
    )
    assert response.headers.get("access-control-allow-private-network") == "true"


def test_pna_header_absent_without_request(client):
    """The PNA grant header must not leak onto responses that did not ask for it."""
    response = client.get("/robots.txt")
    assert response.status_code == 200
    assert "access-control-allow-private-network" not in response.headers