"""Tests for the nginx conditional-header handling in docker/include/.

nginx replaces the client's If-None-Match/If-Modified-Since with its own
cache-revalidation values whenever proxy_cache is configured, which is decided
at config time. These tests run real nginx in front of an echo upstream and
assert on the headers that actually arrive, since that substitution is
invisible from either end alone.

Skipped when no nginx binary is installed.
"""

import json
import shutil
import socket
import subprocess
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.request import Request, urlopen

import pytest

DOCKER_INCLUDE = Path(__file__).resolve().parent.parent / "docker" / "include"
UPSTREAM_ETAG = '"upstream-etag"'
CLIENT_ETAG = '"client-etag"'

pytestmark = pytest.mark.skipif(shutil.which("nginx") is None,
                                reason="nginx binary not installed")


def _free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class _EchoHandler(BaseHTTPRequestHandler):
    """Reflects the request headers it received back as a JSON body."""

    def do_GET(self):
        body = json.dumps(dict(self.headers)).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("ETag", UPSTREAM_ETAG)
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *args):
        pass


@pytest.fixture(scope="module")
def upstream():
    server = ThreadingHTTPServer(("127.0.0.1", 0), _EchoHandler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    yield server.server_address[1]
    server.shutdown()


@pytest.fixture(scope="module")
def nginx(upstream, tmp_path_factory):
    root = tmp_path_factory.mktemp("nginx")
    port = _free_port()
    # proxy_cache_valid is first-match-wins, so this 1s entry must precede the
    # 15m one inside proxy_cache.conf for the revalidation test to be quick.
    (root / "nginx.conf").write_text(f"""
worker_processes 1;
pid {root}/nginx.pid;
error_log {root}/error.log warn;
events {{ worker_connections 64; }}
http {{
    access_log off;
    client_body_temp_path {root}/client_temp;
    proxy_temp_path {root}/proxy_temp;
    fastcgi_temp_path {root}/fastcgi_temp;
    uwsgi_temp_path {root}/uwsgi_temp;
    scgi_temp_path {root}/scgi_temp;
    proxy_cache_path {root}/cache keys_zone=mycache:10m max_size=32m levels=1:2 inactive=1h;

    include {DOCKER_INCLUDE}/proxy_cache_maps.conf;

    server {{
        listen 127.0.0.1:{port};
        location / {{
            proxy_cache_valid 200 1s;
            include {DOCKER_INCLUDE}/proxy_cache.conf;
            proxy_pass http://127.0.0.1:{upstream};
        }}
    }}
}}
""")
    check = subprocess.run(["nginx", "-t", "-c", str(root / "nginx.conf")],
                           capture_output=True, text=True)
    assert check.returncode == 0, check.stderr
    subprocess.run(["nginx", "-c", str(root / "nginx.conf")],
                   capture_output=True, text=True, check=True)
    for _ in range(50):
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.1):
                break
        except OSError:
            time.sleep(0.1)
    yield port
    subprocess.run(["nginx", "-c", str(root / "nginx.conf"), "-s", "quit"],
                   capture_output=True)


def request_through_nginx(port, path, **headers):
    """GET through nginx, returning (cache status, headers the upstream saw)."""
    request = Request(f"http://127.0.0.1:{port}{path}", headers=headers)
    with urlopen(request) as response:
        seen = {k.lower(): v for k, v in json.loads(response.read()).items()}
        return response.headers.get("X-Proxy-Cache"), seen


def upstream_headers(port, path, **headers):
    """GET through nginx and return the headers the upstream actually saw."""
    return request_through_nginx(port, path, **headers)[1]


def test_ranged_request_forwards_client_if_none_match(nginx):
    # Range requests bypass the cache entirely, so nginx is a pass-through and
    # the client's validator has to survive the hop or x2s3 can never answer
    # the 304 that RFC 9110 13.2.2 requires.
    seen = upstream_headers(nginx, "/ranged-inm", Range="bytes=0-9",
                            **{"If-None-Match": CLIENT_ETAG})
    assert seen.get("if-none-match") == CLIENT_ETAG
    assert seen.get("range") == "bytes=0-9"


def test_ranged_request_forwards_client_if_modified_since(nginx):
    since = "Tue, 18 Aug 2026 12:00:00 GMT"
    seen = upstream_headers(nginx, "/ranged-ims", Range="bytes=0-9",
                            **{"If-Modified-Since": since})
    assert seen.get("if-modified-since") == since


def test_cached_request_does_not_leak_client_if_none_match(nginx):
    # Unranged requests take part in nginx's cache, where these headers belong
    # to nginx's own revalidation. Forwarding the client's validator here would
    # let it answer nginx's question and revive a stale cache entry.
    seen = upstream_headers(nginx, "/cached-inm", **{"If-None-Match": CLIENT_ETAG})
    assert "if-none-match" not in seen


def test_cache_revalidation_uses_nginx_own_validator(nginx):
    # The regression guard for the map's '' branch: once the entry goes stale,
    # nginx must revalidate with the ETag it cached, not the client's, or a 304
    # meant for the client would mark a stale entry fresh.
    #
    # Expiry is polled rather than slept through: nginx derives validity from
    # the upstream Date header, which is whole-second, so a 1s entry expires
    # anywhere in roughly 1-3s.
    request_through_nginx(nginx, "/reval")
    deadline = time.time() + 15
    while time.time() < deadline:
        status, seen = request_through_nginx(nginx, "/reval",
                                             **{"If-None-Match": CLIENT_ETAG})
        if status != "HIT":
            assert seen.get("if-none-match") == UPSTREAM_ETAG
            return
        time.sleep(0.25)
    pytest.fail("cache entry never expired, so revalidation was never exercised")


def test_no_cache_request_bypasses_the_cache(nginx):
    # nginx.conf maps Cache-Control: no-cache to a cache bypass, but a
    # location-level proxy_cache_bypass replaces the http-level one rather than
    # adding to it, so including proxy_cache.conf silently dropped that policy.
    status, _ = request_through_nginx(nginx, "/no-cache-probe")
    assert status == "MISS"
    status, _ = request_through_nginx(nginx, "/no-cache-probe",
                                      **{"Cache-Control": "no-cache"})
    assert status == "BYPASS"
