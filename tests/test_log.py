"""Tests for the access log and the JSON (ECS) logging mode."""

import json
import logging

import pytest
from fastapi.testclient import TestClient

from x2s3.app import create_app
from x2s3.log import configure_logging
from x2s3.settings import Settings, Target


@pytest.fixture(autouse=True)
def restore_logging():
    """The app configures logging globally on startup; put it back afterwards.

    JSON mode also reroutes stdlib logging into loguru, which would otherwise
    leak into whatever test runs next.
    """
    root = logging.getLogger()
    handlers, level = root.handlers[:], root.level
    yield
    configure_logging("INFO", "text")
    root.handlers, root.level = handlers, level


def _client(log_format):
    settings = Settings()
    settings.log_format = log_format
    settings.targets = [Target(name='local-files', client='file', options={'path': '.'})]
    return TestClient(create_app(settings))


def _access_lines(captured):
    """The access log lines out of everything the app logged."""
    lines = []
    for line in captured.out.splitlines():
        record = json.loads(line)
        if record.get("event.dataset") == "x2s3.access":
            lines.append(record)
    return lines


def test_json_mode_logs_one_ecs_object_per_request(capsys):
    with _client("json") as client:
        client.get("/local-files/README.md")

    lines = _access_lines(capsys.readouterr())
    assert len(lines) == 1, lines
    record = lines[0]

    assert record["service.name"] == "x2s3"
    assert record["http.request.method"] == "GET"
    assert record["http.response.status_code"] == 200
    assert record["url.path"] == "/local-files/README.md"
    assert record["labels.target"] == "local-files"
    assert record["trace.id"]
    # The whole point: a number Kibana can take percentiles of.
    assert isinstance(record["event.duration"], int) and record["event.duration"] > 0
    # Counted off the wire, so it covers streamed bodies.
    assert record["http.response.body.bytes"] > 0


def test_json_mode_reports_error_status(capsys):
    with _client("json") as client:
        client.get("/no-such-target/key")

    record = _access_lines(capsys.readouterr())[0]
    assert record["http.response.status_code"] == 404
    assert record["log.level"] == "warning"


def test_text_mode_stays_human_readable(capsys):
    """Default mode must remain a plain access line, not JSON."""
    with _client("text") as client:
        client.get("/local-files/README.md")

    err = capsys.readouterr().err
    assert '"GET /local-files/README.md HTTP/1.1" 200' in err
    assert "target=local-files" in err
