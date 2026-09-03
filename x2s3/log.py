"""Access logging and optional JSON (ECS) log output.

Text is the default and looks like a normal access log. Setting
``log_format: json`` (or ``X2S3_LOG_FORMAT=json``) turns every record into a
single-line JSON object using Elastic Common Schema field names, which a log
shipper can parse straight off the container's stdout and hand to Kibana.

ECS fields are written as dotted keys ("http.response.status_code") rather
than nested dicts: Elasticsearch expands them on ingest, so the indexed
document is the same one without any dict-building here.
"""
import json
import logging
import sys
import time
import traceback
from datetime import timezone
from importlib.metadata import PackageNotFoundError, version

from loguru import logger

SERVICE_NAME = "x2s3"

try:
    SERVICE_VERSION = version("x2s3")
except PackageNotFoundError:  # running from a source tree that was never installed
    SERVICE_VERSION = "unknown"


def _log_safe(value: str) -> str:
    """Escape anything in client-controlled text that could disturb a log line.

    Turns control characters into their escapes, so a request target can't add
    lines, recolour a terminal tailing the log, or smuggle loguru's '|' field
    separator into a field. Ordinary percent-encoded paths pass through
    unchanged.
    """
    return value.encode("unicode_escape").decode("ascii")


def _json_sink(message):
    """Write one loguru record as one line of ECS-shaped JSON on stdout."""
    record = message.record
    payload = {
        "@timestamp": record["time"].astimezone(timezone.utc)
                      .isoformat(timespec="milliseconds").replace("+00:00", "Z"),
        "log.level": record["level"].name.lower(),
        "log.logger": record["name"],
        "message": record["message"],
        "service.name": SERVICE_NAME,
        "service.version": SERVICE_VERSION,
        "process.pid": record["process"].id,
    }
    # Anything bound with logger.bind()/contextualize() is already keyed by its
    # ECS name, so it merges straight in.
    payload.update(record["extra"])

    exception = record["exception"]
    if exception:
        payload["error.type"] = getattr(exception.type, "__name__", "Exception")
        payload["error.message"] = str(exception.value)
        payload["error.stack_trace"] = "".join(traceback.format_exception(
            exception.type, exception.value, exception.traceback))

    # loguru serialises calls into a sink, so lines from this process never
    # interleave.
    # ponytail: across x2s3's 8 uvicorn workers they still share one stdout, and
    # a line over the ~4KB atomic-write limit can interleave with another
    # worker's. process.pid tells them apart; go to a per-worker file sink if
    # that ever actually bites.
    sys.stdout.write(json.dumps(payload, default=str) + "\n")
    sys.stdout.flush()


class _InterceptHandler(logging.Handler):
    """Route stdlib logging (uvicorn, botocore) into loguru.

    Only installed in JSON mode, so that a shipper sees one format on the
    stream instead of JSON lines mixed with uvicorn's plain text.
    """

    def emit(self, record: logging.LogRecord) -> None:
        try:
            level = logger.level(record.levelname).name
        except ValueError:
            level = record.levelno
        frame, depth = logging.currentframe(), 2
        while frame and frame.f_code.co_filename == logging.__file__:
            frame = frame.f_back
            depth += 1
        # Report the stdlib logger's own name rather than the frame loguru
        # would infer, which is always the logging module itself.
        logger.opt(depth=depth, exception=record.exc_info).bind(
            **{"log.logger": record.name}).log(level, record.getMessage())


def disable_uvicorn_access_log():
    """Silence Uvicorn's own access logger, which AccessLogMiddleware replaces.

    Left enabled, every request is logged twice: once here with the duration and
    target, once by Uvicorn without them. Uvicorn's ``--no-access-log`` does the
    same thing from the outside, but it has to be remembered on every launch
    command. Doing it where the middleware is installed covers any way the app
    is started, including the pinned command line baked into the Docker image.

    Safe at import time: Uvicorn configures logging before it imports the app,
    including in each --workers subprocess.
    """
    access_logger = logging.getLogger("uvicorn.access")
    access_logger.handlers = []
    access_logger.propagate = False


def configure_logging(log_level: str = "INFO", log_format: str = "text"):
    """Point loguru at stderr (text) or at the ECS JSON sink on stdout."""
    logger.remove()
    if log_format == "json":
        logger.add(_json_sink, level=log_level, format="{message}")
        logging.basicConfig(handlers=[_InterceptHandler()], level=0, force=True)
        # Uvicorn installs its own handlers and turns off propagation, so its
        # lines would stay plain text on a stream that is otherwise all JSON.
        # Hand them to the root logger, which now goes through loguru.
        for name in ("uvicorn", "uvicorn.error", "uvicorn.asgi"):
            uvicorn_logger = logging.getLogger(name)
            uvicorn_logger.handlers = []
            uvicorn_logger.propagate = True
    else:
        logger.add(sys.stderr, level=log_level)


class AccessLogMiddleware:
    """Pure ASGI middleware that logs one line per HTTP request.

    Pure ASGI rather than BaseHTTPMiddleware because x2s3 mostly streams large
    objects: the app coroutine only returns once the last body chunk has been
    sent, so the duration measured here is the real transfer time rather than
    time-to-first-byte, and the response bytes can be counted on the way past.

    Registered outermost so it also covers responses generated by the CORS and
    PNA middlewares. It reads the request id and target name that the inner
    layers leave in ``scope["state"]``, which is the same dict object all the
    way down.
    """

    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        start = time.perf_counter()
        status = 500
        body_bytes = 0

        async def send_with_accounting(message):
            nonlocal status, body_bytes
            if message["type"] == "http.response.start":
                status = message["status"]
            elif message["type"] == "http.response.body":
                body_bytes += len(message.get("body", b""))
            await send(message)

        try:
            await self.app(scope, receive, send_with_accounting)
        finally:
            _log_request(scope, status, body_bytes, time.perf_counter() - start)


def _log_request(scope, status: int, body_bytes: int, duration_s: float):
    client = scope.get("client") or ("unknown", 0)
    http_version = scope.get("http_version", "1.1")

    # Log the target as the client sent it. scope["path"] is percent-decoded per
    # the ASGI spec, so a key containing a literal '%' (sent as '%25') would log
    # as '%' and the line would no longer round-trip to the request that was
    # made. raw_path is the encoded form, which is what Uvicorn's own access log
    # reports too.
    raw_path = scope.get("raw_path")
    path = _log_safe(raw_path.decode("latin-1") if raw_path else scope.get("path", ""))
    query = _log_safe((scope.get("query_string") or b"").decode("latin-1"))

    state = scope.get("state") or {}
    target = state.get("target_name")
    if target:
        target = _log_safe(target)
    # Read from the scope rather than the bound context: this runs outside
    # RequestIdMiddleware's contextualize() block, which has already exited.
    request_id = state.get("request_id")

    message = (
        f"{client[0]}:{client[1]} "
        f'"{scope.get("method", "-")} {path}{"?" + query if query else ""} '
        f'HTTP/{http_version}" '
        f"{status} {body_bytes}b {duration_s * 1000:.2f}ms"
    )
    if target:
        message += f" target={target}"

    fields = {
        "event.dataset": f"{SERVICE_NAME}.access",
        # ECS measures event.duration in nanoseconds.
        "event.duration": int(duration_s * 1_000_000_000),
        "http.request.method": scope.get("method"),
        "http.response.status_code": status,
        "http.response.body.bytes": body_bytes,
        "http.version": http_version,
        "url.path": path,
        "client.ip": client[0],
        "client.port": client[1],
    }
    if request_id:
        fields["trace.id"] = request_id
    if query:
        fields["url.query"] = query
    if target:
        # Custom (non-ECS) dimensions belong under labels. The object key is
        # deliberately not repeated here: it is already url.path, and zarr chunk
        # keys would add a huge high-cardinality field for nothing.
        fields["labels.target"] = target
    for header, field in ((b"user-agent", "user_agent.original"), (b"host", "url.domain")):
        value = next((v for k, v in scope.get("headers", []) if k == header), None)
        if value:
            fields[field] = _log_safe(value.decode("latin-1"))

    log = logger.bind(**fields)
    if status < 400:
        log.info(message)
    elif status < 500:
        log.warning(message)
    else:
        log.error(message)
