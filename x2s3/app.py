import logging
import os
import sys
from contextlib import asynccontextmanager
from typing import Optional

from loguru import logger

# Suppress asyncio SSL connection closed warnings (common during client cancellations)
logging.getLogger("asyncio").setLevel(logging.ERROR)

from fastapi import FastAPI, HTTPException, Request, Query
from fastapi.responses import JSONResponse, FileResponse, PlainTextResponse, Response
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi.middleware.cors import CORSMiddleware
from fastapi.exceptions import RequestValidationError
from starlette.exceptions import HTTPException as StarletteHTTPException

from x2s3.utils import *
from x2s3 import client_registry
from x2s3.settings import get_settings, Target

# Use uvloop for better async performance
try:
    import uvloop
    import asyncio
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
    logger.info("uvloop event loop policy installed")
except ImportError:
    logger.warning("uvloop not available, using default asyncio event loop")

class RequestIdMiddleware:
    """Pure ASGI middleware that attaches an S3-style x-amz-request-id header
    to every response.

    Real S3 returns this header on all responses (GetObject, HeadObject,
    ListObjectsV2, errors, etc.) so clients can reference a specific request
    when correlating logs or reporting issues. We generate one id per request
    and inject it into the response start event, which works for streaming
    responses without buffering the body.
    """

    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        request_id = generate_request_id()
        # Expose to downstream handlers/loggers via request.state.request_id
        scope.setdefault("state", {})["request_id"] = request_id

        async def send_with_request_id(message):
            if message["type"] == "http.response.start":
                headers = message.setdefault("headers", [])
                headers.append((b"x-amz-request-id", request_id.encode("latin-1")))
            await send(message)

        await self.app(scope, receive, send_with_request_id)


class PrivateNetworkAccessMiddleware:
    """Pure ASGI middleware that grants browser Private Network Access (PNA)
    preflights.

    Chromium browsers (Chrome/Edge) send a CORS preflight before any request from
    a public-origin page to a private-network address (e.g. an internal host
    serving objects to a browser-based viewer). The preflight carries
    `Access-Control-Request-Private-Network: true`, and the request only proceeds
    if the response echoes `Access-Control-Allow-Private-Network: true`. Starlette's
    CORSMiddleware does not emit this header, so without it Chromium blocks
    public-origin pages from loading data hosted on an internal network.

    (Firefox uses a separate user-permission model -- Local Network Access -- rather
    than this header, so this neither helps nor harms Firefox.)

    Registered outside CORSMiddleware so it can append the header to the preflight
    response that CORSMiddleware generates. Implemented as pure ASGI so it only
    touches response headers without re-wrapping the body. The header is added only
    when the PNA request header is present, which the browser sends solely on
    preflights, so it never appears on normal data responses.
    """

    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        # ASGI lowercases header names; the request header value is the ASCII "true".
        requested = any(
            name == b"access-control-request-private-network"
            and value.strip().lower() == b"true"
            for name, value in scope.get("headers", [])
        )
        if not requested:
            await self.app(scope, receive, send)
            return

        async def send_with_pna(message):
            if message["type"] == "http.response.start":
                headers = message.setdefault("headers", [])
                headers.append((b"access-control-allow-private-network", b"true"))
            await send(message)

        await self.app(scope, receive, send_with_pna)


def create_app(settings):

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        """Lifespan context manager for startup and shutdown events."""
        # Startup
        if callable(settings):
            app.settings = settings()
        else:
            app.settings = settings

        # Configure logging
        logger.remove()
        logger.add(sys.stderr, level=app.settings.log_level)

        logger.trace("Available protocols:")
        for proto in client_registry.available_protocols():
            logger.trace(f"- {proto}")

        app.clients = {}

        # Add local path client if configured
        if app.settings.local_path:
            local_target = Target(
                name=app.settings.local_name,
                client='file',
                options={
                    'path': str(app.settings.local_path),
                }
            )
            app.settings.targets += [local_target]

        # Configure targets
        for target_name in app.settings.get_target_map():
            target_key = target_name.lower()
            target_config = app.settings.get_target_config(target_key)
            proxy_kwargs = {
                'target_name': target_name,
            }

            # Merge global client options with target-specific options
            merged_options = app.settings.get_merged_client_options(
                target_config.client, target_config.options)
            logger.debug(f"Creating {target_config.client} client for {target_name} with options: {merged_options}")

            client = client_registry.client(target_config.client,
                proxy_kwargs, **merged_options)

            if target_key in app.clients:
                logger.warning(f"Overriding target key: {target_key}")

            app.clients[target_key] = client
            logger.debug(f"Configured target {target_name}")

        logger.info(f"Server ready with {len(app.clients)} targets")

        yield

        # Shutdown
        logger.info("Shutting down, closing client connections...")
        for target_name, client in app.clients.items():
            if hasattr(client, 'close'):
                try:
                    await client.close()
                    logger.debug(f"Closed client for target: {target_name}")
                except Exception as e:
                    logger.error(f"Error closing client for {target_name}: {e}")
        logger.info("All clients closed")

    app = FastAPI(lifespan=lifespan)
    app.add_middleware(RequestIdMiddleware)
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["GET","HEAD"],
        allow_headers=["*"],
        expose_headers=["Range", "Content-Range", "x-amz-request-id"],
    )
    # Echo Access-Control-Allow-Private-Network on PNA preflights. Added after
    # (i.e. outside) CORSMiddleware so it wraps the preflight response CORS emits.
    app.add_middleware(PrivateNetworkAccessMiddleware)
    app.mount("/static", StaticFiles(directory="static"), name="static")
    templates = Jinja2Templates(directory="templates")

    @app.exception_handler(StarletteHTTPException)
    async def http_exception_handler(request, exc):
        return get_error_response(exc.status_code, 'InternalError', exc.detail, request.url.path)

    @app.exception_handler(RequestValidationError)
    async def validation_exception_handler(request, exc):
        error = exc.errors()[0]
        return get_error_response(400, 'InvalidArgument', error['msg'], request.url.path)


    def get_client(target_name):
        target_key = target_name.lower()
        if target_key in app.clients:
            return app.clients[target_key]
        return None


    def get_target(request, path):
        target_path = path
        base_url = app.settings.base_url
        
        logger.trace(f"base_url: {base_url}")
        logger.trace(f"request.url.hostname: {request.url.hostname}")

        subdomain = None
        if app.settings.virtual_buckets:
            if base_url:
                subdomain = request.url.hostname.removesuffix(base_url.host).removesuffix('.')
            else:
                logger.warning("virtual_buckets enabled but no base URL is configured")

        if subdomain:
            # Target is given in the subdomain
            is_virtual = True
            target_name = subdomain.split('.')[0]
        else:
            # Target is encoded as the first element in the path
            is_virtual = False
            # Extract target from path
            ts = target_path.removeprefix('/').split('/', maxsplit=1)
            logger.trace(f"target path components: {ts}")
            if len(ts)==2:
                target_name, target_path = ts
            elif len(ts)==1:
                # This happens if we are at the root of the proxy
                target_name, target_path = ts[0], ''
            else:
                # This shouldn't happen
                target_name, target_path = None, ''

        logger.trace(f"target_name={target_name}, target_path={target_path}, is_virtual={is_virtual}")
        return target_name, target_path, is_virtual


    async def browse_bucket(request: Request,
                            target_name: str,
                            prefix: str,
                            continuation_token: str = None,
                            max_keys: int = 10,
                            is_virtual: bool = False):
        
        target_config = app.settings.get_target_config(target_name)
        if not target_config:
            raise HTTPException(status_code=404, detail="Target bucket not found")

        client = get_client(target_name)
        if client is None:
            raise HTTPException(status_code=500, detail="Client for target bucket not found")

        response = await client.list_objects_v2(continuation_token, '/', None,
                                                False, max_keys, prefix, None)

        if response.status_code != 200:
            # Return error respone
            return response

        xml = response.body.decode("utf-8")
        root = parse_xml(xml)

        common_prefixes = []
        cps = [c for c in root.findall('CommonPrefixes')]
        if cps:
            for cp in cps:
                common_prefixes += [dir_path(e.text) for e in cp.iter('Prefix')] if cps else []

        contents = []
        cs = [c for c in root.findall('Contents')]
        if cs:
            for c in cs:
                key_elem = c.find('Key')
                if key_elem is not None and key_elem.text != prefix:

                    content = {'key': key_elem.text}

                    size_elem = c.find('Size')
                    if size_elem is not None and size_elem.text:
                        num_bytes = int(size_elem.text)
                        content['size'] = humanize_bytes(num_bytes)

                    lm_elem = c.find('LastModified')
                    if lm_elem is not None and lm_elem.text:
                        content['lastmod'] = format_isoformat_as_local(lm_elem.text)

                    contents.append(content)

        next_token = None
        truncated_elem = root.find('IsTruncated')
        if truncated_elem is not None and truncated_elem.text=='true':
            next_ct_elem = root.find('NextContinuationToken')
            next_token = next_ct_elem.text

        target_prefix = '' if is_virtual else '/'+target_name
        parent_prefix = dir_path(os.path.dirname(prefix.rstrip('/')))

        return templates.TemplateResponse(request, "browse.html", context={
            "prefix": prefix,
            "index_url": app.settings.base_url or '/',
            "target_prefix": target_prefix,
            "common_prefixes": common_prefixes,
            "contents": contents,
            "parent_prefix": parent_prefix,
            "remove_prefix": remove_prefix,
            "continuation_token": next_token
        })


    @app.get('/favicon.ico', include_in_schema=False)
    async def favicon():
        return FileResponse('static/favicon.ico')


    @app.get('/robots.txt', response_class=PlainTextResponse)
    def robots():
        return """User-agent: *\nDisallow: /"""


    def _prefers_html(request: Request) -> bool:
        """Check if the client prefers HTML over XML based on the Accept header.
        Returns True only if text/html appears before application/xml.
        """
        accept = request.headers.get("accept", "")
        html_pos = accept.find("text/html")
        xml_pos = accept.find("application/xml")
        if html_pos == -1:
            return False
        if xml_pos == -1:
            return True
        return html_pos < xml_pos

    @app.get("/{path:path}")
    async def target_dispatcher(request: Request,
                                path: str,
                                list_type: int = Query(None, alias="list-type"),
                                continuation_token: Optional[str] = Query(None, alias="continuation-token"),
                                delimiter: Optional[str] = Query(None, alias="delimiter"),
                                encoding_type: Optional[str] = Query(None, alias="encoding-type"),
                                fetch_owner: Optional[bool] = Query(None, alias="fetch-owner"),
                                max_keys: Optional[int] = Query(1000, alias="max-keys", ge=0),
                                prefix: Optional[str] = Query(None, alias="prefix"),
                                start_after: Optional[str] = Query(None, alias="start-after")):

        target_name, target_path, is_virtual = get_target(request, path)
        logger.debug(f"target_name={target_name}, target_path={target_path}, is_virtual={is_virtual}")

        if not target_name or (is_virtual and target_name=='www'):
            # Return target index
            bucket_list = { target: f"/{target}/" for target in app.settings.get_browseable_targets()}
            if app.settings.ui and _prefers_html(request):
                response = templates.TemplateResponse(request, "index.html", context={"links": bucket_list})
            else:
                xml = get_bucket_list_xml(bucket_list)
                response = Response(content=xml, status_code=200, media_type="application/xml")
            # Same URL serves HTML or XML depending on Accept, so shared
            # caches must store the two variants separately
            response.headers["Vary"] = "Accept"
            return response
        
        target_config = app.settings.get_target_config(target_name)
        if not target_config:
            return get_nosuchbucket_response(target_name)

        client = get_client(target_name)
        if client is None:
            raise HTTPException(status_code=500, detail="Client for target bucket not found")

        if 'acl' in request.query_params:
            if not target_config.browseable:
                # Real S3 denies GetBucketAcl/GetObjectAcl without s3:GetBucketAcl/s3:GetObjectAcl
                return get_accessdenied_response()
            return get_read_access_acl()

        async def get_object_or_denied(key):
            """GetObject with S3-style 404 masking: on unbrowseable buckets a
            missing key returns 403 AccessDenied so clients can't probe which
            keys exist (real S3 does this when s3:ListBucket is denied)."""
            response = await client.get_object(key, request.headers.get("range"))
            if response.status_code == 404 and not target_config.browseable:
                return get_accessdenied_response()
            return response

        if list_type:
            if not target_path:
                if not target_config.browseable:
                    return get_accessdenied_response()
                if list_type == 2:
                    return await client.list_objects_v2(continuation_token, delimiter, \
                        encoding_type, fetch_owner, max_keys, prefix, start_after)
                else:
                    raise HTTPException(status_code=400, detail="Invalid list type")
            else:
                return await get_object_or_denied(target_path)

        if not target_path or target_path.endswith("/"):
            if not target_config.browseable:
                return get_accessdenied_response()
            if app.settings.ui and _prefers_html(request):
                response = await browse_bucket(request, target_name, target_path,
                    continuation_token=continuation_token,
                    max_keys=100,
                    is_virtual=is_virtual)
            else:
                response = await client.list_objects_v2(continuation_token, delimiter, \
                    encoding_type, fetch_owner, max_keys, prefix, start_after)
            response.headers["Vary"] = "Accept"
            return response
        else:
            return await get_object_or_denied(target_path)



    @app.head("{path:path}")
    async def head_object(request: Request, path: str):

        target_name, target_path, _ = get_target(request, path)
        if not target_name:
            return get_nosuchbucket_response('')

        try:
            target_config = app.settings.get_target_config(target_name)
            if not target_config:
                return get_nosuchbucket_response(target_name)

            client = get_client(target_name)
            if client is None:
                raise HTTPException(status_code=500, detail="Client for target bucket not found")

            if not target_path:
                # HEAD on bucket root — equivalent to HeadBucket, which
                # requires s3:ListBucket in real S3
                if not target_config.browseable:
                    return Response(status_code=403, media_type="application/xml")
                return Response(status_code=200, media_type="application/xml")

            response = await client.head_object(target_path)
            if response.status_code == 404 and not target_config.browseable:
                # Mask missing keys on unbrowseable buckets; HEAD carries no body
                return Response(status_code=403, media_type="application/xml")
            return response
        except Exception:
            logger.opt(exception=sys.exc_info()).info("Error requesting head")
            return get_error_response(500, "InternalError", "Error requesting HEAD", path)

    return app


app = create_app(get_settings)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
