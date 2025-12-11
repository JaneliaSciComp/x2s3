import asyncio
import os
import sys
import typing
from dataclasses import dataclass
from typing import Any
from typing_extensions import override

from loguru import logger
from starlette.background import BackgroundTask
import botocore
from botocore.exceptions import NoCredentialsError, PartialCredentialsError
from aiobotocore.session import get_session
from aiobotocore.config import AioConfig
from fastapi.responses import Response, StreamingResponse, JSONResponse

from x2s3.utils import *
from x2s3.client import ProxyClient, ObjectHandle


@dataclass
class S3ObjectHandle(ObjectHandle):
    """Handle for S3-based object storage."""
    body: Any = None  # The S3 response body stream
    _closed: bool = False

    def close(self):
        """Close the body stream to release the connection back to pool."""
        if not self._closed and self.body is not None:
            if hasattr(self.body, 'close'):
                self.body.close()
            self._closed = True


def handle_s3_exception(e, key=None):
    """ Handle various cases of generic errors from the boto AWS API.
    """
    if isinstance(e, (NoCredentialsError, PartialCredentialsError)):
        logger.opt(exception=sys.exc_info()).error("AWS credentials not configured properly")
        # 500: Server misconfiguration, not client's fault
        return JSONResponse({"error": "AWS credentials not configured properly"}, status_code=500)
    elif isinstance(e, botocore.exceptions.ConnectTimeoutError):
        logger.warning(f"Connection timeout to upstream S3: {e}")
        # 504: Gateway Timeout - proxy couldn't connect to upstream
        return JSONResponse({"error": "Connection to upstream endpoint timed out"}, status_code=504)
    elif isinstance(e, botocore.exceptions.ReadTimeoutError):
        logger.warning(f"Read timeout from upstream S3: {e}")
        # 504: Gateway Timeout - proxy didn't get timely response from upstream
        return JSONResponse({"error": "Upstream endpoint timed out"}, status_code=504)
    elif isinstance(e, botocore.exceptions.ClientError):
        response = getattr(e, 'response', {}) or {}
        metadata = response.get('ResponseMetadata', {})
        status_code = metadata.get('HTTPStatusCode', 500)
        error = response.get('Error', {})
        error_code = error.get('Code', 'Unknown')
        if error_code == "NoSuchKey" or error_code == "404":
            return get_nosuchkey_response(key)
        else:
            message = error.get('Message', 'Unknown error')
            resource = error.get('Resource', key or 'Unknown')
            return get_error_response(status_code, error_code, message, resource)
    else:
        logger.opt(exception=sys.exc_info()).error("Error communicating with AWS S3")
        return JSONResponse({"error": "Error communicating with AWS S3"}, status_code=500)


class AiobotoProxyClient(ProxyClient):

    def __init__(self, proxy_kwargs, **kwargs):

        self.proxy_kwargs = proxy_kwargs or {}
        self.target_name = self.proxy_kwargs['target_name']
        self.bucket_name = kwargs['bucket']
        self.bucket_prefix = kwargs.get('prefix')

        self.anonymous = True
        access_key,secret_key = '',''

        if 'access_key_path' in kwargs:
            self.anonymous = False
            access_key_path = kwargs['access_key_path']
            secret_key_path = kwargs['secret_key_path']

            try:
                with open(access_key_path, 'r') as ak_file:
                    access_key = ak_file.read().strip()
            except FileNotFoundError:
                raise ValueError(f"Target '{self.target_name}': access_key_path not found: {access_key_path}")
            except PermissionError:
                raise ValueError(f"Target '{self.target_name}': cannot read access_key_path: {access_key_path}")

            try:
                with open(secret_key_path, 'r') as sk_file:
                    secret_key = sk_file.read().strip()
            except FileNotFoundError:
                raise ValueError(f"Target '{self.target_name}': secret_key_path not found: {secret_key_path}")
            except PermissionError:
                raise ValueError(f"Target '{self.target_name}': cannot read secret_key_path: {secret_key_path}")

        self.client_kwargs = {
            'aws_access_key_id': access_key,
            'aws_secret_access_key': secret_key,
        }

        if 'endpoint' in kwargs:
            self.client_kwargs['endpoint_url'] = kwargs.get('endpoint')

        # Create shared session and configure connection pooling
        self.session = get_session()

        # Build AioConfig from user-provided options
        # Start with defaults
        conf_kwargs = {
            'max_pool_connections': 30,  # Default to prevent FD exhaustion
        }

        # Merge user-provided botocore config options (can override defaults)
        # See: https://botocore.amazonaws.com/v1/documentation/api/latest/reference/config.html
        user_config = kwargs.get('config', {})
        if isinstance(user_config, dict):
            conf_kwargs.update(user_config)

        # Force unsigned requests for anonymous access (cannot be overridden)
        if self.anonymous:
            conf_kwargs['signature_version'] = botocore.UNSIGNED

        self.conf = AioConfig(**conf_kwargs)
        self.client = None  # Will be initialized on first use
        self._client_lock = asyncio.Lock()  # Prevents race condition on init


    async def _ensure_client(self):
        """Ensure the shared client is initialized (thread-safe)"""
        if self.client is None:
            async with self._client_lock:
                # Double-check after acquiring lock
                if self.client is None:
                    self.client = await self.session.create_client(
                        's3',
                        config=self.conf,
                        **self.client_kwargs
                    ).__aenter__()
        return self.client


    async def close(self):
        """Clean up the client and release resources."""
        if self.client is not None:
            await self.client.__aexit__(None, None, None)
            self.client = None


    @override
    async def head_object(self, key: str):
        real_key = key
        if self.bucket_prefix:
            real_key = os.path.join(self.bucket_prefix, key) if key else self.bucket_prefix

        # Ensure the shared client is initialized
        await self._ensure_client()

        try:
            s3_res = await self.client.head_object(Bucket=self.bucket_name, Key=real_key)
            headers = {
                "ETag": s3_res.get("ETag"),
                "Accept-Ranges": "bytes",
                "Content-Length": str(s3_res.get("ContentLength")),
                "Last-Modified": s3_res.get("LastModified").strftime("%a, %d %b %Y %H:%M:%S GMT"),
            }

            content_type = guess_content_type(real_key)
            headers['Content-Type'] = content_type

            return Response(headers=headers)
        except Exception as e:
            return handle_s3_exception(e, key)


    @override
    async def open_object(self, key: str, range_header: str = None):
        """Open an S3 object and return a handle for streaming."""
        real_key = key
        if self.bucket_prefix:
            real_key = os.path.join(self.bucket_prefix, key) if key else self.bucket_prefix

        filename = os.path.basename(real_key)
        content_type = guess_content_type(filename)

        headers = {
            'Accept-Ranges': "bytes",
            'Content-Type': content_type,
        }

        if content_type == 'application/octet-stream':
            headers['Content-Disposition'] = f'attachment; filename="{filename}"'

        # Ensure the shared client is initialized
        await self._ensure_client()

        try:
            # Build S3 get_object parameters
            get_object_params = {
                "Bucket": self.bucket_name,
                "Key": real_key,
            }
            if range_header:
                get_object_params["Range"] = range_header

            # Call S3 get_object
            result = await self.client.get_object(**get_object_params)
            res_headers = result["ResponseMetadata"]["HTTPHeaders"]
            body = result["Body"]

            # Determine status code and add response headers
            status_code = 200
            content_length = 0

            if "content-range" in res_headers:
                status_code = 206  # Partial Content
                headers["Content-Range"] = res_headers["content-range"]

            if "content-length" in res_headers:
                headers["Content-Length"] = res_headers["content-length"]
                content_length = int(res_headers["content-length"])

            return S3ObjectHandle(
                target_name=self.target_name,
                key=key,
                status_code=status_code,
                headers=headers,
                media_type=content_type,
                content_length=content_length,
                body=body
            )

        except Exception as e:
            return handle_s3_exception(e, key)

    @override
    def stream_object(self, handle: S3ObjectHandle):
        """Stream content from an opened S3 object handle."""
        return S3Stream(
            body=handle.body,
            status_code=handle.status_code,
            headers=handle.headers,
            media_type=handle.media_type,
            target_name=handle.target_name,
            key=handle.key,
            content_length=handle.content_length,
        )

    @override
    async def get_object(self, key: str, range_header: str = None):
        """Convenience method that combines open_object() and stream_object()."""
        result = await self.open_object(key, range_header)
        if isinstance(result, S3ObjectHandle):
            try:
                return self.stream_object(result)
            except Exception:
                # Ensure body is closed if stream_object fails
                result.close()
                raise
        return result  # Error response


    @override
    async def list_objects_v2(self,
                            continuation_token: str,
                            delimiter: str,
                            encoding_type: str,
                            fetch_owner: str,
                            max_keys: str,
                            prefix: str,
                            start_after: str):

        # prefix user-supplied prefix with configured prefix
        real_prefix = prefix
        if self.bucket_prefix:
            real_prefix = os.path.join(self.bucket_prefix, prefix) if prefix else self.bucket_prefix

        # ensure the prefix ends with a slash
        if real_prefix and not real_prefix.endswith('/'):
            real_prefix += '/'

        # Ensure the shared client is initialized
        await self._ensure_client()

        try:
            params = {
                "Bucket": self.bucket_name,
                "ContinuationToken": continuation_token,
                "Delimiter": delimiter,
                "EncodingType": encoding_type,
                "FetchOwner": fetch_owner,
                "MaxKeys": max_keys,
                "Prefix": real_prefix,
                "StartAfter": start_after
            }
            # Remove any None values because boto3 doesn't like those
            params = {k: v for k, v in params.items() if v is not None}

            response = await self.client.list_objects_v2(**params)
            next_token = remove_prefix(self.bucket_prefix, response.get("NextContinuationToken", ""))
            is_truncated = "true" if response.get("IsTruncated", False) else "false"

            contents = []
            for obj in response.get("Contents", []):
                contents.append({
                    'Key': remove_prefix(self.bucket_prefix, obj["Key"]),
                    'LastModified': obj["LastModified"].isoformat(),
                    'ETag': obj.get("ETag"),
                    'Size': obj.get("Size"),
                    'StorageClass': obj.get("StorageClass")
                })

            common_prefixes = []
            for cp in response.get("CommonPrefixes", []):
                common_prefix = remove_prefix(self.bucket_prefix, cp["Prefix"])
                common_prefixes.append(common_prefix)

            kwargs = {
                'Name': self.target_name,
                'Prefix': prefix,
                'Delimiter': delimiter,
                'MaxKeys': max_keys,
                'EncodingType': encoding_type,
                'KeyCount': response.get("KeyCount", 0),
                'IsTruncated': is_truncated,
                'ContinuationToken': continuation_token,
                'NextContinuationToken': next_token,
                'StartAfter': start_after
            }

            xml = get_list_xml(contents, common_prefixes, url_encode=False, **kwargs)
            return Response(content=xml, media_type="application/xml")

        except Exception as e:
            return handle_s3_exception(e, key=prefix)


# Threshold for logging large transfers (10 MB)
LARGE_TRANSFER_THRESHOLD = 10 * 1024 * 1024


# Adapted from https://stackoverflow.com/questions/69617252/response-file-stream-from-s3-fastapi
class S3Stream(StreamingResponse):
    """Stream content from an S3 body stream."""

    def __init__(
            self,
            body: typing.Any,
            content: typing.Any = None,
            status_code: int = 200,
            headers: dict = None,
            media_type: str = None,
            background: BackgroundTask = None,
            target_name: str = None,
            key: str = None,
            content_length: int = None,
    ):
        super(S3Stream, self).__init__(content, status_code, headers, media_type, background)
        self.body = body
        self.target_name = target_name
        self.key = key
        self.content_length = content_length

    async def stream_response(self, send) -> None:
        body = self.body
        is_large = self.content_length is not None and self.content_length >= LARGE_TRANSFER_THRESHOLD

        await send({
            "type": "http.response.start",
            "status": self.status_code,
            "headers": self.raw_headers,
        })

        if is_large:
            logger.info(f"Large stream start: target={self.target_name}, key={self.key}, content_length={self.content_length}")

        # Stream the body - connection from pool stays active during streaming
        # Wrap in try/finally to ensure cleanup on client cancellation
        completed = False
        try:
            async for chunk in body:

                if not isinstance(chunk, bytes):
                    chunk = chunk.encode(self.charset)

                await send({
                    "type": "http.response.body",
                    "body": chunk,
                    "more_body": True
                })

            await send({
                "type": "http.response.body",
                "body": b"",
                "more_body": False})

            completed = True
            if is_large:
                logger.info(f"Large stream done: target={self.target_name}, key={self.key}, content_length={self.content_length}")

        except Exception as e:
            if is_large:
                logger.warning(f"Large stream error: target={self.target_name}, key={self.key}, content_length={self.content_length}, error={e}")
            raise

        finally:
            if is_large and not completed:
                logger.warning(f"Large stream cancelled: target={self.target_name}, key={self.key}, content_length={self.content_length}")
            # Always close body to release connection back to pool
            # This ensures cleanup even when client cancels mid-stream
            if body is not None and hasattr(body, 'close'):
                body.close()
