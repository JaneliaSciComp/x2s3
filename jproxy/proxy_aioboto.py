import os
import sys
import inspect
import typing
from typing_extensions import override

from loguru import logger
from starlette.background import BackgroundTask
import botocore
from aiobotocore.session import get_session
from aiobotocore.config import AioConfig
from botocore.exceptions import NoCredentialsError, PartialCredentialsError
from fastapi.responses import Response, StreamingResponse, JSONResponse

from jproxy.utils import *
from jproxy.client import ProxyClient
from jproxy.settings import S3LikeTarget


def get_nosuchkey_response(key):
    return Response(content=inspect.cleandoc(f"""
        <?xml version="1.0" encoding="UTF-8"?>
        <Error>
            <Code>NoSuchKey</Code>
            <Message>The specified key does not exist.</Message>
            <Key>{key}</Key>
        </Error>
        """), status_code=404, media_type="application/xml")


def handle_s3_exception(e, key=None):
    """ Handle various cases of generic errors from the boto AWS API.
    """
    if isinstance(e, (NoCredentialsError, PartialCredentialsError)):
        logger.opt(exception=sys.exc_info()).error("AWS credentials not configured properly")
        return JSONResponse({"error":"AWS credentials not configured properly"}, status_code=408)
    elif isinstance(e, botocore.exceptions.ReadTimeoutError):
        return JSONResponse({"error":"Upstream endpoint timed out"}, status_code=408)
    elif isinstance(e, botocore.exceptions.ClientError):
        code = e.response['ResponseMetadata']['HTTPStatusCode']
        if e.response["Error"]["Code"] == "NoSuchKey":
            return get_nosuchkey_response(key)
        elif int(code) == 404 and key:
            return get_nosuchkey_response(key)
        else:
            logger.opt(exception=sys.exc_info()).error("Error using boto S3 API")
            return JSONResponse({"error":"Error communicating with AWS S3"}, status_code=code)
    else:
        logger.opt(exception=sys.exc_info()).error("Error communicating with AWS S3")
        return JSONResponse({"error":"Error communicating with AWS S3"}, status_code=500)


# Adapted from https://stackoverflow.com/questions/69617252/response-file-stream-from-s3-fastapi
class S3Stream(StreamingResponse):
    """ Stream the result of GetObject.
    """
    def __init__(
            self,
            client_creator: typing.Callable,
            content: typing.Any = None,
            status_code: int = 200,
            headers: dict = None,
            media_type: str = None,
            background: BackgroundTask = None,
            bucket: str = None,
            key: str = None
    ):
        super(S3Stream, self).__init__(content, status_code, headers, media_type, background)
        self.client_creator = client_creator
        self.bucket = bucket
        self.key = key

    async def stream_response(self, send) -> None:
        async with self.client_creator() as client:
            try:
                result = await client.get_object(Bucket=self.bucket, Key=self.key)

                await send({
                    "type": "http.response.start",
                    "status": self.status_code,
                    "headers": self.raw_headers,
                })

                async for chunk in result["Body"]:

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

            except client.exceptions.NoSuchKey:
                r = get_nosuchkey_response(self.key)
                await send({
                    "type": "http.response.start",
                    "status": r.status_code,
                    "headers": r.raw_headers,
                })
                await send({
                    "type": "http.response.body",
                    "body": r.body,
                    "more_body": False,
                })


class AiobotoProxyClient(ProxyClient):

    def __init__(self, config: S3LikeTarget):

        self.target_name = config.name
        self.bucket_name = config.bucket
        self.prefix = config.prefix

        self.anonymous = True
        access_key,secret_key = '',''

        if config.credentials:
            self.anonymous = False
            access_key_path = config.credentials.accessKeyPath
            secret_key_path = config.credentials.secretKeyPath

            with open(access_key_path, 'r') as ak_file:
                access_key = ak_file.read().strip()

            with open(secret_key_path, 'r') as sk_file:
                secret_key = sk_file.read().strip()

        self.client_kwargs = {
            'aws_access_key_id': access_key,
            'aws_secret_access_key': secret_key,
            'endpoint_url': str(config.endpoint)
        }


    def get_client_creator(self):
        session = get_session()
        conf = AioConfig(signature_version=botocore.UNSIGNED) if self.anonymous else AioConfig()
        return session.create_client('s3', config=conf, **self.client_kwargs)


    @override
    async def head_object(self, key: str):
        async with self.get_client_creator() as client:
            try:
                s3_res = await client.head_object(Bucket=self.bucket_name, Key=key)
                headers = {
                    "ETag": s3_res.get("ETag"),
                    "Content-Type": s3_res.get("ContentType"),
                    "Content-Length": str(s3_res.get("ContentLength")),
                    "Last-Modified": s3_res.get("LastModified").strftime("%a, %d %b %Y %H:%M:%S GMT")
                }
                return Response(headers=headers)
            except Exception as e:
                return handle_s3_exception(e, key)


    @override
    async def get_object(self, key: str):
        if self.prefix:
            key = os.path.join(self.prefix, key) if key else self.prefix

        try:
            filename = os.path.basename(key)
            return S3Stream(
                self.get_client_creator,
                bucket=self.bucket_name,
                key=key,
                media_type='application/octet-stream',
                headers={
                    'Content-Disposition': f'attachment; filename="{filename}"'
                })
        except Exception as e:
            return handle_s3_exception(e, key)


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
        if self.prefix:
            prefix = os.path.join(self.prefix, prefix) if prefix else self.prefix

        # ensure the prefix ends with a slash
        if prefix and not prefix.endswith('/'):
            prefix += '/'

        async with self.get_client_creator() as client:
            try:
                params = {
                    "Bucket": self.bucket_name,
                    "ContinuationToken": continuation_token,
                    "Delimiter": delimiter,
                    "EncodingType": encoding_type,
                    "FetchOwner": fetch_owner,
                    "MaxKeys": max_keys,
                    "Prefix": prefix,
                    "StartAfter": start_after
                }
                logger.info(params)
                # Remove any None values because boto3 doesn't like those
                params = {k: v for k, v in params.items() if v is not None}

                response = await client.list_objects_v2(**params)
                res_prefix = remove_prefix(self.prefix, prefix)
                next_token = remove_prefix(self.prefix, response.get("NextContinuationToken", ""))
                truncated = "true" if response.get("IsTruncated", False) else "false"

                root = ET.Element("ListBucketResult")
                add_telem(root, "IsTruncated", truncated)
                add_telem(root, "Name", self.target_name)
                add_telem(root, "Prefix", res_prefix)
                add_telem(root, "Delimiter", delimiter)
                add_telem(root, "MaxKeys", max_keys)
                add_telem(root, "EncodingType", encoding_type)
                add_telem(root, "KeyCount", response.get("KeyCount", 0))
                add_telem(root, "ContinuationToken", continuation_token)
                add_telem(root, "NextContinuationToken", next_token)
                add_telem(root, "ContinuationToken", continuation_token)
                add_telem(root, "StartAfter", start_after)

                common_prefixes = add_elem(root, "CommonPrefixes")
                for cp in response.get("CommonPrefixes", []):
                    common_prefix = remove_prefix(self.prefix, cp["Prefix"])
                    add_telem(common_prefixes, "Prefix", common_prefix)

                for obj in response.get("Contents", []):
                    contents = add_elem(root, "Contents")
                    add_telem(contents, "Key", remove_prefix(self.prefix, obj["Key"]))
                    add_telem(contents, "LastModified", obj["LastModified"].isoformat())
                    add_telem(contents, "ETag", obj["ETag"])
                    add_telem(contents, "Size", obj["Size"])
                    add_telem(contents, "StorageClass", obj.get("StorageClass", ""))

                    if "Owner" in obj:
                        display_name = obj["Owner"]["DisplayName"] if "DisplayName" in obj["Owner"] else ''
                        owner_id = obj["Owner"]["ID"] if "ID" in obj["Owner"] else ''
                        owner = add_elem(root, "Owner")
                        add_telem(owner, "DisplayName", display_name)
                        add_telem(owner, "ID", owner_id)

                xml_output = elem_to_str(root)
                return Response(content=xml_output, media_type="application/xml")

            except Exception as e:
                return handle_s3_exception(e, key=prefix)
