import os
import sys
from typing_extensions import override

import fsspec
from loguru import logger
from fsspec.exceptions import FSTimeoutError
from fastapi.responses import Response, JSONResponse, StreamingResponse

from jproxy.utils import *
from jproxy.client import ProxyClient
from jproxy.settings import S3LikeTarget

def handle_s3_exception(e):
    """ Handle various cases of generic errors from the fsspec library.
    """
    if isinstance(e, FSTimeoutError):
        return JSONResponse({"error":"Upstream endpoint timed out"}, status_code=408)
    else:
        logger.opt(exception=sys.exc_info()).error("Error using fsspec S3 API")
        return JSONResponse({"error":"Error communicating with AWS S3"}, status_code=500)


def get_nosuchkey_response(key):
    return Response(content=f"""
        <?xml version="1.0" encoding="UTF-8"?>
        <Error>
            <Code>NoSuchKey</Code>
            <Message>The specified key does not exist.</Message>
            <Key>{key}</Key>
        </Error>
        """, status_code=404, media_type="application/xml")


class FSSpecProxyClient(ProxyClient):

    def __init__(self, config: S3LikeTarget):

        self.target_name = config.name
        self.bucket_name = config.bucket
        self.prefix = config.prefix
        self.full_prefix = os.path.join(self.bucket_name, self.prefix) if self.prefix else self.bucket_name

        anon=True
        access_key = None
        secret_key = None

        if config.credentials:
            access_key_path = config.credentials.accessKeyPath
            secret_key_path = config.credentials.secretKeyPath

            with open(access_key_path, 'r') as ak_file:
                access_key = ak_file.read().strip()

            with open(secret_key_path, 'r') as sk_file:
                secret_key = sk_file.read().strip()

        if access_key and secret_key:
            anon=False

        self.fs = fsspec.filesystem(
            's3',
            anon=anon,
            key=access_key,
            secret=secret_key,
            client_kwargs={'endpoint_url': str(config.endpoint)}
        )


    @override
    async def head_object(self, key: str):
        try:
            path = f"{self.bucket_name}/{key}"
            info = self.fs.info(path)
            content_type = info.get('ContentType')
            if not content_type:
                # TODO: VAST sometimes drops content-type for some reason..
                #       we need a more robust way to infer it
                if key.endswith('.json'):
                    content_type = 'application/json'
                else:
                    content_type = 'application/octet-stream'
            if info['type'] == 'file':
                headers = {
                    "ETag": info.get("ETag"),
                    "Content-Type": content_type,
                    "Content-Length": str(info.get("size")),
                    "Last-Modified": info.get("LastModified").strftime("%a, %d %b %Y %H:%M:%S GMT")
                }
                return Response(headers=headers)
            elif info['type'] == 'directory':
                # S3 does not support HEAD for directories
                return get_nosuchkey_response(key)
            else:
                logger.error(f"Unknown object type {info['type']} for {path}")
                return Response(status_code=500)

        except FileNotFoundError:
            return get_nosuchkey_response(key)
        except Exception as e:
            return handle_s3_exception(e)


    @override
    async def get_object(self, key: str):
        if self.prefix:
            key = os.path.join(self.prefix, key) if key else self.prefix

        if key.endswith('/'):
            # Cheap check for directories. If a user passes a directory here without
            # an ending slash then they will get a 0-length file, which is not correct,
            # but it's something we can't fix without running an extra HEAD call.
            return get_nosuchkey_response(key)

        try:
            file = self.fs.open(f"{self.bucket_name}/{key}", mode='rb')
            filename = os.path.basename(key)
            return StreamingResponse(file, media_type='application/octet-stream', headers={
                'Content-Disposition': f'attachment; filename="{filename}"'
            })
        except FileNotFoundError as e:
            return get_nosuchkey_response(key)
        except Exception as e:
            return handle_s3_exception(e)


    @override
    async def list_objects_v2(self,
                              continuation_token: str = None,
                              delimiter: str = '/',
                              encoding_type: str = None,
                              fetch_owner: str = None,
                              max_keys: int = None,
                              prefix: str = None,
                              start_after: str = None):

        if not prefix:
            prefix = ''

        if self.prefix:
            prefix = os.path.join(self.prefix, prefix) if prefix else self.prefix

        if prefix and not prefix.endswith('/'):
            prefix += '/'

        try:
            paths = self.fs.ls(f"{self.bucket_name}/{prefix}", detail=True)
            res_prefix = remove_prefix(self.full_prefix, prefix)
            root = ET.Element("ListBucketResult")
            add_telem(root, "Name", self.target_name)
            add_telem(root, "Prefix", res_prefix)
            add_telem(root, "Delimiter", delimiter)
            add_telem(root, "MaxKeys", max_keys)
            add_telem(root, "EncodingType", encoding_type)
            add_telem(root, "ContinuationToken", continuation_token)
            add_telem(root, "StartAfter", start_after)

            common_prefixes = add_elem(root, "CommonPrefixes")
            c = 0
            truncated = False
            continued = False
            contents = []
            for path in paths:
                key = remove_prefix(self.full_prefix, path['name'])
                if path['type'] == 'directory':
                    key = dir_path(key)

                # Try to emulate pagination using fsspec, but it's very inefficient
                # since it doesn't have a way to start at a given marker/file.
                if continuation_token and not continued:
                    if key==continuation_token:
                        continued = True
                    continue

                if path['type'] == 'directory':
                    add_telem(common_prefixes, "Prefix", key)
                else:
                    contents = add_elem(root, "Contents")
                    add_telem(contents, "Key", key)
                    add_telem(contents, "LastModified", path['LastModified'].isoformat())
                    add_telem(contents, "ETag", path['ETag'])
                    add_telem(contents, "Size", path['size'])
                    add_telem(contents, "StorageClass", path.get("StorageClass", ""))

                c += 1
                if c == max_keys:
                    truncated = True
                    add_telem(root, "IsTruncated", True)
                    add_telem(root, "NextContinuationToken", key)
                    break

            if not truncated:
                add_telem(root, "IsTruncated", False)
                add_telem(root, "NextContinuationToken", None)

            add_telem(root, "KeyCount", c)
            xml_output = elem_to_str(root)
            return Response(content=xml_output, media_type="application/xml")

        except Exception as e:
            return handle_s3_exception(e)
