import os
import sys
from typing_extensions import override

from loguru import logger
import boto3
import botocore
from botocore.exceptions import NoCredentialsError, PartialCredentialsError
from fastapi import HTTPException
from fastapi.responses import Response, StreamingResponse, HTMLResponse, JSONResponse

from jproxy.utils import *
from jproxy.client import ProxyClient
from jproxy.settings import S3LikeTarget

# For debugging AWS API calls
#boto3.set_stream_logger(name='botocore')

def handle_s3_exception(e):
    """ Handle various cases of generic errors from the boto AWS API.
    """
    if isinstance(e, (NoCredentialsError, PartialCredentialsError)):
        logger.opt(exception=sys.exc_info()).info("AWS credentials not configured properly")
        raise HTTPException(status_code=500, detail="AWS credentials not configured properly")
    elif isinstance(e, botocore.exceptions.ReadTimeoutError):
        raise HTTPException(status_code=408, detail="Upstream endpoint timed out")
    elif isinstance(e, botocore.exceptions.ClientError):
        logger.opt(exception=sys.exc_info()).info("Error using boto S3 API")
        code = e.response['ResponseMetadata']['HTTPStatusCode']
        raise HTTPException(status_code=code, detail="Error communicating with AWS S3")
    else:
        logger.opt(exception=sys.exc_info()).info("Error using boto S3 API")
        raise HTTPException(status_code=500, detail="Error communicating with AWS S3")


def get_nosuchkey_response(key):
    return Response(content=f"""
        <?xml version="1.0" encoding="UTF-8"?>
        <Error>
            <Code>NoSuchKey</Code>
            <Message>The specified key does not exist.</Message>
            <Key>{key}</Key>
        </Error>
        """, status_code=404, media_type="application/xml")


class S3ProxyClient(ProxyClient):

    def __init__(self, config: S3LikeTarget):

        self.target_name = config.name
        self.bucket_name = config.bucket
        self.prefix = config.prefix

        anonymous = True
        access_key,secret_key = '',''

        if config.credentials:
            anonymous = False
            access_key_path = config.credentials.accessKeyPath
            secret_key_path = config.credentials.secretKeyPath

            with open(access_key_path, 'r') as ak_file:
                access_key = ak_file.read().strip()

            with open(secret_key_path, 'r') as sk_file:
                secret_key = sk_file.read().strip()

        self.client = boto3.client(
            's3',
            config=botocore.config.Config(
                user_agent="jproxy"
            ),
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            endpoint_url=str(config.endpoint)
        )

        if anonymous:
            # hack to disable signing:
            # https://stackoverflow.com/questions/34865927/can-i-use-boto3-anonymously
            self.client._request_signer.sign = (lambda *args, **kwargs: None)


    @override
    async def head_object(self, key: str):
        try:
            s3_res = self.client.head_object(Bucket=self.bucket_name, Key=key)
            headers = {
                "ETag": s3_res.get("ETag"),
                "Content-Type": s3_res.get("ContentType"),
                "Content-Length": str(s3_res.get("ContentLength")),
                "Last-Modified": s3_res.get("LastModified").strftime("%a, %d %b %Y %H:%M:%S GMT")
            }
            return Response(headers=headers)
        except self.client.exceptions.NoSuchKey:
            return get_nosuchkey_response(key)
        except Exception as e:
            handle_s3_exception(e)


    @override
    async def get_object(self, key: str):
        if self.prefix:
            key = os.path.join(self.prefix, key) if key else self.prefix

        try:
            response = self.client.get_object(Bucket=self.bucket_name, Key=key)
            filename = os.path.basename(key)
            return StreamingResponse(response['Body'], media_type='application/octet-stream', headers={
                'Content-Disposition': f'attachment; filename="{filename}"'
            })
        except self.client.exceptions.NoSuchKey:
            return get_nosuchkey_response(key)
        except Exception as e:
            handle_s3_exception(e)


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
            # Remove any None values because boto3 doesn't like those
            params = {k: v for k, v in params.items() if v is not None}

            response = self.client.list_objects_v2(**params)
            res_prefix = remove_prefix(self.prefix, prefix)
            next_token = remove_prefix(self.prefix, response.get("NextContinuationToken", ""))

            root = ET.Element("ListBucketResult")
            add_telem(root, "IsTruncated", response.get("IsTruncated", False))
            add_telem(root, "Name", self.target_name)
            add_telem(root, "Prefix", res_prefix)
            add_telem(root, "Delimiter", delimiter)
            add_telem(root, "MaxKeys", max_keys)
            add_telem(root, "EncodingType", encoding_type)
            add_telem(root, "KeyCount", response.get("KeyCount", 0))
            add_telem(root, "ContinuationToken", continuation_token)
            add_telem(root, "NextContinuationToken", next_token)
            add_telem(root, "ContinuationToken", continuation_token)
            #add_telem(root, "Marker", marker)
            #add_telem(root, "NextMarker", next_token) # a little hack
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
            handle_s3_exception(e)
