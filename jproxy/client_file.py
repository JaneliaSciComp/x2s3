import os
import sys
from hashlib import md5
from pathlib import Path
from typing_extensions import override

from loguru import logger
from fastapi.responses import Response, StreamingResponse, JSONResponse

from jproxy.utils import *
from jproxy.client import ProxyClient

# This introduced latency and is usually not necessary
CALCULATE_ETAGS = False

def handle_exception(e, key=None):
    """ Handle various cases of generic errors.
    """
    logger.opt(exception=sys.exc_info()).error(f"Error for {key}")
    return JSONResponse({"error":"Internal server error"}, status_code=500)


def file_iterator(file_path: Path):
    """ Open a file in binary mode and stream the content
    """
    with open(file_path, "rb") as file:
        yield from file


class FileProxyClient(ProxyClient):

    def __init__(self, proxy_kwargs, **kwargs):
        self.proxy_kwargs = proxy_kwargs or {}
        self.target_name = self.proxy_kwargs['target_name']
        self.target_prefix = self.proxy_kwargs.get('prefix')
        self.root_path = str(Path(kwargs['root']).resolve())

    @override
    async def head_object(self, key: str):
        try:
            path = os.path.join(self.root_path, key)
            if not os.path.isfile(path):
                return get_nosuchkey_response(key)
            
            filename = os.path.basename(path)
            headers = {}

            content_type = guess_content_type(filename)
            headers['Content-Type'] = content_type
            if content_type=='application/octet-stream':
                headers['Content-Disposition'] = f'attachment; filename="{filename}"'

            stats = os.stat(path)
            file_size = stats.st_size
            headers["Content-Length"] = str(file_size)
            headers["Last-Modified"] = format_timestamp_s3(stats.st_mtime)

            return Response(headers=headers)
        except Exception as e:
            return handle_exception(e, key)


    @override
    async def get_object(self, key: str):
        if self.target_prefix:
            key = os.path.join(self.target_prefix, key) if key else self.target_prefix

        try:
            path = os.path.join(self.root_path, key)
            if not os.path.isfile(path):
                return get_nosuchkey_response(key)
            
            filename = os.path.basename(path)
            headers = {}

            content_type = guess_content_type(filename)
            headers['Content-Type'] = content_type
            if content_type=='application/octet-stream':
                headers['Content-Disposition'] = f'attachment; filename="{filename}"'

            stats = os.stat(path)
            file_size = stats.st_size
            headers["Content-Length"] = str(file_size)
            headers["Last-Modified"] = format_timestamp_s3(stats.st_mtime)

            return StreamingResponse(file_iterator(path), headers=headers, media_type=content_type)

        except Exception as e:
            return handle_exception(e, key)



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
        if self.target_prefix:
            real_prefix = os.path.join(self.target_prefix, prefix) if prefix else self.target_prefix

        # ensure the prefix ends with a slash
        if real_prefix and not real_prefix.endswith('/'):
            real_prefix += '/'

        try:
            path = str(self.root_path)
            if real_prefix:
                path = os.path.join(path, real_prefix)

            logger.debug(f"root_path: {self.root_path}, real_prefix: {real_prefix}, path: {path}")

            res = self.walk_path(path, continuation_token, delimiter, max_keys)
            contents = res['contents']
            is_truncated = res['is_truncated']
            common_prefixes = sorted(res['common_prefixes'])

            kwargs = {
                'Name': self.target_name,
                'Prefix': prefix,
                'Delimiter': delimiter,
                'MaxKeys': max_keys,
                'EncodingType': encoding_type,
                'KeyCount': len(contents) + len(common_prefixes),
                'IsTruncated': is_truncated,
                'ContinuationToken': continuation_token,
                'NextContinuationToken': res['next_token'],
                'StartAfter': start_after
            }

            root = get_list_xml_elem(contents, common_prefixes, **kwargs)
            return Response(content=elem_to_str(root), media_type="application/xml")

        except Exception as e:
            print(e)
            return handle_exception(e, key=prefix)


    def walk_path(self, path, continuation_token, delimiter, max_keys):
        commons = set()
        contents = []

        if os.path.isdir(path):
            started = continuation_token is None
            for root, dirs, filenames in os.walk(path):
                logger.trace(f"root={root}, dirs={dirs}")

                dirs.sort() # recurse in predictable (sorted) order
                p = remove_prefix(str(self.root_path), root)

                for filename in filenames:
                    file_path = os.path.join(root, filename)
                    key = os.path.join(p, filename)

                    started = started or continuation_token == key
                    logger.trace(f"found {key} (started={started}, len={len(contents)})")

                    if len(contents)+len(commons) == max_keys:
                        # Reached max keys to be retrieved
                        return {
                            'contents': contents, 
                            'common_prefixes': commons, 
                            'next_token': key,
                            'is_truncated': 'true'
                        }

                    if started:
                        # Get details
                        stats = os.stat(file_path)
                        file_size = stats.st_size

                        etag = '"48ed760a742c2263777c00b27df3024c"'
                        if CALCULATE_ETAGS:
                            # This is VERY slow because it needs to read every file it 
                            # the 8388608 part size is used by AWS CLI and boto3
                            etag = f'"{calc_etag(file_path, 8388608)}"'

                        contents.append({
                            'Key': remove_prefix(self.target_prefix, key),
                            'Size': str(file_size),
                            'ETag': etag,
                            'LastModified': format_timestamp_s3(stats.st_mtime),
                            'StorageClass': 'STANDARD'
                        })

                if started and delimiter:
                    # CommonPrefixes are only generated when there is a delimiter
                    for d in dirs:
                        common_prefix = dir_path(os.path.join(p, d))
                        common_prefix = remove_prefix(self.target_prefix, common_prefix)
                        commons.add(common_prefix)

                if delimiter=='/':
                    # Do not recurse
                    break

        return {
            'contents': contents, 
            'common_prefixes': commons, 
            'next_token': None,
            'is_truncated': 'false'
        }

# From https://teppen.io/2018/10/23/aws_s3_verify_etags/
def calc_etag(inputfile, partsize):
    md5_digests = []
    with open(inputfile, 'rb') as f:
        for chunk in iter(lambda: f.read(partsize), b''):
            md5_digests.append(md5(chunk).digest())
    return md5(b''.join(md5_digests)).hexdigest() + '-' + str(len(md5_digests))
