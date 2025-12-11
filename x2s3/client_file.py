import os
import sys
from dataclasses import dataclass
from hashlib import md5
from pathlib import Path
from typing import BinaryIO, Optional, Tuple
from typing_extensions import override

from loguru import logger
from fastapi.responses import Response, StreamingResponse, JSONResponse

from x2s3.utils import *
from x2s3.client import ProxyClient, ObjectHandle


# Default buffer size for file streaming (8 KB)
DEFAULT_BUFFER_SIZE = 8192


@dataclass
class FileObjectHandle(ObjectHandle):
    """Handle for file-based object storage."""
    file_handle: BinaryIO = None
    start: int = 0
    end: Optional[int] = None

    def close(self):
        """Close the file handle to release resources."""
        if self.file_handle is not None:
            self.file_handle.close()
            self.file_handle = None


STATIC_ETAG = '"11111111111111111111111111111111"'

def handle_exception(e, key=None):
    """ Handle various cases of generic errors.
    """
    logger.opt(exception=sys.exc_info()).error(f"Error for {key}")
    return JSONResponse({"error":"Internal server error"}, status_code=500)


def parse_range_header(range_header: str, file_size: int) -> Optional[Tuple[int, int]]:
    """Parse HTTP Range header and return start and end byte positions.
    
    Args:
        range_header: HTTP Range header value (e.g., "bytes=0-499")
        file_size: Total size of the file
        
    Returns:
        Tuple of (start, end) byte positions, or None if invalid
    """
    if not range_header or not range_header.startswith('bytes='):
        return None
    
    try:
        range_spec = range_header[6:]  # Remove 'bytes=' prefix
        if ',' in range_spec:
            # Multiple ranges not supported, return None to trigger 416
            return None

        if '-' not in range_spec:
            return None
            
        start_str, end_str = range_spec.split('-', 1)
        
        if start_str and end_str:
            # Both start and end specified: "bytes=0-499"
            start = int(start_str)
            end = int(end_str)
        elif start_str and not end_str:
            # Start specified, no end: "bytes=500-"
            start = int(start_str)
            end = file_size - 1
        elif not start_str and end_str:
            # End specified, no start (suffix range): "bytes=-500"
            suffix_length = int(end_str)
            start = max(0, file_size - suffix_length)
            end = file_size - 1
        else:
            return None
            
        # Validate range
        if start < 0 or end < 0 or start >= file_size or start > end:
            return None
            
        # Clamp end to file size
        end = min(end, file_size - 1)
        
        return (start, end)
        
    except (ValueError, IndexError):
        return None


# Threshold for logging large transfers (10 MB)
LARGE_TRANSFER_THRESHOLD = 10 * 1024 * 1024


def file_iterator(handle: FileObjectHandle, buffer_size: int = DEFAULT_BUFFER_SIZE):
    """Stream content from a FileObjectHandle.

    Args:
        handle: FileObjectHandle containing file handle and range info
        buffer_size: Size of chunks to read at a time

    Note: The file handle is closed when iteration completes or on error.
    """
    is_large = handle.content_length is not None and handle.content_length >= LARGE_TRANSFER_THRESHOLD
    if is_large:
        logger.info(f"Large stream start: target={handle.target_name}, key={handle.key}, content_length={handle.content_length}")
    completed = False
    try:
        fh = handle.file_handle
        fh.seek(handle.start)
        if handle.end is None:
            yield from fh
        else:
            remaining = handle.end - handle.start + 1
            while remaining > 0:
                chunk_size = min(buffer_size, remaining)
                chunk = fh.read(chunk_size)
                if not chunk:
                    break
                yield chunk
                remaining -= len(chunk)
        completed = True
        if is_large:
            logger.info(f"Large stream done: target={handle.target_name}, key={handle.key}, content_length={handle.content_length}")
    except Exception as e:
        if is_large:
            logger.warning(f"Large stream error: target={handle.target_name}, key={handle.key}, content_length={handle.content_length}, error={e}")
        raise
    finally:
        if is_large and not completed:
            logger.warning(f"Large stream cancelled: target={handle.target_name}, key={handle.key}, content_length={handle.content_length}")
        handle.close()


# From https://teppen.io/2018/10/23/aws_s3_verify_etags/
def calc_etag(inputfile, partsize):
    md5_digests = []
    with open(inputfile, 'rb') as f:
        for chunk in iter(lambda: f.read(partsize), b''):
            md5_digests.append(md5(chunk).digest())
    return md5(b''.join(md5_digests)).hexdigest() + '-' + str(len(md5_digests))


class FileProxyClient(ProxyClient):

    def __init__(self, proxy_kwargs, **kwargs):
        self.proxy_kwargs = proxy_kwargs or {}
        self.target_name = self.proxy_kwargs['target_name']
        self.root_path = str(Path(kwargs['path']).resolve())
        self.calculate_etags = kwargs.get('calculate_etags', False)
        self.buffer_size = kwargs.get('buffer_size', DEFAULT_BUFFER_SIZE)

    def _safe_path(self, key: str) -> Optional[str]:
        """Resolve key to absolute path and validate it's within root_path.

        Returns the safe absolute path, or None if path traversal detected.
        """
        # Join and resolve to absolute path
        path = os.path.realpath(os.path.join(self.root_path, key))
        # Ensure path is within root_path (prevent directory traversal)
        if not path.startswith(self.root_path + os.sep) and path != self.root_path:
            logger.warning(f"Path traversal attempt blocked: {key}")
            return None
        return path

    @override
    async def head_object(self, key: str):
        try:
            path = self._safe_path(key)
            if path is None:
                return get_nosuchkey_response(key)
            if not os.path.isfile(path):
                return get_nosuchkey_response(key)

            filename = os.path.basename(path)
            headers = {}

            content_type = guess_content_type(filename)
            headers['Accept-Ranges'] = 'bytes'
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
    async def open_object(self, key: str, range_header: str = None):
        """Open a file object and return a handle for streaming."""
        file_handle = None
        try:
            path = self._safe_path(key)
            if path is None:
                return get_nosuchkey_response(key)
            if not os.path.isfile(path):
                return get_nosuchkey_response(key)

            filename = os.path.basename(path)
            headers = {}

            content_type = guess_content_type(filename)
            headers['Content-Type'] = content_type
            headers['Accept-Ranges'] = 'bytes'
            if content_type == 'application/octet-stream':
                headers['Content-Disposition'] = f'attachment; filename="{filename}"'

            # Open file and use fstat for atomic metadata (avoids TOCTOU race)
            file_handle = open(path, "rb")
            stats = os.fstat(file_handle.fileno())
            file_size = stats.st_size
            headers["Last-Modified"] = format_timestamp_s3(stats.st_mtime)

            # Handle range requests
            if range_header:
                range_result = parse_range_header(range_header, file_size)
                if range_result is None:
                    # Invalid range, return 416 Range Not Satisfiable
                    file_handle.close()
                    headers["Content-Range"] = f"bytes */{file_size}"
                    return Response(
                        status_code=416,
                        headers=headers
                    )

                start, end = range_result
                content_length = end - start + 1

                # Set partial content headers
                headers["Content-Length"] = str(content_length)
                headers["Content-Range"] = f"bytes {start}-{end}/{file_size}"

                return FileObjectHandle(
                    target_name=self.target_name,
                    key=key,
                    status_code=206,  # Partial Content
                    headers=headers,
                    media_type=content_type,
                    content_length=content_length,
                    file_handle=file_handle,
                    start=start,
                    end=end
                )
            else:
                # Full content
                headers["Content-Length"] = str(file_size)
                return FileObjectHandle(
                    target_name=self.target_name,
                    key=key,
                    status_code=200,
                    headers=headers,
                    media_type=content_type,
                    content_length=file_size,
                    file_handle=file_handle,
                    start=0,
                    end=None
                )

        except Exception as e:
            if file_handle is not None:
                file_handle.close()
            return handle_exception(e, key)

    @override
    def stream_object(self, handle: FileObjectHandle):
        """Stream content from an opened file object handle."""
        return StreamingResponse(
            file_iterator(handle, self.buffer_size),
            status_code=handle.status_code,
            headers=handle.headers,
            media_type=handle.media_type
        )

    @override
    async def get_object(self, key: str, range_header: str = None):
        """Convenience method that combines open_object() and stream_object()."""
        result = await self.open_object(key, range_header)
        if isinstance(result, FileObjectHandle):
            try:
                return self.stream_object(result)
            except Exception:
                # Ensure file is closed if stream_object fails
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

        # ensure the prefix ends with a slash
        if real_prefix and not real_prefix.endswith('/'):
            real_prefix += '/'

        try:
            path = str(self.root_path)
            if real_prefix:
                path = self._safe_path(real_prefix)
                if path is None:
                    # Path traversal attempt - return empty listing
                    return Response(content=get_list_xml([], [], Name=self.target_name),
                                    media_type="application/xml")

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

            xml = get_list_xml(contents, common_prefixes, **kwargs)
            return Response(content=xml, media_type="application/xml")

        except Exception as e:
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

                        etag = STATIC_ETAG
                        if self.calculate_etags:
                            # This is VERY slow because it needs to read every file
                            etag = f'"{calc_etag(file_path, 8388608)}"'

                        contents.append({
                            'Key': key,
                            'Size': str(file_size),
                            'ETag': etag,
                            'LastModified': format_timestamp_s3(stats.st_mtime),
                            'StorageClass': 'STANDARD'
                        })

                if started and delimiter:
                    # CommonPrefixes are only generated when there is a delimiter
                    for d in dirs:
                        common_prefix = dir_path(os.path.join(p, d))
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
