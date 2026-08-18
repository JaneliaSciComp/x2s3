import inspect
import secrets
import urllib
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from email.utils import formatdate, parsedate_to_datetime
from mimetypes import guess_type
from html import escape

from loguru import logger
from dateutil import parser
from fastapi.responses import Response

# From https://stackoverflow.com/questions/1094841/get-a-human-readable-version-of-a-file-size
def humanize_bytes(num, suffix="B"):
    for unit in ("", "Ki", "Mi", "Gi", "Ti", "Pi", "Ei", "Zi"):
        if abs(num) < 1024.0:
            return f"{num:3.1f} {unit}{suffix}"
        num /= 1024.0
    return f"{num:.1f} Yi{suffix}"


def remove_prefix(prefix, key):
    """ Remove prefix from the key, and then the leading slash.
    """
    if key and prefix:
        return key.removeprefix(prefix).removeprefix('/')
    return key


def dir_path(path):
    """ Ensure that the given path ends in a slash, 
        indicating that it points to a folder and not an object.
    """
    if path and not path.endswith('/'):
        return path + '/'
    return path


def add_elem(parent, key):
    """ Add a new child element to the given XML parent.
    """
    return ET.SubElement(parent, key)


def add_telem(parent, key, value):
    """ Add a text element as a child of the given XML parent.
    """
    if value is None:
        return None
    elem = add_elem(parent, key)
    elem.text = str(value)
    return elem


def elem_to_str(elem):
    """ Render the given XML element to a string.
    """
    return ET.tostring(elem, encoding="utf-8", xml_declaration=True)


def parse_xml(xml):
    """ Parse the given XML string into an XML element.
        Strips namespace prefixes so callers can use plain tag names.
    """
    root = ET.fromstring(xml)
    for elem in root.iter():
        if '}' in elem.tag:
            elem.tag = elem.tag.split('}', 1)[1]
    return root


def url_encode(s):
    if not s: return None
    # AWS does something slightly strange here, maybe like this?
    return urllib.parse.quote(s, safe='/ ').replace(' ','+')


S3_XMLNS = "http://s3.amazonaws.com/doc/2006-03-01/"

# Characters used by AWS S3 for the x-amz-request-id value (uppercase alphanumeric).
_REQUEST_ID_ALPHABET = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"


def generate_request_id(length=16):
    """ Generate an S3-style request id: a short uppercase alphanumeric string.
        Used as the value of the x-amz-request-id response header so clients can
        reference a specific request when correlating logs or reporting issues.
    """
    return ''.join(secrets.choice(_REQUEST_ID_ALPHABET) for _ in range(length))


def get_bucket_list_xml(buckets):

    root = ET.Element("ListAllMyBucketsResult", xmlns=S3_XMLNS)
    buckets_elem = add_elem(root, "Buckets")

    for bucket in buckets:
        bucket_elem = add_elem(buckets_elem, "Bucket")
        add_telem(bucket_elem, "Name", bucket)

    return elem_to_str(root)


def get_list_xml(contents, common_prefixes, url_encode=True, **kwargs):
    """ Creates S3-style XML elements for the given object listing.
    """

    is_url_encode = False
    if url_encode and 'EncodingType' in kwargs:
        is_url_encode = kwargs['EncodingType']=='url'

    root = ET.Element("ListBucketResult", xmlns=S3_XMLNS)

    keys = [
        'Name',
        'Prefix',
        'StartAfter',
        'ContinuationToken',
        'NextContinuationToken',
        'KeyCount',
        'MaxKeys',
        'Delimiter',
        'EncodingType',
        'IsTruncated',
    ]

    for key in keys:
        value = kwargs.get(key)
        if is_url_encode and key in ['Delimiter', 'Prefix', 'StartAfter']:
            value = url_encode(value)
        add_telem(root, key, value)

    if common_prefixes:
        for cp in common_prefixes:
            value = cp
            if is_url_encode:
                value = url_encode(value)
            common_prefixes_elem = add_elem(root, "CommonPrefixes")
            add_telem(common_prefixes_elem, "Prefix", value)

    if contents:
        for obj in contents:
            key = obj["Key"]
            if is_url_encode:
                key = url_encode(key)
            contents_elem = add_elem(root, "Contents")
            add_telem(contents_elem, "Key", key)
            add_telem(contents_elem, "ETag", obj.get("ETag"))
            add_telem(contents_elem, "Size", obj.get("Size"))
            add_telem(contents_elem, "LastModified", obj.get("LastModified"))
            add_telem(contents_elem, "StorageClass", obj.get("StorageClass"))

    return elem_to_str(root)


def format_timestamp_s3(timestamp):
    """ Format the given timestamp to ISO date format compatible with AWS S3.
    """
    dt = datetime.fromtimestamp(timestamp, tz=timezone.utc)
    return dt.strftime("%Y-%m-%dT%H:%M:%S.000Z")


def format_isoformat_as_local(isodate):
    """ Given a date formatted with ISO format, parse it and output it as a 
        local date string for human consumption.
    """
    # Parse it
    dt = parser.isoparse(isodate)
    # Convert it to the local timezone
    dt = dt.astimezone()
    # Format it for humans
    return dt.strftime("%Y-%m-%d at %I:%M %p")


def get_nosuchkey_response(key):
    return Response(content=inspect.cleandoc(f"""
        <?xml version="1.0" encoding="UTF-8"?>
        <Error>
            <Code>NoSuchKey</Code>
            <Message>The specified key does not exist.</Message>
            <Key>{escape(key)}</Key>
        </Error>
        """), status_code=404, media_type="application/xml")


def get_nosuchbucket_response(bucket_name):
    return Response(content=inspect.cleandoc(f"""
        <?xml version="1.0" encoding="UTF-8"?>
        <Error>
            <Code>NoSuchBucket</Code>
            <Message>The specified bucket does not exist</Message>
            <BucketName>{escape(bucket_name)}</BucketName>
        </Error>
        """), status_code=404, media_type="application/xml")


def get_accessdenied_response():
    return Response(content=inspect.cleandoc("""
        <?xml version="1.0" encoding="UTF-8"?>
        <Error>
            <Code>AccessDenied</Code>
            <Message>Access Denied</Message>
        </Error>
        """), status_code=403, media_type="application/xml")


def get_error_response(status_code, error_code, message, resource):
    return Response(content=inspect.cleandoc(f"""
        <?xml version="1.0" encoding="UTF-8"?>
        <Error>
            <Code>{escape(error_code)}</Code>
            <Message>{escape(message)}</Message>
            <Resource>{escape(resource)}</Resource>
        </Error>
        """),
        status_code=status_code,
        media_type="application/xml")


def get_read_access_acl():
    """ Returns an S3 ACL that grants full read access
    """
    acl_xml = inspect.cleandoc("""
    <AccessControlPolicy>
        <Owner>
            <ID>1</ID>
            <DisplayName>unknown</DisplayName>
        </Owner>
        <AccessControlList>
            <Grant>
                <Grantee xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:type="Group">
                    <URI>http://acs.amazonaws.com/groups/global/AllUsers</URI>
                </Grantee>
                <Permission>READ</Permission>
            </Grant>
        </AccessControlList>
    </AccessControlPolicy>
    """)
    return Response(content=acl_xml, media_type="application/xml")


def guess_content_type(filename):
    """ A wrapper for guess_type which deals with unknown MIME types
    """
    content_type, _ = guess_type(filename)
    if content_type:
        return content_type
    else:
        if filename.endswith('.yaml'):
            # Should be application/yaml but that doesn't display in current browsers
            # See https://httptoolkit.com/blog/yaml-media-type-rfc/
            return 'text/plain+yaml'
        else:
            return 'application/octet-stream'


# Zarr chunks are effectively immutable by path, but datasets are sometimes
# overwritten in place, so one hour is the worst-case staleness we accept in
# exchange for stopping sub-second chunk re-fetch storms. No `immutable`.
CACHE_CONTROL_PUBLIC = "public, max-age=3600"


def make_file_etag(mtime: float, size: int) -> str:
    """Strong ETag for a file, derived from its mtime and size.

    Same scheme as fileglancer's make_etag, so the two repos agree on the
    validator for the same file.

    ponytail: mtime granularity is the ceiling — two writes within the
    filesystem's mtime resolution that also keep the same size would share an
    ETag. Switch to a content hash if that ever matters.

    The "-" separator is load-bearing, not cosmetic: the AWS Java SDK v1
    skips its MD5 integrity check whenever `eTag.contains("-")`, on the
    assumption that a hyphen means a multipart-upload ETag (which isn't an
    MD5 of the body). That skip is the only reason handing out an ETag here
    doesn't reintroduce the "Unable to verify integrity of data download"
    failure that tests/java/.../S3v1IntegrityTest.java exists to reproduce,
    and that `proxy_etag=False` defaults exist to avoid. Changing the
    separator to `_` or `:` would make the SDK run the MD5 check again and
    fail it, breaking Fiji/N5 Viewer.
    """
    return f'"{mtime:.6f}-{size}"'


def format_http_date(timestamp) -> str:
    """Format a POSIX timestamp as an RFC 7231 HTTP-date.

    Distinct from format_timestamp_s3, which produces the ISO form S3 uses in
    listing XML bodies. Headers must use this one or caches cannot read them.
    """
    return formatdate(timestamp, usegmt=True)


def _etag_matches(if_none_match: str, etag: str) -> bool:
    """True if any entity-tag in an If-None-Match header matches ours."""
    if not etag:
        return False
    for candidate in if_none_match.split(','):
        candidate = candidate.strip()
        if candidate == '*':
            return True
        if candidate.startswith('W/'):
            candidate = candidate[2:]
        if candidate == etag:
            return True
    return False


def if_range_matches(if_range: str, response_headers) -> bool:
    """True if an If-Range validator exactly matches ETag or Last-Modified.

    response_headers may be a plain dict with canonical capitalization (see
    check_not_modified above), so lookups here are lowercased explicitly.

    Unlike If-None-Match, If-Range carries exactly one validator, never a
    comma-separated list, and a weak ETag (W/"...") is never a valid match
    (RFC 9110 13.1.5). So this is just two exact string comparisons: against
    ETag, then against Last-Modified.
    """
    lowered = {k.lower(): v for k, v in response_headers.items()}
    etag = lowered.get('etag')
    last_modified = lowered.get('last-modified')
    if etag is not None and if_range == etag:
        return True
    if last_modified is not None and if_range == last_modified:
        return True
    return False


def check_not_modified(request_headers, response_headers):
    """Return a 304 response if the request's validators match, else None.

    request_headers is a case-insensitive mapping (Starlette Headers).
    response_headers may be a plain dict with canonical capitalization, which
    is what Fileglancer receives back from its user worker, so lookups here are
    lowercased explicitly.
    """
    lowered = {k.lower(): v for k, v in response_headers.items()}
    etag = lowered.get('etag')
    last_modified = lowered.get('last-modified')

    if_none_match = request_headers.get('if-none-match')
    if if_none_match is not None:
        # If-None-Match wins outright: when it is present and does not match,
        # If-Modified-Since must not be consulted (RFC 9110 13.1.3).
        if not _etag_matches(if_none_match, etag):
            return None
    else:
        if_modified_since = request_headers.get('if-modified-since')
        if not if_modified_since or not last_modified:
            return None
        try:
            since = parsedate_to_datetime(if_modified_since)
            modified = parsedate_to_datetime(last_modified)
            if since is None or modified is None or modified > since:
                return None
        except (TypeError, ValueError):
            # parsedate_to_datetime returns a NAIVE datetime for a date with no
            # zone, which clients do send. Comparing it to our aware one raises
            # TypeError, so the comparison has to sit inside the guard or a
            # slightly-off header becomes a 500.
            return None

    headers = {name: lowered[name.lower()]
               for name in ('ETag', 'Last-Modified', 'Cache-Control')
               if lowered.get(name.lower())}
    return Response(status_code=304, headers=headers)
