#!/usr/bin/env python3
"""
Compare real AWS S3 bucket behavior with x2s3 proxy behavior.

AWS:   https://s3.us-east-1.amazonaws.com/janelia-data-examples
Proxy: https://nextflow.int.janelia.org:8003/janelia-data-examples
"""

import requests
import xml.etree.ElementTree as ET
import json
import sys
import hashlib
import traceback
from collections import OrderedDict

AWS_BASE = "https://s3.us-east-1.amazonaws.com"
AWS_BUCKET = f"{AWS_BASE}/janelia-data-examples"
PROXY_BASE = "https://nextflow.int.janelia.org:8003"
PROXY_BUCKET = f"{PROXY_BASE}/janelia-data-examples"

TIMEOUT = 30

# Known files in the bucket
SMALL_JSON_KEY = "jrc_mus_lung_covid.n5/attributes.json"
SMALL_BINARY_KEY = "jrc_mus_lung_covid.n5/render/v1_acquire_align___20210609_224836/s0/0/0/5"
DIR_PREFIX = "jrc_mus_lung_covid.n5/"
NESTED_PREFIX = "jrc_mus_lung_covid.n5/render/"

results = []


def log(msg):
    print(msg)


def normalize_xml(xml_str):
    """Parse XML, stripping namespace prefixes."""
    try:
        root = ET.fromstring(xml_str)
        for elem in root.iter():
            if '}' in elem.tag:
                elem.tag = elem.tag.split('}', 1)[1]
        return root
    except ET.ParseError:
        return None


def compare_xml_elements(aws_root, proxy_root, path=""):
    """Deep compare two XML trees; return list of unique difference descriptions."""
    diffs = set()
    _compare_recursive(aws_root, proxy_root, path, diffs)
    return sorted(diffs)


def _compare_recursive(aws_elem, proxy_elem, path, diffs):
    aws_tag = aws_elem.tag.split('}')[-1] if '}' in aws_elem.tag else aws_elem.tag
    proxy_tag = proxy_elem.tag.split('}')[-1] if '}' in proxy_elem.tag else proxy_elem.tag

    if aws_tag != proxy_tag:
        diffs.add(f"Tag mismatch at {path}: AWS=<{aws_tag}> vs Proxy=<{proxy_tag}>")
        return

    current_path = f"{path}/{aws_tag}"

    # Compare text
    aws_text = (aws_elem.text or '').strip()
    proxy_text = (proxy_elem.text or '').strip()
    if aws_text != proxy_text:
        # Deduplicate by pattern rather than per-item
        if 'LastModified' in current_path:
            diffs.add(f"LastModified format differs: AWS uses '.000Z' suffix vs Proxy uses '+00:00' "
                      f"(e.g. AWS='{aws_text}' vs Proxy='{proxy_text}')")
        elif 'ContinuationToken' in current_path or 'NextContinuationToken' in current_path:
            diffs.add(f"ContinuationToken values differ (expected - opaque tokens are server-specific)")
        else:
            diffs.add(f"Text at {current_path}: AWS='{aws_text[:80]}' vs Proxy='{proxy_text[:80]}'")

    # Compare children
    aws_children = list(aws_elem)
    proxy_children = list(proxy_elem)

    aws_child_tags = [c.tag.split('}')[-1] if '}' in c.tag else c.tag for c in aws_children]
    proxy_child_tags = [c.tag.split('}')[-1] if '}' in c.tag else c.tag for c in proxy_children]

    aws_tag_set = set(aws_child_tags)
    proxy_tag_set = set(proxy_child_tags)

    for tag in aws_tag_set - proxy_tag_set:
        diffs.add(f"Element <{tag}> present in AWS but MISSING in Proxy at {current_path}")
    for tag in proxy_tag_set - aws_tag_set:
        diffs.add(f"Element <{tag}> present in Proxy but MISSING in AWS at {current_path}")

    # Group children by tag
    aws_by_tag = {}
    for c in aws_children:
        t = c.tag.split('}')[-1] if '}' in c.tag else c.tag
        aws_by_tag.setdefault(t, []).append(c)

    proxy_by_tag = {}
    for c in proxy_children:
        t = c.tag.split('}')[-1] if '}' in c.tag else c.tag
        proxy_by_tag.setdefault(t, []).append(c)

    for tag in aws_tag_set & proxy_tag_set:
        aws_list = aws_by_tag[tag]
        proxy_list = proxy_by_tag[tag]
        if len(aws_list) != len(proxy_list):
            diffs.add(f"Count of <{tag}> at {current_path}: AWS={len(aws_list)} vs Proxy={len(proxy_list)}")
        for i in range(min(len(aws_list), len(proxy_list))):
            _compare_recursive(aws_list[i], proxy_list[i], current_path, diffs)


def record_test(name, category, aws_status, proxy_status, diffs, notes="",
                aws_headers=None, proxy_headers=None):
    results.append({
        'name': name,
        'category': category,
        'aws_status': aws_status,
        'proxy_status': proxy_status,
        'status_match': aws_status == proxy_status,
        'diffs': diffs,
        'notes': notes,
        'aws_headers': aws_headers or {},
        'proxy_headers': proxy_headers or {},
    })


def do_request(method, url, headers=None, params=None, retries=2):
    for attempt in range(retries + 1):
        try:
            resp = requests.request(method, url, headers=headers, params=params,
                                    timeout=TIMEOUT, allow_redirects=False)
            return resp
        except Exception as e:
            log(f"  REQUEST EXCEPTION (attempt {attempt+1}): {type(e).__name__}: {e}")
            if attempt < retries:
                import time
                time.sleep(1)
    return None


def compare_headers(aws_resp, proxy_resp, keys):
    diffs = []
    aws_h, proxy_h = {}, {}
    for key in keys:
        aws_val = aws_resp.headers.get(key)
        proxy_val = proxy_resp.headers.get(key)
        if aws_val:
            aws_h[key] = aws_val
        if proxy_val:
            proxy_h[key] = proxy_val
        if aws_val != proxy_val:
            diffs.append(f"Header '{key}': AWS='{aws_val}' vs Proxy='{proxy_val}'")
    return aws_h, proxy_h, diffs


# ===========================================================================
# Tests
# ===========================================================================

def test_list_v2_basic():
    name = "ListObjectsV2 - basic defaults"
    log(f"\n--- {name} ---")
    params = {"list-type": "2", "max-keys": "5"}
    aws = do_request("GET", AWS_BUCKET, params=params)
    proxy = do_request("GET", PROXY_BUCKET, params=params)
    if aws is None or proxy is None:
        return record_test(name, "ListObjectsV2", getattr(aws, 'status_code', None),
                          getattr(proxy, 'status_code', None),
                          ["One or both requests failed"])

    diffs = []
    if aws.status_code != proxy.status_code:
        diffs.append(f"Status: AWS={aws.status_code} vs Proxy={proxy.status_code}")

    aws_root = normalize_xml(aws.text)
    proxy_root = normalize_xml(proxy.text)
    if aws_root is not None and proxy_root is not None:
        diffs.extend(compare_xml_elements(aws_root, proxy_root))

    aws_h, proxy_h, hd = compare_headers(aws, proxy, ['Content-Type', 'Server'])
    diffs.extend(hd)

    record_test(name, "ListObjectsV2", aws.status_code, proxy.status_code, diffs,
                aws_headers=aws_h, proxy_headers=proxy_h)


def test_list_v2_delimiter():
    name = "ListObjectsV2 - with delimiter=/"
    log(f"\n--- {name} ---")
    params = {"list-type": "2", "delimiter": "/"}
    aws = do_request("GET", AWS_BUCKET, params=params)
    proxy = do_request("GET", PROXY_BUCKET, params=params)
    if aws is None or proxy is None:
        return record_test(name, "ListObjectsV2", None, None, ["Request failed"])

    diffs = []
    if aws.status_code != proxy.status_code:
        diffs.append(f"Status: {aws.status_code} vs {proxy.status_code}")

    aws_root = normalize_xml(aws.text)
    proxy_root = normalize_xml(proxy.text)
    if aws_root is not None and proxy_root is not None:
        diffs.extend(compare_xml_elements(aws_root, proxy_root))

    record_test(name, "ListObjectsV2", aws.status_code, proxy.status_code, diffs)


def test_list_v2_prefix_delimiter():
    name = "ListObjectsV2 - prefix + delimiter"
    log(f"\n--- {name} ---")
    params = {"list-type": "2", "prefix": DIR_PREFIX, "delimiter": "/", "max-keys": "10"}
    aws = do_request("GET", AWS_BUCKET, params=params)
    proxy = do_request("GET", PROXY_BUCKET, params=params)
    if aws is None or proxy is None:
        return record_test(name, "ListObjectsV2", None, None, ["Request failed"])

    diffs = []
    if aws.status_code != proxy.status_code:
        diffs.append(f"Status: {aws.status_code} vs {proxy.status_code}")

    aws_root = normalize_xml(aws.text)
    proxy_root = normalize_xml(proxy.text)
    if aws_root is not None and proxy_root is not None:
        diffs.extend(compare_xml_elements(aws_root, proxy_root))

    record_test(name, "ListObjectsV2", aws.status_code, proxy.status_code, diffs)


def test_list_v2_max_keys():
    name = "ListObjectsV2 - max-keys=2 with delimiter"
    log(f"\n--- {name} ---")
    params = {"list-type": "2", "max-keys": "2", "delimiter": "/"}
    aws = do_request("GET", AWS_BUCKET, params=params)
    proxy = do_request("GET", PROXY_BUCKET, params=params)
    if aws is None or proxy is None:
        return record_test(name, "ListObjectsV2", None, None, ["Request failed"])

    diffs = []
    if aws.status_code != proxy.status_code:
        diffs.append(f"Status: {aws.status_code} vs {proxy.status_code}")

    aws_root = normalize_xml(aws.text)
    proxy_root = normalize_xml(proxy.text)
    if aws_root is not None and proxy_root is not None:
        diffs.extend(compare_xml_elements(aws_root, proxy_root))

    record_test(name, "ListObjectsV2", aws.status_code, proxy.status_code, diffs)


def test_list_v2_start_after():
    name = "ListObjectsV2 - start-after"
    log(f"\n--- {name} ---")
    params = {"list-type": "2", "start-after": "fly-efish/", "delimiter": "/", "max-keys": "5"}
    aws = do_request("GET", AWS_BUCKET, params=params)
    proxy = do_request("GET", PROXY_BUCKET, params=params)
    if aws is None or proxy is None:
        return record_test(name, "ListObjectsV2", None, None, ["Request failed"])

    diffs = []
    if aws.status_code != proxy.status_code:
        diffs.append(f"Status: {aws.status_code} vs {proxy.status_code}")

    aws_root = normalize_xml(aws.text)
    proxy_root = normalize_xml(proxy.text)
    if aws_root is not None and proxy_root is not None:
        diffs.extend(compare_xml_elements(aws_root, proxy_root))

    record_test(name, "ListObjectsV2", aws.status_code, proxy.status_code, diffs)


def test_list_v2_encoding_type():
    name = "ListObjectsV2 - encoding-type=url"
    log(f"\n--- {name} ---")
    params = {"list-type": "2", "encoding-type": "url", "delimiter": "/", "max-keys": "5"}
    aws = do_request("GET", AWS_BUCKET, params=params)
    proxy = do_request("GET", PROXY_BUCKET, params=params)
    if aws is None or proxy is None:
        return record_test(name, "ListObjectsV2", None, None, ["Request failed"])

    diffs = []
    if aws.status_code != proxy.status_code:
        diffs.append(f"Status: {aws.status_code} vs {proxy.status_code}")

    aws_root = normalize_xml(aws.text)
    proxy_root = normalize_xml(proxy.text)
    if aws_root is not None and proxy_root is not None:
        diffs.extend(compare_xml_elements(aws_root, proxy_root))

    record_test(name, "ListObjectsV2", aws.status_code, proxy.status_code, diffs)


def test_list_v2_pagination():
    name = "ListObjectsV2 - pagination"
    log(f"\n--- {name} ---")

    # Page 1
    params = {"list-type": "2", "max-keys": "1", "delimiter": "/"}
    aws1 = do_request("GET", AWS_BUCKET, params=params)
    proxy1 = do_request("GET", PROXY_BUCKET, params=params)
    if not aws1 or not proxy1:
        return record_test(name, "ListObjectsV2", None, None, ["Request failed"])

    diffs = []

    # Extract tokens
    aws_root1 = normalize_xml(aws1.text)
    proxy_root1 = normalize_xml(proxy1.text)

    aws_token = proxy_token = None
    if aws_root1 is not None:
        nct = aws_root1.find('NextContinuationToken')
        aws_token = nct.text if nct is not None else None
    if proxy_root1 is not None:
        nct = proxy_root1.find('NextContinuationToken')
        proxy_token = nct.text if nct is not None else None

    notes = f"Page 1 - AWS IsTruncated/token present: {aws_token is not None}, Proxy: {proxy_token is not None}"

    # Compare page 1 structure (ignoring token values)
    if aws_root1 is not None and proxy_root1 is not None:
        diffs.extend(compare_xml_elements(aws_root1, proxy_root1))

    # Page 2 - use each system's own token
    if aws_token and proxy_token:
        params2a = {"list-type": "2", "max-keys": "1", "delimiter": "/",
                    "continuation-token": aws_token}
        params2p = {"list-type": "2", "max-keys": "1", "delimiter": "/",
                    "continuation-token": proxy_token}
        aws2 = do_request("GET", AWS_BUCKET, params=params2a)
        proxy2 = do_request("GET", PROXY_BUCKET, params=params2p)

        if aws2 and proxy2:
            aws_root2 = normalize_xml(aws2.text)
            proxy_root2 = normalize_xml(proxy2.text)
            if aws_root2 is not None and proxy_root2 is not None:
                page2_diffs = compare_xml_elements(aws_root2, proxy_root2)
                for d in page2_diffs:
                    diffs.append(f"[Page2] {d}")
            notes += f"\nPage 2 status: AWS={aws2.status_code} Proxy={proxy2.status_code}"

    record_test(name, "ListObjectsV2", aws1.status_code, proxy1.status_code, diffs, notes)


def test_list_v2_nonexistent_prefix():
    name = "ListObjectsV2 - nonexistent prefix"
    log(f"\n--- {name} ---")
    params = {"list-type": "2", "prefix": "this-does-not-exist-xyz/"}
    aws = do_request("GET", AWS_BUCKET, params=params)
    proxy = do_request("GET", PROXY_BUCKET, params=params)
    if aws is None or proxy is None:
        return record_test(name, "ListObjectsV2", None, None, ["Request failed"])

    diffs = []
    if aws.status_code != proxy.status_code:
        diffs.append(f"Status: {aws.status_code} vs {proxy.status_code}")

    aws_root = normalize_xml(aws.text)
    proxy_root = normalize_xml(proxy.text)
    if aws_root is not None and proxy_root is not None:
        diffs.extend(compare_xml_elements(aws_root, proxy_root))

    record_test(name, "ListObjectsV2", aws.status_code, proxy.status_code, diffs)


def test_list_v2_no_list_type():
    """GET bucket without list-type param. AWS returns ListObjects v1, x2s3 returns v2."""
    name = "ListObjectsV2 - no list-type (v1 vs v2 behavior)"
    log(f"\n--- {name} ---")

    aws = do_request("GET", AWS_BUCKET, headers={"Accept": "application/xml"})
    proxy = do_request("GET", PROXY_BUCKET, headers={"Accept": "application/xml"})
    if aws is None or proxy is None:
        return record_test(name, "ListObjectsV2", None, None, ["Request failed"])

    diffs = []
    if aws.status_code != proxy.status_code:
        diffs.append(f"Status: {aws.status_code} vs {proxy.status_code}")

    aws_root = normalize_xml(aws.text)
    proxy_root = normalize_xml(proxy.text)

    aws_tag = aws_root.tag if aws_root is not None else None
    proxy_tag = proxy_root.tag if proxy_root is not None else None

    notes = f"AWS root: <{aws_tag}>, Proxy root: <{proxy_tag}>"

    # Check structural differences between v1 and v2
    if aws_root is not None and proxy_root is not None:
        diffs.extend(compare_xml_elements(aws_root, proxy_root))

    record_test(name, "ListObjectsV2", aws.status_code, proxy.status_code, diffs, notes)


def test_list_v2_trailing_slash():
    name = "ListObjectsV2 - trailing slash on bucket path"
    log(f"\n--- {name} ---")
    params = {"list-type": "2", "delimiter": "/", "max-keys": "5"}
    aws = do_request("GET", AWS_BUCKET + "/", params=params)
    proxy = do_request("GET", PROXY_BUCKET + "/", params=params)
    if aws is None or proxy is None:
        return record_test(name, "ListObjectsV2", None, None, ["Request failed"])

    diffs = []
    if aws.status_code != proxy.status_code:
        diffs.append(f"Status: {aws.status_code} vs {proxy.status_code}")

    aws_root = normalize_xml(aws.text)
    proxy_root = normalize_xml(proxy.text)
    if aws_root is not None and proxy_root is not None:
        diffs.extend(compare_xml_elements(aws_root, proxy_root))

    record_test(name, "ListObjectsV2", aws.status_code, proxy.status_code, diffs)


def test_list_v2_max_keys_zero():
    name = "ListObjectsV2 - max-keys=0 (edge case)"
    log(f"\n--- {name} ---")
    params = {"list-type": "2", "max-keys": "0"}
    aws = do_request("GET", AWS_BUCKET, params=params)
    proxy = do_request("GET", PROXY_BUCKET, params=params)

    aws_status = aws.status_code if aws is not None else None
    proxy_status = proxy.status_code if proxy is not None else None

    diffs = []
    if aws_status != proxy_status:
        diffs.append(f"Status: AWS={aws_status} vs Proxy={proxy_status}")

    notes = ""
    if aws is not None:
        notes += f"AWS: {aws.status_code} {aws.text[:300]}\n"
    if proxy is not None:
        notes += f"Proxy: {proxy.status_code} {proxy.text[:300]}"

    record_test(name, "ListObjectsV2", aws_status, proxy_status, diffs, notes)


def test_list_v2_max_keys_above_limit():
    name = "ListObjectsV2 - max-keys=1001 (above S3 limit)"
    log(f"\n--- {name} ---")
    params = {"list-type": "2", "max-keys": "1001"}
    aws = do_request("GET", AWS_BUCKET, params=params)
    proxy = do_request("GET", PROXY_BUCKET, params=params)

    aws_status = aws.status_code if aws is not None else None
    proxy_status = proxy.status_code if proxy is not None else None

    diffs = []
    if aws_status != proxy_status:
        diffs.append(f"Status: AWS={aws_status} vs Proxy={proxy_status}")

    notes = ""
    if aws is not None:
        notes += f"AWS: {aws.status_code} {aws.text[:300]}\n"
    if proxy is not None:
        notes += f"Proxy: {proxy.status_code} {proxy.text[:300]}"

    record_test(name, "ListObjectsV2", aws_status, proxy_status, diffs, notes)


def test_list_v1():
    name = "ListObjects - v1 (list-type=1)"
    log(f"\n--- {name} ---")
    params = {"list-type": "1"}
    aws = do_request("GET", AWS_BUCKET, params=params)
    proxy = do_request("GET", PROXY_BUCKET, params=params)

    aws_status = aws.status_code if aws is not None else None
    proxy_status = proxy.status_code if proxy is not None else None

    diffs = []
    if aws_status != proxy_status:
        diffs.append(f"Status: AWS={aws_status} vs Proxy={proxy_status}")

    notes = ""
    if aws:
        notes += f"AWS: {aws.status_code} {aws.text[:400]}\n"
    if proxy:
        notes += f"Proxy: {proxy.status_code} {proxy.text[:400]}"

    record_test(name, "ListObjects", aws_status, proxy_status, diffs, notes)


def test_list_v2_fetch_owner():
    name = "ListObjectsV2 - fetch-owner=true"
    log(f"\n--- {name} ---")
    params = {"list-type": "2", "fetch-owner": "true", "max-keys": "3", "prefix": DIR_PREFIX}
    aws = do_request("GET", AWS_BUCKET, params=params)
    proxy = do_request("GET", PROXY_BUCKET, params=params)
    if aws is None or proxy is None:
        return record_test(name, "ListObjectsV2", None, None, ["Request failed"])

    diffs = []
    if aws.status_code != proxy.status_code:
        diffs.append(f"Status: {aws.status_code} vs {proxy.status_code}")

    aws_root = normalize_xml(aws.text)
    proxy_root = normalize_xml(proxy.text)
    if aws_root is not None and proxy_root is not None:
        diffs.extend(compare_xml_elements(aws_root, proxy_root))

    record_test(name, "ListObjectsV2", aws.status_code, proxy.status_code, diffs)


def test_list_v2_no_delimiter_flat():
    name = "ListObjectsV2 - no delimiter (flat listing)"
    log(f"\n--- {name} ---")
    params = {"list-type": "2", "prefix": DIR_PREFIX, "max-keys": "5"}
    aws = do_request("GET", AWS_BUCKET, params=params)
    proxy = do_request("GET", PROXY_BUCKET, params=params)
    if aws is None or proxy is None:
        return record_test(name, "ListObjectsV2", None, None, ["Request failed"])

    diffs = []
    if aws.status_code != proxy.status_code:
        diffs.append(f"Status: {aws.status_code} vs {proxy.status_code}")

    aws_root = normalize_xml(aws.text)
    proxy_root = normalize_xml(proxy.text)
    if aws_root is not None and proxy_root is not None:
        diffs.extend(compare_xml_elements(aws_root, proxy_root))

    record_test(name, "ListObjectsV2", aws.status_code, proxy.status_code, diffs)


def test_invalid_continuation_token():
    name = "ListObjectsV2 - invalid continuation token"
    log(f"\n--- {name} ---")
    params = {"list-type": "2", "continuation-token": "BOGUS_TOKEN_XYZ"}
    aws = do_request("GET", AWS_BUCKET, params=params)
    proxy = do_request("GET", PROXY_BUCKET, params=params)

    aws_status = aws.status_code if aws is not None else None
    proxy_status = proxy.status_code if proxy is not None else None

    diffs = []
    if aws_status != proxy_status:
        diffs.append(f"Status: AWS={aws_status} vs Proxy={proxy_status}")

    notes = ""
    if aws:
        notes += f"AWS: {aws.status_code} {aws.text[:400]}\n"
    if proxy:
        notes += f"Proxy: {proxy.status_code} {proxy.text[:400]}"

    record_test(name, "ListObjectsV2", aws_status, proxy_status, diffs, notes)


def test_get_object_small_json():
    name = "GetObject - small JSON file"
    log(f"\n--- {name} ---")

    aws = do_request("GET", f"{AWS_BUCKET}/{SMALL_JSON_KEY}")
    proxy = do_request("GET", f"{PROXY_BUCKET}/{SMALL_JSON_KEY}")
    if aws is None or proxy is None:
        return record_test(name, "GetObject", None, None,
                          [f"Request failed - AWS={aws is not None}, Proxy={proxy is not None}"])

    diffs = []
    if aws.status_code != proxy.status_code:
        diffs.append(f"Status: AWS={aws.status_code} vs Proxy={proxy.status_code}")

    if aws.content != proxy.content:
        diffs.append(f"Body differs: AWS={aws.text[:200]!r} vs Proxy={proxy.text[:200]!r}")

    header_keys = ['Content-Type', 'Content-Length', 'ETag', 'Accept-Ranges',
                   'Last-Modified', 'Content-Disposition']
    aws_h, proxy_h, hd = compare_headers(aws, proxy, header_keys)
    diffs.extend(hd)

    record_test(name, "GetObject", aws.status_code, proxy.status_code, diffs,
                f"Key: {SMALL_JSON_KEY}", aws_h, proxy_h)


def test_get_object_binary():
    name = "GetObject - binary file (content hash)"
    log(f"\n--- {name} ---")

    aws = do_request("GET", f"{AWS_BUCKET}/{SMALL_BINARY_KEY}")
    proxy = do_request("GET", f"{PROXY_BUCKET}/{SMALL_BINARY_KEY}")
    if aws is None or proxy is None:
        return record_test(name, "GetObject", None, None, ["Request failed"])

    diffs = []
    if aws.status_code != proxy.status_code:
        diffs.append(f"Status: AWS={aws.status_code} vs Proxy={proxy.status_code}")

    aws_md5 = hashlib.md5(aws.content).hexdigest()
    proxy_md5 = hashlib.md5(proxy.content).hexdigest()
    if aws_md5 != proxy_md5:
        diffs.append(f"Content MD5: AWS={aws_md5} vs Proxy={proxy_md5}")
        diffs.append(f"Content length: AWS={len(aws.content)} vs Proxy={len(proxy.content)}")

    header_keys = ['Content-Type', 'Content-Length', 'ETag', 'Accept-Ranges',
                   'Content-Disposition']
    aws_h, proxy_h, hd = compare_headers(aws, proxy, header_keys)
    diffs.extend(hd)

    notes = f"Key: {SMALL_BINARY_KEY}, size={len(aws.content)}, md5 match={aws_md5==proxy_md5}"

    record_test(name, "GetObject", aws.status_code, proxy.status_code, diffs,
                notes, aws_h, proxy_h)


def test_get_object_nonexistent():
    name = "GetObject - nonexistent key"
    log(f"\n--- {name} ---")

    key = "this-key-xyz-definitely-does-not-exist.txt"
    aws = do_request("GET", f"{AWS_BUCKET}/{key}")
    proxy = do_request("GET", f"{PROXY_BUCKET}/{key}")
    if aws is None or proxy is None:
        return record_test(name, "GetObject", None, None, ["Request failed"])

    diffs = []
    if aws.status_code != proxy.status_code:
        diffs.append(f"Status: AWS={aws.status_code} vs Proxy={proxy.status_code}")

    aws_root = normalize_xml(aws.text)
    proxy_root = normalize_xml(proxy.text)
    if aws_root is not None and proxy_root is not None:
        diffs.extend(compare_xml_elements(aws_root, proxy_root))
    elif aws.text != proxy.text:
        diffs.append(f"Response body differs")

    notes = f"AWS body: {aws.text[:400]}\nProxy body: {proxy.text[:400]}"

    record_test(name, "GetObject", aws.status_code, proxy.status_code, diffs, notes)


def test_get_object_range():
    name = "GetObject - Range bytes=0-9"
    log(f"\n--- {name} ---")

    headers = {"Range": "bytes=0-9"}
    aws = do_request("GET", f"{AWS_BUCKET}/{SMALL_JSON_KEY}", headers=headers)
    proxy = do_request("GET", f"{PROXY_BUCKET}/{SMALL_JSON_KEY}", headers=headers)
    if aws is None or proxy is None:
        return record_test(name, "GetObject", None, None, ["Request failed"])

    diffs = []
    if aws.status_code != proxy.status_code:
        diffs.append(f"Status: AWS={aws.status_code} vs Proxy={proxy.status_code}")

    if aws.content != proxy.content:
        diffs.append(f"Body: AWS={aws.content!r} vs Proxy={proxy.content!r}")

    header_keys = ['Content-Type', 'Content-Length', 'Content-Range', 'Accept-Ranges', 'ETag']
    aws_h, proxy_h, hd = compare_headers(aws, proxy, header_keys)
    diffs.extend(hd)

    record_test(name, "GetObject", aws.status_code, proxy.status_code, diffs,
                "Range: bytes=0-9", aws_h, proxy_h)


def test_get_object_range_suffix():
    name = "GetObject - Range bytes=-5 (last 5)"
    log(f"\n--- {name} ---")

    headers = {"Range": "bytes=-5"}
    aws = do_request("GET", f"{AWS_BUCKET}/{SMALL_JSON_KEY}", headers=headers)
    proxy = do_request("GET", f"{PROXY_BUCKET}/{SMALL_JSON_KEY}", headers=headers)
    if aws is None or proxy is None:
        return record_test(name, "GetObject", None, None, ["Request failed"])

    diffs = []
    if aws.status_code != proxy.status_code:
        diffs.append(f"Status: AWS={aws.status_code} vs Proxy={proxy.status_code}")

    if aws.content != proxy.content:
        diffs.append(f"Body: AWS={aws.content!r} vs Proxy={proxy.content!r}")

    header_keys = ['Content-Range', 'Content-Length']
    aws_h, proxy_h, hd = compare_headers(aws, proxy, header_keys)
    diffs.extend(hd)

    record_test(name, "GetObject", aws.status_code, proxy.status_code, diffs,
                "", aws_h, proxy_h)


def test_get_object_range_open():
    name = "GetObject - Range bytes=5- (from 5 to end)"
    log(f"\n--- {name} ---")

    headers = {"Range": "bytes=5-"}
    aws = do_request("GET", f"{AWS_BUCKET}/{SMALL_JSON_KEY}", headers=headers)
    proxy = do_request("GET", f"{PROXY_BUCKET}/{SMALL_JSON_KEY}", headers=headers)
    if aws is None or proxy is None:
        return record_test(name, "GetObject", None, None, ["Request failed"])

    diffs = []
    if aws.status_code != proxy.status_code:
        diffs.append(f"Status: AWS={aws.status_code} vs Proxy={proxy.status_code}")

    if aws.content != proxy.content:
        diffs.append(f"Body length: AWS={len(aws.content)} vs Proxy={len(proxy.content)}")

    header_keys = ['Content-Range', 'Content-Length']
    aws_h, proxy_h, hd = compare_headers(aws, proxy, header_keys)
    diffs.extend(hd)

    record_test(name, "GetObject", aws.status_code, proxy.status_code, diffs,
                "", aws_h, proxy_h)


def test_get_object_range_invalid():
    name = "GetObject - Range bytes=99999-100000 (unsatisfiable)"
    log(f"\n--- {name} ---")

    headers = {"Range": "bytes=99999-100000"}
    aws = do_request("GET", f"{AWS_BUCKET}/{SMALL_JSON_KEY}", headers=headers)
    proxy = do_request("GET", f"{PROXY_BUCKET}/{SMALL_JSON_KEY}", headers=headers)
    if aws is None or proxy is None:
        return record_test(name, "GetObject", None, None, ["Request failed"])

    diffs = []
    if aws.status_code != proxy.status_code:
        diffs.append(f"Status: AWS={aws.status_code} vs Proxy={proxy.status_code}")

    notes = f"AWS: {aws.status_code} {aws.text[:300]}\nProxy: {proxy.status_code} {proxy.text[:300]}"

    record_test(name, "GetObject", aws.status_code, proxy.status_code, diffs, notes)


def test_get_directory_no_slash():
    name = "GetObject - directory path (no trailing slash)"
    log(f"\n--- {name} ---")

    path = "jrc_mus_lung_covid.n5"
    aws = do_request("GET", f"{AWS_BUCKET}/{path}", headers={"Accept": "application/xml"})
    proxy = do_request("GET", f"{PROXY_BUCKET}/{path}", headers={"Accept": "application/xml"})
    if aws is None or proxy is None:
        return record_test(name, "GetObject", None, None, ["Request failed"])

    diffs = []
    if aws.status_code != proxy.status_code:
        diffs.append(f"Status: AWS={aws.status_code} vs Proxy={proxy.status_code}")

    notes = f"AWS: {aws.status_code} body[:300]={aws.text[:300]}\nProxy: {proxy.status_code} body[:300]={proxy.text[:300]}"

    record_test(name, "GetObject", aws.status_code, proxy.status_code, diffs, notes)


def test_get_directory_with_slash():
    name = "GetObject - directory path (with trailing slash)"
    log(f"\n--- {name} ---")

    path = "jrc_mus_lung_covid.n5/"
    aws = do_request("GET", f"{AWS_BUCKET}/{path}",
                     headers={"Accept": "application/xml"},
                     params={"list-type": "2", "max-keys": "3", "delimiter": "/"})
    proxy = do_request("GET", f"{PROXY_BUCKET}/{path}",
                       headers={"Accept": "application/xml"},
                       params={"list-type": "2", "max-keys": "3", "delimiter": "/"})
    if aws is None or proxy is None:
        return record_test(name, "GetObject", None, None, ["Request failed"])

    diffs = []
    if aws.status_code != proxy.status_code:
        diffs.append(f"Status: {aws.status_code} vs {proxy.status_code}")

    aws_root = normalize_xml(aws.text)
    proxy_root = normalize_xml(proxy.text)
    if aws_root is not None and proxy_root is not None:
        diffs.extend(compare_xml_elements(aws_root, proxy_root))

    notes = f"AWS: {aws.status_code}, Proxy: {proxy.status_code}"

    record_test(name, "GetObject", aws.status_code, proxy.status_code, diffs, notes)


def test_head_object():
    name = "HeadObject - existing key"
    log(f"\n--- {name} ---")

    aws = do_request("HEAD", f"{AWS_BUCKET}/{SMALL_JSON_KEY}")
    proxy = do_request("HEAD", f"{PROXY_BUCKET}/{SMALL_JSON_KEY}")
    if aws is None or proxy is None:
        return record_test(name, "HeadObject", None, None, ["Request failed"])

    diffs = []
    if aws.status_code != proxy.status_code:
        diffs.append(f"Status: {aws.status_code} vs {proxy.status_code}")

    header_keys = ['Content-Type', 'Content-Length', 'ETag', 'Accept-Ranges',
                   'Last-Modified', 'Cache-Control', 'x-amz-server-side-encryption',
                   'x-amz-storage-class']
    aws_h, proxy_h, hd = compare_headers(aws, proxy, header_keys)
    diffs.extend(hd)

    record_test(name, "HeadObject", aws.status_code, proxy.status_code, diffs,
                f"Key: {SMALL_JSON_KEY}", aws_h, proxy_h)


def test_head_object_nonexistent():
    name = "HeadObject - nonexistent key"
    log(f"\n--- {name} ---")

    key = "this-key-xyz-does-not-exist.txt"
    aws = do_request("HEAD", f"{AWS_BUCKET}/{key}")
    proxy = do_request("HEAD", f"{PROXY_BUCKET}/{key}")
    if aws is None or proxy is None:
        return record_test(name, "HeadObject", None, None, ["Request failed"])

    diffs = []
    if aws.status_code != proxy.status_code:
        diffs.append(f"Status: AWS={aws.status_code} vs Proxy={proxy.status_code}")

    # For HEAD, check error-related headers
    header_keys = ['Content-Type', 'x-amz-error-code', 'x-amz-error-message']
    aws_h, proxy_h, hd = compare_headers(aws, proxy, header_keys)
    diffs.extend(hd)

    record_test(name, "HeadObject", aws.status_code, proxy.status_code, diffs,
                "", aws_h, proxy_h)


def test_head_object_directory():
    name = "HeadObject - directory prefix"
    log(f"\n--- {name} ---")

    aws = do_request("HEAD", f"{AWS_BUCKET}/{DIR_PREFIX}")
    proxy = do_request("HEAD", f"{PROXY_BUCKET}/{DIR_PREFIX}")
    if aws is None or proxy is None:
        return record_test(name, "HeadObject", None, None, ["Request failed"])

    diffs = []
    if aws.status_code != proxy.status_code:
        diffs.append(f"Status: AWS={aws.status_code} vs Proxy={proxy.status_code}")

    record_test(name, "HeadObject", aws.status_code, proxy.status_code, diffs)


def test_head_bucket():
    name = "HeadBucket"
    log(f"\n--- {name} ---")

    aws = do_request("HEAD", AWS_BUCKET)
    proxy = do_request("HEAD", PROXY_BUCKET)
    if aws is None or proxy is None:
        return record_test(name, "HeadBucket", None, None, ["Request failed"])

    diffs = []
    if aws.status_code != proxy.status_code:
        diffs.append(f"Status: AWS={aws.status_code} vs Proxy={proxy.status_code}")

    header_keys = ['Content-Type', 'x-amz-bucket-region', 'x-amz-request-id']
    aws_h, proxy_h, hd = compare_headers(aws, proxy, header_keys)
    diffs.extend(hd)

    record_test(name, "HeadBucket", aws.status_code, proxy.status_code, diffs,
                "", aws_h, proxy_h)


def test_get_bucket_acl():
    name = "GetBucketAcl"
    log(f"\n--- {name} ---")

    params = {"acl": ""}
    aws = do_request("GET", AWS_BUCKET, params=params)
    proxy = do_request("GET", PROXY_BUCKET, params=params)
    if aws is None or proxy is None:
        return record_test(name, "ACL", None, None, ["Request failed"])

    diffs = []
    if aws.status_code != proxy.status_code:
        diffs.append(f"Status: AWS={aws.status_code} vs Proxy={proxy.status_code}")

    aws_root = normalize_xml(aws.text)
    proxy_root = normalize_xml(proxy.text)
    if aws_root is not None and proxy_root is not None:
        diffs.extend(compare_xml_elements(aws_root, proxy_root))

    notes = f"AWS: {aws.status_code} body={aws.text[:400]}\nProxy: {proxy.status_code} body={proxy.text[:400]}"

    record_test(name, "ACL", aws.status_code, proxy.status_code, diffs, notes)


def test_nonexistent_bucket():
    name = "Nonexistent bucket"
    log(f"\n--- {name} ---")

    aws = do_request("GET", f"{AWS_BASE}/this-bucket-xyz-does-not-exist-99",
                     params={"list-type": "2"})
    proxy = do_request("GET", f"{PROXY_BASE}/this-bucket-xyz-does-not-exist-99",
                       params={"list-type": "2"})
    if aws is None or proxy is None:
        return record_test(name, "Error", None, None, ["Request failed"])

    diffs = []
    if aws.status_code != proxy.status_code:
        diffs.append(f"Status: AWS={aws.status_code} vs Proxy={proxy.status_code}")

    aws_root = normalize_xml(aws.text)
    proxy_root = normalize_xml(proxy.text)
    if aws_root is not None and proxy_root is not None:
        diffs.extend(compare_xml_elements(aws_root, proxy_root))

    notes = f"AWS: {aws.status_code} {aws.text[:400]}\nProxy: {proxy.status_code} {proxy.text[:400]}"

    record_test(name, "Error", aws.status_code, proxy.status_code, diffs, notes)


def test_list_buckets():
    name = "ListBuckets (GET /)"
    log(f"\n--- {name} ---")

    aws = do_request("GET", AWS_BASE, headers={"Accept": "application/xml"})
    proxy = do_request("GET", PROXY_BASE, headers={"Accept": "application/xml"})
    if aws is None or proxy is None:
        return record_test(name, "ListBuckets", None, None, ["Request failed"])

    diffs = []
    if aws.status_code != proxy.status_code:
        diffs.append(f"Status: AWS={aws.status_code} vs Proxy={proxy.status_code}")

    notes = f"AWS: {aws.status_code} body[:300]={aws.text[:300]}\nProxy: {proxy.status_code} body[:300]={proxy.text[:300]}"

    record_test(name, "ListBuckets", aws.status_code, proxy.status_code, diffs, notes)


def test_xml_namespace():
    name = "XML namespace and declaration"
    log(f"\n--- {name} ---")

    params = {"list-type": "2", "max-keys": "1"}
    aws = do_request("GET", AWS_BUCKET, params=params)
    proxy = do_request("GET", PROXY_BUCKET, params=params)
    if aws is None or proxy is None:
        return record_test(name, "XML", None, None, ["Request failed"])

    diffs = []

    # xmlns
    if ('xmlns' in aws.text[:500]) != ('xmlns' in proxy.text[:500]):
        aws_has = 'xmlns' in aws.text[:500]
        proxy_has = 'xmlns' in proxy.text[:500]
        diffs.append(f"xmlns namespace: AWS={aws_has}, Proxy={proxy_has}")

    # XML declaration encoding
    if 'encoding="UTF-8"' in aws.text[:100] and "encoding='utf-8'" in proxy.text[:100]:
        diffs.append("XML declaration encoding quotes: AWS uses double quotes, Proxy uses single quotes")

    # Element ordering
    aws_root = normalize_xml(aws.text)
    proxy_root = normalize_xml(proxy.text)
    if aws_root is not None and proxy_root is not None:
        aws_order = [c.tag for c in aws_root]
        proxy_order = [c.tag for c in proxy_root]
        if aws_order != proxy_order:
            diffs.append(f"Child element order differs:\n    AWS:   {aws_order}\n    Proxy: {proxy_order}")

    notes = f"AWS first line: {aws.text[:150]}\nProxy first line: {proxy.text[:150]}"

    record_test(name, "XML", aws.status_code, proxy.status_code, diffs, notes)


def test_content_type_json():
    name = "Content-Type for JSON file"
    log(f"\n--- {name} ---")

    aws = do_request("HEAD", f"{AWS_BUCKET}/{SMALL_JSON_KEY}")
    proxy = do_request("HEAD", f"{PROXY_BUCKET}/{SMALL_JSON_KEY}")
    if aws is None or proxy is None:
        return record_test(name, "ContentType", None, None, ["Request failed"])

    diffs = []
    aws_ct = aws.headers.get('Content-Type', '')
    proxy_ct = proxy.headers.get('Content-Type', '')
    if aws_ct != proxy_ct:
        diffs.append(f"Content-Type: AWS='{aws_ct}' vs Proxy='{proxy_ct}'")

    record_test(name, "ContentType", aws.status_code, proxy.status_code, diffs,
                "", {'Content-Type': aws_ct}, {'Content-Type': proxy_ct})


def test_content_type_binary():
    name = "Content-Type for extensionless binary file"
    log(f"\n--- {name} ---")

    key = "jrc_mus_lung_covid.n5/render/v1_acquire_align___20210609_224836/s0/0/0/0"
    aws = do_request("HEAD", f"{AWS_BUCKET}/{key}")
    proxy = do_request("HEAD", f"{PROXY_BUCKET}/{key}")
    if aws is None or proxy is None:
        return record_test(name, "ContentType", None, None, ["Request failed"])

    diffs = []
    aws_ct = aws.headers.get('Content-Type', '')
    proxy_ct = proxy.headers.get('Content-Type', '')
    if aws_ct != proxy_ct:
        diffs.append(f"Content-Type: AWS='{aws_ct}' vs Proxy='{proxy_ct}'")

    record_test(name, "ContentType", aws.status_code, proxy.status_code, diffs,
                f"Key: {key}", {'Content-Type': aws_ct}, {'Content-Type': proxy_ct})


def test_get_object_special_chars():
    name = "GetObject - URL-encoded key (404 handling)"
    log(f"\n--- {name} ---")

    key = "test%20file%20with%20spaces.txt"
    aws = do_request("GET", f"{AWS_BUCKET}/{key}")
    proxy = do_request("GET", f"{PROXY_BUCKET}/{key}")
    if aws is None or proxy is None:
        return record_test(name, "GetObject", None, None, ["Request failed"])

    diffs = []
    if aws.status_code != proxy.status_code:
        diffs.append(f"Status: AWS={aws.status_code} vs Proxy={proxy.status_code}")

    aws_root = normalize_xml(aws.text)
    proxy_root = normalize_xml(proxy.text)
    if aws_root is not None and proxy_root is not None:
        diffs.extend(compare_xml_elements(aws_root, proxy_root))

    record_test(name, "GetObject", aws.status_code, proxy.status_code, diffs)


# ===========================================================================
# Report
# ===========================================================================

def generate_report():
    lines = []
    lines.append("=" * 80)
    lines.append("S3 COMPATIBILITY REPORT: AWS S3 vs x2s3 Proxy")
    lines.append("=" * 80)
    lines.append(f"AWS:   {AWS_BUCKET}")
    lines.append(f"Proxy: {PROXY_BUCKET}")
    lines.append(f"Tests: {len(results)}")
    lines.append("")

    matching = [r for r in results if not r['diffs']]
    differing = [r for r in results if r['diffs']]

    lines.append(f"MATCHING:  {len(matching)}/{len(results)}")
    lines.append(f"DIFFERENT: {len(differing)}/{len(results)}")
    lines.append("")

    # Summary table
    lines.append("-" * 80)
    lines.append(f"{'Test':<55} {'AWS':>5} {'Proxy':>5} {'Result':>8}")
    lines.append("-" * 80)
    for r in results:
        icon = "MATCH" if not r['diffs'] else "DIFF"
        lines.append(f"{r['name']:<55} {str(r['aws_status']):>5} {str(r['proxy_status']):>5} {icon:>8}")
    lines.append("")

    # Detailed diff section
    if differing:
        lines.append("=" * 80)
        lines.append("DIFFERENCES FOUND")
        lines.append("=" * 80)

        for r in differing:
            lines.append(f"\n### {r['name']}")
            lines.append(f"    Category: {r['category']}")
            lines.append(f"    Status: AWS={r['aws_status']} Proxy={r['proxy_status']} "
                        f"(match={r['status_match']})")

            for d in r['diffs']:
                lines.append(f"    * {d}")

            if r.get('aws_headers') or r.get('proxy_headers'):
                if r['aws_headers']:
                    lines.append(f"    AWS headers:   {json.dumps(r['aws_headers'])}")
                if r['proxy_headers']:
                    lines.append(f"    Proxy headers: {json.dumps(r['proxy_headers'])}")

            if r['notes']:
                lines.append(f"    Notes: {r['notes'][:500]}")

    # Matching section
    if matching:
        lines.append("")
        lines.append("=" * 80)
        lines.append("MATCHING TESTS (no differences)")
        lines.append("=" * 80)
        for r in matching:
            lines.append(f"  - {r['name']} (both {r['aws_status']})")

    return "\n".join(lines)


# ===========================================================================
# Main
# ===========================================================================

if __name__ == "__main__":
    log("Starting S3 compatibility comparison...")
    log(f"AWS:   {AWS_BUCKET}")
    log(f"Proxy: {PROXY_BUCKET}")

    tests = [
        test_list_v2_basic,
        test_list_v2_delimiter,
        test_list_v2_prefix_delimiter,
        test_list_v2_max_keys,
        test_list_v2_start_after,
        test_list_v2_encoding_type,
        test_list_v2_pagination,
        test_list_v2_nonexistent_prefix,
        test_list_v2_no_list_type,
        test_list_v2_trailing_slash,
        test_list_v2_max_keys_zero,
        test_list_v2_max_keys_above_limit,
        test_list_v1,
        test_list_v2_no_delimiter_flat,
        test_list_v2_fetch_owner,
        test_invalid_continuation_token,
        test_get_object_small_json,
        test_get_object_binary,
        test_get_object_nonexistent,
        test_get_object_range,
        test_get_object_range_suffix,
        test_get_object_range_open,
        test_get_object_range_invalid,
        test_get_directory_no_slash,
        test_get_directory_with_slash,
        test_get_object_special_chars,
        test_head_object,
        test_head_object_nonexistent,
        test_head_object_directory,
        test_head_bucket,
        test_get_bucket_acl,
        test_nonexistent_bucket,
        test_list_buckets,
        test_xml_namespace,
        test_content_type_json,
        test_content_type_binary,
    ]

    for fn in tests:
        try:
            fn()
        except Exception as e:
            log(f"ERROR in {fn.__name__}: {e}")
            traceback.print_exc()

    report = generate_report()

    report_path = "s3_comparison_report.txt"
    with open(report_path, "w") as f:
        f.write(report)

    print("\n\n")
    print(report)
    print(f"\nReport saved to {report_path}")
