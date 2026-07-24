# Unbrowseable (Key-Only) Buckets Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Strengthen `browseable: false` so a bucket's objects are accessible only by exact key — all listing, browsing, and ACL requests return `403 AccessDenied`, and missing keys return 403 instead of 404 so key existence cannot be probed.

**Architecture:** All changes live in the dispatch layer (`x2s3/app.py`), so every client backend (file, aioboto, future ones) is covered with zero client changes. The behavior mirrors a real AWS S3 bucket policy that grants `s3:GetObject` but denies `s3:ListBucket`: list attempts get 403 AccessDenied XML, and GET/HEAD of a missing key also gets 403 (real S3 masks 404s this way when ListBucket is denied). The existing `Target.browseable` config flag is the entire interface — no new settings.

**Tech Stack:** Python 3, FastAPI, pytest with `fastapi.testclient.TestClient`.

## Global Constraints

- No new dependencies, no new config fields. `Target.browseable: bool = True` in `x2s3/settings.py` already exists and is unchanged.
- Error XML must use existing helpers in `x2s3/utils.py` (`get_accessdenied_response()`), never hand-built XML in `app.py`.
- HEAD responses must not carry a body — use `Response(status_code=403, media_type="application/xml")`, not `get_accessdenied_response()`, in the HEAD handler.
- Tests for this feature go in `tests/test_file.py` (local filesystem client, no network). `tests/test_awss3.py` requires network access to a public AWS bucket; only run it if network is available.
- Run tests with: `python -m pytest tests/test_file.py -v -W ignore::DeprecationWarning` (from repo root, virtualenv active).
- Behavior change is intentional: deployments using `browseable: false` as "hidden from index but listable" will now get 403 on listing. This is the feature.

---

### Task 1: Deny listing, browsing, and ACL reads on unbrowseable buckets

**Files:**
- Modify: `x2s3/app.py` (inside `target_dispatcher`, currently lines ~380–405)
- Test: `tests/test_file.py`

**Interfaces:**
- Consumes: `Target.browseable` (`x2s3/settings.py:19`), `get_accessdenied_response()` (`x2s3/utils.py:197`, returns 403 `Response` with AccessDenied XML).
- Produces: `target_dispatcher` returns 403 for any request on a `browseable=False` target that would list objects, render the browse UI, or read an ACL. Task 2 adds the local helper `get_object_or_denied` in the same function; this task leaves the two `client.get_object(...)` call sites as they are.

- [ ] **Step 1: Add a `browseable=False` target to the test fixture**

In `tests/test_file.py`, add a third target to the `get_settings` fixture list (after `local-files-with-etags`):

```python
        Target(
            name='hidden-files',
            browseable=False,
            client='file',
            options={'path':'.'}
        )
```

The existing `test_get_html_root` already asserts that non-browseable target names do not appear on the index page, so it now also covers `hidden-files` for free.

- [ ] **Step 2: Write the failing tests**

Append to `tests/test_file.py`:

```python
def test_unbrowseable_list_denied(app):
    with TestClient(app) as client:
        response = client.get("/hidden-files?list-type=2")
        assert response.status_code == 403
        assert response.headers['content-type'] == "application/xml"
        root = parse_xml(response.text)
        assert root.find('Code').text == 'AccessDenied'


def test_unbrowseable_browse_denied(app):
    with TestClient(app) as client:
        # HTML browse UI on bucket root and on a subdirectory
        response = client.get("/hidden-files/", headers={"Accept": "text/html"})
        assert response.status_code == 403
        response = client.get("/hidden-files/tests/", headers={"Accept": "text/html"})
        assert response.status_code == 403
        # Trailing-slash XML listing (no HTML preference)
        response = client.get("/hidden-files/tests/")
        assert response.status_code == 403
        root = parse_xml(response.text)
        assert root.find('Code').text == 'AccessDenied'


def test_unbrowseable_acl_denied(app):
    with TestClient(app) as client:
        # GetBucketAcl
        response = client.get("/hidden-files?acl")
        assert response.status_code == 403
        # GetObjectAcl
        response = client.get("/hidden-files/README.md?acl")
        assert response.status_code == 403


def test_unbrowseable_get_object_allowed(app):
    with TestClient(app) as client:
        response = client.get("/hidden-files/README.md")
        assert response.status_code == 200
        assert 'x2s3' in response.text
        # GetObject takes precedence over list-type when a key is present
        response = client.get("/hidden-files/README.md?list-type=2")
        assert response.status_code == 200
```

- [ ] **Step 3: Run the new tests to verify they fail**

Run: `python -m pytest tests/test_file.py -k unbrowseable -v -W ignore::DeprecationWarning`

Expected: `test_unbrowseable_get_object_allowed` PASSES (GET already works today); the other three FAIL with 200 != 403. If anything errors rather than failing on the assert, stop and investigate before proceeding.

- [ ] **Step 4: Implement the deny guards in `target_dispatcher`**

In `x2s3/app.py`, replace the body of `target_dispatcher` from the `if 'acl' in request.query_params:` line through the final `else:` branch with:

```python
        if 'acl' in request.query_params:
            if not target_config.browseable:
                # Real S3 denies GetBucketAcl/GetObjectAcl without s3:GetBucketAcl/s3:GetObjectAcl
                return get_accessdenied_response()
            return get_read_access_acl()

        if list_type:
            if not target_path:
                if list_type == 2:
                    if not target_config.browseable:
                        return get_accessdenied_response()
                    return await client.list_objects_v2(continuation_token, delimiter, \
                        encoding_type, fetch_owner, max_keys, prefix, start_after)
                else:
                    raise HTTPException(status_code=400, detail="Invalid list type")
            else:
                range_header = request.headers.get("range")
                return await client.get_object(target_path, range_header)

        if not target_path or target_path.endswith("/"):
            if not target_config.browseable:
                return get_accessdenied_response()
            if app.settings.ui and _prefers_html(request):
                return await browse_bucket(request, target_name, target_path,
                    continuation_token=continuation_token,
                    max_keys=100,
                    is_virtual=is_virtual)
            else:
                return await client.list_objects_v2(continuation_token, delimiter, \
                    encoding_type, fetch_owner, max_keys, prefix, start_after)
        else:
            range_header = request.headers.get("range")
            return await client.get_object(target_path, range_header)
```

This is the existing code with three added `if not target_config.browseable: return get_accessdenied_response()` guards (ACL, ListObjectsV2, trailing-slash list/browse). `get_accessdenied_response` is already imported via `from x2s3.utils import *`. Nothing else in the function changes.

- [ ] **Step 5: Run the full local test file to verify everything passes**

Run: `python -m pytest tests/test_file.py -v -W ignore::DeprecationWarning`

Expected: all tests PASS, including the pre-existing ones (`test_get_html_root` now checks `hidden-files` is absent from the index).

- [ ] **Step 6: Commit**

```bash
git add tests/test_file.py x2s3/app.py
git commit -m "feat: deny listing, browsing and ACL reads on unbrowseable buckets"
```

---

### Task 2: Mask missing keys as 403 on unbrowseable buckets (GET, HEAD, HeadBucket)

**Files:**
- Modify: `x2s3/app.py` (`target_dispatcher` GET call sites; `head_object` handler, currently lines ~409–432)
- Test: `tests/test_file.py`

**Interfaces:**
- Consumes: the Task 1 guards already in place; `get_accessdenied_response()` from `x2s3/utils.py`; client responses where a missing key is a `Response` with `status_code == 404` (both `FileProxyClient` and `AiobotoProxyClient` return `get_nosuchkey_response()` for missing keys — success responses are `StreamingResponse` objects whose status is known before streaming starts, so checking `.status_code` is safe).
- Produces: a local async helper `get_object_or_denied(key)` inside `target_dispatcher` used by both GetObject call sites. `head_object` returns bodiless 403 for HeadBucket and missing keys on unbrowseable targets.

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_file.py`:

```python
def test_unbrowseable_get_missing_masked(app):
    with TestClient(app) as client:
        # Missing key returns 403 (not 404) so key existence can't be probed,
        # matching real S3 behavior when s3:ListBucket is denied
        response = client.get("/hidden-files/missing")
        assert response.status_code == 403
        root = parse_xml(response.text)
        assert root.find('Code').text == 'AccessDenied'
        # Same masking on the list-type+key (GetObject precedence) path
        response = client.get("/hidden-files/missing?list-type=2")
        assert response.status_code == 403


def test_unbrowseable_head(app):
    with TestClient(app) as client:
        # Existing key: works
        response = client.head("/hidden-files/README.md")
        assert response.status_code == 200
        # Missing key: masked as 403
        response = client.head("/hidden-files/missing")
        assert response.status_code == 403
        # HeadBucket: real S3 requires s3:ListBucket, so deny
        response = client.head("/hidden-files")
        assert response.status_code == 403
        # Browseable bucket unaffected
        response = client.head("/local-files")
        assert response.status_code == 200
        response = client.head("/local-files/missing")
        assert response.status_code == 404
```

- [ ] **Step 2: Run the new tests to verify they fail**

Run: `python -m pytest tests/test_file.py -k "masked or unbrowseable_head" -v -W ignore::DeprecationWarning`

Expected: both FAIL (404 != 403 for missing keys, 200 != 403 for HeadBucket).

- [ ] **Step 3: Implement the 404 masking**

In `x2s3/app.py` `target_dispatcher`, insert this local helper directly after the `if 'acl' ...` block added in Task 1, and change both `return await client.get_object(target_path, range_header)` call sites to use it:

```python
        async def get_object_or_denied(key):
            """GetObject with S3-style 404 masking: on unbrowseable buckets a
            missing key returns 403 AccessDenied so clients can't probe which
            keys exist (real S3 does this when s3:ListBucket is denied)."""
            response = await client.get_object(key, request.headers.get("range"))
            if response.status_code == 404 and not target_config.browseable:
                return get_accessdenied_response()
            return response
```

The two GET call sites (inside `if list_type:` with a `target_path`, and the final `else:`) each become:

```python
            return await get_object_or_denied(target_path)
```

(The two `range_header = request.headers.get("range")` lines are absorbed into the helper — delete them.)

In `head_object`, replace the bucket-root and HEAD-object code (after the `client is None` check) with:

```python
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
```

- [ ] **Step 4: Run the full local test file to verify everything passes**

Run: `python -m pytest tests/test_file.py -v -W ignore::DeprecationWarning`

Expected: all PASS. Pay attention to the pre-existing `test_head_object` and `test_get_object_missing` (browseable buckets must still return 404).

- [ ] **Step 5: Commit**

```bash
git add tests/test_file.py x2s3/app.py
git commit -m "feat: mask missing keys as 403 AccessDenied on unbrowseable buckets"
```

---

### Task 3: Documentation and S3-backend test coverage

**Files:**
- Modify: `docs/Config.md` (lines 30 and 116)
- Modify: `config.template.yaml` (comment at lines ~56–58)
- Modify: `tests/test_awss3.py`

**Interfaces:**
- Consumes: the behavior implemented in Tasks 1–2.
- Produces: docs describing key-only semantics; one aioboto-backend regression test (the deny guard fires before any client call, so it runs without network I/O even though the file is network-gated).

- [ ] **Step 1: Update `docs/Config.md`**

Replace line 30:

```markdown
* `browseable`: Can this bucket be listed and browsed interactively?
```

with:

```markdown
* `browseable`: If `false`, the bucket becomes key-only: it is hidden from the main listing, all list/browse/ACL requests return `403 AccessDenied`, and objects can only be retrieved by exact key.
```

Replace the sentence in the Notes section (line 116) `Set `browseable: false` to hide the bucket from the main listing -- you may also want to obfuscate the bucket name.` with:

```markdown
Set `browseable: false` to make a bucket key-only: it is hidden from the main listing, all listing and browsing requests return `403 AccessDenied`, and `GET`/`HEAD` of a missing key returns `403` instead of `404` so that key existence cannot be probed. Objects are accessible only to clients that know the exact key. This mirrors AWS S3 behavior for a bucket policy that grants `s3:GetObject` but denies `s3:ListBucket`.
```

- [ ] **Step 2: Update `config.template.yaml`**

Replace the comment above the `cosem-data-hidden` target:

```yaml
  #
  # Exposes s3://janelia-cosem-datasets at /cosem-data-hidden
  # using the default client (currently aioboto) and makes it unbrowseable
  #
```

with:

```yaml
  #
  # Exposes s3://janelia-cosem-datasets at /cosem-data-hidden
  # using the default client (currently aioboto) and makes it key-only:
  # hidden from the main listing, listing/browsing denied with 403,
  # objects retrievable only by exact key
  #
```

- [ ] **Step 3: Add aioboto-backend regression test**

Append to `tests/test_awss3.py` (the fixture already has the `hidden-with-endpoint` target with `browseable=False`, and `test_get_object_hidden` already proves GET still works):

```python
def test_list_objects_hidden_denied(app):
    with TestClient(app) as client:
        response = client.get("/hidden-with-endpoint?list-type=2")
        assert response.status_code == 403
        root = parse_xml(response.text)
        assert root.find('Code').text == 'AccessDenied'
```

- [ ] **Step 4: Run the tests**

Run: `python -m pytest tests/test_file.py -v -W ignore::DeprecationWarning`
Expected: all PASS.

If network access to AWS is available, also run: `python -m pytest tests/test_awss3.py -v -W ignore::DeprecationWarning`
Expected: all PASS (the new test itself needs no network — the request is denied before any S3 call — but the rest of the file does).

- [ ] **Step 5: Commit**

```bash
git add docs/Config.md config.template.yaml tests/test_awss3.py
git commit -m "docs: describe key-only semantics of browseable:false"
```
