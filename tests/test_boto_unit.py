"""Unit tests for AiobotoProxyClient against a fake aiobotocore client (no network)."""

import asyncio

from x2s3.client_aioboto import AiobotoProxyClient
from x2s3.utils import parse_xml


class FakeListClient:
    def __init__(self, response):
        self.response = response
        self.calls = []

    async def list_objects_v2(self, **params):
        self.calls.append(params)
        return self.response


def _client(fake, prefix):
    client = AiobotoProxyClient({'target_name': 't'}, bucket='b', prefix=prefix)
    client.client = fake  # skips _ensure_client's create_client
    return client


def test_next_continuation_token_passes_through_unchanged():
    """Tokens are opaque: one that happens to start with the prefix must not be trimmed."""
    token = "pre/opaque-token-value"
    fake = FakeListClient({"IsTruncated": True, "NextContinuationToken": token, "KeyCount": 0})
    response = asyncio.run(_client(fake, "pre").list_objects_v2(continuation_token=None, delimiter="/", encoding_type=None, fetch_owner=None, max_keys=1000, prefix="", start_after=None))
    root = parse_xml(response.body.decode())
    assert root.find("NextContinuationToken").text == token
