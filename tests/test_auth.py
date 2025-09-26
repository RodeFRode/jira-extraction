from __future__ import annotations

import httpx
import pytest

from jira_extraction.http_client import JiraHTTPClient
from jira_extraction.jira_api import JiraAPI


def test_get_myself_success() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.headers["Authorization"] == "Bearer token"
        if request.url.path == "/rest/api/2/myself":
            return httpx.Response(200, json={"name": "bot"})
        raise AssertionError("Unexpected URL")

    transport = httpx.MockTransport(handler)
    client = JiraHTTPClient(base_url="https://example.com", pat="token", transport=transport)
    api = JiraAPI(client)
    payload = api.get_myself()
    assert payload["name"] == "bot"


def test_get_myself_unauthorised() -> None:
    def handler(_: httpx.Request) -> httpx.Response:
        return httpx.Response(401, json={"error": "nope"})

    transport = httpx.MockTransport(handler)
    client = JiraHTTPClient(base_url="https://example.com", pat="token", transport=transport)
    api = JiraAPI(client)
    with pytest.raises(httpx.HTTPStatusError):
        api.get_myself()
