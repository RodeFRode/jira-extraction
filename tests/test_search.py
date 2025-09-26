from __future__ import annotations

import json
from collections import defaultdict

import httpx

from jira_extraction.http_client import JiraHTTPClient
from jira_extraction.jira_api import JiraAPI


def test_search_paginates() -> None:
    responses = {
        0: {"issues": [{"id": "1", "key": "ABC-1", "fields": {"updated": "2024-01-01T00:00:00.000+0000"}}], "total": 3, "maxResults": 1},
        1: {"issues": [{"id": "2", "key": "ABC-2", "fields": {"updated": "2024-01-02T00:00:00.000+0000"}}], "total": 3, "maxResults": 1},
        2: {"issues": [{"id": "3", "key": "ABC-3", "fields": {"updated": "2024-01-03T00:00:00.000+0000"}}], "total": 3, "maxResults": 1},
    }

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/rest/api/2/search"
        payload = json.loads(request.content.decode("utf-8"))
        start_at = payload["startAt"]
        assert payload["maxResults"] == 1
        assert payload["fields"] == ["summary"]
        return httpx.Response(200, json=responses[start_at])

    transport = httpx.MockTransport(handler)
    client = JiraHTTPClient(base_url="https://example.com", pat="token", transport=transport)
    api = JiraAPI(client)
    issues = list(
        api.search_stream(
            jql="project = ABC",
            fields=["summary"],
            page_size=1,
        )
    )
    assert [issue["key"] for issue in issues] == ["ABC-1", "ABC-2", "ABC-3"]
