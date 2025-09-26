from __future__ import annotations

import json
from typing import Iterator

import httpx

from jira_extraction.config import IssueTypeConfig, ScopeConfig, WindowsConfig
from jira_extraction.extract import stream_scope
from jira_extraction.http_client import JiraHTTPClient
from jira_extraction.jira_api import JiraAPI
from jira_extraction.state_store import InMemoryStateStore


def build_transport(responses: dict[int, dict[str, object]]) -> httpx.MockTransport:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/rest/api/2/search"
        payload = json.loads(request.content.decode("utf-8"))
        start_at = payload["startAt"]
        response = responses.get(start_at)
        if response is None:
            raise AssertionError(f"Unexpected startAt {start_at}")
        return httpx.Response(200, json=response)

    return httpx.MockTransport(handler)


def test_stream_scope_resumes_after_page() -> None:
    scope = ScopeConfig(project="ABC", issue_types=[IssueTypeConfig(name="Bug", fields=["summary", "updated"])])
    issue_type = scope.issue_types[0]
    windows = WindowsConfig(initial_days=90, safety_skew_s=60)
    store = InMemoryStateStore()

    responses = {
        0: {
            "issues": [
                {"id": "1", "key": "ABC-1", "fields": {"updated": "2024-01-01T00:00:00.000+0000"}},
                {"id": "2", "key": "ABC-2", "fields": {"updated": "2024-01-01T00:00:00.000+0000"}},
            ],
            "total": 4,
            "maxResults": 2,
        },
        2: {
            "issues": [
                {"id": "3", "key": "ABC-3", "fields": {"updated": "2024-01-02T00:00:00.000+0000"}},
                {"id": "4", "key": "ABC-4", "fields": {"updated": "2024-01-03T00:00:00.000+0000"}},
            ],
            "total": 4,
            "maxResults": 2,
        },
    }

    transport = build_transport(responses)
    client = JiraHTTPClient(base_url="https://example.com", pat="token", transport=transport)
    api = JiraAPI(client)

    iterator = stream_scope(
        api,
        scope,
        issue_type,
        windows=windows,
        store=store,
        mode="initial",
        page_size=2,
        validate_query=True,
    )

    first_page = next(iterator)
    assert [issue["key"] for issue in first_page.issues] == ["ABC-1", "ABC-2"]

    # Simulate crash by not consuming the second page.  The cursor should have been
    # persisted with resume_page_at = 2 and the last processed timestamp.

    transport_second = build_transport({2: responses[2]})
    client_second = JiraHTTPClient(base_url="https://example.com", pat="token", transport=transport_second)
    api_second = JiraAPI(client_second)

    resumed_pages = list(
        stream_scope(
            api_second,
            scope,
            issue_type,
            windows=windows,
            store=store,
            mode="initial",
            page_size=2,
            validate_query=True,
        )
    )
    assert len(resumed_pages) == 1
    assert resumed_pages[0].page.start_at == 2
    assert [issue["key"] for issue in resumed_pages[0].issues] == ["ABC-3", "ABC-4"]
