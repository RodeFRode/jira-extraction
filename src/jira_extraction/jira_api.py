"""Low level Jira REST API helpers."""
from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Dict, Generator, Iterable, Iterator, List, Mapping, MutableMapping, Sequence

import httpx

from .http_client import JiraHTTPClient

LOGGER = logging.getLogger(__name__)


@dataclass(slots=True)
class SearchPage:
    """A single page of search results."""

    start_at: int
    max_results: int
    total: int
    issues: List[Mapping[str, object]]


class JiraAPI:
    """Thin wrapper exposing the Jira REST API endpoints used in the ETL."""

    def __init__(self, client: JiraHTTPClient) -> None:
        self._client = client

    def get_myself(self) -> Mapping[str, object]:
        """Return information about the authenticated user."""

        response = self._client.get("/rest/api/2/myself")
        return response.json()

    def get_fields(self) -> List[Mapping[str, object]]:
        """Fetch field metadata."""

        response = self._client.get("/rest/api/2/field")
        payload = response.json()
        if not isinstance(payload, list):
            msg = "Unexpected payload for /field endpoint"
            raise ValueError(msg)
        return payload

    def search_pages(
        self,
        *,
        jql: str,
        fields: Sequence[str],
        expand: Sequence[str] | None = None,
        validate_query: bool = True,
        page_size: int = 100,
        start_at: int = 0,
    ) -> Iterator[SearchPage]:
        """Yield Jira search pages sequentially."""

        expand = list(expand) if expand else ["changelog"]
        current = start_at
        total: int | None = None
        while total is None or current < total:
            payload = {
                "jql": jql,
                "startAt": current,
                "maxResults": page_size,
                "fields": list(fields),
                "expand": expand,
                "validateQuery": validate_query,
            }
            LOGGER.debug("Fetching Jira search page", extra={"start_at": current})
            response = self._client.post("/rest/api/2/search", json=payload)
            data = response.json()
            issues = data.get("issues", [])
            if not isinstance(issues, list):
                msg = "Unexpected response structure from Jira search"
                raise ValueError(msg)
            total = int(data.get("total", len(issues)))
            max_results = int(data.get("maxResults", page_size))
            yield SearchPage(start_at=current, max_results=max_results, total=total, issues=issues)
            current += len(issues)
            if len(issues) == 0:
                break

    def search_stream(
        self,
        *,
        jql: str,
        fields: Sequence[str],
        expand: Sequence[str] | None = None,
        validate_query: bool = True,
        page_size: int = 100,
        start_at: int = 0,
    ) -> Iterator[Mapping[str, object]]:
        """Yield issues sequentially across pages."""

        for page in self.search_pages(
            jql=jql,
            fields=fields,
            expand=expand,
            validate_query=validate_query,
            page_size=page_size,
            start_at=start_at,
        ):
            for issue in page.issues:
                yield issue


__all__ = ["JiraAPI", "SearchPage"]
