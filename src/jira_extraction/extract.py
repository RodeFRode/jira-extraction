"""High level extraction helpers that orchestrate API usage."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Iterable, Iterator, List, Mapping, MutableMapping, Sequence

from .config import IssueTypeConfig, ScopeConfig, WindowsConfig, scope_name
from .jira_api import JiraAPI, SearchPage
from .state_store import Cursor, StateStore


@dataclass(slots=True)
class ExtractedPage:
    """Represents a processed page of issues."""

    scope: str
    page: SearchPage
    issues: List[Mapping[str, object]]


def parse_jira_datetime(value: str) -> datetime:
    """Parse the timestamp format returned by Jira."""

    # Jira generally returns values in the format 2024-01-01T12:34:56.789+0000
    # which is not directly handled by :meth:`datetime.fromisoformat`.
    if value.endswith("Z"):
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    for fmt in ("%Y-%m-%dT%H:%M:%S.%f%z", "%Y-%m-%dT%H:%M:%S%z"):
        try:
            return datetime.strptime(value, fmt)
        except ValueError:
            continue
    return datetime.fromisoformat(value)


def build_initial_jql(scope: ScopeConfig, issue_type: IssueTypeConfig, windows: WindowsConfig) -> str:
    """Construct the JQL for an initial backfill run."""

    if scope.jql_base:
        base = scope.jql_base
    else:
        base = f"project = {scope.project}"
    return (
        f"{base} AND issuetype = \"{issue_type.name}\" AND updated >= -{windows.initial_days}d "
        "ORDER BY updated ASC, key ASC"
    )


def build_incremental_jql(
    scope: ScopeConfig,
    issue_type: IssueTypeConfig,
    windows: WindowsConfig,
    cursor: Cursor,
) -> str:
    """Construct the JQL for an incremental run."""

    if scope.jql_base:
        base = scope.jql_base
    else:
        base = f"project = {scope.project}"
    if cursor.last_updated_at:
        anchor = parse_jira_datetime(cursor.last_updated_at) - timedelta(seconds=windows.safety_skew_s)
    else:
        anchor = datetime.now(timezone.utc) - timedelta(days=windows.initial_days)
    anchor_str = anchor.strftime("%Y-%m-%d %H:%M")
    return (
        f"{base} AND issuetype = \"{issue_type.name}\" AND updated >= '{anchor_str}' "
        "ORDER BY updated ASC, key ASC"
    )


def filter_incremental_issues(issues: Sequence[Mapping[str, object]], cursor: Cursor) -> List[Mapping[str, object]]:
    if not cursor.last_updated_at:
        return list(issues)
    anchor = parse_jira_datetime(cursor.last_updated_at)
    filtered: List[Mapping[str, object]] = []
    for issue in issues:
        fields = issue.get("fields", {})
        updated = parse_jira_datetime(str(fields.get("updated"))) if fields.get("updated") else anchor
        key = str(issue.get("key"))
        if updated > anchor:
            filtered.append(issue)
        elif updated == anchor and cursor.last_issue_key and key > cursor.last_issue_key:
            filtered.append(issue)
    return filtered


def update_cursor_from_issues(cursor: Cursor, issues: Sequence[Mapping[str, object]]) -> Cursor:
    if not issues:
        return cursor
    max_updated = cursor.last_updated_at
    max_key = cursor.last_issue_key
    max_dt = parse_jira_datetime(max_updated) if max_updated else None
    for issue in issues:
        fields = issue.get("fields", {})
        updated_raw = fields.get("updated")
        key = str(issue.get("key"))
        if not updated_raw:
            continue
        updated_dt = parse_jira_datetime(str(updated_raw))
        if max_dt is None or updated_dt > max_dt:
            max_dt = updated_dt
            max_key = key
        elif updated_dt == max_dt and (max_key is None or key > max_key):
            max_key = key
    if max_dt is None:
        return cursor
    return Cursor(
        last_updated_at=max_dt.strftime("%Y-%m-%dT%H:%M:%S.%f%z"),
        last_issue_key=max_key,
        resume_page_at=cursor.resume_page_at,
    )


def stream_scope(
    api: JiraAPI,
    scope: ScopeConfig,
    issue_type: IssueTypeConfig,
    *,
    windows: WindowsConfig,
    store: StateStore,
    mode: str,
    page_size: int,
    validate_query: bool,
) -> Iterator[ExtractedPage]:
    """Stream issues for a scope while updating the state store."""

    scope_id = scope_name(scope.project, issue_type.name)
    cursor = store.load(scope_id)
    if mode == "initial":
        jql = build_initial_jql(scope, issue_type, windows)
        start_at = cursor.resume_page_at
    elif mode == "incremental":
        jql = build_incremental_jql(scope, issue_type, windows, cursor)
        start_at = cursor.resume_page_at
    else:
        msg = "Unsupported extraction mode"
        raise ValueError(msg)

    for page in api.search_pages(
        jql=jql,
        fields=issue_type.fields,
        page_size=page_size,
        validate_query=validate_query,
        start_at=start_at,
    ):
        issues = list(page.issues)
        if mode == "incremental":
            issues = filter_incremental_issues(issues, cursor)
        next_cursor = update_cursor_from_issues(cursor, issues)
        next_cursor.resume_page_at = page.start_at + len(page.issues)
        store.save(scope_id, next_cursor)
        yield ExtractedPage(scope=scope_id, page=page, issues=issues)
        cursor = next_cursor


__all__ = [
    "ExtractedPage",
    "build_initial_jql",
    "build_incremental_jql",
    "filter_incremental_issues",
    "parse_jira_datetime",
    "stream_scope",
    "update_cursor_from_issues",
]
