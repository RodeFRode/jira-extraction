"""Transform Jira issues into relational friendly structures."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Iterable, Iterator, List, Mapping, MutableMapping, Optional


@dataclass(slots=True)
class IssueTransform:
    """Container for all derived rows from an issue."""

    issue: Dict[str, object]
    labels: List[Dict[str, object]]
    components: List[Dict[str, object]]
    fix_versions: List[Dict[str, object]]
    links: List[Dict[str, object]]
    changes: List[Dict[str, object]]


def _extract_custom_fields(fields: Mapping[str, object]) -> Dict[str, object]:
    return {key: value for key, value in fields.items() if key.startswith("customfield_")}


def transform_issue(issue: Mapping[str, object]) -> IssueTransform:
    fields = issue.get("fields", {})
    if not isinstance(fields, Mapping):
        fields = {}

    project = fields.get("project", {})
    issue_type = fields.get("issuetype", {})
    priority = fields.get("priority")
    status = fields.get("status")

    snapshot: Dict[str, object] = {
        "issue_id": int(issue.get("id")),
        "issue_key": issue.get("key"),
        "project_id": project.get("id"),
        "project_key": project.get("key"),
        "project_name": project.get("name"),
        "issue_type_id": issue_type.get("id") if isinstance(issue_type, Mapping) else None,
        "issue_type_name": issue_type.get("name") if isinstance(issue_type, Mapping) else None,
        "summary": fields.get("summary"),
        "description": fields.get("description"),
        "priority_id": priority.get("id") if isinstance(priority, Mapping) else None,
        "priority_name": priority.get("name") if isinstance(priority, Mapping) else None,
        "status_id": status.get("id") if isinstance(status, Mapping) else None,
        "status_name": status.get("name") if isinstance(status, Mapping) else None,
        "reporter_id": (fields.get("reporter") or {}).get("accountId") if isinstance(fields.get("reporter"), Mapping) else None,
        "assignee_id": (fields.get("assignee") or {}).get("accountId") if isinstance(fields.get("assignee"), Mapping) else None,
        "created_at": fields.get("created"),
        "updated_at": fields.get("updated"),
        "resolution_date": fields.get("resolutiondate"),
        "due_date": fields.get("duedate"),
        "custom_fields": _extract_custom_fields(fields),
        "raw_issue": issue,
    }
    if "changelog" in issue:
        snapshot["raw_changelog"] = issue["changelog"]

    labels = []
    for label in fields.get("labels", []) or []:
        labels.append({"issue_id": snapshot["issue_id"], "label": label})

    components = []
    for component in fields.get("components", []) or []:
        if isinstance(component, Mapping):
            components.append(
                {
                    "issue_id": snapshot["issue_id"],
                    "component_id": component.get("id"),
                    "component_name": component.get("name"),
                    "project_id": project.get("id"),
                }
            )

    fix_versions = []
    for version in fields.get("fixVersions", []) or []:
        if isinstance(version, Mapping):
            fix_versions.append(
                {
                    "issue_id": snapshot["issue_id"],
                    "fix_version_id": version.get("id"),
                    "fix_version_name": version.get("name"),
                    "released": version.get("released"),
                    "release_date": version.get("releaseDate"),
                    "project_id": project.get("id"),
                }
            )

    links = []
    for link in fields.get("issuelinks", []) or []:
        if not isinstance(link, Mapping):
            continue
        link_type = link.get("type", {}) if isinstance(link.get("type"), Mapping) else {}
        type_name = link_type.get("name")
        type_key = link_type.get("id") or link_type.get("name")
        for direction, value in ("outward", link.get("outwardIssue")), ("inward", link.get("inwardIssue")):
            if isinstance(value, Mapping) and value.get("key"):
                links.append(
                    {
                        "src_issue_id": snapshot["issue_id"],
                        "dst_issue_key": value.get("key"),
                        "link_type_key": type_key,
                        "link_type_name": type_name,
                        "direction": direction,
                    }
                )

    changes = []
    changelog = issue.get("changelog", {})
    histories = changelog.get("histories", []) if isinstance(changelog, Mapping) else []
    for history in histories:
        if not isinstance(history, Mapping):
            continue
        history_id = history.get("id")
        items = history.get("items", [])
        author = history.get("author", {}) if isinstance(history.get("author"), Mapping) else {}
        for item in items or []:
            if not isinstance(item, Mapping):
                continue
            changes.append(
                {
                    "history_id": history_id,
                    "issue_id": snapshot["issue_id"],
                    "author_id": author.get("accountId"),
                    "created_at": history.get("created"),
                    "field": item.get("field"),
                    "field_type": item.get("fieldtype"),
                    "from": item.get("from"),
                    "to": item.get("to"),
                    "from_string": item.get("fromString"),
                    "to_string": item.get("toString"),
                }
            )

    return IssueTransform(
        issue=snapshot,
        labels=labels,
        components=components,
        fix_versions=fix_versions,
        links=links,
        changes=changes,
    )


__all__ = ["IssueTransform", "transform_issue"]
