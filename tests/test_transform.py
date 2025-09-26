from __future__ import annotations

from jira_extraction.transform import transform_issue


def test_transform_issue_extracts_links_and_changes() -> None:
    issue = {
        "id": "1",
        "key": "ABC-1",
        "fields": {
            "summary": "Example",
            "description": "Body",
            "project": {"id": "10", "key": "ABC", "name": "Example"},
            "issuetype": {"id": "100", "name": "Bug"},
            "priority": {"id": "2", "name": "High"},
            "status": {"id": "3", "name": "In Progress"},
            "labels": ["backend"],
            "components": [{"id": "200", "name": "API"}],
            "fixVersions": [{"id": "300", "name": "v1.0", "released": False}],
            "issuelinks": [
                {
                    "type": {"id": "1000", "name": "Relates"},
                    "outwardIssue": {"key": "ABC-2"},
                    "inwardIssue": {"key": "ABC-3"},
                }
            ],
            "customfield_123": "value",
        },
        "changelog": {
            "histories": [
                {
                    "id": "42",
                    "created": "2024-01-01T00:00:00.000+0000",
                    "author": {"accountId": "user"},
                    "items": [
                        {
                            "field": "status",
                            "fieldtype": "jira",
                            "from": "1",
                            "to": "3",
                            "fromString": "Open",
                            "toString": "In Progress",
                        }
                    ],
                }
            ]
        },
    }

    transformed = transform_issue(issue)
    assert transformed.issue["issue_id"] == 1
    assert transformed.issue["custom_fields"] == {"customfield_123": "value"}
    assert transformed.labels == [{"issue_id": 1, "label": "backend"}]
    assert transformed.components[0]["component_id"] == "200"
    assert transformed.fix_versions[0]["fix_version_name"] == "v1.0"
    assert {link["direction"] for link in transformed.links} == {"outward", "inward"}
    assert transformed.links[0]["dst_issue_key"] in {"ABC-2", "ABC-3"}
    assert transformed.changes[0]["field"] == "status"
