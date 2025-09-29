"""Load transformed Jira data into Postgres or alternative sinks."""
from __future__ import annotations

import json
import sqlite3
from dataclasses import asdict, dataclass
import sys
from pathlib import Path
from typing import IO, Sequence

import psycopg
from psycopg.types.json import Json

from .transform import IssueTransform


@dataclass(slots=True)
class LoadStats:
    """Simple statistics about a load operation."""

    issues: int = 0
    links: int = 0
    changes: int = 0


class PostgresLoader:
    """Perform batched upserts into the warehouse schema."""

    def __init__(self, dsn: str) -> None:
        self._dsn = dsn

    def load_page(self, transforms: Sequence[IssueTransform]) -> LoadStats:
        stats = LoadStats()
        if not transforms:
            return stats
        with psycopg.connect(self._dsn) as conn:
            with conn.transaction():
                with conn.cursor() as cur:
                    for transform in transforms:
                        self._upsert_dimensions(cur, transform)
                    for transform in transforms:
                        self._upsert_issue(cur, transform)
                        stats.issues += 1
                    stats.links += self._upsert_links(conn, transforms)
                    stats.changes += self._insert_changes(cur, transforms)
        return stats


class ConsoleLoader:
    """Emit transformed issues to the console instead of persisting them."""

    def __init__(self, *, stream: IO[str] | None = None, indent: int = 2) -> None:
        self._stream = stream or sys.stdout
        self._indent = indent

    def load_page(self, transforms: Sequence[IssueTransform]) -> LoadStats:
        stats = LoadStats()
        for transform in transforms:
            payload = asdict(transform)
            json.dump(payload, self._stream, indent=self._indent, default=str)
            self._stream.write("\n")
            stats.issues += 1
            stats.links += len(transform.links)
            stats.changes += len(transform.changes)
        if transforms:
            self._stream.flush()
        return stats

    # Dimension helpers -------------------------------------------------

    def _upsert_dimensions(self, cur: psycopg.Cursor, transform: IssueTransform) -> None:
        issue = transform.issue
        project_id = _to_int(issue.get("project_id"))
        if project_id is not None:
            cur.execute(
                "INSERT INTO projects (project_id, project_key, name) VALUES (%s, %s, %s)"
                " ON CONFLICT(project_id) DO UPDATE SET project_key = EXCLUDED.project_key, name = EXCLUDED.name",
                (project_id, issue.get("project_key"), issue.get("project_name")),
            )
        issue_type_id = _to_int(issue.get("issue_type_id"))
        if issue_type_id is not None:
            cur.execute(
                "INSERT INTO issue_types (issue_type_id, name) VALUES (%s, %s)"
                " ON CONFLICT(issue_type_id) DO UPDATE SET name = EXCLUDED.name",
                (issue_type_id, issue.get("issue_type_name")),
            )
        priority_id = _to_int(issue.get("priority_id"))
        if priority_id is not None:
            cur.execute(
                "INSERT INTO priorities (priority_id, name) VALUES (%s, %s)"
                " ON CONFLICT(priority_id) DO UPDATE SET name = EXCLUDED.name",
                (priority_id, issue.get("priority_name")),
            )
        status_id = _to_int(issue.get("status_id"))
        if status_id is not None:
            cur.execute(
                "INSERT INTO statuses (status_id, name) VALUES (%s, %s)"
                " ON CONFLICT(status_id) DO UPDATE SET name = EXCLUDED.name",
                (status_id, issue.get("status_name")),
            )
        for component in transform.components:
            component_id = _to_int(component.get("component_id"))
            project_id = _to_int(component.get("project_id"))
            if component_id is None or project_id is None:
                continue
            cur.execute(
                "INSERT INTO components (component_id, project_id, name) VALUES (%s, %s, %s)"
                " ON CONFLICT(component_id) DO UPDATE SET name = EXCLUDED.name",
                (component_id, project_id, component.get("component_name")),
            )
        for version in transform.fix_versions:
            version_id = _to_int(version.get("fix_version_id"))
            project_id = _to_int(version.get("project_id"))
            if version_id is None or project_id is None:
                continue
            cur.execute(
                "INSERT INTO fix_versions (fix_version_id, project_id, name, released, release_date)"
                " VALUES (%s, %s, %s, %s, %s)"
                " ON CONFLICT(fix_version_id) DO UPDATE SET"
                " name = EXCLUDED.name, released = EXCLUDED.released, release_date = EXCLUDED.release_date",
                (
                    version_id,
                    project_id,
                    version.get("fix_version_name"),
                    version.get("released"),
                    version.get("release_date"),
                ),
            )
        for label in transform.labels:
            cur.execute(
                "INSERT INTO labels (label) VALUES (%s) ON CONFLICT(label) DO NOTHING",
                (label.get("label"),),
            )

    def _upsert_issue(self, cur: psycopg.Cursor, transform: IssueTransform) -> None:
        issue = transform.issue
        cur.execute(
            "INSERT INTO issues (issue_id, issue_key, project_id, issue_type_id, status_id, priority_id, summary, description,"
            " reporter_id, assignee_id, created_at, updated_at, resolution_date, due_date, custom_fields, raw_issue, raw_changelog)"
            " VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
            " ON CONFLICT(issue_id) DO UPDATE SET"
            " issue_key = EXCLUDED.issue_key,"
            " project_id = EXCLUDED.project_id,"
            " issue_type_id = EXCLUDED.issue_type_id,"
            " status_id = EXCLUDED.status_id,"
            " priority_id = EXCLUDED.priority_id,"
            " summary = EXCLUDED.summary,"
            " description = EXCLUDED.description,"
            " reporter_id = EXCLUDED.reporter_id,"
            " assignee_id = EXCLUDED.assignee_id,"
            " created_at = EXCLUDED.created_at,"
            " updated_at = EXCLUDED.updated_at,"
            " resolution_date = EXCLUDED.resolution_date,"
            " due_date = EXCLUDED.due_date,"
            " custom_fields = EXCLUDED.custom_fields,"
            " raw_issue = EXCLUDED.raw_issue,"
            " raw_changelog = EXCLUDED.raw_changelog",
            (
                _to_int(issue.get("issue_id")),
                issue.get("issue_key"),
                _to_int(issue.get("project_id")),
                _to_int(issue.get("issue_type_id")),
                _to_int(issue.get("status_id")),
                _to_int(issue.get("priority_id")),
                issue.get("summary"),
                issue.get("description"),
                issue.get("reporter_id"),
                issue.get("assignee_id"),
                issue.get("created_at"),
                issue.get("updated_at"),
                issue.get("resolution_date"),
                issue.get("due_date"),
                Json(issue.get("custom_fields", {})),
                Json(issue.get("raw_issue")),
                Json(issue.get("raw_changelog")) if issue.get("raw_changelog") is not None else None,
            ),
        )
        issue_id = _to_int(issue.get("issue_id"))
        cur.execute("DELETE FROM issue_labels WHERE issue_id = %s", (issue_id,))
        label_rows = [(issue_id, label.get("label")) for label in transform.labels]
        if label_rows:
            cur.executemany(
                "INSERT INTO issue_labels (issue_id, label) VALUES (%s, %s) ON CONFLICT DO NOTHING",
                label_rows,
            )
        cur.execute("DELETE FROM issue_components WHERE issue_id = %s", (issue_id,))
        component_rows = [
            (issue_id, _to_int(component.get("component_id"))) for component in transform.components
        ]
        component_rows = [row for row in component_rows if row[1] is not None]
        if component_rows:
            cur.executemany(
                "INSERT INTO issue_components (issue_id, component_id) VALUES (%s, %s) ON CONFLICT DO NOTHING",
                component_rows,
            )
        cur.execute("DELETE FROM issue_fix_versions WHERE issue_id = %s", (issue_id,))
        version_rows = [
            (issue_id, _to_int(version.get("fix_version_id"))) for version in transform.fix_versions
        ]
        version_rows = [row for row in version_rows if row[1] is not None]
        if version_rows:
            cur.executemany(
                "INSERT INTO issue_fix_versions (issue_id, fix_version_id) VALUES (%s, %s) ON CONFLICT DO NOTHING",
                version_rows,
            )

    def _upsert_links(self, conn: psycopg.Connection, transforms: Sequence[IssueTransform]) -> int:
        keys = {link["dst_issue_key"] for transform in transforms for link in transform.links if link.get("dst_issue_key")}
        if not keys:
            return 0
        with conn.cursor() as cur:
            cur.execute(
                "SELECT issue_key, issue_id FROM issues WHERE issue_key = ANY(%s)",
                (list(keys),),
            )
            mapping = {row[0]: row[1] for row in cur.fetchall()}
            inserted = 0
            for transform in transforms:
                src_issue_id = _to_int(transform.issue.get("issue_id"))
                for link in transform.links:
                    dst_issue_key = link.get("dst_issue_key")
                    dst_issue_id = mapping.get(dst_issue_key)
                    if not dst_issue_id:
                        continue
                    cur.execute(
                        "INSERT INTO issue_links (src_issue_id, dst_issue_id, link_type_key, link_type_name, direction)"
                        " VALUES (%s, %s, %s, %s, %s)"
                        " ON CONFLICT DO NOTHING",
                        (
                            src_issue_id,
                            dst_issue_id,
                            link.get("link_type_key"),
                            link.get("link_type_name"),
                            link.get("direction"),
                        ),
                    )
                    inserted += 1
            return inserted

    def _insert_changes(self, cur: psycopg.Cursor, transforms: Sequence[IssueTransform]) -> int:
        inserted = 0
        for transform in transforms:
            for change in transform.changes:
                cur.execute(
                    "INSERT INTO change_groups (history_id, issue_id, author_id, created_at)"
                    " VALUES (%s, %s, %s, %s)"
                    " ON CONFLICT(history_id) DO UPDATE SET author_id = EXCLUDED.author_id, created_at = EXCLUDED.created_at",
                    (
                        _to_int(change.get("history_id")),
                        _to_int(change.get("issue_id")),
                        change.get("author_id"),
                        change.get("created_at"),
                    ),
                )
                cur.execute(
                    "INSERT INTO change_items (history_id, field, field_type, from_string, to_string, from_value, to_value)"
                    " VALUES (%s, %s, %s, %s, %s, %s, %s)"
                    " ON CONFLICT(history_id, field, from_value, to_value) DO UPDATE SET"
                    " field_type = EXCLUDED.field_type, from_string = EXCLUDED.from_string, to_string = EXCLUDED.to_string",
                    (
                        _to_int(change.get("history_id")),
                        change.get("field"),
                        change.get("field_type"),
                        change.get("from_string"),
                        change.get("to_string"),
                        change.get("from"),
                        change.get("to"),
                    ),
                )
                inserted += 1
        return inserted


class SQLiteLoader:
    """Persist issue transforms into a local SQLite database."""

    def __init__(self, path: str | Path) -> None:
        self._path = Path(path)
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._ensure_schema()

    def load_page(self, transforms: Sequence[IssueTransform]) -> LoadStats:
        stats = LoadStats()
        if not transforms:
            return stats

        with sqlite3.connect(self._path) as conn:
            for transform in transforms:
                issue_id = _to_int(transform.issue.get("issue_id"))
                if issue_id is None:
                    msg = "Issue transform is missing an issue_id"
                    raise ValueError(msg)
                payload = json.dumps(
                    {
                        "issue": transform.issue,
                        "labels": transform.labels,
                        "components": transform.components,
                        "fix_versions": transform.fix_versions,
                        "links": transform.links,
                        "changes": transform.changes,
                    },
                    ensure_ascii=False,
                )
                conn.execute(
                    "INSERT OR REPLACE INTO issue_transforms (issue_id, issue_key, payload) VALUES (?, ?, ?)",
                    (issue_id, transform.issue.get("issue_key"), payload),
                )
                stats.issues += 1
                stats.links += len(transform.links)
                stats.changes += len(transform.changes)
            conn.commit()

        return stats

    def _ensure_schema(self) -> None:
        with sqlite3.connect(self._path) as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS issue_transforms (
                    issue_id INTEGER PRIMARY KEY,
                    issue_key TEXT,
                    payload TEXT NOT NULL
                )
                """
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS ix_issue_transforms_issue_key ON issue_transforms(issue_key)"
            )
            conn.commit()


def _to_int(value: object) -> int | None:
    try:
        if value is None:
            return None
        return int(value)
    except (TypeError, ValueError):
        return None


__all__ = ["LoadStats", "PostgresLoader", "SQLiteLoader", "ConsoleLoader"]
