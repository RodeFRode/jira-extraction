"""State store helpers used for resumable extraction."""
from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Dict, Mapping, Optional, Protocol

try:  # pragma: no cover - optional dependency
    import psycopg  # type: ignore
except ModuleNotFoundError:  # pragma: no cover - optional dependency
    psycopg = None  # type: ignore

LOGGER = logging.getLogger(__name__)


@dataclass(slots=True)
class Cursor:
    """Cursor information used to resume a scope."""

    last_updated_at: Optional[str] = None
    last_issue_key: Optional[str] = None
    resume_page_at: int = 0


class StateStore(Protocol):
    """Protocol describing the behaviour required from a state store."""

    def load(self, scope_name: str) -> Cursor:
        ...

    def save(self, scope_name: str, cursor: Cursor) -> None:
        ...


class InMemoryStateStore:
    """Simple in memory store used primarily in tests."""

    def __init__(self) -> None:
        self._data: Dict[str, Cursor] = {}

    def load(self, scope_name: str) -> Cursor:
        return self._data.get(scope_name, Cursor())

    def save(self, scope_name: str, cursor: Cursor) -> None:
        self._data[scope_name] = cursor


class PostgresStateStore:
    """Persist ETL cursor information inside the etl_cursors table."""

    def __init__(self, dsn: str) -> None:
        if psycopg is None:  # pragma: no cover - requires optional dependency
            msg = "psycopg is required to use PostgresStateStore"
            raise RuntimeError(msg)
        self._dsn = dsn
        self._ensure_table()

    def _ensure_table(self) -> None:
        assert psycopg is not None  # for type checkers
        query = (
            "CREATE TABLE IF NOT EXISTS etl_cursors ("
            " scope_name TEXT PRIMARY KEY,"  # noqa: E131 - readability
            " last_updated_at TIMESTAMPTZ,"  # noqa: E131
            " last_issue_key TEXT,"  # noqa: E131
            " resume_page_at INTEGER DEFAULT 0"  # noqa: E131
            ")"
        )
        with psycopg.connect(self._dsn) as conn, conn.cursor() as cur:
            cur.execute(query)
            conn.commit()

    def load(self, scope_name: str) -> Cursor:
        assert psycopg is not None
        with psycopg.connect(self._dsn) as conn, conn.cursor() as cur:
            cur.execute(
                "SELECT last_updated_at, last_issue_key, resume_page_at FROM etl_cursors WHERE scope_name = %s",
                (scope_name,),
            )
            row = cur.fetchone()
        if not row:
            return Cursor()
        last_updated_at, last_issue_key, resume_page_at = row
        return Cursor(
            last_updated_at=last_updated_at.isoformat() if last_updated_at else None,
            last_issue_key=last_issue_key,
            resume_page_at=resume_page_at or 0,
        )

    def save(self, scope_name: str, cursor: Cursor) -> None:
        assert psycopg is not None
        with psycopg.connect(self._dsn) as conn, conn.cursor() as cur:
            cur.execute(
                "INSERT INTO etl_cursors (scope_name, last_updated_at, last_issue_key, resume_page_at)"
                " VALUES (%s, %s, %s, %s)"
                " ON CONFLICT(scope_name) DO UPDATE SET"
                " last_updated_at = EXCLUDED.last_updated_at,"  # noqa: E131
                " last_issue_key = EXCLUDED.last_issue_key,"  # noqa: E131
                " resume_page_at = EXCLUDED.resume_page_at",
                (
                    scope_name,
                    cursor.last_updated_at,
                    cursor.last_issue_key,
                    cursor.resume_page_at,
                ),
            )
            conn.commit()


__all__ = ["Cursor", "StateStore", "InMemoryStateStore", "PostgresStateStore"]
