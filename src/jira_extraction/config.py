"""Configuration helpers for the Jira ETL package.

This module provides a light weight typed wrapper around the YAML configuration
file used by the CLI commands.  The structure mirrors the documentation in the
project README and keeps validation intentionally small so the module remains
free of third party dependencies.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, Iterable, List, Mapping, Optional, Sequence
import os


@dataclass(slots=True)
class IssueTypeConfig:
    """Configuration for a Jira issue type inside a scope."""

    name: str
    fields: Sequence[str] = field(default_factory=list)

    def __post_init__(self) -> None:
        if not self.name:
            msg = "Issue type configuration requires a name"
            raise ValueError(msg)
        self.fields = tuple(self.fields)


@dataclass(slots=True)
class ScopeConfig:
    """Configuration describing a project/issue type extraction scope."""

    project: str
    issue_types: Sequence[IssueTypeConfig]
    jql_base: Optional[str] = None

    def __post_init__(self) -> None:
        if not self.project:
            msg = "Scope configuration requires a project key"
            raise ValueError(msg)
        if not self.issue_types:
            msg = "Scope configuration requires at least one issue type"
            raise ValueError(msg)
        self.issue_types = tuple(self.issue_types)


@dataclass(slots=True)
class WindowsConfig:
    """Configuration for extraction windows and safety skew."""

    initial_days: int = 90
    safety_skew_s: int = 60

    def __post_init__(self) -> None:
        if self.initial_days <= 0:
            msg = "Initial days window must be positive"
            raise ValueError(msg)
        if self.safety_skew_s < 0:
            msg = "Safety skew must be non negative"
            raise ValueError(msg)


@dataclass(slots=True)
class JiraConfig:
    """HTTP connectivity configuration for Jira."""

    base_url: str
    pat_env: str
    ca_bundle: Optional[str] = None
    page_size: int = 100
    parallelism: int = 2
    validate_query: bool = True

    def __post_init__(self) -> None:
        if not self.base_url:
            msg = "Jira configuration requires a base_url"
            raise ValueError(msg)
        if not self.pat_env:
            msg = "Jira configuration requires a PAT environment variable name"
            raise ValueError(msg)
        if self.page_size <= 0:
            msg = "Page size must be positive"
            raise ValueError(msg)
        if self.parallelism <= 0:
            msg = "Parallelism must be at least one"
            raise ValueError(msg)

    def get_pat(self) -> str:
        """Fetch the PAT from the configured environment variable."""

        token = os.getenv(self.pat_env)
        if not token:
            msg = f"Environment variable {self.pat_env} is not set"
            raise RuntimeError(msg)
        return token


@dataclass(slots=True)
class DatabaseConfig:
    """Database connectivity configuration."""

    dsn_env: str

    def get_dsn(self) -> str:
        dsn = os.getenv(self.dsn_env)
        if not dsn:
            msg = f"Environment variable {self.dsn_env} is not set"
            raise RuntimeError(msg)
        return dsn


@dataclass(slots=True)
class AppConfig:
    """Top level configuration for the ETL application."""

    jira: JiraConfig
    scopes: Sequence[ScopeConfig]
    windows: WindowsConfig = field(default_factory=WindowsConfig)
    database: Optional[DatabaseConfig] = None

    def iter_issue_type_scopes(self) -> Iterable[tuple[ScopeConfig, IssueTypeConfig]]:
        """Iterate over every project/issue type combination."""

        for scope in self.scopes:
            for issue_type in scope.issue_types:
                yield scope, issue_type


def _parse_issue_types(raw_issue_types: Iterable[Mapping[str, object]]) -> List[IssueTypeConfig]:
    parsed: List[IssueTypeConfig] = []
    for raw in raw_issue_types:
        name = str(raw["name"])
        fields_obj = raw.get("fields", [])
        fields: Sequence[str]
        if isinstance(fields_obj, Sequence) and not isinstance(fields_obj, (str, bytes)):
            fields = [str(value) for value in fields_obj]
        else:
            fields = [str(fields_obj)] if fields_obj else []
        parsed.append(IssueTypeConfig(name=name, fields=fields))
    return parsed


def _parse_scopes(raw_scopes: Iterable[Mapping[str, object]]) -> List[ScopeConfig]:
    parsed: List[ScopeConfig] = []
    for raw in raw_scopes:
        project = str(raw["project"])
        issue_types_raw = raw.get("issue_types", [])
        jql_base = raw.get("jql_base")
        issue_types = _parse_issue_types(issue_types_raw) if issue_types_raw else []
        parsed.append(ScopeConfig(project=project, issue_types=issue_types, jql_base=jql_base))
    return parsed


def _load_yaml(path: Path) -> Mapping[str, object]:
    try:
        import yaml  # type: ignore
    except ModuleNotFoundError as exc:  # pragma: no cover - requires optional dependency
        msg = "PyYAML is required to load configuration files"
        raise RuntimeError(msg) from exc
    with path.open("r", encoding="utf-8") as handle:
        data = yaml.safe_load(handle)
    if not isinstance(data, Mapping):
        msg = "Configuration file must contain a mapping"
        raise ValueError(msg)
    return data


def load_config(path: Path | str) -> AppConfig:
    """Load application configuration from a YAML file."""

    path = Path(path)
    data = _load_yaml(path)

    jira = data.get("jira")
    if not isinstance(jira, Mapping):
        msg = "Configuration requires a 'jira' mapping"
        raise ValueError(msg)
    jira_config = JiraConfig(
        base_url=str(jira["base_url"]),
        pat_env=str(jira.get("pat_env", "JIRA_PAT")),
        ca_bundle=jira.get("ca_bundle"),
        page_size=int(jira.get("page_size", 100)),
        parallelism=int(jira.get("parallelism", 2)),
        validate_query=bool(jira.get("validate_query", True)),
    )

    raw_scopes = data.get("scopes")
    if not isinstance(raw_scopes, Iterable):
        msg = "Configuration requires a 'scopes' sequence"
        raise ValueError(msg)
    scopes = _parse_scopes(raw_scopes)

    windows_raw = data.get("windows", {})
    if isinstance(windows_raw, Mapping):
        windows = WindowsConfig(
            initial_days=int(windows_raw.get("initial_days", 90)),
            safety_skew_s=int(windows_raw.get("safety_skew_s", 60)),
        )
    else:
        windows = WindowsConfig()

    database_raw = data.get("database")
    database = None
    if isinstance(database_raw, Mapping):
        database = DatabaseConfig(dsn_env=str(database_raw.get("dsn_env", "DATABASE_URL")))

    return AppConfig(jira=jira_config, scopes=tuple(scopes), windows=windows, database=database)


def scope_name(project: str, issue_type: str) -> str:
    """Return the canonical name for a scope."""

    return f"{project}:{issue_type}"


__all__ = [
    "AppConfig",
    "DatabaseConfig",
    "IssueTypeConfig",
    "JiraConfig",
    "ScopeConfig",
    "WindowsConfig",
    "load_config",
    "scope_name",
]
