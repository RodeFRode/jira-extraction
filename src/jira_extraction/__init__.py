"""Jira Data Center extraction toolkit."""

from __future__ import annotations

import os
from pathlib import Path


def _load_local_env() -> None:
    """Load environment variables from the nearest .env file if present."""

    search_roots = [Path.cwd(), *Path.cwd().parents, Path(__file__).resolve().parent, *Path(__file__).resolve().parents]
    seen: set[Path] = set()
    for directory in search_roots:
        if directory in seen:
            continue
        seen.add(directory)
        env_path = directory / ".env"
        if env_path.is_file():
            with env_path.open("r", encoding="utf-8") as env_file:
                for raw_line in env_file:
                    line = raw_line.strip()
                    if not line or line.startswith("#"):
                        continue
                    if "=" not in line:
                        continue
                    key, value = line.split("=", 1)
                    key = key.strip()
                    value = value.strip().strip('"').strip("'")
                    os.environ.setdefault(key, value)
            break


_load_local_env()

__all__ = [
    "config",
    "http_client",
    "jira_api",
    "extract",
    "transform",
    "load",
    "state_store",
]
