"""Logging helpers for the Jira ETL package."""
from __future__ import annotations

import logging
from typing import Iterable


def configure_logging(level: int = logging.INFO, *, modules: Iterable[str] | None = None) -> None:
    """Configure simple structured logging for CLI commands."""

    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )
    if modules:
        for module in modules:
            logging.getLogger(module).setLevel(level)


__all__ = ["configure_logging"]
