"""CLI entry point for the initial Jira backfill."""
from __future__ import annotations

import argparse
import logging
from pathlib import Path

from jira_extraction.config import AppConfig, load_config
from jira_extraction.extract import stream_scope
from jira_extraction.http_client import JiraHTTPClient
from jira_extraction.jira_api import JiraAPI
from jira_extraction.load import ConsoleLoader, PostgresLoader, SQLiteLoader
from jira_extraction.logging_setup import configure_logging
from jira_extraction.state_store import InMemoryStateStore, PostgresStateStore, SQLiteStateStore
from jira_extraction.transform import transform_issue


LOGGER = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run an initial Jira ETL backfill")
    parser.add_argument("--config", default="config/etl.yml", help="Path to the ETL configuration file")
    parser.add_argument(
        "--local-db",
        action="store_true",
        help="Write data to a local jira.db SQLite file instead of the configured database",
    )
    parser.add_argument(
        "--local-db-path",
        default="jira.db",
        help="Destination SQLite file used when --local-db is enabled",
    )
    return parser.parse_args()


def ensure_connectivity(api: JiraAPI) -> None:
    api.get_myself()


def run_backfill(config: AppConfig, *, use_local_db: bool = False, local_db_path: Path | str = "jira.db") -> None:
    if config.output.should_print_only():
        if use_local_db:
            LOGGER.warning("--local-db flag ignored because console output mode is enabled")
        LOGGER.info("Printing backfill output to console; no data will be persisted")
        store = InMemoryStateStore()
        loader = ConsoleLoader()
    elif use_local_db:
        path = Path(local_db_path)
        LOGGER.info("Writing backfill output to local SQLite database", extra={"path": str(path)})
        store = SQLiteStateStore(path)
        loader = SQLiteLoader(path)
    else:
        if config.database is None:
            msg = "Database configuration is required for backfill"
            raise RuntimeError(msg)

        dsn = config.database.get_dsn()
        store = PostgresStateStore(dsn)
        loader = PostgresLoader(dsn)

    with JiraHTTPClient(base_url=config.jira.base_url, pat=config.jira.get_pat(), ca_bundle=config.jira.ca_bundle) as client:
        api = JiraAPI(client)
        ensure_connectivity(api)
        for scope, issue_type in config.iter_issue_type_scopes():
            LOGGER.info("Backfilling scope", extra={"project": scope.project, "issue_type": issue_type.name})
            for page in stream_scope(
                api,
                scope,
                issue_type,
                windows=config.windows,
                store=store,
                mode="initial",
                page_size=config.jira.page_size,
                validate_query=config.jira.validate_query,
            ):
                transforms = [transform_issue(issue) for issue in page.issues]
                loader.load_page(transforms)


def main() -> None:
    args = parse_args()
    configure_logging()
    config = load_config(args.config)
    run_backfill(config, use_local_db=args.local_db, local_db_path=args.local_db_path)


if __name__ == "__main__":
    main()
