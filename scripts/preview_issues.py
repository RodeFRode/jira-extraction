"""CLI helper that performs a quick Jira search and prints the results."""
from __future__ import annotations

import argparse
import json
import logging
from typing import List

from jira_extraction.config import load_config
from jira_extraction.http_client import JiraHTTPClient
from jira_extraction.jira_api import JiraAPI
from jira_extraction.logging_setup import configure_logging

LOGGER = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Preview Jira issues from the API")
    parser.add_argument("--config", default="config/etl.yml", help="Path to the ETL configuration file")
    parser.add_argument("--jql", required=True, help="JQL query to execute")
    parser.add_argument("--fields", default="summary,issuetype,priority", help="Comma separated list of fields")
    parser.add_argument("--max", type=int, default=5, help="Maximum number of issues to display")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    configure_logging(level=logging.WARNING)
    config = load_config(args.config)
    fields = [field.strip() for field in args.fields.split(",") if field.strip()]
    with JiraHTTPClient(base_url=config.jira.base_url, pat=config.jira.get_pat(), ca_bundle=config.jira.ca_bundle) as client:
        api = JiraAPI(client)
        api.get_myself()
        count = 0
        for issue in api.search_stream(
            jql=args.jql,
            fields=fields,
            page_size=config.jira.page_size,
            validate_query=config.jira.validate_query,
        ):
            print(json.dumps(issue, indent=2))
            count += 1
            if count >= args.max:
                break
        LOGGER.info("Displayed %s issues", count)


if __name__ == "__main__":
    main()
