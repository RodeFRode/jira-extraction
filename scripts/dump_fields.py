"""Dump Jira field metadata to a JSON file."""
from __future__ import annotations

import argparse
import json
import logging
from pathlib import Path

from jira_extraction.config import load_config
from jira_extraction.http_client import JiraHTTPClient
from jira_extraction.jira_api import JiraAPI
from jira_extraction.logging_setup import configure_logging

LOGGER = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Dump Jira field metadata")
    parser.add_argument("--config", default="config/etl.yml", help="Path to the ETL configuration file")
    parser.add_argument("--output", default="out/fields.json", help="Destination JSON file")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    configure_logging(level=logging.INFO)
    config = load_config(args.config)
    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    with JiraHTTPClient(base_url=config.jira.base_url, pat=config.jira.get_pat(), ca_bundle=config.jira.ca_bundle) as client:
        api = JiraAPI(client)
        api.get_myself()
        fields = api.get_fields()
    output.write_text(json.dumps(fields, indent=2), encoding="utf-8")
    LOGGER.info("Wrote %s field definitions", len(fields))


if __name__ == "__main__":
    main()
