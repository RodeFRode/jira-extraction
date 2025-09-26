# Jira Data Center Extraction Toolkit

This project provides a small Python package and command line interface for
synchronising Jira Data Center issues (including changelogs and issue links)
into a PostgreSQL reporting schema.  The implementation focuses on reliable
incremental operation, typed modules, and ease of local testing.

## Features

- Shared `httpx` client with PAT authentication, TLS verification, and
  exponential backoff retry handling.
- Configurable scopes per project/issue type with field selection.
- Generators that stream issues via `POST /rest/api/2/search` while respecting
  pagination and changelog expansion.
- State store abstraction with a PostgreSQL implementation for resumable
  checkpoints per scope.
- Transformation helpers that parse Jira JSON into structures suitable for the
  provided reporting schema, including label/component/version bridges,
  changelog entries, and issue links.
- Loader that performs UPSERT-style operations into the warehouse tables.
- CLI scripts for initial backfill, incremental sync, ad-hoc previews, and field
  metadata dumps.
- Unit tests powered by `pytest` and `httpx.MockTransport`.

## Getting started

1. **Install dependencies**

   ```bash
   pip install -r requirements.txt  # or use your preferred environment manager
   ```

   The project targets Python 3.12+.

2. **Configure environment variables**

   Copy `.env.example` to `.env` and replace the placeholder values:

   ```bash
   cp .env.example .env
   ```

   The application loads this file automatically on startup.  Populate the
   following variables with your credentials:

   - `JIRA_PAT`: Personal access token used for Jira authentication.
   - `DATABASE_URL`: PostgreSQL connection string that points to the reporting
     database where the schema from `sql/schema.sql` has been applied.

3. **Review configuration**

   The default configuration lives in `config/etl.yml`.  Adjust scopes, field
   lists, page size, or the safety window as required.  Each scope defines a
   Jira project and a collection of issue types with their own field lists.

4. **Run the initial backfill**

   ```bash
   python -m scripts.backfill --config config/etl.yml
   ```

   The command validates connectivity via `GET /rest/api/2/myself`, streams all
   configured scopes using the initial extraction window, and persists cursor
   checkpoints after each page.

5. **Schedule the incremental sync**

   ```bash
   python -m scripts.sync --config config/etl.yml
   ```

   The incremental job resumes from the stored cursor and applies the safety
   skew to avoid missing updates near the resume boundary.  Rerunning the sync
   after an interruption will continue from the last committed page.

6. **Inspect data or metadata**

   - Preview live issues: `python -m scripts.preview_issues --jql "project = ABC" --fields summary,status`
   - Dump field metadata: `python -m scripts.dump_fields`

## Testing

Run the unit test suite with:

```bash
pytest
```

The tests mock the Jira HTTP API and exercise authentication, pagination,
resumability, and transformation logic.  An additional integration test harness
can be introduced by providing real Jira credentials via environment variables
and marking the test with `pytest.mark.integration` (not included by default).

## Project layout

```
src/jira_extraction/      # Core package modules
scripts/                  # CLI entry points
config/etl.yml            # Default configuration
sql/schema.sql            # Reporting schema (apply to PostgreSQL before running)
```

## License

This repository is released under the terms of the MIT License.  See the
`LICENSE` file for details.
