-- Jira Reporting Schema (PostgreSQL)
-- v0.3 — includes issue links bridge, no user dimension (store user ids inline),
--        fixVersion kept, boards omitted, custom fields via JSONB + view.

BEGIN;

CREATE TABLE IF NOT EXISTS projects (
  project_id        BIGINT PRIMARY KEY,
  project_key       TEXT NOT NULL UNIQUE,
  name              TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS issue_types (
  issue_type_id     BIGINT PRIMARY KEY,
  name              TEXT NOT NULL,
  description       TEXT
);

CREATE TABLE IF NOT EXISTS priorities (
  priority_id       BIGINT PRIMARY KEY,
  name              TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS statuses (
  status_id         BIGINT PRIMARY KEY,
  name              TEXT NOT NULL,
  category_key      TEXT,
  category_name     TEXT
);

CREATE TABLE IF NOT EXISTS components (
  component_id      BIGINT PRIMARY KEY,
  project_id        BIGINT NOT NULL REFERENCES projects(project_id) ON DELETE CASCADE,
  name              TEXT NOT NULL,
  UNIQUE(project_id, name)
);

CREATE TABLE IF NOT EXISTS fix_versions (
  fix_version_id    BIGINT PRIMARY KEY,
  project_id        BIGINT NOT NULL REFERENCES projects(project_id) ON DELETE CASCADE,
  name              TEXT NOT NULL,
  released          BOOLEAN,
  release_date      DATE,
  UNIQUE(project_id, name)
);

CREATE TABLE IF NOT EXISTS issues (
  issue_id          BIGINT PRIMARY KEY,
  issue_key         TEXT NOT NULL UNIQUE,
  project_id        BIGINT NOT NULL REFERENCES projects(project_id),
  issue_type_id     BIGINT NOT NULL REFERENCES issue_types(issue_type_id),
  status_id         BIGINT REFERENCES statuses(status_id),
  priority_id       BIGINT REFERENCES priorities(priority_id),
  summary           TEXT,
  description       TEXT,
  reporter_id       TEXT,
  assignee_id       TEXT,
  created_at        TIMESTAMPTZ,
  updated_at        TIMESTAMPTZ,
  resolution_date   TIMESTAMPTZ,
  due_date          TIMESTAMPTZ,
  custom_fields     JSONB DEFAULT '{}'::jsonb,
  raw_issue         JSONB,
  raw_changelog     JSONB
);
CREATE INDEX IF NOT EXISTS idx_issues_project_updated ON issues(project_id, updated_at);
CREATE INDEX IF NOT EXISTS idx_issues_custom_fields_gin ON issues USING GIN (custom_fields);

CREATE TABLE IF NOT EXISTS labels (
  label            TEXT PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS issue_labels (
  issue_id         BIGINT REFERENCES issues(issue_id) ON DELETE CASCADE,
  label            TEXT REFERENCES labels(label) ON DELETE CASCADE,
  PRIMARY KEY (issue_id, label)
);

CREATE TABLE IF NOT EXISTS issue_components (
  issue_id         BIGINT REFERENCES issues(issue_id) ON DELETE CASCADE,
  component_id     BIGINT REFERENCES components(component_id) ON DELETE CASCADE,
  PRIMARY KEY (issue_id, component_id)
);

CREATE TABLE IF NOT EXISTS issue_fix_versions (
  issue_id         BIGINT REFERENCES issues(issue_id) ON DELETE CASCADE,
  fix_version_id   BIGINT REFERENCES fix_versions(fix_version_id) ON DELETE CASCADE,
  PRIMARY KEY (issue_id, fix_version_id)
);

CREATE TABLE IF NOT EXISTS issue_links (
  link_id          BIGSERIAL PRIMARY KEY,
  src_issue_id     BIGINT NOT NULL REFERENCES issues(issue_id) ON DELETE CASCADE,
  dst_issue_id     BIGINT NOT NULL REFERENCES issues(issue_id) ON DELETE CASCADE,
  link_type_key    TEXT,
  link_type_name   TEXT,
  direction        TEXT CHECK (direction IN ('outward', 'inward')) NOT NULL,
  created_at       TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS idx_issue_links_src ON issue_links(src_issue_id);
CREATE INDEX IF NOT EXISTS idx_issue_links_dst ON issue_links(dst_issue_id);
CREATE INDEX IF NOT EXISTS idx_issue_links_type ON issue_links(link_type_name, direction);

CREATE TABLE IF NOT EXISTS change_groups (
  history_id       BIGINT PRIMARY KEY,
  issue_id         BIGINT NOT NULL REFERENCES issues(issue_id) ON DELETE CASCADE,
  author_id        TEXT,
  created_at       TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS change_items (
  history_id       BIGINT NOT NULL REFERENCES change_groups(history_id) ON DELETE CASCADE,
  field            TEXT NOT NULL,
  field_type       TEXT,
  from_string      TEXT,
  to_string        TEXT,
  from_value       TEXT,
  to_value         TEXT
);
CREATE INDEX IF NOT EXISTS idx_change_groups_issue ON change_groups(issue_id, created_at);
CREATE INDEX IF NOT EXISTS idx_change_items_field ON change_items(field);

CREATE TABLE IF NOT EXISTS custom_field_defs (
  field_id         TEXT PRIMARY KEY,
  name             TEXT NOT NULL,
  schema_type      TEXT,
  schema_custom    TEXT,
  schema_items     TEXT
);

CREATE TABLE IF NOT EXISTS etl_runs (
  run_id           UUID PRIMARY KEY,
  started_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
  finished_at      TIMESTAMPTZ,
  status           TEXT CHECK (status IN ('running','success','failed')) NOT NULL DEFAULT 'running',
  note             TEXT
);

CREATE TABLE IF NOT EXISTS etl_cursors (
  scope_name       TEXT PRIMARY KEY,
  last_updated_at  TIMESTAMPTZ,
  last_issue_id    BIGINT,
  resume_page_at   INTEGER,
  resume_token     TEXT
);

CREATE OR REPLACE VIEW v_issue_custom_fields AS
SELECT
  i.issue_id,
  i.issue_key,
  i.custom_fields->>'customfield_11881' AS cf_11881,
  i.custom_fields->>'customfield_11882' AS cf_11882,
  i.custom_fields->>'customfield_11883' AS cf_11883,
  i.custom_fields->>'customfield_11885' AS cf_11885,
  i.custom_fields->>'customfield_11880' AS cf_11880,
  i.custom_fields->>'customfield_11884' AS cf_11884,
  i.custom_fields->>'customfield_10312' AS cf_10312,
  i.custom_fields->>'customfield_10355' AS cf_10355,
  i.custom_fields->>'customfield_10317' AS cf_10317,
  i.custom_fields->>'customfield_10363' AS cf_10363,
  i.custom_fields->>'customfield_10364' AS cf_10364,
  i.custom_fields->>'customfield_10341' AS cf_10341,
  i.custom_fields->>'customfield_10318' AS cf_10318,
  i.custom_fields->>'customfield_10319' AS cf_10319,
  i.custom_fields->>'customfield_10320' AS cf_10320,
  i.custom_fields->>'customfield_10310' AS cf_10310,
  i.custom_fields->>'customfield_10345' AS cf_10345,
  i.custom_fields->>'customfield_10303' AS cf_10303,
  i.custom_fields->>'customfield_10302' AS cf_10302,
  i.custom_fields->>'customfield_10354' AS cf_10354,
  i.custom_fields->>'customfield_10365' AS cf_10365,
  i.custom_fields->>'customfield_10348' AS cf_10348,
  i.custom_fields->>'customfield_10351' AS cf_10351,
  i.custom_fields->>'customfield_10366' AS cf_10366,
  i.custom_fields->>'customfield_11167' AS cf_11167,
  i.custom_fields->>'customfield_10367' AS cf_10367,
  i.custom_fields->>'customfield_10368' AS cf_10368,
  i.custom_fields->>'customfield_10369' AS cf_10369,
  i.custom_fields->>'customfield_10333' AS cf_10333,
  i.custom_fields->>'customfield_10311' AS cf_10311,
  i.custom_fields->>'customfield_10353' AS cf_10353,
  i.custom_fields->>'customfield_10349' AS cf_10349,
  i.custom_fields->>'customfield_10304' AS cf_10304,
  i.custom_fields->>'customfield_10305' AS cf_10305,
  i.custom_fields->>'customfield_10308' AS cf_10308,
  i.custom_fields->>'customfield_10315' AS cf_10315,
  i.custom_fields->>'customfield_10314' AS cf_10314,
  i.custom_fields->>'customfield_11001' AS cf_11001,
  i.custom_fields->>'customfield_10107' AS cf_10107,
  i.custom_fields->>'customfield_10758' AS cf_10758,
  i.custom_fields->>'customfield_10757' AS cf_10757,
  i.custom_fields->>'customfield_10358' AS cf_10358,
  i.custom_fields->>'customfield_10357' AS cf_10357,
  i.custom_fields->>'customfield_10356' AS cf_10356,
  i.custom_fields->>'customfield_10618' AS cf_10618,
  i.custom_fields->>'customfield_10301' AS cf_10301,
  i.custom_fields->>'customfield_11897' AS cf_11897,
  i.custom_fields->>'customfield_11672' AS cf_11672,
  i.custom_fields->>'customfield_10326' AS cf_10326,
  i.custom_fields->>'customfield_10328' AS cf_10328,
  i.custom_fields->>'customfield_10327' AS cf_10327,
  i.custom_fields->>'customfield_14290' AS cf_14290,
  i.custom_fields->>'customfield_11896' AS cf_11896,
  i.custom_fields->>'customfield_10362' AS cf_10362,
  i.custom_fields->>'customfield_13268' AS cf_13268,
  i.custom_fields->>'customfield_11864' AS cf_11864,
  i.custom_fields->>'customfield_11334' AS cf_11334
FROM issues i;

COMMIT;
