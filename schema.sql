-- SkagitValleyJobs D1 Schema
-- Run: wrangler d1 execute skagitjobs --file=schema.sql

CREATE TABLE IF NOT EXISTS businesses (
  id                  INTEGER PRIMARY KEY AUTOINCREMENT,
  business_name       TEXT    NOT NULL,
  city                TEXT    NOT NULL,
  industry            TEXT    NOT NULL,
  homepage_url        TEXT    NOT NULL,
  careers_url         TEXT,
  last_hash           TEXT,
  last_crawled_at     INTEGER,
  crawl_failure_count INTEGER NOT NULL DEFAULT 0,
  is_active           INTEGER NOT NULL DEFAULT 1,
  created_at          INTEGER NOT NULL DEFAULT (unixepoch())
);

CREATE INDEX IF NOT EXISTS idx_businesses_active ON businesses(is_active);
CREATE INDEX IF NOT EXISTS idx_businesses_scout  ON businesses(is_active, careers_url);

CREATE TABLE IF NOT EXISTS job_postings (
  id              INTEGER PRIMARY KEY AUTOINCREMENT,
  business_id     INTEGER NOT NULL REFERENCES businesses(id),
  job_title       TEXT    NOT NULL,
  department      TEXT,
  salary_info     TEXT,
  application_url TEXT,
  first_seen_at   INTEGER NOT NULL DEFAULT (unixepoch()),
  last_seen_at    INTEGER NOT NULL DEFAULT (unixepoch()),
  is_active       INTEGER NOT NULL DEFAULT 1
);

CREATE INDEX IF NOT EXISTS idx_jobs_active   ON job_postings(is_active);
CREATE INDEX IF NOT EXISTS idx_jobs_business ON job_postings(business_id);
CREATE INDEX IF NOT EXISTS idx_jobs_seen     ON job_postings(last_seen_at);

CREATE TABLE IF NOT EXISTS crawl_log (
  id          INTEGER PRIMARY KEY AUTOINCREMENT,
  business_id INTEGER REFERENCES businesses(id),
  status      TEXT    NOT NULL, -- unchanged | changed | error | scout_ok | extract_ok
  message     TEXT,
  created_at  INTEGER NOT NULL DEFAULT (unixepoch())
);

CREATE INDEX IF NOT EXISTS idx_log_created ON crawl_log(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_log_status  ON crawl_log(status);
