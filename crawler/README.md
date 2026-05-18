# Postgres Jobs Crawler

Railway Postgres is the source of truth. Set `DATABASE_PUBLIC_URL` in `crawler/.env`; local SQLite is deprecated and should only be used as an old migration source.

## Setup

```powershell
cd crawler
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
python -m playwright install chromium
```

Put keys in `crawler/.env`:

```env
OPENAI_API_KEY=...
AI_DISCOVERY_MODEL=gpt-5.4-nano
AI_VERIFICATION_MODEL=gpt-5.4-nano
DATABASE_PUBLIC_URL=postgresql://...
```

## Seed Data

`seeds.json` is only an import file. After import, the crawler reads from Railway Postgres.

Preferred seed shape:

```json
[
  {
    "business_name": "Skagit Regional Health",
    "city": "Mount Vernon",
    "state": "WA",
    "location": "Mount Vernon, WA",
    "industry": "Healthcare",
    "homepage_url": "https://www.skagitregionalhealth.org",
    "jobs_url": "https://www.skagitregionalhealth.org/careers/career-opportunities",
    "source_type": "general_jobs"
  }
]
```

A plain string also works and is treated as both the company seed and known jobs URL:

```json
[
  "https://www.schooljobs.com/careers/skagitedu"
]
```

## Commands

Import seed records into Postgres:

```powershell
python job_crawler.py import-seeds --seeds seeds.json
```

Crawl companies due for refresh:

```powershell
python job_crawler.py crawl
```

Useful options:

```powershell
python job_crawler.py crawl --force --limit 5 --workers 2
python job_crawler.py crawl --recrawl-days 3
```

## One-time SQLite Migration

The old `jobs.sqlite` file can be copied into Railway:

```powershell
python migrate_sqlite_to_postgres.py --sqlite jobs.sqlite --replace
```

## Admin Dashboard

Run the local dashboard:

```powershell
python admin_app.py
```

Open `http://127.0.0.1:5050`.

The dashboard can:

- Add one company/job source URL.
- Paste-import a JSON object or list of companies.
- View current active jobs.
- Delete companies or individual jobs.
- Mark a job as sticky/featured.
- Mark a company as featured.

Batch import accepts either rich company objects or plain URL strings:

```json
[
  {
    "business_name": "Example Co",
    "homepage_url": "https://example.com",
    "jobs_url": "https://example.com/careers",
    "city": "Burlington",
    "state": "WA"
  },
  "https://another-example.com/jobs"
]
```

## Runtime Flow

1. `companies` stores business info, `homepage_url`, `jobs_url`, source type, status, and `last_checked_at` in Postgres.
2. `crawl` selects rows where `last_checked_at` is missing, errored, or older than `--recrawl-days`.
3. The crawler starts from `companies.jobs_url`.
4. Playwright renders the job source URL, follows pagination links, and visits detail pages for listings that have them.
5. AI validates/classifies the source and extracts jobs.
6. Jobs are deduped by `job_title + company`, prefiltered to Skagit County/city locations, and verified with `AI_VERIFICATION_MODEL`.
7. Only verified Skagit County jobs are upserted into `job_postings`.
8. `companies.last_checked_at` and status are updated.

No `state.json` is used for the normal workflow.
