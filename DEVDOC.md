# SkagitValleyJobs.com — Developer Doc

Automated local job board for Skagit County, WA. Fully serverless on Cloudflare.

## Stack

| Layer | Service |
|---|---|
| Compute | Cloudflare Workers (paid, $5/mo) |
| Database | Cloudflare D1 (SQLite) |
| Blob Storage | Cloudflare R2 |
| Queues | Cloudflare Queues (3 queues) |
| Browser Rendering | Cloudflare Browser Rendering API |
| AI Extraction | Google Gemini 1.5 Flash |
| Seed Scraping | Outscraper (Google Maps) |
| Frontend | Cloudflare Pages + HTMX |
| Scheduling | Cloudflare Cron Triggers |

---

## D1 Schema

```sql
-- businesses: seeded from Outscraper, one row per company
CREATE TABLE businesses (
  id                  INTEGER PRIMARY KEY AUTOINCREMENT,
  business_name       TEXT    NOT NULL,
  city                TEXT    NOT NULL,
  industry            TEXT    NOT NULL,
  homepage_url        TEXT    NOT NULL,
  careers_url         TEXT,                        -- NULL until scout runs
  last_hash           TEXT,                        -- SHA-256 of last crawled markdown
  last_crawled_at     INTEGER,                     -- unix epoch
  crawl_failure_count INTEGER NOT NULL DEFAULT 0,
  is_active           INTEGER NOT NULL DEFAULT 1,
  created_at          INTEGER NOT NULL DEFAULT (unixepoch())
);

CREATE INDEX idx_businesses_active ON businesses(is_active);
CREATE INDEX idx_businesses_scout  ON businesses(is_active, careers_url); -- scout queue filter

-- job_postings: extracted by Gemini, one row per open position
CREATE TABLE job_postings (
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

CREATE INDEX idx_jobs_active      ON job_postings(is_active);
CREATE INDEX idx_jobs_business    ON job_postings(business_id);
CREATE INDEX idx_jobs_seen        ON job_postings(last_seen_at);

-- crawl_log: every crawl attempt, powers the dashboard
CREATE TABLE crawl_log (
  id          INTEGER PRIMARY KEY AUTOINCREMENT,
  business_id INTEGER REFERENCES businesses(id),
  status      TEXT    NOT NULL,  -- 'unchanged' | 'changed' | 'error' | 'scout_ok' | 'extract_ok'
  message     TEXT,
  created_at  INTEGER NOT NULL DEFAULT (unixepoch())
);

CREATE INDEX idx_log_created ON crawl_log(created_at DESC);
CREATE INDEX idx_log_status  ON crawl_log(status);
```

---

## Queue Topology

```
Outscraper import
      │
      ▼
[scout-queue] ──► scout-consumer
                        │ writes careers_url to D1
                        ▼
              (no further action — daily cron picks it up)

Cron (3 AM daily)
      │ enqueues all active businesses
      ▼
[crawl-queue] ──► crawl-consumer
                        │ Browser Rendering → hash check
                        │ if changed: save .md to R2, enqueue business_id
                        ▼
               [extract-queue] ──► extract-consumer
                                         │ Gemini → upsert job_postings
                                         │ expire stale jobs
                                         ▼
                                       done
```

All three queues use `max_retries: 3`, `dead_letter_queue` optional but recommended.

---

## Workers

### 1. `import-worker`
**Trigger:** HTTP POST `/import` (called manually or via script after Outscraper run)  
**Input:** JSON array of Outscraper results  
**Logic:**
1. For each result, INSERT OR IGNORE into `businesses`
2. Enqueue new `business_id`s into `scout-queue`
3. Return count of imported / skipped

```json
// Outscraper result shape → normalized to:
{
  "business_name": "Janicki Industries",
  "city": "Sedro-Woolley",
  "industry": "Manufacturing",
  "homepage_url": "https://janicki.com"
}
```

Protect this endpoint with a `Bearer` token in `Authorization` header (Workers Secret: `IMPORT_SECRET`).

---

### 2. `scout-consumer`
**Trigger:** `scout-queue`  
**Input:** `{ business_id }`  
**Logic:**
1. Fetch `homepage_url` from D1
2. Call Browser Rendering API, request full HTML
3. Find anchor tags where text or href contains: `jobs|careers|hiring|employment|work-with-us` (case-insensitive)
4. If found: UPDATE `businesses SET careers_url = <link>` 
5. If not found: UPDATE `businesses SET careers_url = homepage_url`
6. INSERT into `crawl_log` (`status = 'scout_ok'`)
7. On failure: increment `crawl_failure_count`, log `status = 'error'`

---

### 3. `cron-worker`
**Trigger:** Cron `0 3 * * *` (3 AM daily)  
**Logic:**
1. SELECT all `WHERE is_active = 1 AND careers_url IS NOT NULL` from `businesses`
2. For each: enqueue `{ business_id, careers_url }` into `crawl-queue`
3. Log total enqueued count

Keep this worker lightweight — just a fan-out. No crawling here.

---

### 4. `crawl-consumer`
**Trigger:** `crawl-queue`  
**Input:** `{ business_id, careers_url }`  
**Logic:**
1. Call Cloudflare Browser Rendering `/markdown` endpoint with `careers_url`
2. SHA-256 hash the returned markdown
3. Fetch `last_hash` from D1 for this business
4. **If hashes match:** log `unchanged`, done
5. **If different:**
   - Save markdown to R2: `careers/{business_id}/{YYYY-MM-DD}.md`
   - UPDATE `businesses SET last_hash = <new_hash>, last_crawled_at = unixepoch()`
   - Enqueue `{ business_id, r2_key }` into `extract-queue`
   - Log `changed`
6. **On fetch error:**
   - Increment `crawl_failure_count`
   - If `crawl_failure_count >= 5`: SET `is_active = 0`
   - Log `error` with message

---

### 5. `extract-consumer`
**Trigger:** `extract-queue`  
**Input:** `{ business_id, r2_key }`  
**Logic:**
1. Read markdown from R2 using `r2_key`
2. POST to Gemini 1.5 Flash with this prompt:

```
Analyze this markdown from a company careers page.
Extract all active job postings.
Return ONLY a JSON array. Each object must have:
  - job_title (string)
  - department (string or null)
  - salary_info (string or null)
  - application_url (string or null)
If no jobs are found, return [].
Do not include any explanation or markdown formatting.
```

3. Parse the JSON response
4. **Upsert logic:**
   - For each job in response: INSERT or UPDATE `job_postings` where `business_id` + `job_title` match
     - On match: UPDATE `last_seen_at = unixepoch(), is_active = 1`
     - On new: INSERT with defaults
5. **Expire stale jobs:**
   - UPDATE `job_postings SET is_active = 0 WHERE business_id = ? AND last_seen_at < unixepoch() - 86400`
   - (Any job not seen in this crawl cycle is expired)
6. Log `extract_ok` with job count

---

### 6. `api-worker`
**Trigger:** HTTP (bound to Cloudflare Pages via Functions or standalone)  
**Endpoints:**

```
GET /api/jobs
  ?city=Burlington
  ?industry=Manufacturing
  ?q=forklift               ← keyword search on job_title
  ?page=1                   ← default 1, 20 per page

GET /api/jobs/:id           ← single job detail

GET /api/stats              ← dashboard summary data

GET /api/dashboard          ← error log + crawl stats (protected by ADMIN_SECRET)
```

All responses are JSON. CORS open for Pages domain.

---

## Frontend (Cloudflare Pages)

Single HTML file with HTMX. No build step.

**`/index.html`** — job search page
- Filter bar: City dropdown, Industry dropdown, keyword text input
- Each filter triggers `hx-get="/api/jobs?..."` and swaps `#results`
- Job card shows: title, company, city, industry, salary (if any), "Apply" button → `application_url`
- "New" badge if `first_seen_at > unixepoch() - 86400`
- Pagination via HTMX infinite scroll or "Load More"

**`/dashboard/index.html`** — protected admin view
- Loads on page open via `hx-get="/api/dashboard"` with admin token in header
- Summary cards: Active Businesses, Jobs Found Today, Changed Pages Today, Errors Today
- Table: last 50 `crawl_log` entries, color-coded by status
- Table: businesses with `crawl_failure_count > 0`, with failure counts

Cities dropdown values: `Mount Vernon, Burlington, Sedro-Woolley, Anacortes, Burlington, Concrete, La Conner, Bow`  
Industry dropdown values: `Manufacturing, Agriculture, Healthcare, Hospitality, Retail, Construction, Technology, Other`

---

## wrangler.toml Structure

```toml
name = "skagitvalleyjobs"
compatibility_date = "2024-01-01"

[[d1_databases]]
binding = "DB"
database_name = "skagitjobs"
database_id = "<your-d1-id>"

[[r2_buckets]]
binding = "R2"
bucket_name = "skagitjobs-careers"

[[queues.producers]]
binding = "SCOUT_QUEUE"
queue = "scout-queue"

[[queues.producers]]
binding = "CRAWL_QUEUE"
queue = "crawl-queue"

[[queues.producers]]
binding = "EXTRACT_QUEUE"
queue = "extract-queue"

[[queues.consumers]]
queue = "scout-queue"
max_batch_size = 5
max_retries = 3

[[queues.consumers]]
queue = "crawl-queue"
max_batch_size = 3       # Browser Rendering is slow — keep concurrency low
max_retries = 3

[[queues.consumers]]
queue = "extract-queue"
max_batch_size = 5
max_retries = 3

[triggers]
crons = ["0 3 * * *"]
```

---

## Secrets

Set via `wrangler secret put <NAME>`:

| Secret | Used By | Notes |
|---|---|---|
| `GEMINI_API_KEY` | extract-consumer | Gemini 1.5 Flash |
| `IMPORT_SECRET` | import-worker | Bearer token for POST /import |
| `ADMIN_SECRET` | api-worker | Dashboard auth header |

---

## Deploy Checklist

```
1. [ ] Cloudflare account → upgrade to Workers Paid ($5/mo)
2. [ ] wrangler login
3. [ ] wrangler d1 create skagitjobs
       → copy database_id into wrangler.toml
4. [ ] wrangler d1 execute skagitjobs --file=schema.sql
5. [ ] wrangler r2 bucket create skagitjobs-careers
6. [ ] wrangler queues create scout-queue
       wrangler queues create crawl-queue
       wrangler queues create extract-queue
7. [ ] wrangler secret put GEMINI_API_KEY
       wrangler secret put IMPORT_SECRET
       wrangler secret put ADMIN_SECRET
8. [ ] wrangler deploy
9. [ ] Connect Pages project to /frontend folder
10.[ ] Run Outscraper seed queries (see below)
11.[ ] POST seed data to /import endpoint
12.[ ] Verify scout-queue processes → careers_url populated in D1
13.[ ] Manually trigger cron or wait for 3 AM
```

---

## Outscraper Seed Queries

Run these in Outscraper → Google Maps Scraper. Export as JSON.

```
"Manufacturing companies in Skagit County WA"
"Agriculture farms in Skagit County WA"
"Healthcare clinics hospitals in Skagit County WA"
"Restaurants breweries in Mount Vernon WA"
"Restaurants breweries in Sedro-Woolley WA"
"Construction companies in Burlington WA"
"Technology companies in Skagit County WA"
"Retail stores in Anacortes WA"
```

~300-400 credits total. Normalize `city` and `industry` fields during import.

---

## R2 Key Convention

```
careers/{business_id}/{YYYY-MM-DD}.md
```

Old daily snapshots are kept automatically. No TTL needed unless storage cost becomes a concern (it won't at this scale).

---

## Cost Estimate (Steady State, ~200 businesses)

| Service | Monthly Cost |
|---|---|
| Workers Paid Plan | $5.00 |
| D1 | Free (well within limits) |
| R2 | Free (< 10 GB) |
| Queues | Free (< 1M ops) |
| Browser Rendering | ~$1-3 (6,000 req/mo) |
| Gemini 1.5 Flash | ~$0.50 (only on changed pages) |
| **Total** | **~$7-9/mo** |
