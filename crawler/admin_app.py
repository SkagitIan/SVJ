from __future__ import annotations

import argparse
import asyncio
import os
import json
import tempfile
import threading
import traceback
from pathlib import Path
from typing import Any
from datetime import datetime
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode, urlparse
from urllib.request import Request, urlopen

from flask import Flask, flash, redirect, render_template, request, url_for
from openai import OpenAI
from dotenv import load_dotenv

import db

from job_crawler import (
    build_ai_client_and_model,
    company_row_to_seed,
    import_seed_items_to_db,
    init_db,
    normalize_url,
    now_iso,
    process_seed,
    upsert_seed_to_db,
)


DEFAULT_DB = Path("postgres")
TASK_THREADS: dict[str, list[threading.Thread]] = {}
TASK_LOCK = threading.Lock()
DB_INIT_LOCK = threading.Lock()
DB_INITIALIZED: set[str] = set()
ADMIN_TASK_WORKERS = max(1, min(8, int(os.environ.get("ADMIN_TASK_WORKERS", "4"))))
TERMINAL_BATCH_STATUSES = {"completed", "failed", "expired", "cancelled", "canceled"}
FAILED_BATCH_STATUSES = {"failed", "expired", "cancelled", "canceled"}
SKAGIT_DISCOVERY_CITIES = [
    "Mount Vernon",
    "Burlington",
    "Sedro-Woolley",
    "Anacortes",
    "Concrete",
    "La Conner",
    "Hamilton",
    "Lyman",
    "Bow",
    "Edison",
    "Bay View",
    "Clear Lake",
    "Conway",
    "Rockport",
    "Marblemount",
    "Alger",
]
DISCOVERY_INDUSTRIES = [
    "Manufacturing",
    "Healthcare",
    "Education",
    "Government",
    "Construction",
    "Agriculture",
    "Food Processing",
    "Aerospace",
    "Marine/Shipbuilding",
    "Energy/Refinery",
    "Logistics/Warehousing",
    "Tribal Enterprises",
    "Hospitality/Lodging",
    "Utilities",
    "Financial/Insurance Services",
]
OUTSCRAPER_SEARCH_URL = "https://api.outscraper.cloud/google-maps-search"
ENRICHMENT_PROMPT = """You are enriching company records for a local job board called Skagit Valley Jobs.

Your job is to research the company from the provided website content and return clean structured data.

Company:
- Name: {company_name}
- Website: {company_url}
- Career page URL: {job_source_url}

Website content:
{scraped_text}

Return ONLY valid JSON using this schema, add the missing fields from the existing database like address, state, update company name to marketing name, whatever is public and used commonly.:

{{
  "company_summary": "",
  "hiring_summary": "",
  "industry": "",
  "job_categories": [],
  "common_job_titles": [],
  "city": "",
  "state": "",
  "company_size_guess": "small | medium | large | unknown",
  "local_relevance": "",
  "career_page_quality": "good | okay | poor | broken | unknown",
  "career_page_notes": "",
  "search_keywords": [],
  "confidence_score": 0,
  "needs_manual_review": true
}}

Rules:
- Do not invent facts.
- If the website does not clearly say something, use "unknown".
- Keep company_summary under 80 words.
- Keep hiring_summary under 60 words.
- Use simple job board language, not corporate marketing language.
- job_categories must be broad categories useful for filtering.
- common_job_titles should be likely or observed job titles only.
- city and state must come from the website or career page, not guessing.
- confidence_score should reflect how much useful evidence was found.
"""
JOB_ENRICHMENT_PROMPT = """You are enhancing job listings for a local job board called Skagit Valley Jobs.

Your job is to convert a raw job posting into clear structured data for job seekers.

Company:
- Name: {company_name}
- Industry: {company_industry}
- City: {company_city}
- State: {company_state}

Job:
- Title: {job_title}
- Location: {job_location}
- URL: {job_url}

Raw job posting:
{job_description}

Return ONLY valid JSON using this schema:

{{
  "plain_english_summary": "",
  "best_for": "",
  "job_category": "",
  "experience_level": "entry-level | some-experience | experienced | management | unknown",
  "worker_tags": [],
  "physical_demands": [],
  "estimated_pay_range": "",
  "pay_range_type": "posted | estimated | unknown",
  "confidence_score": 0,
  "needs_manual_review": true
}}

Rules:
- Do not invent exact facts.
- If pay is listed in the posting, copy it exactly into estimated_pay_range and set pay_range_type = "posted".
- If pay is not listed, you may estimate a realistic local range based on title, duties, experience level, industry, and location.
- Estimated pay must be clearly marked with pay_range_type = "estimated".
- If there is not enough evidence to estimate pay, use pay_range_type = "unknown".
- Do not claim remote work, benefits, flexibility, union status, or advancement unless clearly stated.
- Use "unknown" when the posting does not provide enough evidence.
- Keep plain_english_summary under 70 words.
- Keep best_for under 50 words.
- Use simple job seeker language.
- worker_tags should be short filter tags.
- physical_demands must be factual and neutral.
- confidence_score should reflect how much useful evidence was found.
"""


def initialize_db_once(db_path: Path) -> None:
    key = str(db_path.resolve())
    if key in DB_INITIALIZED:
        return
    with DB_INIT_LOCK:
        if key in DB_INITIALIZED:
            return
        init_db(db_path)
        with db.connect(db_path, timeout=30) as conn:
            conn.execute("PRAGMA busy_timeout=30000")
            ensure_admin_tables(conn)
            recover_orphaned_tasks(conn)
        DB_INITIALIZED.add(key)


def connect(db_path: Path) -> db.connection:
    initialize_db_once(db_path)
    conn = db.connect(db_path, timeout=30)
    conn.execute("PRAGMA busy_timeout=30000")
    conn.row_factory = db.Row
    return conn


def connect_with_schema(db_path: Path) -> db.connection:
    init_db(db_path)
    conn = db.connect(db_path, timeout=30)
    conn.execute("PRAGMA busy_timeout=30000")
    conn.row_factory = db.Row
    ensure_admin_tables(conn)
    return conn


def ensure_admin_tables(conn: db.connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS admin_tasks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            task_type TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'pending',
            payload_json TEXT,
            message TEXT,
            total_count INTEGER NOT NULL DEFAULT 0,
            completed_count INTEGER NOT NULL DEFAULT 0,
            cancel_requested INTEGER NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL,
            started_at TEXT,
            finished_at TEXT
        )
        """
    )
    columns = {row[1] for row in conn.execute("PRAGMA table_info(admin_tasks)")}
    if "cancel_requested" not in columns:
        conn.execute("ALTER TABLE admin_tasks ADD COLUMN cancel_requested INTEGER NOT NULL DEFAULT 0")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_admin_tasks_status ON admin_tasks(status, created_at)")
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS app_settings (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
        """
    )
    timestamp = now_iso()
    conn.execute(
        "INSERT OR IGNORE INTO app_settings (key, value, updated_at) VALUES ('job_refresh_days', '7', ?)",
        (timestamp,),
    )
    conn.execute(
        "INSERT OR IGNORE INTO app_settings (key, value, updated_at) VALUES ('job_refresh_day', 'sunday', ?)",
        (timestamp,),
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS discovery_industries (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL UNIQUE,
            sort_order INTEGER NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
        """
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_discovery_industries_order ON discovery_industries(sort_order, name)")
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS job_alerts (
            id INTEGER GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
            email TEXT NOT NULL UNIQUE,
            cities_json TEXT NOT NULL DEFAULT '[]',
            categories_json TEXT NOT NULL DEFAULT '[]',
            is_active INTEGER NOT NULL DEFAULT 1,
            created_at TEXT NOT NULL
        )
        """
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_job_alerts_email ON job_alerts(email)")
    for index, industry in enumerate(DISCOVERY_INDUSTRIES):
        conn.execute(
            """
            INSERT OR IGNORE INTO discovery_industries (name, sort_order, created_at, updated_at)
            VALUES (?, ?, ?, ?)
            """,
            (industry, index, timestamp, timestamp),
        )


def recover_orphaned_tasks(conn: db.connection) -> None:
    conn.execute(
        """
        UPDATE admin_tasks
        SET status = 'failed',
            message = COALESCE(message || ' | ', '') || 'Stopped when the admin server restarted.',
            finished_at = COALESCE(finished_at, ?)
        WHERE status = 'running'
          AND finished_at IS NULL
        """,
        (now_iso(),),
    )


def clean_form_value(name: str) -> str | None:
    value = request.form.get(name, "").strip()
    return value or None


def load_settings(conn: db.connection) -> dict[str, str]:
    rows = conn.execute("SELECT key, value FROM app_settings").fetchall()
    settings = {row["key"]: row["value"] for row in rows}
    settings.setdefault("job_refresh_days", "7")
    settings.setdefault("job_refresh_day", "sunday")
    return settings


def display_time(value: str | None) -> str:
    if not value:
        return ""
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return value
    if parsed.tzinfo is not None:
        parsed = parsed.astimezone()
    return parsed.strftime("%b %#d, %Y %#I:%M %p")


def clean_ai_value(value: Any, max_length: int | None = None) -> str | None:
    text = " ".join(str(value or "").split())
    if not text or text.lower() in {"unknown", "null", "none"}:
        return None
    return text[:max_length] if max_length else text


def json_list_text(value: Any, max_items: int = 12, max_item_length: int = 80) -> str:
    if not isinstance(value, list):
        return "[]"
    items: list[str] = []
    seen: set[str] = set()
    for item in value:
        text = clean_ai_value(item, max_item_length)
        if not text:
            continue
        key = text.lower()
        if key in seen:
            continue
        seen.add(key)
        items.append(text)
        if len(items) >= max_items:
            break
    return json.dumps(items, ensure_ascii=False)


def int_between(value: Any, minimum: int = 0, maximum: int = 100) -> int:
    try:
        return max(minimum, min(maximum, int(value)))
    except (TypeError, ValueError):
        return minimum


def attr_value(obj: Any, name: str, default: Any = None) -> Any:
    if isinstance(obj, dict):
        return obj.get(name, default)
    return getattr(obj, name, default)


def batch_request_counts(batch: Any) -> tuple[int, int, int]:
    counts = attr_value(batch, "request_counts")
    return (
        int_between(attr_value(counts, "total"), 0, 1_000_000),
        int_between(attr_value(counts, "completed"), 0, 1_000_000),
        int_between(attr_value(counts, "failed"), 0, 1_000_000),
    )


def create_task_history(
    conn: db.connection,
    task_type: str,
    status: str,
    message: str,
    total: int = 0,
    completed: int = 0,
    payload: dict[str, Any] | None = None,
) -> int:
    timestamp = now_iso()
    cursor = conn.execute(
        """
            INSERT INTO admin_tasks (
                task_type, status, payload_json, message, total_count,
                completed_count, cancel_requested, created_at, started_at, finished_at
            )
            VALUES (?, ?, ?, ?, ?, ?, 0, ?, ?, ?)
            RETURNING id
            """,
        (
            task_type,
            status,
            json.dumps(payload or {}, sort_keys=True),
            message,
            total,
            completed,
            timestamp,
            timestamp,
            timestamp,
        ),
    )
    return int(cursor.fetchone()[0])


def task_label(task_type: str) -> str:
    return {
        "fetch_company": "Fetch company jobs",
        "fetch_all": "Fetch all jobs",
        "delete_all_jobs": "Delete all jobs",
        "discover_businesses": "Discover businesses",
        "continue_discovery": "Continue discovery",
        "enhance_companies": "Enhance company data",
        "enhance_jobs": "Enhance job data",
    }.get(task_type, task_type.replace("_", " ").title())


def discovery_task_subject(conn: db.connection, payload_json: str | None) -> str:
    try:
        payload = json.loads(payload_json or "{}")
        discovery_id = int(payload.get("discovery_id") or 0)
    except (TypeError, ValueError, json.JSONDecodeError):
        return ""
    if discovery_id <= 0:
        return ""
    row = conn.execute(
        "SELECT business_name, city, industry FROM discovered_businesses WHERE id = ?",
        (discovery_id,),
    ).fetchone()
    if not row:
        return f"discovery #{discovery_id}"
    details = ", ".join(part for part in [row["city"], row["industry"]] if part)
    return f"{row['business_name']} ({details})" if details else str(row["business_name"])


def task_subject(conn: db.connection, row: db.Row) -> str:
    task_type = row["task_type"]
    if task_type == "continue_discovery":
        return discovery_task_subject(conn, row["payload_json"])
    if task_type == "discover_businesses":
        try:
            payload = json.loads(row["payload_json"] or "{}")
        except json.JSONDecodeError:
            return ""
        return clean_ai_value(payload.get("industry"), 120) or ""
    if task_type == "fetch_company":
        try:
            payload = json.loads(row["payload_json"] or "{}")
            company_id = int(payload.get("company_id") or 0)
        except (TypeError, ValueError, json.JSONDecodeError):
            return ""
        company = conn.execute("SELECT business_name FROM companies WHERE id = ?", (company_id,)).fetchone()
        return company["business_name"] if company else f"company #{company_id}"
    return ""


def enqueue_task(db_path: Path, task_type: str, payload: dict[str, Any] | None = None) -> int:
    timestamp = now_iso()
    with connect(db_path) as conn:
        cursor = conn.execute(
            """
            INSERT INTO admin_tasks (task_type, status, payload_json, message, created_at)
            VALUES (?, 'pending', ?, ?, ?)
            RETURNING id
            """,
            (task_type, json.dumps(payload or {}, sort_keys=True), "Queued", timestamp),
        )
        task_id = int(cursor.fetchone()[0])
    start_task_worker(db_path)
    return task_id


def start_task_worker(db_path: Path) -> None:
    key = str(db_path.resolve())
    with TASK_LOCK:
        current = [thread for thread in TASK_THREADS.get(key, []) if thread.is_alive()]
        needed = ADMIN_TASK_WORKERS - len(current)
        for _ in range(max(0, needed)):
            thread = threading.Thread(target=task_worker, args=(db_path,), daemon=True)
            current.append(thread)
            thread.start()
        TASK_THREADS[key] = current


def task_worker(db_path: Path) -> None:
    while True:
        task = claim_next_task(db_path)
        if not task:
            return
        try:
            run_task(db_path, task)
        except Exception as exc:
            with connect(db_path) as conn:
                conn.execute(
                    """
                    UPDATE admin_tasks
                    SET status = 'failed', message = ?, finished_at = ?
                    WHERE id = ?
                    """,
                    (str(exc), now_iso(), task["id"]),
                )


def claim_next_task(db_path: Path) -> dict[str, Any] | None:
    with connect(db_path) as conn:
        conn.execute("BEGIN IMMEDIATE")
        row = conn.execute(
            """
            SELECT *
            FROM admin_tasks
            WHERE status = 'pending'
            ORDER BY id ASC
            LIMIT 1
            """
        ).fetchone()
        if not row:
            conn.execute("COMMIT")
            return None
        timestamp = now_iso()
        cursor = conn.execute(
            """
            UPDATE admin_tasks
            SET status = 'running', started_at = ?, message = 'Running'
            WHERE id = ? AND status = 'pending'
            """,
            (timestamp, row["id"]),
        )
        if cursor.rowcount != 1:
            conn.execute("COMMIT")
            return None
        conn.execute("COMMIT")
        claimed = dict(row)
        claimed["status"] = "running"
        claimed["started_at"] = timestamp
        return claimed


def update_task(
    db_path: Path,
    task_id: int,
    message: str,
    completed: int | None = None,
    total: int | None = None,
) -> None:
    fields = ["message = ?"]
    params: list[Any] = [message]
    if completed is not None:
        fields.append("completed_count = ?")
        params.append(completed)
    if total is not None:
        fields.append("total_count = ?")
        params.append(total)
    params.append(task_id)
    with connect(db_path) as conn:
        conn.execute(f"UPDATE admin_tasks SET {', '.join(fields)} WHERE id = ?", params)


def task_cancel_requested(db_path: Path, task_id: int) -> bool:
    with connect(db_path) as conn:
        row = conn.execute("SELECT cancel_requested, status FROM admin_tasks WHERE id = ?", (task_id,)).fetchone()
    return bool(row and (row["cancel_requested"] or row["status"] == "canceled"))


def format_exception_context(exc: BaseException, **context: Any) -> str:
    lines = [str(exc) or exc.__class__.__name__, "", "Diagnostics:"]
    lines.append(f"- exception_type: {exc.__class__.__module__}.{exc.__class__.__name__}")
    for key, value in context.items():
        if value is not None:
            lines.append(f"- {key}: {value}")
    cause = getattr(exc, "__cause__", None)
    if cause:
        lines.append(f"- cause: {cause.__class__.__module__}.{cause.__class__.__name__}: {cause}")
    context_exc = getattr(exc, "__context__", None)
    if context_exc and context_exc is not cause:
        lines.append(f"- context: {context_exc.__class__.__module__}.{context_exc.__class__.__name__}: {context_exc}")
    lines.extend(["", "Traceback:", "".join(traceback.format_exception(type(exc), exc, exc.__traceback__)).strip()])
    return "\n".join(lines)


def finish_task(db_path: Path, task_id: int, status: str, message: str) -> None:
    with connect(db_path) as conn:
        conn.execute(
            """
            UPDATE admin_tasks
            SET status = ?, message = ?, finished_at = ?
            WHERE id = ?
            """,
            (status, message, now_iso(), task_id),
        )


def openai_client_and_enrichment_model() -> tuple[OpenAI, str]:
    load_dotenv(Path(__file__).with_name(".env"))
    api_key = os.environ.get("OPENAI_API_KEY")
    if not api_key:
        raise RuntimeError("OPENAI_API_KEY is required to enhance company data.")
    model = (
        os.environ.get("AI_COMPANY_ENRICHMENT_MODEL")
        or os.environ.get("AI_DISCOVERY_MODEL")
        or os.environ.get("AI_VERIFICATION_MODEL")
        or "gpt-5.4-mini"
    )
    return OpenAI(api_key=api_key), model


def openai_client_and_job_enrichment_model() -> tuple[OpenAI, str]:
    load_dotenv(Path(__file__).with_name(".env"))
    api_key = os.environ.get("OPENAI_API_KEY")
    if not api_key:
        raise RuntimeError("OPENAI_API_KEY is required to enhance job data.")
    model = (
        os.environ.get("AI_JOB_ENRICHMENT_MODEL")
        or os.environ.get("AI_VERIFICATION_MODEL")
        or os.environ.get("AI_DISCOVERY_MODEL")
        or "gpt-5.4-mini"
    )
    return OpenAI(api_key=api_key), model


def collect_company_context(conn: db.connection, company_id: int) -> str:
    source_rows = conn.execute(
        """
        SELECT source_url, source_type, confidence, evidence_json
        FROM job_sources
        WHERE company_id = ?
        ORDER BY confidence DESC, updated_at DESC
        LIMIT 8
        """,
        (company_id,),
    ).fetchall()
    job_rows = conn.execute(
        """
        SELECT job_title, department, location, description
        FROM job_postings
        WHERE company_id = ? AND is_active = 1
        ORDER BY last_seen_at DESC
        LIMIT 12
        """,
        (company_id,),
    ).fetchall()
    parts: list[str] = []
    if source_rows:
        parts.append("Known job/career sources:")
        for row in source_rows:
            evidence: list[str] = []
            try:
                parsed = json.loads(row["evidence_json"] or "[]")
                if isinstance(parsed, list):
                    evidence = [str(item) for item in parsed[:3]]
            except json.JSONDecodeError:
                evidence = []
            parts.append(
                f"- {row['source_url']} ({row['source_type']}, confidence {row['confidence']}): "
                + "; ".join(evidence)
            )
    if job_rows:
        parts.append("Current active job samples:")
        for row in job_rows:
            bits = [
                clean_ai_value(row["job_title"], 120),
                clean_ai_value(row["department"], 80),
                clean_ai_value(row["location"], 120),
                clean_ai_value(row["description"], 260),
            ]
            parts.append("- " + " | ".join(bit for bit in bits if bit))
    return "\n".join(parts)[:12000] or "No local crawl text is available. Use web search and the provided URLs."


def build_enrichment_request(row: db.Row, scraped_text: str, model: str) -> dict[str, Any]:
    prompt = ENRICHMENT_PROMPT.format(
        company_name=row["business_name"],
        company_url=row["homepage_url"] or row["seed_url"],
        job_source_url=row["jobs_url"] or "",
        scraped_text="\n".join(
            [
                f"Existing city: {row['city'] or 'unknown'}",
                f"Existing state: {row['state'] or 'unknown'}",
                f"Existing industry: {row['industry'] or 'unknown'}",
                f"Existing location: {row['location'] or 'unknown'}",
                scraped_text,
            ]
        ),
    )
    return {
        "custom_id": f"company-{row['id']}",
        "method": "POST",
        "url": "/v1/responses",
        "body": {
            "model": model,
            "tools": [{"type": "web_search"}],
            "tool_choice": "auto",
            "input": prompt,
        },
    }


def ensure_web_search_batch_compatible(request_obj: dict[str, Any]) -> dict[str, Any]:
    body = request_obj.get("body")
    if not isinstance(body, dict):
        raise ValueError("Batch request is missing a body.")
    tools = body.get("tools") if isinstance(body.get("tools"), list) else []
    uses_web_search = any(isinstance(tool, dict) and tool.get("type") == "web_search" for tool in tools)
    if not uses_web_search:
        return request_obj

    # OpenAI rejects web_search with JSON mode. Keep this centralized so future
    # edits cannot accidentally upload incompatible batch rows.
    body.pop("response_format", None)
    text_options = body.get("text")
    if isinstance(text_options, dict):
        text_format = text_options.get("format")
        if isinstance(text_format, dict) and text_format.get("type") in {"json_object", "json_schema"}:
            body.pop("text", None)
    text_options = body.get("text")
    if isinstance(text_options, dict):
        text_format = text_options.get("format")
        if isinstance(text_format, dict) and text_format.get("type") in {"json_object", "json_schema"}:
            raise ValueError("web_search batch requests cannot use JSON mode or structured output.")
    if "response_format" in body:
        raise ValueError("web_search batch requests cannot use response_format.")
    return request_obj


def latest_open_enrichment_batch(conn: db.connection) -> db.Row | None:
    return conn.execute(
        """
        SELECT *
        FROM ai_company_enrichment_batches
        WHERE imported = 0
          AND status NOT IN ('failed', 'expired', 'cancelled', 'canceled')
        ORDER BY id DESC
        LIMIT 1
        """
    ).fetchone()


def collect_company_enrichment_status(conn: db.connection) -> dict[str, Any]:
    pending_count = conn.execute(
        "SELECT COUNT(*) FROM companies WHERE ai_enriched_at IS NULL"
    ).fetchone()[0]
    open_batch = latest_open_enrichment_batch(conn)
    latest_batch = conn.execute(
        """
        SELECT *
        FROM ai_company_enrichment_batches
        ORDER BY id DESC
        LIMIT 1
        """
    ).fetchone()
    batch = open_batch or latest_batch
    action_label = "Check Status" if open_batch else "Create Batch"
    return {
        "pending_count": pending_count,
        "batch": dict(batch) if batch else None,
        "has_open_batch": open_batch is not None,
        "action_label": action_label,
    }


def update_local_batch_from_openai(conn: db.connection, local_id: int, batch: Any, message: str | None = None) -> dict[str, Any]:
    total, completed, failed = batch_request_counts(batch)
    status = str(attr_value(batch, "status") or "unknown")
    output_file_id = attr_value(batch, "output_file_id")
    error_file_id = attr_value(batch, "error_file_id")
    timestamp = now_iso()
    conn.execute(
        """
        UPDATE ai_company_enrichment_batches
        SET status = ?,
            output_file_id = ?,
            error_file_id = ?,
            total_count = ?,
            completed_count = ?,
            failed_count = ?,
            checked_at = ?,
            completed_at = CASE WHEN ? = 'completed' THEN COALESCE(completed_at, ?) ELSE completed_at END,
            message = COALESCE(?, message)
        WHERE id = ?
        """,
        (
            status,
            output_file_id,
            error_file_id,
            total,
            completed,
            failed,
            timestamp,
            status,
            timestamp,
            message,
            local_id,
        ),
    )
    return {
        "status": status,
        "output_file_id": output_file_id,
        "error_file_id": error_file_id,
        "total": total,
        "completed": completed,
        "failed": failed,
    }


def create_company_enrichment_batch(db_path: Path, limit: int | None = None) -> str:
    client, model = openai_client_and_enrichment_model()
    timestamp = now_iso()
    temp_path: Path | None = None
    with connect(db_path) as conn:
        existing = latest_open_enrichment_batch(conn)
        if existing:
            return f"Batch {existing['openai_batch_id'] or existing['id']} is already {existing['status']}. Check status instead."
        query = """
            SELECT *
            FROM companies
            WHERE ai_enriched_at IS NULL
            ORDER BY business_name COLLATE NOCASE ASC
        """
        params: list[Any] = []
        if limit is not None:
            query += " LIMIT ?"
            params.append(max(1, int(limit)))
        rows = conn.execute(query, params).fetchall()
        if not rows:
            create_task_history(conn, "enhance_companies", "completed", "No companies need enrichment.")
            return "No companies need enrichment."
        requests = [
            ensure_web_search_batch_compatible(
                build_enrichment_request(row, collect_company_context(conn, int(row["id"])), model)
            )
            for row in rows
        ]
        request_rows = [(int(row["id"]), requests[index]["custom_id"]) for index, row in enumerate(rows)]

    with connect(db_path) as conn:
        cursor = conn.execute(
            """
            INSERT INTO ai_company_enrichment_batches (status, model, total_count, message, created_at)
            VALUES ('local_created', ?, ?, 'Preparing OpenAI batch', ?)
            RETURNING id
            """,
            (model, len(rows), timestamp),
        )
        local_batch_id = int(cursor.fetchone()[0])
        for company_id, custom_id in request_rows:
            conn.execute(
                """
                INSERT INTO ai_company_enrichment_requests (batch_id, company_id, custom_id, status, created_at, updated_at)
                VALUES (?, ?, ?, 'pending', ?, ?)
                """,
                (local_batch_id, company_id, custom_id, timestamp, timestamp),
            )
        payload = "\n".join(json.dumps(item, ensure_ascii=False) for item in requests) + "\n"

    try:
        with tempfile.NamedTemporaryFile("w", encoding="utf-8", suffix=".jsonl", delete=False) as handle:
            temp_path = Path(handle.name)
            handle.write(payload)
        with temp_path.open("rb") as handle:
            uploaded = client.files.create(file=handle, purpose="batch")
        batch = client.batches.create(
            input_file_id=uploaded.id,
            endpoint="/v1/responses",
            completion_window="24h",
        )
        with connect(db_path) as conn:
            total, completed, failed = batch_request_counts(batch)
            status = str(attr_value(batch, "status") or "submitted")
            conn.execute(
                """
                UPDATE ai_company_enrichment_batches
                SET openai_batch_id = ?,
                    input_file_id = ?,
                    status = ?,
                    total_count = ?,
                    completed_count = ?,
                    failed_count = ?,
                    message = ?,
                    submitted_at = ?,
                    checked_at = ?
                WHERE id = ?
                """,
                (
                    batch.id,
                    uploaded.id,
                    status,
                    total or len(requests),
                    completed,
                    failed,
                    f"OpenAI batch {batch.id} created.",
                    now_iso(),
                    now_iso(),
                    local_batch_id,
                ),
            )
            batch_kind = "test " if limit == 1 else ""
            create_task_history(
                conn,
                "enhance_companies",
                "completed",
                f"Created OpenAI {batch_kind}batch {batch.id} for {len(requests)} companies.",
                total=len(requests),
                payload={"batch_id": batch.id, "local_batch_id": local_batch_id, "limit": limit},
            )
        return f"Created OpenAI {'test ' if limit == 1 else ''}batch {batch.id} for {len(requests)} companies."
    except Exception as exc:
        with connect(db_path) as conn:
            conn.execute(
                """
                UPDATE ai_company_enrichment_batches
                SET status = 'failed', message = ?, checked_at = ?
                WHERE id = ?
                """,
                (str(exc), now_iso(), local_batch_id),
            )
            create_task_history(conn, "enhance_companies", "failed", str(exc), total=len(requests))
        raise
    finally:
        if temp_path and temp_path.exists():
            temp_path.unlink(missing_ok=True)


def extract_response_text(body: dict[str, Any]) -> str:
    output_text = body.get("output_text")
    if isinstance(output_text, str) and output_text.strip():
        return output_text
    parts: list[str] = []
    for item in body.get("output") or []:
        if not isinstance(item, dict):
            continue
        for content in item.get("content") or []:
            if isinstance(content, dict) and content.get("type") in {"output_text", "text"}:
                text = content.get("text")
                if isinstance(text, str):
                    parts.append(text)
    return "\n".join(parts)


def parse_ai_json_text(text: str) -> dict[str, Any]:
    cleaned = text.strip()
    if cleaned.startswith("```"):
        cleaned = cleaned.removeprefix("```json").removeprefix("```").strip()
        if cleaned.endswith("```"):
            cleaned = cleaned[:-3].strip()
    data = json.loads(cleaned)
    if not isinstance(data, dict):
        raise ValueError("AI output was not a JSON object.")
    return data


def parse_batch_output_lines(content: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for line in content.splitlines():
        if not line.strip():
            continue
        rows.append(json.loads(line))
    return rows


def apply_company_enrichment(conn: db.connection, company_id: int, data: dict[str, Any]) -> bool:
    row = conn.execute("SELECT * FROM companies WHERE id = ?", (company_id,)).fetchone()
    if not row:
        return False
    manual_review = bool(data.get("needs_manual_review"))
    updates: dict[str, Any] = {
        "summary": clean_ai_value(data.get("company_summary"), 1200),
        "hiring_summary": clean_ai_value(data.get("hiring_summary"), 900),
        "job_categories": json_list_text(data.get("job_categories")),
        "common_job_titles": json_list_text(data.get("common_job_titles")),
        "search_keywords": json_list_text(data.get("search_keywords"), max_items=18),
        "career_page_quality": clean_ai_value(data.get("career_page_quality"), 20),
        "ai_confidence_score": int_between(data.get("confidence_score")),
        "ai_enriched_at": now_iso(),
    }
    for column, key in (("industry", "industry"), ("city", "city"), ("state", "state")):
        value = clean_ai_value(data.get(key), 120)
        existing = clean_ai_value(row[column], 120)
        if value and not existing:
            updates[column] = value
        elif value and existing and value.lower() != existing.lower():
            manual_review = True
    city = clean_ai_value(data.get("city"), 120)
    state = clean_ai_value(data.get("state"), 120)
    if city and state and not clean_ai_value(row["location"], 180):
        updates["location"] = f"{city}, {state}"
    updates["needs_manual_review"] = 1 if manual_review else 0
    updates["updated_at"] = now_iso()
    assignments = ", ".join(f"{column} = ?" for column in updates)
    conn.execute(
        f"UPDATE companies SET {assignments} WHERE id = ?",
        [*updates.values(), company_id],
    )
    return True


def import_company_enrichment_output(db_path: Path, local_batch: db.Row, file_ids: list[str], client: OpenAI) -> tuple[int, int]:
    content_parts: list[str] = []
    for file_id in file_ids:
        if not file_id:
            continue
        content_response = client.files.content(file_id)
        content_parts.append(content_response.text)
    content = "\n".join(content_parts)
    rows = parse_batch_output_lines(content)
    imported = 0
    failures = 0
    timestamp = now_iso()
    with connect(db_path) as conn:
        for item in rows:
            custom_id = str(item.get("custom_id") or "")
            request_row = conn.execute(
                """
                SELECT *
                FROM ai_company_enrichment_requests
                WHERE batch_id = ? AND custom_id = ?
                """,
                (local_batch["id"], custom_id),
            ).fetchone()
            if not request_row:
                failures += 1
                continue
            error = item.get("error")
            response = item.get("response") if isinstance(item.get("response"), dict) else {}
            body = response.get("body") if isinstance(response.get("body"), dict) else {}
            try:
                if error:
                    raise ValueError(json.dumps(error, sort_keys=True))
                if int(response.get("status_code") or 0) >= 400:
                    raise ValueError(f"OpenAI response status {response.get('status_code')}")
                text = extract_response_text(body)
                data = parse_ai_json_text(text)
                if apply_company_enrichment(conn, int(request_row["company_id"]), data):
                    imported += 1
                    status = "imported"
                    error_text = None
                else:
                    raise ValueError("Company no longer exists.")
            except Exception as exc:
                failures += 1
                status = "failed"
                error_text = str(exc)
            conn.execute(
                """
                UPDATE ai_company_enrichment_requests
                SET status = ?, error = ?, raw_response_json = ?, updated_at = ?
                WHERE id = ?
                """,
                (status, error_text, json.dumps(item, sort_keys=True), timestamp, request_row["id"]),
            )
        conn.execute(
            """
            UPDATE ai_company_enrichment_batches
            SET imported = 1,
                imported_at = ?,
                message = ?,
                failed_count = CASE WHEN ? > failed_count THEN ? ELSE failed_count END
            WHERE id = ?
            """,
            (
                timestamp,
                f"Imported {imported} companies with {failures} failures.",
                failures,
                failures,
                local_batch["id"],
            ),
        )
        create_task_history(
            conn,
            "enhance_companies",
            "completed" if failures == 0 else "failed",
            f"Imported {imported} companies with {failures} failures.",
            total=len(rows),
            completed=imported,
            payload={"batch_id": local_batch["openai_batch_id"], "local_batch_id": local_batch["id"]},
        )
    return imported, failures


def check_or_import_company_enrichment_batch(db_path: Path) -> str:
    client, _model = openai_client_and_enrichment_model()
    with connect(db_path) as conn:
        local_batch = latest_open_enrichment_batch(conn)
        if not local_batch:
            return create_company_enrichment_batch(db_path)
        if not local_batch["openai_batch_id"]:
            raise RuntimeError("Latest enrichment batch was not submitted to OpenAI.")
        batch = client.batches.retrieve(local_batch["openai_batch_id"])
        state = update_local_batch_from_openai(conn, int(local_batch["id"]), batch)
        local_batch = conn.execute(
            "SELECT * FROM ai_company_enrichment_batches WHERE id = ?",
            (local_batch["id"],),
        ).fetchone()
        if state["status"] in FAILED_BATCH_STATUSES:
            message = f"OpenAI batch {local_batch['openai_batch_id']} is {state['status']}."
            conn.execute(
                "UPDATE ai_company_enrichment_batches SET message = ? WHERE id = ?",
                (message, local_batch["id"]),
            )
            create_task_history(
                conn,
                "enhance_companies",
                "failed",
                message,
                total=state["total"],
                completed=state["completed"],
                payload={"batch_id": local_batch["openai_batch_id"], "local_batch_id": local_batch["id"]},
            )
            return message
        if state["status"] != "completed":
            message = f"OpenAI batch {local_batch['openai_batch_id']} is {state['status']} ({state['completed']} of {state['total']} completed)."
            conn.execute(
                "UPDATE ai_company_enrichment_batches SET message = ? WHERE id = ?",
                (message, local_batch["id"]),
            )
            create_task_history(
                conn,
                "enhance_companies",
                "completed",
                message,
                total=state["total"],
                completed=state["completed"],
                payload={"batch_id": local_batch["openai_batch_id"], "local_batch_id": local_batch["id"]},
            )
            return message
        output_file_id = state["output_file_id"] or local_batch["output_file_id"]
        error_file_id = state["error_file_id"] or local_batch["error_file_id"]
    file_ids = [file_id for file_id in (output_file_id, error_file_id) if file_id]
    if not file_ids:
        raise RuntimeError("OpenAI batch completed without an output or error file.")
    imported, failures = import_company_enrichment_output(db_path, local_batch, file_ids, client)
    return f"Imported {imported} companies with {failures} failures."


def build_job_enrichment_request(row: db.Row, model: str) -> dict[str, Any]:
    description = clean_ai_value(row["description"], 16000) or ""
    prompt = JOB_ENRICHMENT_PROMPT.format(
        company_name=row["business_name"] or "unknown",
        company_industry=row["company_industry"] or "unknown",
        company_city=row["company_city"] or "unknown",
        company_state=row["company_state"] or "unknown",
        job_title=row["job_title"],
        job_location=row["location"] or "unknown",
        job_url=row["application_url"] or row["source_url"] or "",
        job_description=description,
    )
    return {
        "custom_id": f"job-{row['id']}",
        "method": "POST",
        "url": "/v1/responses",
        "body": {
            "model": model,
            "input": prompt,
            "text": {"format": {"type": "json_object"}},
        },
    }


def latest_open_job_enrichment_batch(conn: db.connection) -> db.Row | None:
    return conn.execute(
        """
        SELECT *
        FROM ai_job_enrichment_batches
        WHERE imported = 0
          AND status NOT IN ('failed', 'expired', 'cancelled', 'canceled')
        ORDER BY id DESC
        LIMIT 1
        """
    ).fetchone()


def collect_job_enrichment_status(conn: db.connection) -> dict[str, Any]:
    pending_count = conn.execute(
        """
        SELECT COUNT(*)
        FROM job_postings
        WHERE ai_enriched_at IS NULL
          AND description IS NOT NULL
          AND TRIM(description) != ''
        """
    ).fetchone()[0]
    open_batch = latest_open_job_enrichment_batch(conn)
    latest_batch = conn.execute(
        """
        SELECT *
        FROM ai_job_enrichment_batches
        ORDER BY id DESC
        LIMIT 1
        """
    ).fetchone()
    batch = open_batch or latest_batch
    return {
        "pending_count": pending_count,
        "batch": dict(batch) if batch else None,
        "has_open_batch": open_batch is not None,
        "action_label": "Check Status" if open_batch else "Create Batch",
    }


def update_local_job_batch_from_openai(conn: db.connection, local_id: int, batch: Any, message: str | None = None) -> dict[str, Any]:
    total, completed, failed = batch_request_counts(batch)
    status = str(attr_value(batch, "status") or "unknown")
    output_file_id = attr_value(batch, "output_file_id")
    error_file_id = attr_value(batch, "error_file_id")
    timestamp = now_iso()
    conn.execute(
        """
        UPDATE ai_job_enrichment_batches
        SET status = ?,
            output_file_id = ?,
            error_file_id = ?,
            total_requests = ?,
            completed_requests = ?,
            failed_requests = ?,
            checked_at = ?,
            completed_at = CASE WHEN ? = 'completed' THEN COALESCE(completed_at, ?) ELSE completed_at END,
            message = COALESCE(?, message)
        WHERE id = ?
        """,
        (
            status,
            output_file_id,
            error_file_id,
            total,
            completed,
            failed,
            timestamp,
            status,
            timestamp,
            message,
            local_id,
        ),
    )
    return {
        "status": status,
        "output_file_id": output_file_id,
        "error_file_id": error_file_id,
        "total": total,
        "completed": completed,
        "failed": failed,
    }


def create_job_enrichment_batch(db_path: Path, limit: int = 500) -> str:
    client, model = openai_client_and_job_enrichment_model()
    timestamp = now_iso()
    temp_path: Path | None = None
    with connect(db_path) as conn:
        existing = latest_open_job_enrichment_batch(conn)
        if existing:
            return f"Job batch {existing['openai_batch_id'] or existing['id']} is already {existing['status']}. Check status instead."
        rows = conn.execute(
            """
            SELECT
                j.*,
                c.business_name,
                c.industry AS company_industry,
                c.city AS company_city,
                c.state AS company_state
            FROM job_postings j
            JOIN companies c ON c.id = j.company_id
            WHERE j.ai_enriched_at IS NULL
              AND j.description IS NOT NULL
              AND TRIM(j.description) != ''
            ORDER BY j.last_seen_at DESC, j.id ASC
            LIMIT ?
            """,
            (max(1, int(limit)),),
        ).fetchall()
        if not rows:
            create_task_history(conn, "enhance_jobs", "completed", "No jobs need enrichment.")
            return "No jobs need enrichment."
        cursor = conn.execute(
            """
            INSERT INTO ai_job_enrichment_batches (status, model, total_requests, message, created_at)
            VALUES ('local_created', ?, ?, 'Preparing OpenAI job batch', ?)
            RETURNING id
            """,
            (model, len(rows), timestamp),
        )
        local_batch_id = int(cursor.fetchone()[0])
        requests = []
        for row in rows:
            request_obj = build_job_enrichment_request(row, model)
            requests.append(request_obj)
            conn.execute(
                """
                INSERT INTO ai_job_enrichment_requests (batch_id, job_id, custom_id, status, created_at, updated_at)
                VALUES (?, ?, ?, 'pending', ?, ?)
                """,
                (local_batch_id, int(row["id"]), request_obj["custom_id"], timestamp, timestamp),
            )
        payload = "\n".join(json.dumps(item, ensure_ascii=False) for item in requests) + "\n"

    try:
        with tempfile.NamedTemporaryFile("w", encoding="utf-8", suffix=".jsonl", delete=False) as handle:
            temp_path = Path(handle.name)
            handle.write(payload)
        with temp_path.open("rb") as handle:
            uploaded = client.files.create(file=handle, purpose="batch")
        batch = client.batches.create(
            input_file_id=uploaded.id,
            endpoint="/v1/responses",
            completion_window="24h",
        )
        with connect(db_path) as conn:
            total, completed, failed = batch_request_counts(batch)
            status = str(attr_value(batch, "status") or "submitted")
            conn.execute(
                """
                UPDATE ai_job_enrichment_batches
                SET openai_batch_id = ?,
                    input_file_id = ?,
                    status = ?,
                    total_requests = ?,
                    completed_requests = ?,
                    failed_requests = ?,
                    message = ?,
                    submitted_at = ?,
                    checked_at = ?
                WHERE id = ?
                """,
                (
                    batch.id,
                    uploaded.id,
                    status,
                    total or len(requests),
                    completed,
                    failed,
                    f"OpenAI job batch {batch.id} created.",
                    now_iso(),
                    now_iso(),
                    local_batch_id,
                ),
            )
            create_task_history(
                conn,
                "enhance_jobs",
                "completed",
                f"Created OpenAI job batch {batch.id} for {len(requests)} jobs.",
                total=len(requests),
                payload={"batch_id": batch.id, "local_batch_id": local_batch_id},
            )
        return f"Created OpenAI job batch {batch.id} for {len(requests)} jobs."
    except Exception as exc:
        with connect(db_path) as conn:
            conn.execute(
                """
                UPDATE ai_job_enrichment_batches
                SET status = 'failed', message = ?, checked_at = ?
                WHERE id = ?
                """,
                (str(exc), now_iso(), local_batch_id),
            )
            create_task_history(conn, "enhance_jobs", "failed", str(exc), total=len(requests))
        raise
    finally:
        if temp_path and temp_path.exists():
            temp_path.unlink(missing_ok=True)


def apply_job_enrichment(conn: db.connection, job_id: int, data: dict[str, Any]) -> bool:
    row = conn.execute("SELECT * FROM job_postings WHERE id = ?", (job_id,)).fetchone()
    if not row:
        return False
    confidence = int_between(data.get("confidence_score"))
    category = clean_ai_value(data.get("job_category"), 80) or "unknown"
    pay_type = clean_ai_value(data.get("pay_range_type"), 20) or "unknown"
    description = clean_ai_value(row["description"])
    manual_review = bool(data.get("needs_manual_review"))
    if confidence < 60 or category.lower() == "unknown" or len(description or "") < 300:
        manual_review = True
    if pay_type not in {"posted", "estimated", "unknown"}:
        pay_type = "unknown"
        manual_review = True
    updates = {
        "ai_summary": clean_ai_value(data.get("plain_english_summary"), 900),
        "ai_best_for": clean_ai_value(data.get("best_for"), 700),
        "ai_job_category": category,
        "ai_experience_level": clean_ai_value(data.get("experience_level"), 40) or "unknown",
        "ai_worker_tags": json_list_text(data.get("worker_tags"), max_items=14, max_item_length=40),
        "ai_physical_demands": json_list_text(data.get("physical_demands"), max_items=10, max_item_length=120),
        "ai_estimated_pay_range": clean_ai_value(data.get("estimated_pay_range"), 120),
        "ai_pay_range_type": pay_type,
        "ai_confidence_score": confidence,
        "ai_needs_manual_review": 1 if manual_review else 0,
        "ai_enriched_at": now_iso(),
    }
    assignments = ", ".join(f"{column} = ?" for column in updates)
    conn.execute(f"UPDATE job_postings SET {assignments} WHERE id = ?", [*updates.values(), job_id])
    return True


def import_job_enrichment_output(db_path: Path, local_batch: db.Row, file_ids: list[str], client: OpenAI) -> tuple[int, int]:
    content_parts: list[str] = []
    for file_id in file_ids:
        if file_id:
            content_parts.append(client.files.content(file_id).text)
    rows = parse_batch_output_lines("\n".join(content_parts))
    imported = 0
    failures = 0
    timestamp = now_iso()
    with connect(db_path) as conn:
        for item in rows:
            custom_id = str(item.get("custom_id") or "")
            request_row = conn.execute(
                """
                SELECT *
                FROM ai_job_enrichment_requests
                WHERE batch_id = ? AND custom_id = ?
                """,
                (local_batch["id"], custom_id),
            ).fetchone()
            if not request_row:
                failures += 1
                continue
            error = item.get("error")
            response = item.get("response") if isinstance(item.get("response"), dict) else {}
            body = response.get("body") if isinstance(response.get("body"), dict) else {}
            try:
                if error:
                    raise ValueError(json.dumps(error, sort_keys=True))
                if int(response.get("status_code") or 0) >= 400:
                    raise ValueError(f"OpenAI response status {response.get('status_code')}")
                data = parse_ai_json_text(extract_response_text(body))
                if apply_job_enrichment(conn, int(request_row["job_id"]), data):
                    imported += 1
                    status = "imported"
                    error_text = None
                else:
                    raise ValueError("Job no longer exists.")
            except Exception as exc:
                failures += 1
                status = "failed"
                error_text = str(exc)
            conn.execute(
                """
                UPDATE ai_job_enrichment_requests
                SET status = ?, error = ?, raw_response_json = ?, updated_at = ?
                WHERE id = ?
                """,
                (status, error_text, json.dumps(item, sort_keys=True), timestamp, request_row["id"]),
            )
        conn.execute(
            """
            UPDATE ai_job_enrichment_batches
            SET imported = 1,
                imported_at = ?,
                message = ?,
                failed_requests = CASE WHEN ? > failed_requests THEN ? ELSE failed_requests END
            WHERE id = ?
            """,
            (
                timestamp,
                f"Imported {imported} jobs with {failures} failures.",
                failures,
                failures,
                local_batch["id"],
            ),
        )
        create_task_history(
            conn,
            "enhance_jobs",
            "completed" if failures == 0 else "failed",
            f"Imported {imported} jobs with {failures} failures.",
            total=len(rows),
            completed=imported,
            payload={"batch_id": local_batch["openai_batch_id"], "local_batch_id": local_batch["id"]},
        )
    return imported, failures


def check_or_import_job_enrichment_batch(db_path: Path) -> str:
    client, _model = openai_client_and_job_enrichment_model()
    with connect(db_path) as conn:
        local_batch = latest_open_job_enrichment_batch(conn)
        if not local_batch:
            return create_job_enrichment_batch(db_path)
        if not local_batch["openai_batch_id"]:
            raise RuntimeError("Latest job enrichment batch was not submitted to OpenAI.")
        batch = client.batches.retrieve(local_batch["openai_batch_id"])
        state = update_local_job_batch_from_openai(conn, int(local_batch["id"]), batch)
        local_batch = conn.execute(
            "SELECT * FROM ai_job_enrichment_batches WHERE id = ?",
            (local_batch["id"],),
        ).fetchone()
        if state["status"] in FAILED_BATCH_STATUSES:
            message = f"OpenAI job batch {local_batch['openai_batch_id']} is {state['status']}."
            conn.execute("UPDATE ai_job_enrichment_batches SET message = ? WHERE id = ?", (message, local_batch["id"]))
            create_task_history(
                conn,
                "enhance_jobs",
                "failed",
                message,
                total=state["total"],
                completed=state["completed"],
                payload={"batch_id": local_batch["openai_batch_id"], "local_batch_id": local_batch["id"]},
            )
            return message
        if state["status"] != "completed":
            message = f"OpenAI job batch {local_batch['openai_batch_id']} is {state['status']} ({state['completed']} of {state['total']} completed)."
            conn.execute("UPDATE ai_job_enrichment_batches SET message = ? WHERE id = ?", (message, local_batch["id"]))
            create_task_history(
                conn,
                "enhance_jobs",
                "completed",
                message,
                total=state["total"],
                completed=state["completed"],
                payload={"batch_id": local_batch["openai_batch_id"], "local_batch_id": local_batch["id"]},
            )
            return message
        output_file_id = state["output_file_id"] or local_batch["output_file_id"]
        error_file_id = state["error_file_id"] or local_batch["error_file_id"]
    file_ids = [file_id for file_id in (output_file_id, error_file_id) if file_id]
    if not file_ids:
        raise RuntimeError("OpenAI job batch completed without an output or error file.")
    imported, failures = import_job_enrichment_output(db_path, local_batch, file_ids, client)
    return f"Imported {imported} jobs with {failures} failures."


def record_company_task_error(db_path: Path, company_id: int, message: str) -> None:
    timestamp = now_iso()
    with connect(db_path) as conn:
        row = conn.execute("SELECT * FROM companies WHERE id = ?", (company_id,)).fetchone()
        company_name = row["business_name"] if row else "Unknown company"
        listing_url = row["jobs_url"] if row else None
        conn.execute(
            """
            INSERT INTO crawl_errors (company_id, company_name, listing_url, job_title, error_type, message, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (company_id, company_name, listing_url, None, "company", message, timestamp),
        )
        conn.execute(
            """
            UPDATE companies
            SET last_status = 'error', error = ?, last_checked_at = ?, updated_at = ?
            WHERE id = ?
            """,
            (message, timestamp, timestamp, company_id),
        )


async def crawl_company_by_id(
    db_path: Path,
    company_id: int,
    ai_client: Any,
    ai_model: str,
) -> int:
    with connect(db_path) as conn:
        row = conn.execute("SELECT * FROM companies WHERE id = ?", (company_id,)).fetchone()
        if not row:
            raise ValueError("Company not found.")
        seed = company_row_to_seed(row)
    if str(seed.get("extraction_provider") or "").lower() != "cloudflare":
        seed["extraction_provider"] = "cloudflare"
    await process_seed(
        seed,
        max_candidate_pages=1,
        max_ai_links=0,
        max_pages=12,
        max_detail_pages=20,
        ai_client=ai_client,
        ai_model=ai_model,
    )
    upsert_seed_to_db(db_path, seed)
    return len(seed.get("jobs", []))


async def crawl_companies_for_task(db_path: Path, task_id: int, company_ids: list[int], workers: int) -> tuple[int, int]:
    total = len(company_ids)
    completed = 0
    failures = 0
    lock = asyncio.Lock()
    queue: asyncio.Queue[int] = asyncio.Queue()
    for company_id in company_ids:
        queue.put_nowait(company_id)

    async def run_one(worker_index: int) -> None:
        nonlocal completed, failures
        ai_client, ai_model = build_ai_client_and_model(None)
        try:
            while not queue.empty():
                if task_cancel_requested(db_path, task_id):
                    return
                try:
                    company_id = queue.get_nowait()
                except asyncio.QueueEmpty:
                    return
                company_name = f"company {company_id}"
                try:
                    with connect(db_path) as conn:
                        row = conn.execute("SELECT business_name FROM companies WHERE id = ?", (company_id,)).fetchone()
                        if row:
                            company_name = row["business_name"]
                    count = await crawl_company_by_id(db_path, company_id, ai_client, ai_model)
                    message_tail = f"worker {worker_index} saved {count} jobs for {company_name}"
                except Exception as exc:
                    failures += 1
                    record_company_task_error(
                        db_path,
                        company_id,
                        format_exception_context(
                            exc,
                            task_id=task_id,
                            phase="fetch_all_company",
                            worker=worker_index,
                            company_id=company_id,
                            company_name=company_name,
                        ),
                    )
                    message_tail = f"worker {worker_index} failed {company_name}: {exc}"
                finally:
                    queue.task_done()
                async with lock:
                    completed += 1
                    update_task(
                        db_path,
                        task_id,
                        f"Fetched {completed} of {total} with {workers} workers; {failures} failures; {message_tail}",
                        completed=completed,
                        total=total,
                    )
        finally:
            await ai_client.close()

    await asyncio.gather(*(run_one(index) for index in range(1, max(1, workers) + 1)))
    return completed, failures


def run_task(db_path: Path, task: dict[str, Any]) -> None:
    task_id = int(task["id"])
    task_type = str(task["task_type"])
    payload = json.loads(task.get("payload_json") or "{}")
    if task_cancel_requested(db_path, task_id):
        finish_task(db_path, task_id, "canceled", "Canceled before start.")
        return
    if task_type == "delete_all_jobs":
        with connect(db_path) as conn:
            count = conn.execute("SELECT COUNT(*) FROM job_postings").fetchone()[0]
            conn.execute("DELETE FROM job_postings")
        finish_task(db_path, task_id, "completed", f"Deleted {count} jobs.")
        return

    if task_type == "fetch_company":
        company_id = int(payload["company_id"])
        try:
            with connect(db_path) as conn:
                company = conn.execute("SELECT business_name FROM companies WHERE id = ?", (company_id,)).fetchone()
            company_name = company["business_name"] if company else f"company {company_id}"
            update_task(db_path, task_id, f"Fetching jobs for {company_name}")
            async def runner():
                ai_client, ai_model = build_ai_client_and_model(None)
                try:
                    return await crawl_company_by_id(db_path, company_id, ai_client, ai_model)
                finally:
                    await ai_client.close()

            count = asyncio.run(runner())
            if count == 0:
                finish_task(db_path, task_id, "completed", f"No active jobs found for {company_name}; company flagged to skip routine scans.")
            else:
                finish_task(db_path, task_id, "completed", f"Fetched {count} jobs for {company_name}.")
        except Exception as exc:
            message = format_exception_context(exc, task_id=task_id, phase="fetch_company", company_id=company_id)
            record_company_task_error(db_path, company_id, message)
            finish_task(db_path, task_id, "failed", str(exc))
        return

    if task_type == "fetch_all":
        workers = max(1, min(8, int(payload.get("workers") or os.environ.get("FETCH_ALL_WORKERS") or 4)))
        with connect(db_path) as conn:
            company_ids = [
                int(row["id"])
                for row in conn.execute(
                    "SELECT id FROM companies WHERE jobs_url IS NOT NULL AND jobs_url != '' AND no_jobs_verified = 0 ORDER BY business_name COLLATE NOCASE ASC"
                )
            ]
        total = len(company_ids)
        update_task(db_path, task_id, f"Fetching 0 of {total} with {workers} workers", completed=0, total=total)
        completed, failures = asyncio.run(crawl_companies_for_task(db_path, task_id, company_ids, workers))
        if task_cancel_requested(db_path, task_id):
            finish_task(db_path, task_id, "canceled", f"Canceled after {completed} of {total} companies with {failures} failures.")
            return
        status = "completed" if failures == 0 else "failed"
        finish_task(db_path, task_id, status, f"Finished {completed} of {total} companies with {failures} failures using {workers} workers.")
        return

    if task_type == "discover_businesses":
        industry = clean_ai_value(payload.get("industry"), 120)
        if not industry:
            finish_task(db_path, task_id, "failed", "Missing industry.")
            return
        update_task(db_path, task_id, f"Running Outscraper discovery for {industry}")
        try:
            inserted, skipped = discover_businesses_for_industry(db_path, industry)
            finish_task(db_path, task_id, "completed", f"Discovery finished for {industry}: {inserted} staged or refreshed, {skipped} skipped.")
        except Exception as exc:
            finish_task(db_path, task_id, "failed", str(exc))
        return

    if task_type == "continue_discovery":
        discovery_id = int(payload["discovery_id"])
        with connect(db_path) as conn:
            discovery_row = conn.execute(
                "SELECT business_name FROM discovered_businesses WHERE id = ?",
                (discovery_id,),
            ).fetchone()
        discovery_name = discovery_row["business_name"] if discovery_row else f"discovery #{discovery_id}"
        update_task(db_path, task_id, f"Finding and verifying job source for {discovery_name}")
        try:
            message = verify_and_import_discovery(db_path, discovery_id)
            finish_task(db_path, task_id, "completed", message)
        except Exception as exc:
            with connect(db_path) as conn:
                conn.execute(
                    """
                    UPDATE discovered_businesses
                    SET status = 'needs_review',
                        verification_status = 'error',
                        verification_message = ?,
                        updated_at = ?
                    WHERE id = ?
                    """,
                    (str(exc), now_iso(), discovery_id),
                )
            finish_task(db_path, task_id, "failed", str(exc))
        return

    finish_task(db_path, task_id, "failed", f"Unknown task type: {task_type}")


def collect_errors(conn: db.connection) -> list[dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT
            ce.created_at,
            COALESCE(c.business_name, ce.company_name) AS company_name,
            ce.listing_url,
            ce.job_title,
            ce.error_type,
            ce.message
        FROM crawl_errors ce
        LEFT JOIN companies c ON c.id = ce.company_id
        WHERE ce.error_type != 'rejected_job'
        ORDER BY ce.created_at DESC, ce.id DESC
        LIMIT 300
        """
    ).fetchall()
    errors = [
        {
            **dict(row),
            "created_label": display_time(row["created_at"]),
            "short_message": clean_ai_value(str(row["message"]).splitlines()[0], 180) or "",
            "has_details": "\n" in (row["message"] or ""),
        }
        for row in rows
    ]
    company_rows = conn.execute(
        """
        SELECT business_name, jobs_url, last_status, error, last_checked_at
        FROM companies
        WHERE COALESCE(error, '') != ''
        ORDER BY COALESCE(last_checked_at, updated_at) DESC
        LIMIT 100
        """
    ).fetchall()
    for row in company_rows:
        errors.append(
            {
                "created_at": row["last_checked_at"],
                "created_label": display_time(row["last_checked_at"]),
                "company_name": row["business_name"],
                "listing_url": row["jobs_url"],
                "job_title": None,
                "error_type": "company",
                "message": row["error"] or row["last_status"],
                "short_message": clean_ai_value(str(row["error"] or row["last_status"]).splitlines()[0], 180) or "",
                "has_details": "\n" in (row["error"] or ""),
            }
        )
    job_rows = conn.execute(
        """
        SELECT c.business_name, j.application_url, j.job_title, j.raw_json, j.last_seen_at
        FROM job_postings j
        JOIN companies c ON c.id = j.company_id
        WHERE COALESCE(j.raw_json, '') LIKE '%detail_error%'
        ORDER BY j.last_seen_at DESC
        LIMIT 100
        """
    ).fetchall()
    for row in job_rows:
        message = "Job detail page error"
        try:
            raw = json.loads(row["raw_json"] or "{}")
            message = raw.get("detail_error") or message
        except json.JSONDecodeError:
            pass
        errors.append(
            {
                "created_at": row["last_seen_at"],
                "created_label": display_time(row["last_seen_at"]),
                "company_name": row["business_name"],
                "listing_url": row["application_url"],
                "job_title": row["job_title"],
                "error_type": "detail",
                "message": message,
                "short_message": clean_ai_value(str(message).splitlines()[0], 180) or "",
                "has_details": "\n" in (message or ""),
            }
        )
    return errors


def collect_tasks(conn: db.connection) -> list[dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT *
        FROM admin_tasks
        ORDER BY id DESC
        LIMIT 100
        """
    ).fetchall()
    return [
        {
            **dict(row),
            "label": task_label(row["task_type"]),
            "subject": task_subject(conn, row),
            "is_active": row["status"] in {"pending", "running"},
            "can_cancel": row["status"] in {"pending", "running"},
            "created_label": display_time(row["created_at"]),
            "started_label": display_time(row["started_at"]),
            "finished_label": display_time(row["finished_at"]),
        }
        for row in rows
    ]


def normalized_domain(url: str | None) -> str | None:
    parsed = urlparse(url or "")
    host = parsed.netloc.lower().removeprefix("www.")
    return host or None


def clean_json_response(raw: str) -> dict[str, Any]:
    text = (raw or "").strip()
    if text.startswith("```json"):
        text = text.removeprefix("```json").strip()
    if text.startswith("```"):
        text = text.removeprefix("```").strip()
    if text.endswith("```"):
        text = text[:-3].strip()
    data = json.loads(text or "{}")
    return data if isinstance(data, dict) else {}


def flatten_outscraper_places(payload: Any) -> list[dict[str, Any]]:
    places: list[dict[str, Any]] = []
    if isinstance(payload, list):
        for item in payload:
            places.extend(flatten_outscraper_places(item))
        return places
    if not isinstance(payload, dict):
        return places
    if any(key in payload for key in ("name", "site", "website", "full_address", "place_id", "google_id")):
        places.append(payload)
        return places
    for key in ("data", "results", "items", "places"):
        if key in payload:
            places.extend(flatten_outscraper_places(payload[key]))
    return places


def place_value(place: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        value = place.get(key)
        if value not in (None, ""):
            return value
    return None


def city_from_place(place: dict[str, Any], query_city: str) -> str:
    city = place_value(place, "city", "municipality", "locality")
    if city:
        return str(city)
    address = str(place_value(place, "full_address", "address", "location") or "")
    lowered = address.lower()
    for candidate in SKAGIT_DISCOVERY_CITIES:
        if candidate.lower() in lowered:
            return candidate
    return query_city


def is_skagit_discovery_city(value: str | None) -> bool:
    text = clean_ai_value(value, 500)
    if not text:
        return False
    lowered = text.lower()
    return any(city.lower() in lowered for city in SKAGIT_DISCOVERY_CITIES)


def place_is_in_skagit(place: dict[str, Any]) -> bool:
    city = clean_ai_value(place_value(place, "city", "municipality", "locality"), 120)
    address = clean_ai_value(place_value(place, "full_address", "address", "location"), 500)
    if city:
        return is_skagit_discovery_city(city)
    if address:
        return is_skagit_discovery_city(address)
    return False


def discovery_row_is_in_skagit(row: db.Row | dict[str, Any]) -> bool:
    if row["full_address"]:
        return is_skagit_discovery_city(row["full_address"])
    return is_skagit_discovery_city(row["city"] if row["city"] else None)


def mark_outside_skagit_discoveries(conn: db.connection) -> int:
    rows = conn.execute(
        """
        SELECT id, city, query_city, full_address
        FROM discovered_businesses
        WHERE status = 'discovered'
        """
    ).fetchall()
    outside_ids = [int(row["id"]) for row in rows if not discovery_row_is_in_skagit(row)]
    if not outside_ids:
        return 0
    placeholders = ",".join("?" for _ in outside_ids)
    conn.execute(
        f"""
        UPDATE discovered_businesses
        SET status = 'outside_area',
            verification_status = 'outside_area',
            verification_message = 'Skipped because Outscraper did not return a Skagit city/address.',
            updated_at = ?
        WHERE id IN ({placeholders})
        """,
        (now_iso(), *outside_ids),
    )
    return len(outside_ids)


def discover_businesses_for_industry(db_path: Path, industry: str) -> tuple[int, int]:
    api_key = os.environ.get("OUTSCRAPER_API_KEY")
    if not api_key:
        raise RuntimeError("OUTSCRAPER_API_KEY is required for business discovery.")
    inserted = 0
    skipped = 0
    timestamp = now_iso()
    industry = industry.strip()
    with connect(db_path) as conn:
        for query_city in SKAGIT_DISCOVERY_CITIES:
            params = urlencode(
                {
                    "query": f"{industry} in {query_city} WA",
                    "limit": int(os.environ.get("OUTSCRAPER_DISCOVERY_LIMIT", "500")),
                    "totalLimit": int(os.environ.get("OUTSCRAPER_DISCOVERY_TOTAL_LIMIT", "500")),
                    "dropDuplicates": "true",
                    "language": "en",
                    "region": "US",
                    "async": "false",
                }
            )
            req = Request(f"{OUTSCRAPER_SEARCH_URL}?{params}", headers={"X-API-KEY": api_key})
            try:
                with urlopen(req, timeout=120) as response:
                    payload = json.loads(response.read().decode("utf-8"))
            except (HTTPError, URLError, TimeoutError, json.JSONDecodeError) as exc:
                raise RuntimeError(f"Outscraper query failed for {industry} in {query_city}: {exc}") from exc

            for place in flatten_outscraper_places(payload):
                if not place_is_in_skagit(place):
                    skipped += 1
                    continue
                website = normalize_url(str(place_value(place, "site", "website", "url", "domain") or ""))
                name = clean_ai_value(place_value(place, "name", "business_name", "title"), 200)
                if not website or not name:
                    skipped += 1
                    continue
                domain = normalized_domain(website)
                existing = conn.execute(
                    """
                    SELECT 1
                    FROM companies
                    WHERE homepage_url = ?
                       OR seed_url = ?
                       OR lower(replace(replace(homepage_url, 'https://www.', ''), 'http://www.', '')) LIKE ?
                       OR lower(replace(replace(seed_url, 'https://www.', ''), 'http://www.', '')) LIKE ?
                    LIMIT 1
                    """,
                    (website, website, f"%{domain}%" if domain else "", f"%{domain}%" if domain else ""),
                ).fetchone()
                if existing:
                    skipped += 1
                    continue
                try:
                    cursor = conn.execute(
                        """
                        INSERT INTO discovered_businesses (
                            business_name, industry, city, query_city, website_url, phone,
                            full_address, google_place_id, google_id, rating, reviews, raw_json,
                            status, discovered_at, updated_at
                        )
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'discovered', ?, ?)
                        ON CONFLICT(website_url) DO UPDATE SET
                            business_name = excluded.business_name,
                            industry = excluded.industry,
                            city = COALESCE(discovered_businesses.city, excluded.city),
                            query_city = excluded.query_city,
                            phone = COALESCE(discovered_businesses.phone, excluded.phone),
                            full_address = COALESCE(discovered_businesses.full_address, excluded.full_address),
                            google_place_id = COALESCE(discovered_businesses.google_place_id, excluded.google_place_id),
                            google_id = COALESCE(discovered_businesses.google_id, excluded.google_id),
                            rating = COALESCE(discovered_businesses.rating, excluded.rating),
                            reviews = COALESCE(discovered_businesses.reviews, excluded.reviews),
                            raw_json = excluded.raw_json,
                            updated_at = excluded.updated_at
                        RETURNING id
                        """,
                        (
                            name,
                            industry,
                            city_from_place(place, query_city),
                            query_city,
                            website,
                            clean_ai_value(place_value(place, "phone", "phone_number"), 80),
                            clean_ai_value(place_value(place, "full_address", "address"), 500),
                            clean_ai_value(place_value(place, "place_id", "google_place_id"), 200),
                            clean_ai_value(place_value(place, "google_id", "cid"), 200),
                            clean_ai_value(place_value(place, "rating", "reviews_rating"), 40),
                            int(place_value(place, "reviews", "reviews_count") or 0),
                            json.dumps(place, ensure_ascii=False, sort_keys=True),
                            timestamp,
                            timestamp,
                        ),
                    )
                    if cursor.fetchone():
                        inserted += 1
                except db.IntegrityError:
                    skipped += 1
    return inserted, skipped


def build_job_source_discovery_prompt(row: db.Row) -> str:
    return json.dumps(
        {
            "task": "Find the actual page where this Skagit-area employer publishes active job listings.",
            "canonical_company_website": row["website_url"],
            "business_name": row["business_name"],
            "industry": row["industry"],
            "city": row["city"],
            "outscraper_context": {
                "phone": row["phone"],
                "full_address": row["full_address"],
                "google_place_id": row["google_place_id"],
                "google_id": row["google_id"],
                "raw": json.loads(row["raw_json"] or "{}"),
            },
            "instructions": [
                "Anchor the search in canonical_company_website first.",
                "Look for careers, jobs, employment, join us, and applicant links on or clearly connected to that domain.",
                "If the final job listings are on a third-party ATS, require evidence that the ATS is linked from or clearly associated with the canonical website.",
                "Consider Workday, Greenhouse, iCIMS, GovernmentJobs/NeoGov, PowerSchool, schooljobs.com, CATSOne, Infor, Phenom, ADP, UKG, Paylocity, BambooHR, and direct careers pages.",
                "Return only JSON. Do not include markdown.",
            ],
            "output_schema": {
                "job_source_url": "absolute URL or null",
                "source_type": "general_jobs | provider_jobs | internship | volunteer | culture | unrelated | unknown",
                "platform": "string or null",
                "canonical_website_url": "absolute URL",
                "association_evidence": "short explanation of how the job source is tied to the canonical website",
                "confidence": "integer 0-100",
                "evidence": ["short evidence strings"],
                "needs_manual_review": "boolean",
            },
        },
        ensure_ascii=False,
    )


def openai_client_and_discovery_model() -> tuple[OpenAI, str]:
    load_dotenv(Path(__file__).with_name(".env"))
    api_key = os.environ.get("OPENAI_API_KEY")
    if not api_key:
        raise RuntimeError("OPENAI_API_KEY is required for job source discovery.")
    model = os.environ.get("AI_DISCOVERY_MODEL") or os.environ.get("AI_VERIFICATION_MODEL") or "gpt-5.4-nano"
    return OpenAI(api_key=api_key), model


def find_job_source_with_openai(row: db.Row) -> dict[str, Any]:
    client, model = openai_client_and_discovery_model()
    domain = normalized_domain(row["website_url"])
    tools: list[dict[str, Any]] = [{"type": "web_search"}]
    if domain:
        tools[0]["filters"] = {"allowed_domains": [domain]}
    prompt = build_job_source_discovery_prompt(row)
    try:
        response = client.responses.create(model=model, tools=tools, tool_choice="auto", input=prompt)
    except Exception:
        # Some OpenAI web search variants do not support domain filters. Keep the
        # prompt anchored to the canonical site and retry without the filter.
        response = client.responses.create(model=model, tools=[{"type": "web_search"}], tool_choice="auto", input=prompt)
    data = clean_json_response(getattr(response, "output_text", "") or "{}")
    data["job_source_url"] = normalize_url(str(data.get("job_source_url") or ""), row["website_url"])
    data["canonical_website_url"] = normalize_url(str(data.get("canonical_website_url") or row["website_url"]), row["website_url"])
    try:
        data["confidence"] = max(0, min(100, int(data.get("confidence") or 0)))
    except (TypeError, ValueError):
        data["confidence"] = 0
    if not isinstance(data.get("evidence"), list):
        data["evidence"] = []
    return data


def source_type_or_default(value: Any) -> str:
    allowed = {"general_jobs", "provider_jobs", "internship", "volunteer", "culture", "unrelated", "unknown"}
    source_type = str(value or "general_jobs")
    return source_type if source_type in allowed else "general_jobs"


def promote_discovery_company_without_jobs(db_path: Path, seed: dict[str, Any]) -> int | None:
    init_db(db_path)
    timestamp = now_iso()
    seed_url = normalize_url(seed.get("seed_url") or seed.get("homepage_url") or seed.get("url"))
    if not seed_url:
        return None
    with connect(db_path) as conn:
        cursor = conn.execute(
            """
            INSERT INTO companies (
                seed_url, business_name, city, state, location, industry, homepage_url, jobs_url, source_type,
                last_checked_at, last_status, error, no_jobs_verified, no_jobs_verified_at, no_jobs_note, created_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?, ?, ?, ?)
            ON CONFLICT(seed_url) DO UPDATE SET
                business_name=excluded.business_name,
                city=COALESCE(companies.city, excluded.city),
                state=COALESCE(companies.state, excluded.state),
                location=COALESCE(companies.location, excluded.location),
                industry=COALESCE(companies.industry, excluded.industry),
                homepage_url=COALESCE(companies.homepage_url, excluded.homepage_url),
                jobs_url=excluded.jobs_url,
                source_type=excluded.source_type,
                last_checked_at=excluded.last_checked_at,
                last_status=excluded.last_status,
                error=excluded.error,
                no_jobs_verified=1,
                no_jobs_verified_at=excluded.no_jobs_verified_at,
                no_jobs_note=excluded.no_jobs_note,
                updated_at=excluded.updated_at
            RETURNING id
            """,
            (
                seed_url,
                clean_ai_value(seed.get("business_name"), 240) or seed_url,
                clean_ai_value(seed.get("city"), 120),
                clean_ai_value(seed.get("state") or seed.get("region"), 40),
                clean_ai_value(seed.get("location") or ", ".join(part for part in [seed.get("city"), seed.get("state") or seed.get("region")] if part), 240),
                clean_ai_value(seed.get("industry"), 160),
                normalize_url(seed.get("homepage_url") or seed_url),
                normalize_url(seed.get("jobs_url") or seed_url),
                source_type_or_default(seed.get("primary_source_type") or seed.get("source_type")),
                seed.get("last_checked_at") or timestamp,
                seed.get("last_status") or "ok",
                seed.get("error"),
                timestamp,
                seed.get("no_jobs_note") or "No active jobs were verified on the configured job source.",
                timestamp,
                timestamp,
            ),
        )
        company_id = int(cursor.fetchone()[0])
        sources = seed.get("job_sources") or [
            {
                "url": seed.get("jobs_url") or seed_url,
                "source_type": seed.get("primary_source_type") or seed.get("source_type"),
                "confidence": 0,
                "evidence": ["No active jobs extracted during discovery verification."],
            }
        ]
        for source in sources:
            source_url = normalize_url(source.get("url") or seed.get("jobs_url") or seed_url) or seed_url
            conn.execute(
                """
                INSERT INTO job_sources (
                    company_id, source_url, source_type, confidence,
                    active_job_count, evidence_json, last_checked_at, created_at, updated_at
                )
                VALUES (?, ?, ?, ?, 0, ?, ?, ?, ?)
                ON CONFLICT(company_id, source_url) DO UPDATE SET
                    source_type=excluded.source_type,
                    confidence=excluded.confidence,
                    active_job_count=0,
                    evidence_json=excluded.evidence_json,
                    last_checked_at=excluded.last_checked_at,
                    updated_at=excluded.updated_at
                """,
                (
                    company_id,
                    source_url,
                    source_type_or_default(source.get("source_type")),
                    int(source.get("confidence") or 0),
                    json.dumps(source.get("evidence") or [], sort_keys=True),
                    seed.get("last_checked_at") or timestamp,
                    timestamp,
                    timestamp,
                ),
            )
        return company_id


def verify_and_import_discovery(db_path: Path, discovery_id: int) -> str:
    with connect(db_path) as conn:
        row = conn.execute("SELECT * FROM discovered_businesses WHERE id = ?", (discovery_id,)).fetchone()
        if not row:
            raise RuntimeError("Discovery record not found.")
        conn.execute(
            "UPDATE discovered_businesses SET status = 'checking', continued_at = ?, updated_at = ? WHERE id = ?",
            (now_iso(), now_iso(), discovery_id),
        )

    discovery = find_job_source_with_openai(row)
    job_source_url = discovery.get("job_source_url")
    if not job_source_url:
        with connect(db_path) as conn:
            conn.execute(
                """
                UPDATE discovered_businesses
                SET status = 'needs_review',
                    verification_status = 'no_job_source',
                    verification_message = ?,
                    confidence = ?,
                    evidence_json = ?,
                    association_evidence = ?,
                    updated_at = ?
                WHERE id = ?
                """,
                (
                    "OpenAI could not identify a job source URL anchored to the company website.",
                    discovery.get("confidence") or 0,
                    json.dumps(discovery.get("evidence") or [], ensure_ascii=False),
                    clean_ai_value(discovery.get("association_evidence"), 1000),
                    now_iso(),
                    discovery_id,
                ),
            )
        return f"No job source URL found for {row['business_name']}; marked for review."

    async def runner() -> dict[str, Any]:
        ai_client, ai_model = build_ai_client_and_model(os.environ.get("AI_VERIFICATION_MODEL"))
        try:
            seed = {
                "business_name": row["business_name"],
                "homepage_url": row["website_url"],
                "seed_url": row["website_url"],
                "jobs_url": job_source_url,
                "city": row["city"],
                "state": "WA",
                "industry": row["industry"],
                "source_type": source_type_or_default(discovery.get("source_type")),
                "extraction_provider": "cloudflare",
            }
            return await process_seed(
                seed,
                max_candidate_pages=12,
                max_ai_links=60,
                max_pages=12,
                max_detail_pages=20,
                ai_client=ai_client,
                ai_model=ai_model,
            )
        finally:
            await ai_client.close()

    try:
        processed = asyncio.run(runner())
    except Exception as exc:
        with connect(db_path) as conn:
            conn.execute(
                """
                UPDATE discovered_businesses
                SET status = 'failed_verification',
                    job_source_url = ?,
                    source_type = ?,
                    platform = ?,
                    confidence = ?,
                    evidence_json = ?,
                    association_evidence = ?,
                    verification_status = 'error',
                    verification_message = ?,
                    extraction_provider = 'cloudflare',
                    updated_at = ?
                WHERE id = ?
                """,
                (
                    job_source_url,
                    source_type_or_default(discovery.get("source_type")),
                    clean_ai_value(discovery.get("platform"), 120),
                    discovery.get("confidence") or 0,
                    json.dumps(discovery.get("evidence") or [], ensure_ascii=False),
                    clean_ai_value(discovery.get("association_evidence"), 1000),
                    str(exc),
                    now_iso(),
                    discovery_id,
                ),
            )
        return f"Verification failed: {exc}"

    verified_job_count = len(processed.get("jobs") or [])
    provider_config = processed.get("provider_config") if isinstance(processed.get("provider_config"), dict) else {}
    provider_config_json = json.dumps(provider_config, sort_keys=True) if provider_config else None
    extraction_mode = clean_ai_value(processed.get("extraction_mode"), 80)
    extraction_evidence: list[str] = list(discovery.get("evidence") or [])
    for source in processed.get("job_sources") or []:
        if isinstance(source, dict):
            extraction_evidence.extend(source.get("evidence") or [])
    if verified_job_count <= 0:
        upsert_seed_to_db(db_path, processed)
        with connect(db_path) as conn:
            imported = conn.execute("SELECT id FROM companies WHERE seed_url = ?", (processed["seed_url"],)).fetchone()
            imported_company_id = int(imported["id"]) if imported else None
            conn.execute(
                """
                UPDATE discovered_businesses
                SET status = 'imported',
                    job_source_url = ?,
                    source_type = ?,
                    platform = ?,
                    confidence = ?,
                    evidence_json = ?,
                    association_evidence = ?,
                    verification_status = 'no_active_jobs',
                    verification_message = ?,
                    extraction_provider = 'cloudflare',
                    extraction_mode = ?,
                    provider_config = ?,
                    verified_job_count = 0,
                    imported_company_id = ?,
                    imported_at = ?,
                    updated_at = ?
                WHERE id = ?
                """,
                (
                    job_source_url,
                    source_type_or_default(discovery.get("source_type")),
                    clean_ai_value(discovery.get("platform"), 120),
                    max(int(discovery.get("confidence") or 0), int(processed.get("last_job_count") or 0)),
                    json.dumps(extraction_evidence, ensure_ascii=False),
                    clean_ai_value(discovery.get("association_evidence"), 1000),
                    "Company was added, but Cloudflare Browser Run did not extract active job listings.",
                    extraction_mode,
                    provider_config_json,
                    imported_company_id,
                    now_iso(),
                    now_iso(),
                    discovery_id,
                ),
            )
        return f"Added {row['business_name']} to companies; no verified active listings were extracted."

    upsert_seed_to_db(db_path, processed)
    with connect(db_path) as conn:
        imported = conn.execute("SELECT id FROM companies WHERE seed_url = ?", (processed["seed_url"],)).fetchone()
        imported_company_id = int(imported["id"]) if imported else None
        conn.execute(
            """
            UPDATE discovered_businesses
            SET status = 'imported',
                job_source_url = ?,
                source_type = ?,
                platform = ?,
                confidence = ?,
                evidence_json = ?,
                association_evidence = ?,
                verification_status = 'verified',
                verification_message = ?,
                extraction_provider = 'cloudflare',
                extraction_mode = ?,
                provider_config = ?,
                verified_job_count = ?,
                imported_company_id = ?,
                imported_at = ?,
                updated_at = ?
            WHERE id = ?
            """,
            (
                job_source_url,
                processed.get("primary_source_type") or source_type_or_default(discovery.get("source_type")),
                clean_ai_value(discovery.get("platform"), 120),
                max(int(discovery.get("confidence") or 0), 85),
                json.dumps(extraction_evidence, ensure_ascii=False),
                clean_ai_value(discovery.get("association_evidence"), 1000),
                f"Imported with {verified_job_count} Cloudflare-extracted active job(s).",
                extraction_mode,
                provider_config_json,
                verified_job_count,
                imported_company_id,
                now_iso(),
                now_iso(),
                discovery_id,
            ),
        )
    return f"Imported {row['business_name']} with {verified_job_count} verified active job(s)."


def collect_discovery_view(conn: db.connection) -> dict[str, Any]:
    mark_outside_skagit_discoveries(conn)
    industries = [
        dict(row)
        for row in conn.execute(
            """
            SELECT id, name
            FROM discovery_industries
            ORDER BY sort_order ASC, name COLLATE NOCASE ASC
            """
        )
    ]
    counts = {
        row["industry"]: dict(row)
        for row in conn.execute(
            """
            SELECT industry,
                   COUNT(*) AS total,
                   SUM(CASE WHEN status = 'discovered' THEN 1 ELSE 0 END) AS pending,
                   SUM(CASE WHEN status IN ('queued', 'checking') THEN 1 ELSE 0 END) AS queued,
                   SUM(CASE WHEN status = 'imported' THEN 1 ELSE 0 END) AS imported
            FROM discovered_businesses
            GROUP BY industry
            """
        )
    }
    open_count = conn.execute(
        """
        SELECT COUNT(*)
        FROM discovered_businesses
        WHERE status IN ('discovered', 'queued', 'checking', 'needs_review', 'failed_verification')
        """
    ).fetchone()[0]
    discoveries = [
        {
            **dict(row),
            "updated_label": display_time(row["updated_at"]),
            "evidence_count": len(json.loads(row["evidence_json"] or "[]")),
        }
        for row in conn.execute(
            """
            SELECT *
            FROM discovered_businesses
            WHERE status IN ('discovered', 'queued', 'checking', 'needs_review', 'failed_verification')
            ORDER BY
                CASE status
                  WHEN 'discovered' THEN 0
                  WHEN 'queued' THEN 1
                  WHEN 'checking' THEN 2
                  WHEN 'needs_review' THEN 3
                  WHEN 'failed_verification' THEN 4
                  ELSE 5
                END,
                updated_at DESC,
                business_name COLLATE NOCASE ASC
            LIMIT 300
            """
        )
    ]
    return {"industries": industries, "counts": counts, "discoveries": discoveries, "open_count": open_count}


def create_app(db_path: Path = DEFAULT_DB) -> Flask:
    load_dotenv(Path(__file__).with_name(".env"))
    app = Flask(__name__)
    app.secret_key = os.environ.get("SECRET_KEY", "local-admin-dashboard")
    app.config["DB_PATH"] = db_path

    @app.get("/admin")
    def index() -> str:
        active_tab = request.args.get("tab", "home")
        if active_tab not in {"home", "companies", "discovery", "jobs", "tasks", "errors", "settings"}:
            active_tab = "home"
        query = request.args.get("q", "").strip()
        with connect(app.config["DB_PATH"]) as conn:
            if active_tab == "discovery":
                mark_outside_skagit_discoveries(conn)
            companies = conn.execute(
                """
                SELECT
                    c.*,
                    COUNT(j.id) AS job_count,
                    SUM(CASE WHEN j.is_active = 1 THEN 1 ELSE 0 END) AS active_job_count
                FROM companies c
                LEFT JOIN job_postings j ON j.company_id = c.id
                GROUP BY c.id
                ORDER BY c.is_featured DESC, c.business_name COLLATE NOCASE ASC
                """
            ).fetchall()

            params: list[Any] = []
            where = "WHERE j.is_active = 1"
            if query:
                where += """
                    AND (
                        j.job_title LIKE ?
                        OR c.business_name LIKE ?
                        OR COALESCE(j.location, '') LIKE ?
                        OR COALESCE(j.department, '') LIKE ?
                    )
                """
                like = f"%{query}%"
                params.extend([like, like, like, like])
            jobs = conn.execute(
                f"""
                SELECT
                    j.*,
                    c.business_name,
                    c.is_featured AS company_is_featured
                FROM job_postings j
                JOIN companies c ON c.id = j.company_id
                {where}
                ORDER BY j.is_featured DESC, c.is_featured DESC, j.last_seen_at DESC, j.job_title COLLATE NOCASE ASC
                LIMIT 250
                """,
                params,
            ).fetchall()

            stats = conn.execute(
                """
                SELECT
                    (SELECT COUNT(*) FROM companies) AS companies,
                    (SELECT COUNT(*) FROM companies WHERE is_featured = 1) AS featured_companies,
                    (SELECT COUNT(*) FROM job_postings WHERE is_active = 1) AS active_jobs,
                    (SELECT COUNT(*) FROM job_postings WHERE is_featured = 1 AND is_active = 1) AS featured_jobs,
                    (SELECT COUNT(*) FROM discovered_businesses WHERE status IN ('discovered', 'queued', 'checking', 'needs_review', 'failed_verification')) AS discoveries,
                    (SELECT COUNT(*) FROM crawl_errors WHERE error_type != 'rejected_job') AS errors,
                    (SELECT COUNT(*) FROM admin_tasks WHERE status IN ('pending', 'running')) AS active_tasks
                """
            ).fetchone()
            errors = collect_errors(conn) if active_tab == "errors" else []
            tasks = collect_tasks(conn) if active_tab == "tasks" else []
            active_tasks = [task for task in tasks if task["is_active"]]
            settings = load_settings(conn)
            company_enrichment = collect_company_enrichment_status(conn)
            job_enrichment = collect_job_enrichment_status(conn)
            discovery = collect_discovery_view(conn) if active_tab == "discovery" else {"industries": [], "counts": {}, "discoveries": [], "open_count": stats["discoveries"]}

        if stats["active_tasks"]:
            start_task_worker(app.config["DB_PATH"])

        return render_template(
            "admin/index.html",
            active_tab=active_tab,
            companies=companies,
            jobs=jobs,
            errors=errors,
            tasks=tasks,
            active_tasks=active_tasks,
            stats=stats,
            settings=settings,
            company_enrichment=company_enrichment,
            job_enrichment=job_enrichment,
            discovery=discovery,
            query=query,
            display_time=display_time,
        )

    @app.post("/companies")
    def add_company():
        jobs_url = normalize_url(clean_form_value("jobs_url") or "")
        if not jobs_url:
            flash("Job source URL is required.", "error")
            return redirect(url_for("index"))

        seed = {
            "business_name": clean_form_value("business_name"),
            "homepage_url": normalize_url(clean_form_value("homepage_url") or jobs_url),
            "jobs_url": jobs_url,
            "city": clean_form_value("city"),
            "state": clean_form_value("state"),
            "industry": clean_form_value("industry"),
        }
        import_seed_items_to_db(app.config["DB_PATH"], [seed])
        flash("Company saved.", "ok")
        return redirect(url_for("index", tab="companies"))

    @app.post("/companies/<int:company_id>")
    def update_company(company_id: int):
        jobs_url = normalize_url(clean_form_value("jobs_url") or "")
        if not jobs_url:
            flash("Job source URL is required.", "error")
            return redirect(url_for("index", tab="companies"))
        homepage_url = normalize_url(clean_form_value("homepage_url") or "")
        seed_url = normalize_url(clean_form_value("seed_url") or homepage_url or jobs_url)
        if not seed_url:
            flash("Company URL is required.", "error")
            return redirect(url_for("index", tab="companies"))
        no_jobs_verified = 1 if request.form.get("no_jobs_verified") == "1" else 0
        no_jobs_note = clean_form_value("no_jobs_note")
        timestamp = now_iso()

        with connect(app.config["DB_PATH"]) as conn:
            existing = conn.execute(
                "SELECT id FROM companies WHERE seed_url = ? AND id != ?",
                (seed_url, company_id),
            ).fetchone()
            if existing:
                flash("Another company already uses that company URL.", "error")
                return redirect(url_for("index", tab="companies"))
            conn.execute(
                """
                UPDATE companies
                SET seed_url = ?,
                    business_name = ?,
                    city = ?,
                    state = ?,
                    industry = ?,
                    homepage_url = ?,
                    jobs_url = ?,
                    no_jobs_verified = ?,
                    no_jobs_verified_at = CASE WHEN ? = 1 AND no_jobs_verified = 0 THEN ? WHEN ? = 0 THEN NULL ELSE no_jobs_verified_at END,
                    no_jobs_note = ?,
                    updated_at = ?
                WHERE id = ?
                """,
                (
                    seed_url,
                    clean_form_value("business_name") or seed_url,
                    clean_form_value("city"),
                    clean_form_value("state"),
                    clean_form_value("industry"),
                    homepage_url,
                    jobs_url,
                    no_jobs_verified,
                    no_jobs_verified,
                    timestamp,
                    no_jobs_verified,
                    no_jobs_note or ("No active jobs were verified on the configured job source." if no_jobs_verified else None),
                    timestamp,
                    company_id,
                ),
            )
        flash("Company updated.", "ok")
        return redirect(url_for("index", tab="companies"))

    @app.post("/companies/<int:company_id>/crawl")
    def crawl_company(company_id: int):
        task_id = enqueue_task(app.config["DB_PATH"], "fetch_company", {"company_id": company_id})
        flash(f"Queued get jobs task #{task_id}.", "ok")
        return redirect(url_for("index", tab="companies"))

    @app.post("/companies/import")
    def import_companies():
        raw_text = request.form.get("json_payload", "").strip()
        if not raw_text:
            flash("Paste a JSON object or list.", "error")
            return redirect(url_for("index"))
        try:
            payload = json.loads(raw_text)
            count = import_seed_items_to_db(app.config["DB_PATH"], payload)
        except Exception as exc:
            flash(f"Import failed: {exc}", "error")
            return redirect(url_for("index", tab="companies"))
        flash(f"Imported {count} company records.", "ok")
        return redirect(url_for("index", tab="companies"))

    @app.post("/companies/<int:company_id>/featured")
    def toggle_company_featured(company_id: int):
        with connect(app.config["DB_PATH"]) as conn:
            conn.execute(
                """
                UPDATE companies
                SET is_featured = CASE WHEN is_featured = 1 THEN 0 ELSE 1 END,
                    updated_at = ?
                WHERE id = ?
                """,
                (now_iso(), company_id),
            )
        return redirect(request.referrer or url_for("index", tab="companies"))

    @app.post("/companies/<int:company_id>/delete")
    def delete_company(company_id: int):
        with connect(app.config["DB_PATH"]) as conn:
            conn.execute("DELETE FROM job_postings WHERE company_id = ?", (company_id,))
            conn.execute("DELETE FROM job_sources WHERE company_id = ?", (company_id,))
            conn.execute("DELETE FROM companies WHERE id = ?", (company_id,))
        flash("Company and related jobs deleted.", "ok")
        return redirect(url_for("index", tab="companies"))

    @app.post("/discovery/<path:industry>/discover")
    def discover_industry(industry: str):
        task_id = enqueue_task(app.config["DB_PATH"], "discover_businesses", {"industry": industry})
        flash(f"Queued discovery task #{task_id} for {industry}.", "ok")
        return redirect(url_for("index", tab="discovery"))

    @app.post("/discovery/<int:discovery_id>/continue")
    def continue_discovery(discovery_id: int):
        with connect(app.config["DB_PATH"]) as conn:
            conn.execute(
                """
                UPDATE discovered_businesses
                SET status = 'queued',
                    verification_status = 'queued',
                    verification_message = 'Queued for web search and verification.',
                    updated_at = ?
                WHERE id = ? AND status IN ('discovered', 'needs_review', 'failed_verification')
                """,
                (now_iso(), discovery_id),
            )
        task_id = enqueue_task(app.config["DB_PATH"], "continue_discovery", {"discovery_id": discovery_id})
        flash(f"Queued continue task #{task_id}.", "ok")
        return redirect(url_for("index", tab="discovery"))

    @app.post("/discovery/<int:discovery_id>")
    def update_discovery(discovery_id: int):
        business_name = clean_form_value("business_name")
        website_url = normalize_url(clean_form_value("website_url") or "")
        if not business_name:
            flash("Business name is required.", "error")
            return redirect(url_for("index", tab="discovery"))
        if not website_url:
            flash("Website URL is required.", "error")
            return redirect(url_for("index", tab="discovery"))

        job_source_raw = clean_form_value("job_source_url")
        job_source_url = normalize_url(job_source_raw, website_url) if job_source_raw else None
        status = clean_form_value("status") or "discovered"
        allowed_statuses = {"discovered", "needs_review", "failed_verification"}
        if status not in allowed_statuses:
            status = "discovered"

        with connect(app.config["DB_PATH"]) as conn:
            try:
                conn.execute(
                    """
                    UPDATE discovered_businesses
                    SET business_name = ?,
                        industry = ?,
                        city = ?,
                        website_url = ?,
                        phone = ?,
                        full_address = ?,
                        job_source_url = ?,
                        status = ?,
                        verification_status = CASE WHEN ? = 'discovered' THEN NULL ELSE verification_status END,
                        verification_message = CASE WHEN ? = 'discovered' THEN NULL ELSE verification_message END,
                        updated_at = ?
                    WHERE id = ?
                    """,
                    (
                        business_name,
                        clean_form_value("industry") or "Unknown",
                        clean_form_value("city"),
                        website_url,
                        clean_form_value("phone"),
                        clean_form_value("full_address"),
                        job_source_url,
                        status,
                        status,
                        status,
                        now_iso(),
                        discovery_id,
                    ),
                )
            except db.IntegrityError:
                flash("Another discovery row already uses that website URL.", "error")
                return redirect(url_for("index", tab="discovery"))
        flash("Discovery row updated.", "ok")
        return redirect(url_for("index", tab="discovery"))

    @app.post("/discovery/<int:discovery_id>/delete")
    def delete_discovery(discovery_id: int):
        with connect(app.config["DB_PATH"]) as conn:
            conn.execute("DELETE FROM discovered_businesses WHERE id = ?", (discovery_id,))
        flash("Discovery row deleted.", "ok")
        return redirect(url_for("index", tab="discovery"))

    @app.post("/discovery/industries/<int:industry_id>")
    def update_discovery_industry(industry_id: int):
        name = clean_form_value("name")
        if not name:
            flash("Industry name is required.", "error")
            return redirect(url_for("index", tab="discovery"))
        with connect(app.config["DB_PATH"]) as conn:
            existing = conn.execute("SELECT name FROM discovery_industries WHERE id = ?", (industry_id,)).fetchone()
            if not existing:
                flash("Industry not found.", "error")
                return redirect(url_for("index", tab="discovery"))
            old_name = existing["name"]
            try:
                conn.execute(
                    "UPDATE discovery_industries SET name = ?, updated_at = ? WHERE id = ?",
                    (name, now_iso(), industry_id),
                )
            except db.IntegrityError:
                flash("Another industry already uses that name.", "error")
                return redirect(url_for("index", tab="discovery"))
            conn.execute("UPDATE discovered_businesses SET industry = ?, updated_at = ? WHERE industry = ?", (name, now_iso(), old_name))
        flash("Industry updated.", "ok")
        return redirect(url_for("index", tab="discovery"))

    @app.post("/discovery/industries/<int:industry_id>/delete")
    def delete_discovery_industry(industry_id: int):
        with connect(app.config["DB_PATH"]) as conn:
            row = conn.execute("SELECT name FROM discovery_industries WHERE id = ?", (industry_id,)).fetchone()
            if row:
                conn.execute("DELETE FROM discovery_industries WHERE id = ?", (industry_id,))
        flash("Industry removed from discovery.", "ok")
        return redirect(url_for("index", tab="discovery"))

    @app.post("/jobs/<int:job_id>/featured")
    def toggle_job_featured(job_id: int):
        with connect(app.config["DB_PATH"]) as conn:
            conn.execute(
                "UPDATE job_postings SET is_featured = CASE WHEN is_featured = 1 THEN 0 ELSE 1 END WHERE id = ?",
                (job_id,),
            )
        return redirect(request.referrer or url_for("index", tab="jobs"))

    @app.post("/jobs/<int:job_id>/delete")
    def delete_job(job_id: int):
        with connect(app.config["DB_PATH"]) as conn:
            conn.execute("DELETE FROM job_postings WHERE id = ?", (job_id,))
        flash("Job deleted.", "ok")
        return redirect(request.referrer or url_for("index", tab="jobs"))

    @app.post("/tasks/fetch-all")
    def fetch_all_jobs():
        try:
            workers = int(request.form.get("workers", "4"))
        except ValueError:
            workers = 4
        workers = max(1, min(8, workers))
        task_id = enqueue_task(app.config["DB_PATH"], "fetch_all", {"workers": workers})
        flash(f"Queued fetch all jobs task #{task_id} with {workers} workers.", "ok")
        return redirect(url_for("index", tab="tasks"))

    @app.post("/tasks/<int:task_id>/cancel")
    def cancel_task(task_id: int):
        timestamp = now_iso()
        with connect(app.config["DB_PATH"]) as conn:
            row = conn.execute("SELECT status, task_type FROM admin_tasks WHERE id = ?", (task_id,)).fetchone()
            if not row:
                flash("Task not found.", "error")
                return redirect(url_for("index", tab="tasks"))
            if row["status"] == "pending":
                conn.execute(
                    """
                    UPDATE admin_tasks
                    SET status = 'canceled',
                        cancel_requested = 1,
                        message = 'Canceled before start.',
                        finished_at = ?
                    WHERE id = ?
                    """,
                    (timestamp, task_id),
                )
                flash(f"Canceled task #{task_id}.", "ok")
            elif row["status"] == "running":
                conn.execute(
                    """
                    UPDATE admin_tasks
                    SET cancel_requested = 1,
                        message = COALESCE(message || ' | ', '') || 'Cancel requested.'
                    WHERE id = ?
                    """,
                    (task_id,),
                )
                flash(f"Cancel requested for task #{task_id}. It will stop after the current company finishes.", "ok")
            else:
                flash(f"Task #{task_id} is already {row['status']}.", "ok")
        start_task_worker(app.config["DB_PATH"])
        return redirect(url_for("index", tab="tasks"))

    @app.post("/tasks/delete-all-jobs")
    def delete_all_jobs():
        confirm_text = request.form.get("confirm_text", "").strip()
        checked = request.form.get("confirm_checked") == "yes"
        if confirm_text != "DELETE ALL JOBS" or not checked:
            flash("Delete all jobs requires the checkbox and exact confirmation text.", "error")
            return redirect(url_for("index", tab="tasks"))
        task_id = enqueue_task(app.config["DB_PATH"], "delete_all_jobs")
        flash(f"Queued delete all jobs task #{task_id}.", "ok")
        return redirect(url_for("index", tab="tasks"))

    @app.post("/tasks/enhance-companies")
    def enhance_companies():
        try:
            message = check_or_import_company_enrichment_batch(app.config["DB_PATH"])
            flash(message, "ok")
        except Exception as exc:
            with connect(app.config["DB_PATH"]) as conn:
                create_task_history(conn, "enhance_companies", "failed", str(exc))
            flash(f"Company enrichment failed: {exc}", "error")
        return redirect(url_for("index", tab="tasks"))

    @app.post("/tasks/enhance-companies-test")
    def enhance_companies_test():
        try:
            message = create_company_enrichment_batch(app.config["DB_PATH"], limit=1)
            flash(message, "ok")
        except Exception as exc:
            with connect(app.config["DB_PATH"]) as conn:
                create_task_history(conn, "enhance_companies", "failed", str(exc), payload={"limit": 1})
            flash(f"Company enrichment test failed: {exc}", "error")
        return redirect(url_for("index", tab="tasks"))

    @app.post("/tasks/enhance-jobs")
    def enhance_jobs():
        try:
            message = check_or_import_job_enrichment_batch(app.config["DB_PATH"])
            flash(message, "ok")
        except Exception as exc:
            with connect(app.config["DB_PATH"]) as conn:
                create_task_history(conn, "enhance_jobs", "failed", str(exc))
            flash(f"Job enrichment failed: {exc}", "error")
        return redirect(url_for("index", tab="tasks"))

    @app.post("/errors/delete-all")
    def delete_all_errors():
        with connect(app.config["DB_PATH"]) as conn:
            count = conn.execute("SELECT COUNT(*) FROM crawl_errors WHERE error_type != 'rejected_job'").fetchone()[0]
            conn.execute("DELETE FROM crawl_errors WHERE error_type != 'rejected_job'")
            conn.execute("UPDATE companies SET error = NULL, last_status = NULL WHERE COALESCE(error, '') != ''")
        flash(f"Deleted {count} error notifications.", "ok")
        return redirect(url_for("index", tab="errors"))

    @app.post("/settings")
    def update_settings():
        try:
            refresh_days = int(request.form.get("job_refresh_days", "7"))
        except ValueError:
            refresh_days = 7
        refresh_days = max(1, min(365, refresh_days))
        refresh_day = request.form.get("job_refresh_day", "sunday").strip().lower()
        allowed_days = {"sunday", "monday", "tuesday", "wednesday", "thursday", "friday", "saturday"}
        if refresh_day not in allowed_days:
            refresh_day = "sunday"
        timestamp = now_iso()
        with connect(app.config["DB_PATH"]) as conn:
            conn.execute(
                """
                INSERT INTO app_settings (key, value, updated_at)
                VALUES ('job_refresh_days', ?, ?)
                ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_at = excluded.updated_at
                """,
                (str(refresh_days), timestamp),
            )
            conn.execute(
                """
                INSERT INTO app_settings (key, value, updated_at)
                VALUES ('job_refresh_day', ?, ?)
                ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_at = excluded.updated_at
                """,
                (refresh_day, timestamp),
            )
        flash("Settings saved.", "ok")
        return redirect(url_for("index", tab="settings"))

    from frontend_app import public_bp
    app.register_blueprint(public_bp)

    @app.context_processor
    def inject_now():
        return {"now": datetime.now()}

    return app



def main() -> None:
    load_dotenv(Path(__file__).with_name(".env"))
    parser = argparse.ArgumentParser(description="Local admin dashboard for the Railway Postgres crawler database.")
    parser.add_argument("--db", type=Path, default=DEFAULT_DB, help="Deprecated; DATABASE_PUBLIC_URL is used.")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=5050)
    parser.add_argument("--debug", action="store_true")
    args = parser.parse_args()

    app = create_app(args.db)
    app.run(host=args.host, port=args.port, debug=args.debug,use_reloader=False)


if __name__ == "__main__":
    main()
