from __future__ import annotations

import json
import re
from datetime import datetime, timedelta, timezone
from typing import Any

from flask import Blueprint, current_app, jsonify, render_template, request

import db

public_bp = Blueprint("public", __name__, url_prefix="/")

SKAGIT_CITIES = [
    "Mount Vernon",
    "Burlington",
    "Anacortes",
    "Sedro-Woolley",
    "La Conner",
    "Concrete",
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

SPOTLIGHT_INDUSTRIES = [
    ("Healthcare", "fa-solid fa-heart-pulse"),
    ("Manufacturing", "fa-solid fa-industry"),
    ("Education", "fa-solid fa-graduation-cap"),
    ("Agriculture", "fa-solid fa-tractor"),
    ("Aerospace", "fa-solid fa-jet-fighter"),
    ("Marine/Shipbuilding", "fa-solid fa-anchor"),
]

PAGE_SIZE = 25


def _connect() -> db.Connection:
    from admin_app import connect as admin_connect
    return admin_connect(current_app.config["DB_PATH"])


def time_ago(value: str | None) -> str:
    if not value:
        return ""
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return value
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    now = datetime.now(timezone.utc)
    delta = now - parsed
    days = delta.days
    if days == 0:
        hours = delta.seconds // 3600
        return "today" if hours < 1 else f"{hours}h ago"
    if days == 1:
        return "yesterday"
    if days < 7:
        return f"{days}d ago"
    if days < 30:
        weeks = days // 7
        return f"{weeks}w ago"
    months = days // 30
    return f"{months}mo ago"


def avatar_color(name: str) -> str:
    colors = [
        "bg-red-700", "bg-slate-700", "bg-teal-700", "bg-indigo-700",
        "bg-emerald-700", "bg-amber-700", "bg-violet-700", "bg-cyan-700",
    ]
    index = sum(ord(c) for c in (name or "")) % len(colors)
    return colors[index]


def parse_json_list(value: Any) -> list[str]:
    if isinstance(value, list):
        return value
    try:
        result = json.loads(value or "[]")
        return result if isinstance(result, list) else []
    except (json.JSONDecodeError, TypeError):
        return []


def _week_ago_iso() -> str:
    return (datetime.now(timezone.utc) - timedelta(days=7)).isoformat()


@public_bp.get("/")
def index() -> str:
    with _connect() as conn:
        city_rows = conn.execute(
            """
            SELECT c.city, COUNT(jp.id) AS cnt
            FROM job_postings jp
            JOIN companies c ON c.id = jp.company_id
            WHERE jp.is_active = 1 AND c.city IS NOT NULL AND c.city != ''
            GROUP BY c.city
            ORDER BY cnt DESC
            """
        ).fetchall()
        city_counts = {row["city"]: row["cnt"] for row in city_rows}

        industry_rows = conn.execute(
            """
            SELECT c.industry, COUNT(jp.id) AS cnt
            FROM job_postings jp
            JOIN companies c ON c.id = jp.company_id
            WHERE jp.is_active = 1 AND c.industry IS NOT NULL AND c.industry != ''
            GROUP BY c.industry
            """
        ).fetchall()
        industry_counts = {row["industry"]: row["cnt"] for row in industry_rows}

        new_jobs = conn.execute(
            """
            SELECT jp.*, c.business_name, c.city AS company_city, c.industry AS company_industry,
                   c.id AS company_db_id
            FROM job_postings jp
            JOIN companies c ON c.id = jp.company_id
            WHERE jp.is_active = 1 AND (jp.is_new = 1 OR jp.first_seen_at >= ?)
            ORDER BY jp.first_seen_at DESC
            LIMIT 12
            """,
            (_week_ago_iso(),),
        ).fetchall()

        stats = conn.execute(
            """
            SELECT
                (SELECT COUNT(*) FROM companies) AS company_count,
                (SELECT COUNT(*) FROM job_postings WHERE is_active = 1) AS job_count
            """
        ).fetchone()

        featured_companies = conn.execute(
            """
            SELECT c.*, COUNT(jp.id) AS active_job_count
            FROM companies c
            LEFT JOIN job_postings jp ON jp.company_id = c.id AND jp.is_active = 1
            WHERE c.is_featured = 1
            GROUP BY c.id
            ORDER BY c.business_name ASC
            LIMIT 6
            """
        ).fetchall()

    return render_template(
        "public/index.html",
        city_counts=city_counts,
        skagit_cities=SKAGIT_CITIES,
        spotlight_industries=SPOTLIGHT_INDUSTRIES,
        industry_counts=industry_counts,
        new_jobs=new_jobs,
        stats=stats,
        featured_companies=featured_companies,
        time_ago=time_ago,
        avatar_color=avatar_color,
    )


@public_bp.get("/jobs")
def jobs() -> str:
    q = request.args.get("q", "").strip()
    city = request.args.get("city", "").strip()
    industry = request.args.get("industry", "").strip()
    category = request.args.get("category", "").strip()
    sort = request.args.get("sort", "newest")
    view = request.args.get("view", "list")
    try:
        page = max(1, int(request.args.get("page", "1")))
    except ValueError:
        page = 1

    conditions = ["jp.is_active = 1"]
    params: list[Any] = []

    if q:
        like = f"%{q}%"
        conditions.append(
            "(LOWER(jp.job_title) LIKE LOWER(?) OR LOWER(c.business_name) LIKE LOWER(?)"
            " OR LOWER(COALESCE(jp.location,'')) LIKE LOWER(?)"
            " OR LOWER(COALESCE(jp.department,'')) LIKE LOWER(?))"
        )
        params.extend([like, like, like, like])

    if city:
        conditions.append("LOWER(c.city) = LOWER(?)")
        params.append(city)

    if industry:
        conditions.append("LOWER(c.industry) LIKE LOWER(?)")
        params.append(f"%{industry}%")

    if category:
        conditions.append("LOWER(COALESCE(jp.ai_job_category,'')) LIKE LOWER(?)")
        params.append(f"%{category}%")

    where = " AND ".join(conditions)
    order = "jp.first_seen_at DESC, jp.job_title ASC" if sort == "newest" else "jp.job_title ASC"
    offset = (page - 1) * PAGE_SIZE

    with _connect() as conn:
        count_row = conn.execute(
            f"SELECT COUNT(*) AS cnt FROM job_postings jp JOIN companies c ON c.id = jp.company_id WHERE {where}",
            params,
        ).fetchone()
        total = count_row["cnt"] if count_row else 0

        job_rows = conn.execute(
            f"""
            SELECT jp.*, c.business_name, c.city AS company_city, c.industry AS company_industry,
                   c.id AS company_db_id
            FROM job_postings jp
            JOIN companies c ON c.id = jp.company_id
            WHERE {where}
            ORDER BY {order}
            LIMIT {PAGE_SIZE} OFFSET {offset}
            """,
            params,
        ).fetchall()

        city_counts_rows = conn.execute(
            """
            SELECT c.city, COUNT(jp.id) AS cnt
            FROM job_postings jp JOIN companies c ON c.id = jp.company_id
            WHERE jp.is_active = 1 AND c.city IS NOT NULL AND c.city != ''
            GROUP BY c.city ORDER BY cnt DESC LIMIT 12
            """
        ).fetchall()

        cat_rows = conn.execute(
            """
            SELECT DISTINCT ai_job_category AS cat FROM job_postings
            WHERE is_active = 1 AND ai_job_category IS NOT NULL AND ai_job_category != ''
            ORDER BY ai_job_category
            """
        ).fetchall()

    total_pages = max(1, (total + PAGE_SIZE - 1) // PAGE_SIZE)

    return render_template(
        "public/jobs.html",
        job_rows=job_rows,
        q=q,
        city=city,
        industry=industry,
        category=category,
        sort=sort,
        view=view,
        page=page,
        total=total,
        total_pages=total_pages,
        city_counts_rows=city_counts_rows,
        cat_rows=cat_rows,
        skagit_cities=SKAGIT_CITIES,
        spotlight_industries=SPOTLIGHT_INDUSTRIES,
        time_ago=time_ago,
        avatar_color=avatar_color,
        parse_json_list=parse_json_list,
        week_ago=_week_ago_iso(),
    )


@public_bp.get("/companies")
def companies() -> str:
    city = request.args.get("city", "").strip()
    industry = request.args.get("industry", "").strip()
    sort = request.args.get("sort", "jobs")

    conditions: list[str] = []
    params: list[Any] = []

    if city:
        conditions.append("LOWER(c.city) = LOWER(?)")
        params.append(city)
    if industry:
        conditions.append("LOWER(c.industry) LIKE LOWER(?)")
        params.append(f"%{industry}%")

    where = ("WHERE " + " AND ".join(conditions)) if conditions else ""
    order = "active_job_count DESC, c.business_name ASC" if sort == "jobs" else "c.business_name ASC"

    with _connect() as conn:
        company_rows = conn.execute(
            f"""
            SELECT c.*, COUNT(jp.id) AS active_job_count
            FROM companies c
            LEFT JOIN job_postings jp ON jp.company_id = c.id AND jp.is_active = 1
            {where}
            GROUP BY c.id
            ORDER BY {order}
            """,
            params,
        ).fetchall()

        city_counts_rows = conn.execute(
            """
            SELECT c.city, COUNT(jp.id) AS cnt
            FROM job_postings jp JOIN companies c ON c.id = jp.company_id
            WHERE jp.is_active = 1 AND c.city IS NOT NULL AND c.city != ''
            GROUP BY c.city ORDER BY cnt DESC LIMIT 12
            """
        ).fetchall()

        industry_rows = conn.execute(
            """
            SELECT c.industry, COUNT(jp.id) AS cnt
            FROM job_postings jp JOIN companies c ON c.id = jp.company_id
            WHERE jp.is_active = 1 AND c.industry IS NOT NULL
            GROUP BY c.industry ORDER BY cnt DESC
            """
        ).fetchall()

    return render_template(
        "public/companies.html",
        company_rows=company_rows,
        city=city,
        industry=industry,
        sort=sort,
        city_counts_rows=city_counts_rows,
        industry_rows=industry_rows,
        avatar_color=avatar_color,
    )


@public_bp.get("/companies/<int:company_id>")
def company(company_id: int) -> str:
    with _connect() as conn:
        company_row = conn.execute(
            "SELECT * FROM companies WHERE id = ?",
            (company_id,),
        ).fetchone()

        if company_row is None:
            return render_template("public/404.html"), 404

        job_rows = conn.execute(
            """
            SELECT * FROM job_postings
            WHERE company_id = ? AND is_active = 1
            ORDER BY is_featured DESC, first_seen_at DESC
            """,
            (company_id,),
        ).fetchall()

    return render_template(
        "public/company.html",
        company=company_row,
        job_rows=job_rows,
        time_ago=time_ago,
        avatar_color=avatar_color,
        parse_json_list=parse_json_list,
        week_ago=_week_ago_iso(),
    )


@public_bp.get("/api/autocomplete")
def autocomplete():
    q = request.args.get("q", "").strip()
    if len(q) < 2:
        return jsonify([])

    like = f"%{q}%"
    results: list[dict] = []

    with _connect() as conn:
        job_rows = conn.execute(
            """
            SELECT DISTINCT jp.job_title, c.business_name
            FROM job_postings jp
            JOIN companies c ON c.id = jp.company_id
            WHERE jp.is_active = 1 AND LOWER(jp.job_title) LIKE LOWER(?)
            ORDER BY jp.job_title
            LIMIT 7
            """,
            (like,),
        ).fetchall()

        company_rows = conn.execute(
            """
            SELECT business_name, industry, city
            FROM companies
            WHERE LOWER(business_name) LIKE LOWER(?)
            ORDER BY business_name
            LIMIT 3
            """,
            (like,),
        ).fetchall()

    for row in job_rows:
        results.append({"type": "job", "label": row["job_title"], "sub": row["business_name"]})
    for row in company_rows:
        sub_parts = [p for p in [row["industry"], row["city"]] if p]
        results.append({"type": "company", "label": row["business_name"], "sub": " · ".join(sub_parts)})

    return jsonify(results)


@public_bp.post("/subscribe")
def subscribe():
    if request.is_json:
        data = request.get_json(silent=True) or {}
        email = (data.get("email") or "").strip().lower()
        cities = data.get("cities", [])
        categories = data.get("categories", [])
    else:
        email = (request.form.get("email") or "").strip().lower()
        cities = request.form.getlist("cities")
        categories = request.form.getlist("categories")

    if not email or not re.match(r"^[^@\s]+@[^@\s]+\.[^@\s]+$", email):
        return jsonify({"error": "Please enter a valid email address."}), 400

    from job_crawler import now_iso
    timestamp = now_iso()

    try:
        with _connect() as conn:
            existing = conn.execute(
                "SELECT id FROM job_alerts WHERE email = ?", (email,)
            ).fetchone()
            if existing:
                conn.execute(
                    "UPDATE job_alerts SET cities_json = ?, categories_json = ?, is_active = 1 WHERE email = ?",
                    (json.dumps(cities), json.dumps(categories), email),
                )
                return jsonify({"ok": True, "updated": True})
            conn.execute(
                "INSERT INTO job_alerts (email, cities_json, categories_json, is_active, created_at) VALUES (?, ?, ?, 1, ?)",
                (email, json.dumps(cities), json.dumps(categories), timestamp),
            )
        return jsonify({"ok": True})
    except Exception as exc:
        return jsonify({"error": "Could not save your subscription. Please try again."}), 500
