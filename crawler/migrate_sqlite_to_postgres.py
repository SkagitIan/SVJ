from __future__ import annotations

import argparse
import sqlite3
from pathlib import Path
from typing import Any

import psycopg
from dotenv import load_dotenv
from psycopg import sql

import db
from admin_app import ensure_admin_tables
from job_crawler import init_db


TABLES = [
    "companies",
    "app_settings",
    "job_sources",
    "job_postings",
    "crawl_errors",
    "discovered_businesses",
    "ai_company_enrichment_batches",
    "ai_company_enrichment_requests",
    "ai_job_enrichment_batches",
    "ai_job_enrichment_requests",
    "admin_tasks",
    "discovery_industries",
]


def sqlite_tables(conn: sqlite3.Connection) -> set[str]:
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'").fetchall()
    return {row[0] for row in rows}


def sqlite_columns(conn: sqlite3.Connection, table: str) -> list[str]:
    return [row[1] for row in conn.execute(f"PRAGMA table_info({table})")]


def postgres_columns(conn: psycopg.Connection[Any], table: str) -> list[str]:
    rows = conn.execute(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = %s
        ORDER BY ordinal_position
        """,
        (table,),
    ).fetchall()
    return [row[0] for row in rows]


def table_count(conn: psycopg.Connection[Any], table: str) -> int:
    return int(conn.execute(sql.SQL("SELECT COUNT(*) FROM {}").format(sql.Identifier(table))).fetchone()[0])


def reset_identity(conn: psycopg.Connection[Any], table: str) -> None:
    conn.execute(
        sql.SQL(
            """
            SELECT setval(
                pg_get_serial_sequence(%s, 'id'),
                GREATEST(COALESCE((SELECT MAX(id) FROM {}), 0) + 1, 1),
                false
            )
            """
        ).format(sql.Identifier(table)),
        (table,),
    )


def migrate(sqlite_path: Path, replace: bool) -> dict[str, int]:
    load_dotenv(Path(__file__).with_name(".env"))
    if not sqlite_path.exists():
        raise FileNotFoundError(f"SQLite database not found: {sqlite_path}")

    init_db(Path("postgres"))
    with db.connect() as pg_schema:
        ensure_admin_tables(pg_schema)

    copied: dict[str, int] = {}
    with sqlite3.connect(sqlite_path) as source, psycopg.connect(db.database_url()) as target:
        source.row_factory = sqlite3.Row
        source.execute("PRAGMA wal_checkpoint(FULL)")
        existing_sqlite_tables = sqlite_tables(source)

        non_empty = {table: table_count(target, table) for table in TABLES if table_count(target, table)}
        if non_empty and not replace:
            details = ", ".join(f"{table}={count}" for table, count in sorted(non_empty.items()))
            raise RuntimeError(f"Postgres already has data ({details}). Re-run with --replace to overwrite it.")

        if replace:
            target.execute(
                sql.SQL("TRUNCATE {} RESTART IDENTITY CASCADE").format(
                    sql.SQL(", ").join(sql.Identifier(table) for table in TABLES)
                )
            )

        for table in TABLES:
            if table not in existing_sqlite_tables:
                copied[table] = 0
                continue
            source_columns = sqlite_columns(source, table)
            target_columns = postgres_columns(target, table)
            columns = [column for column in source_columns if column in target_columns]
            if not columns:
                copied[table] = 0
                continue

            rows = source.execute(
                f"SELECT {', '.join(columns)} FROM {table}"
            ).fetchall()
            if rows:
                payload = [tuple(row[column] for column in columns) for row in rows]
                company_ids = {row[0] for row in source.execute("SELECT id FROM companies").fetchall()}
                job_ids = {row[0] for row in source.execute("SELECT id FROM job_postings").fetchall()} if "job_postings" in existing_sqlite_tables else set()
                company_batch_ids = {row[0] for row in source.execute("SELECT id FROM ai_company_enrichment_batches").fetchall()} if "ai_company_enrichment_batches" in existing_sqlite_tables else set()
                job_batch_ids = {row[0] for row in source.execute("SELECT id FROM ai_job_enrichment_batches").fetchall()} if "ai_job_enrichment_batches" in existing_sqlite_tables else set()
                if table in {"job_sources", "job_postings"} and "company_id" in columns:
                    company_index = columns.index("company_id")
                    payload = [values for values in payload if values[company_index] in company_ids]
                if table == "crawl_errors" and "company_id" in columns:
                    company_index = columns.index("company_id")
                    payload = [
                        (*values[:company_index], values[company_index] if values[company_index] in company_ids else None, *values[company_index + 1 :])
                        for values in payload
                    ]
                if table == "discovered_businesses" and "imported_company_id" in columns:
                    id_index = columns.index("imported_company_id")
                    payload = [
                        (*values[:id_index], values[id_index] if values[id_index] in company_ids else None, *values[id_index + 1 :])
                        for values in payload
                    ]
                if table == "ai_company_enrichment_requests":
                    batch_index = columns.index("batch_id")
                    company_index = columns.index("company_id")
                    payload = [
                        values
                        for values in payload
                        if values[batch_index] in company_batch_ids and values[company_index] in company_ids
                    ]
                if table == "ai_job_enrichment_requests":
                    batch_index = columns.index("batch_id")
                    job_index = columns.index("job_id")
                    payload = [
                        values
                        for values in payload
                        if values[batch_index] in job_batch_ids and values[job_index] in job_ids
                    ]
                insert_sql = sql.SQL("INSERT INTO {} ({}) VALUES ({})").format(
                    sql.Identifier(table),
                    sql.SQL(", ").join(sql.Identifier(column) for column in columns),
                    sql.SQL(", ").join(sql.Placeholder() for _ in columns),
                )
                with target.cursor() as cursor:
                    cursor.executemany(insert_sql, payload)
            copied[table] = len(payload) if rows else 0
            if "id" in columns:
                reset_identity(target, table)

    return copied


def main() -> None:
    parser = argparse.ArgumentParser(description="Copy crawler/jobs.sqlite into DATABASE_PUBLIC_URL Postgres.")
    parser.add_argument("--sqlite", type=Path, default=Path(__file__).with_name("jobs.sqlite"))
    parser.add_argument("--replace", action="store_true", help="Truncate Postgres crawler tables before importing.")
    args = parser.parse_args()

    copied = migrate(args.sqlite, args.replace)
    for table, count in copied.items():
        print(f"{table}: {count}")


if __name__ == "__main__":
    main()
