from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any


class StructuredStore:
    """
    Phase 4 structured truth layer.
    SQLite for local development; can be swapped for PostgreSQL.
    """

    def __init__(self, db_path: Path) -> None:
        self.db_path = db_path
        self.db_path.parent.mkdir(parents=True, exist_ok=True)

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def init_schema(self) -> None:
        with self._connect() as conn:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS schemes (
                    scheme_id TEXT PRIMARY KEY,
                    scheme_name TEXT NOT NULL,
                    amc_name TEXT NOT NULL,
                    category TEXT NOT NULL,
                    subcategory TEXT
                );

                CREATE TABLE IF NOT EXISTS scheme_metrics_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    scheme_id TEXT NOT NULL,
                    expense_ratio REAL,
                    exit_load TEXT,
                    min_sip REAL,
                    lock_in_period_days INTEGER,
                    riskometer TEXT,
                    benchmark TEXT,
                    nav_value REAL,
                    nav_date TEXT,
                    aum_value REAL,
                    aum_date TEXT,
                    source_url TEXT NOT NULL,
                    source_timestamp TEXT NOT NULL,
                    ingestion_run_id TEXT NOT NULL,
                    version TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS scheme_managers (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    scheme_id TEXT NOT NULL,
                    manager_name TEXT NOT NULL,
                    ingestion_run_id TEXT NOT NULL,
                    version TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS scheme_holdings (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    scheme_id TEXT NOT NULL,
                    security_name TEXT NOT NULL,
                    sector TEXT,
                    weight REAL NOT NULL,
                    as_of_date TEXT NOT NULL,
                    ingestion_run_id TEXT NOT NULL,
                    version TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS quality_flags (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    scheme_id TEXT NOT NULL,
                    flag TEXT NOT NULL,
                    ingestion_run_id TEXT NOT NULL,
                    version TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS ingestion_versions (
                    ingestion_run_id TEXT PRIMARY KEY,
                    version TEXT NOT NULL,
                    status TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );
                """
            )

    def upsert_curated_record(self, record: dict[str, Any]) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO schemes (scheme_id, scheme_name, amc_name, category, subcategory)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(scheme_id) DO UPDATE SET
                    scheme_name=excluded.scheme_name,
                    amc_name=excluded.amc_name,
                    category=excluded.category,
                    subcategory=excluded.subcategory
                """,
                (
                    record["scheme_id"],
                    record["scheme_name"],
                    record["amc_name"],
                    record["category"],
                    record.get("subcategory"),
                ),
            )

            conn.execute(
                """
                INSERT INTO scheme_metrics_history
                (scheme_id, expense_ratio, exit_load, min_sip, lock_in_period_days, riskometer, benchmark,
                 nav_value, nav_date, aum_value, aum_date, source_url, source_timestamp, ingestion_run_id, version)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    record["scheme_id"],
                    record.get("expense_ratio"),
                    record.get("exit_load"),
                    record.get("min_sip"),
                    record.get("lock_in_period_days"),
                    record.get("riskometer"),
                    record.get("benchmark"),
                    record.get("nav_value"),
                    record.get("nav_date"),
                    record.get("aum_value"),
                    record.get("aum_date"),
                    record["source_url"],
                    record["source_timestamp"],
                    record["ingestion_run_id"],
                    record["version"],
                ),
            )

            conn.execute(
                "DELETE FROM scheme_managers WHERE scheme_id=? AND ingestion_run_id=?",
                (record["scheme_id"], record["ingestion_run_id"]),
            )
            for manager in record.get("fund_managers", []):
                conn.execute(
                    """
                    INSERT INTO scheme_managers (scheme_id, manager_name, ingestion_run_id, version)
                    VALUES (?, ?, ?, ?)
                    """,
                    (record["scheme_id"], manager, record["ingestion_run_id"], record["version"]),
                )

            conn.execute(
                "DELETE FROM scheme_holdings WHERE scheme_id=? AND ingestion_run_id=?",
                (record["scheme_id"], record["ingestion_run_id"]),
            )
            for holding in record.get("portfolio_holdings", []):
                conn.execute(
                    """
                    INSERT INTO scheme_holdings
                    (scheme_id, security_name, sector, weight, as_of_date, ingestion_run_id, version)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        record["scheme_id"],
                        holding["security_name"],
                        holding.get("sector"),
                        holding["weight"],
                        holding["as_of_date"],
                        record["ingestion_run_id"],
                        record["version"],
                    ),
                )

            for flag in record.get("quality_flags", []):
                conn.execute(
                    """
                    INSERT INTO quality_flags (scheme_id, flag, ingestion_run_id, version)
                    VALUES (?, ?, ?, ?)
                    """,
                    (record["scheme_id"], flag, record["ingestion_run_id"], record["version"]),
                )

            conn.execute(
                """
                INSERT OR REPLACE INTO ingestion_versions (ingestion_run_id, version, status, created_at)
                VALUES (?, ?, ?, ?)
                """,
                (
                    record["ingestion_run_id"],
                    record["version"],
                    "active",
                    record["source_timestamp"],
                ),
            )

    def get_latest_scheme_facts(self, scheme_id: str) -> dict[str, Any] | None:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT s.scheme_id, s.scheme_name, s.amc_name, s.category, s.subcategory,
                       m.expense_ratio, m.exit_load, m.min_sip, m.lock_in_period_days,
                       m.riskometer, m.benchmark, m.nav_value, m.nav_date, m.aum_value, m.aum_date,
                       m.source_url, m.source_timestamp, m.ingestion_run_id, m.version
                FROM schemes s
                JOIN scheme_metrics_history m ON m.scheme_id = s.scheme_id
                WHERE s.scheme_id = ?
                ORDER BY m.id DESC
                LIMIT 1
                """,
                (scheme_id,),
            ).fetchone()
            return dict(row) if row else None

    def find_scheme_candidates(self, query_text: str, limit: int = 5) -> list[dict[str, Any]]:
        like = f"%{query_text.strip().lower()}%"
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT scheme_id, scheme_name, amc_name, category, subcategory
                FROM schemes
                WHERE lower(scheme_name) LIKE ?
                ORDER BY scheme_name ASC
                LIMIT ?
                """,
                (like, limit),
            ).fetchall()
            return [dict(r) for r in rows]

    def list_schemes(self) -> list[dict[str, Any]]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT scheme_id, scheme_name, amc_name, category, subcategory
                FROM schemes
                ORDER BY scheme_name ASC
                """
            ).fetchall()
            return [dict(r) for r in rows]

    def get_scheme_managers(self, scheme_id: str, ingestion_run_id: str) -> list[str]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT manager_name
                FROM scheme_managers
                WHERE scheme_id = ? AND ingestion_run_id = ?
                ORDER BY manager_name ASC
                """,
                (scheme_id, ingestion_run_id),
            ).fetchall()
            return [str(r["manager_name"]) for r in rows]

    def get_scheme_holdings(self, scheme_id: str, ingestion_run_id: str, limit: int = 10) -> list[dict[str, Any]]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT security_name, sector, weight, as_of_date
                FROM scheme_holdings
                WHERE scheme_id = ? AND ingestion_run_id = ?
                ORDER BY weight DESC, security_name ASC
                LIMIT ?
                """,
                (scheme_id, ingestion_run_id, limit),
            ).fetchall()
            return [dict(r) for r in rows]
