from __future__ import annotations

from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation
import re
from typing import Any

from mf_rag.models import CanonicalSchemeRecord

RISKOMETER_CANONICAL_MAP = {
    "low": "Low",
    "moderately low": "Moderately Low",
    "moderate": "Moderate",
    "moderately high": "Moderately High",
    "high": "High",
    "very high": "Very High",
}


def _to_decimal(value: Any) -> Decimal | None:
    if value is None or value == "":
        return None
    if isinstance(value, (int, float, Decimal)):
        return Decimal(str(value))
    if isinstance(value, str):
        cleaned = value.strip().replace("₹", "").replace("Rs.", "").replace("INR", "")
        cleaned = cleaned.replace("%", "").replace(",", "").strip()
        if not cleaned:
            return None
        # Handle common Indian units for AUM/fund size.
        mult = Decimal("1")
        lowered = cleaned.lower()
        if lowered.endswith("cr") or lowered.endswith("crore"):
            cleaned = re.sub(r"(cr|crore)\s*$", "", lowered).strip()
            mult = Decimal("10000000")  # 1 crore INR
        elif lowered.endswith("l") or lowered.endswith("lakh"):
            cleaned = re.sub(r"(l|lakh)\s*$", "", lowered).strip()
            mult = Decimal("100000")  # 1 lakh INR
        try:
            return Decimal(cleaned) * mult
        except InvalidOperation:
            return None
    return None


def _to_date(value: Any) -> date | None:
    if value is None or value == "":
        return None
    if isinstance(value, date):
        return value
    if isinstance(value, str):
        text = value.strip()
        # ISO format first
        try:
            return datetime.fromisoformat(text).date()
        except ValueError:
            pass
        # Common scraped format: "08 Apr 2026"
        for fmt in ("%d %b %Y", "%d %B %Y", "%d-%m-%Y", "%d/%m/%Y"):
            try:
                return datetime.strptime(text, fmt).date()
            except ValueError:
                continue
    return None


def _normalize_riskometer(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip().lower()
    return RISKOMETER_CANONICAL_MAP.get(text)


def _age_days(value: date | None, reference_date: date) -> int | None:
    if value is None:
        return None
    return (reference_date - value).days


def _normalize_holdings(raw_holdings: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for h in raw_holdings:
        security_name = str(h.get("security_name") or "").strip()
        if not security_name:
            continue
        weight = _to_decimal(h.get("weight"))
        as_of = _to_date(h.get("as_of_date"))
        if weight is None or as_of is None:
            continue
        out.append(
            {
                "security_name": security_name,
                "sector": h.get("sector"),
                "weight": weight,
                "as_of_date": as_of,
            }
        )
    return out


def normalize_scheme_record(raw: dict[str, Any], ingestion_run_id: str, version: str) -> CanonicalSchemeRecord:
    quality_flags: list[str] = []

    category = str(raw.get("category") or "").strip()
    subcategory = raw.get("subcategory")
    subcategory_str = str(subcategory).strip() if subcategory else None

    lock_in_period_days = raw.get("lock_in_period_days")
    if isinstance(lock_in_period_days, str) and lock_in_period_days.strip().isdigit():
        lock_in_period_days = int(lock_in_period_days.strip())

    # ELSS business rule: enforce a 3-year lock-in if missing.
    if subcategory_str and subcategory_str.lower() == "elss" and not lock_in_period_days:
        lock_in_period_days = 1095
        quality_flags.append("derived_elss_lock_in")

    riskometer = _normalize_riskometer(raw.get("riskometer"))
    if raw.get("riskometer") and riskometer is None:
        quality_flags.append("anomaly_flag:riskometer_unrecognized")

    nav_date = _to_date(raw.get("nav_date"))
    aum_date = _to_date(raw.get("aum_date"))
    today = datetime.now(tz=timezone.utc).date()
    nav_age = _age_days(nav_date, today)
    aum_age = _age_days(aum_date, today)

    if nav_age is None:
        quality_flags.append("missing_field:nav_date")
    elif nav_age > 3:
        quality_flags.append("stale_field:nav_date")
    if aum_age is None:
        quality_flags.append("missing_field:aum_date")
    elif aum_age > 40:
        quality_flags.append("stale_field:aum_date")

    if raw.get("expense_ratio") in (None, ""):
        quality_flags.append("missing_field:expense_ratio")
    if raw.get("min_sip") in (None, ""):
        quality_flags.append("missing_field:min_sip")

    return CanonicalSchemeRecord(
        scheme_id=str(raw["scheme_id"]),
        scheme_name=str(raw["scheme_name"]),
        amc_name=str(raw["amc_name"]),
        category=category,
        subcategory=subcategory_str,
        expense_ratio=_to_decimal(raw.get("expense_ratio")),
        exit_load=raw.get("exit_load"),
        min_sip=_to_decimal(raw.get("min_sip")),
        lock_in_period_days=lock_in_period_days,
        riskometer=riskometer,  # type: ignore[arg-type]
        benchmark=raw.get("benchmark"),
        nav_value=_to_decimal(raw.get("nav_value")),
        nav_date=nav_date,
        aum_value=_to_decimal(raw.get("aum_value")),
        aum_date=aum_date,
        fund_managers=list(raw.get("fund_managers", [])),
        portfolio_holdings=_normalize_holdings(list(raw.get("portfolio_holdings", []))),
        source_url=raw["source_url"],
        source_timestamp=raw["source_timestamp"],
        ingestion_run_id=ingestion_run_id,
        version=version,
        quality_flags=quality_flags,
    )
