from __future__ import annotations

from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, HttpUrl


class PortfolioHolding(BaseModel):
    security_name: str
    sector: str | None = None
    weight: Decimal = Field(ge=0, le=100)
    as_of_date: date


class CanonicalSchemeRecord(BaseModel):
    model_config = ConfigDict(extra="forbid")

    scheme_id: str
    scheme_name: str
    amc_name: str
    category: str
    subcategory: str | None = None
    expense_ratio: Decimal | None = Field(default=None, ge=0, le=100)
    exit_load: str | None = None
    min_sip: Decimal | None = Field(default=None, ge=0)
    lock_in_period_days: int | None = Field(default=None, ge=0)
    riskometer: Literal["Low", "Moderately Low", "Moderate", "Moderately High", "High", "Very High"] | None = None
    benchmark: str | None = None
    nav_value: Decimal | None = Field(default=None, ge=0)
    nav_date: date | None = None
    aum_value: Decimal | None = Field(default=None, ge=0)
    aum_date: date | None = None
    fund_managers: list[str] = Field(default_factory=list)
    portfolio_holdings: list[PortfolioHolding] = Field(default_factory=list)
    source_url: HttpUrl
    source_timestamp: datetime
    ingestion_run_id: str
    version: str
    quality_flags: list[str] = Field(default_factory=list)

    @staticmethod
    def now_utc() -> datetime:
        return datetime.now(tz=timezone.utc)
