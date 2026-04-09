from __future__ import annotations

import json
import os
from pathlib import Path
import re
import hashlib
from typing import Any
from urllib import request
from datetime import datetime, timezone
from bs4 import BeautifulSoup

from mf_rag.ingestion.browser_fetch import fetch_rendered_html

SELECTED_GROWW_FUND_URLS = [
    "https://groww.in/mutual-funds/axis-long-term-equity-fund-direct-growth",
    "https://groww.in/mutual-funds/parag-parikh-long-term-value-fund-direct-growth",
    "https://groww.in/mutual-funds/sbi-small-midcap-fund-direct-growth",
    "https://groww.in/mutual-funds/icici-prudential-focused-bluechip-equity-fund-direct-growth",
    "https://groww.in/mutual-funds/mirae-asset-large-cap-fund-direct-growth",
    "https://groww.in/mutual-funds/kotak-emerging-equity-scheme-direct-growth",
    "https://groww.in/mutual-funds/hdfc-balanced-advantage-fund-direct-growth",
    "https://groww.in/mutual-funds/quant-small-cap-fund-direct-plan-growth",
    "https://groww.in/mutual-funds/uti-nifty-fund-direct-growth",
    "https://groww.in/mutual-funds/birla-sun-life-tax-relief-96-direct-growth",
]


class GrowwClient:
    @staticmethod
    def _stable_scheme_id(seed: str) -> str:
        return "groww_" + hashlib.sha1(seed.encode("utf-8")).hexdigest()[:12]

    """
    Groww ingestion client.
    Supports live scrape with deterministic local fallback.
    """

    def __init__(self, sample_file: Path, use_live: bool = False, selected_urls: list[str] | None = None) -> None:
        self.sample_file = sample_file
        self.use_live = use_live
        self.master_url = "https://groww.in/mutual-funds"
        self.selected_urls = selected_urls or SELECTED_GROWW_FUND_URLS

    def _fetch_sample(self) -> list[dict[str, Any]]:
        with self.sample_file.open("r", encoding="utf-8") as f:
            payload = json.load(f)
        return payload["schemes"]

    @staticmethod
    def _extract_next_data_json(html: str) -> dict[str, Any] | None:
        match = re.search(
            r'<script id="__NEXT_DATA__" type="application/json">\s*(\{.*?\})\s*</script>',
            html,
            re.DOTALL,
        )
        if not match:
            return None
        try:
            return json.loads(match.group(1))
        except json.JSONDecodeError:
            return None

    @staticmethod
    def _iter_dicts(node: Any):
        if isinstance(node, dict):
            yield node
            for value in node.values():
                yield from GrowwClient._iter_dicts(value)
        elif isinstance(node, list):
            for item in node:
                yield from GrowwClient._iter_dicts(item)

    @staticmethod
    def _normalize_live_scheme(raw: dict[str, Any]) -> dict[str, Any]:
        # Groww payload fields vary by page version; this maps commonly seen keys.
        scheme_name = raw.get("scheme_name") or raw.get("name") or raw.get("fundName") or ""
        scheme_id = raw.get("scheme_code") or raw.get("schemeId") or raw.get("id") or raw.get("search_id") or ""
        amc_name = raw.get("amc_name") or raw.get("fundHouse") or raw.get("amc") or "Unknown AMC"
        category = raw.get("category") or raw.get("fundCategory") or "Unknown"
        subcategory = raw.get("sub_category") or raw.get("subCategory")

        return {
            "scheme_id": str(scheme_id) or GrowwClient._stable_scheme_id(str(scheme_name)),
            "scheme_name": str(scheme_name).strip(),
            "amc_name": str(amc_name).strip(),
            "category": str(category).strip(),
            "subcategory": str(subcategory).strip() if subcategory else None,
            "expense_ratio": raw.get("expense_ratio") or raw.get("expenseRatio") or raw.get("expRatio"),
            "exit_load": raw.get("exit_load") or raw.get("exitLoad"),
            "min_sip": raw.get("minimum_sip") or raw.get("minSip") or raw.get("minSIP") or raw.get("sipMin"),
            "lock_in_period_days": raw.get("lock_in_period_days") or raw.get("lockIn") or raw.get("lockinDays"),
            "riskometer": raw.get("riskometer") or raw.get("risk") or raw.get("riskLevel"),
            "benchmark": raw.get("benchmark") or raw.get("benchmarkName"),
            "nav_value": raw.get("nav") or raw.get("nav_value") or raw.get("navValue"),
            "nav_date": raw.get("nav_date") or raw.get("navDate") or raw.get("navAsOn"),
            "aum_value": raw.get("aum") or raw.get("aum_value") or raw.get("aumValue"),
            "aum_date": raw.get("aum_date") or raw.get("aumDate") or raw.get("aumAsOn"),
            "fund_managers": raw.get("fund_managers") or raw.get("fundManagers") or raw.get("managers") or [],
            "portfolio_holdings": raw.get("portfolio_holdings") or raw.get("holdings") or [],
            "source_url": raw.get("source_url") or "https://groww.in/mutual-funds",
            "source_timestamp": raw.get("source_timestamp") or "2026-04-08T00:00:00+00:00",
            "version": "v_live",
        }

    @staticmethod
    def _tokenize(text: str) -> set[str]:
        return {t for t in re.sub(r"[^a-z0-9 ]+", " ", text.lower()).split() if len(t) > 1}

    @staticmethod
    def _candidate_name(node: dict[str, Any]) -> str:
        return str(node.get("scheme_name") or node.get("fundName") or node.get("name") or "").strip()

    @staticmethod
    def _candidate_id(node: dict[str, Any]) -> str:
        return str(node.get("scheme_code") or node.get("schemeId") or node.get("id") or node.get("search_id") or "").strip()

    @staticmethod
    def _score_candidate(node: dict[str, Any], expected_tokens: set[str]) -> int:
        name = GrowwClient._candidate_name(node)
        name_tokens = GrowwClient._tokenize(name)
        overlap = len(expected_tokens & name_tokens)

        # How "fact-rich" is this dict?
        keys = set(node.keys())
        richness_keys = {
            "nav",
            "navValue",
            "nav_date",
            "navDate",
            "expense_ratio",
            "expenseRatio",
            "exit_load",
            "exitLoad",
            "aum",
            "aumValue",
            "aumDate",
            "benchmark",
            "benchmarkName",
            "riskometer",
            "risk",
            "minSip",
            "minimum_sip",
        }
        richness = len(keys & richness_keys)

        has_id = 1 if GrowwClient._candidate_id(node) else 0
        return overlap * 10 + richness * 3 + has_id

    def _pick_best_scheme_node(self, next_data: dict[str, Any], expected_name: str) -> dict[str, Any] | None:
        expected_tokens = self._tokenize(expected_name)
        best_node: dict[str, Any] | None = None
        best_score = 0
        for node in self._iter_dicts(next_data):
            if not isinstance(node, dict):
                continue
            if not self._candidate_name(node):
                continue
            score = self._score_candidate(node, expected_tokens)
            if score > best_score:
                best_node = node
                best_score = score
        return best_node

    def _augment_from_related_nodes(self, next_data: dict[str, Any], base: dict[str, Any]) -> dict[str, Any]:
        """
        Sometimes facts are stored in a separate dict than the scheme metadata.
        If we have a scheme id, search for other dicts with same id and fill missing fields.
        """
        scheme_id = str(base.get("schemeId") or base.get("scheme_code") or base.get("id") or base.get("search_id") or "")
        if not scheme_id:
            return base

        merged = dict(base)
        for node in self._iter_dicts(next_data):
            if not isinstance(node, dict):
                continue
            node_id = self._candidate_id(node)
            if not node_id or node_id != scheme_id:
                continue
            for k, v in node.items():
                if merged.get(k) in (None, "", []) and v not in (None, "", []):
                    merged[k] = v
        return merged

    def _fetch_live_selected(self) -> list[dict[str, Any]]:
        funds: list[dict[str, Any]] = []
        for url in self.selected_urls:
            html = ""
            use_cdp = bool(os.getenv("MF_CDP_ENDPOINT", "").strip())
            if not use_cdp:
                try:
                    req = request.Request(
                        url,
                        headers={
                            "Accept": "text/html,application/xhtml+xml",
                            "User-Agent": "mf-rag-chatbot/0.1 (+python urllib)",
                        },
                        method="GET",
                    )
                    with request.urlopen(req, timeout=30) as resp:
                        html = resp.read().decode("utf-8", "replace")
                except Exception:
                    html = ""

            if not html:
                # Fallback to browser-rendered HTML (handles bot-blocking / client-side rendering).
                try:
                    html = fetch_rendered_html(url)
                except Exception:
                    html = ""

            next_data = self._extract_next_data_json(html)
            if not next_data:
                slug = url.rstrip("/").split("/")[-1].replace("-", " ").title()
                extracted = self._extract_facts_from_rendered_html(html) if html else {}
                # If we still got a 404-like page, preserve scheme_name as slug for matching,
                # but keep the source URL for citations.
                if extracted.get("scheme_name", "").lower().startswith("404"):
                    extracted["scheme_name"] = slug
                funds.append(
                    {
                        "scheme_id": self._stable_scheme_id(url),
                        "scheme_name": extracted.get("scheme_name") or slug,
                        "amc_name": "Unknown AMC",
                        "category": "Unknown",
                        "subcategory": None,
                        "expense_ratio": extracted.get("expense_ratio"),
                        "exit_load": extracted.get("exit_load"),
                        "min_sip": extracted.get("min_sip"),
                        "lock_in_period_days": None,
                        "riskometer": extracted.get("riskometer"),
                        "benchmark": extracted.get("benchmark"),
                        "nav_value": extracted.get("nav_value"),
                        "nav_date": extracted.get("nav_date"),
                        "aum_value": extracted.get("aum_value"),
                        "aum_date": extracted.get("aum_date"),
                        "fund_managers": [],
                        "portfolio_holdings": extracted.get("portfolio_holdings", []),
                        "source_url": url,
                        "source_timestamp": datetime.now(tz=timezone.utc).isoformat(),
                        "version": "v_live",
                    }
                )
                continue

            expected_name = url.rstrip("/").split("/")[-1].replace("-", " ").strip()
            best_node = self._pick_best_scheme_node(next_data, expected_name=expected_name)
            if best_node is None:
                slug = expected_name.replace("-", " ").title()
                picked = self._normalize_live_scheme({"scheme_name": slug, "scheme_code": self._stable_scheme_id(url)})
                picked["source_url"] = url
                picked["source_timestamp"] = datetime.now(tz=timezone.utc).isoformat()
                funds.append(picked)
                continue

            best_node = self._augment_from_related_nodes(next_data, best_node)
            picked = self._normalize_live_scheme(best_node)
            picked["source_url"] = url
            picked["source_timestamp"] = datetime.now(tz=timezone.utc).isoformat()
            # Always attempt rendered-text enrichment for missing fields.
            if html:
                extracted = self._extract_facts_from_rendered_html(html)
                for field in (
                    "scheme_name",
                    "expense_ratio",
                    "exit_load",
                    "min_sip",
                    "riskometer",
                    "benchmark",
                    "nav_value",
                    "nav_date",
                    "aum_value",
                    "aum_date",
                    "portfolio_holdings",
                ):
                    if picked.get(field) in (None, "", []):
                        if extracted.get(field) not in (None, "", []):
                            picked[field] = extracted[field]
                if not picked.get("fund_managers") and extracted.get("fund_managers"):
                    picked["fund_managers"] = extracted["fund_managers"]

            funds.append(picked)
        return funds

    @staticmethod
    def _extract_facts_from_rendered_html(html: str) -> dict[str, Any]:
        soup = BeautifulSoup(html, "html.parser")
        text = re.sub(r"\s+", " ", soup.get_text(" ", strip=True))

        out: dict[str, Any] = {}

        # Scheme name (page title or H1-like)
        title = soup.title.string.strip() if soup.title and soup.title.string else None
        if title:
            out["scheme_name"] = title.replace(" - Groww", "").strip()

        def pick(patterns: list[str]) -> str | None:
            for pattern in patterns:
                m = re.search(pattern, text, flags=re.IGNORECASE)
                if m:
                    return m.group(1).strip()
            return None

        out["expense_ratio"] = pick(
            [
                r"Expense ratio\s*[:\-]?\s*([0-9]+(?:\.[0-9]+)?)\s*%",
                r"Expense Ratio\s*([0-9]+(?:\.[0-9]+)?)\s*%",
            ]
        )
        out["min_sip"] = pick(
            [
                r"Minimum SIP Investment is set to ₹\s*([0-9][0-9,]*)",
                r"Minimum SIP amount\s*[:\-]?\s*₹?\s*([0-9][0-9,]*)",
                r"Minimum SIP\s*[:\-]?\s*₹?\s*([0-9][0-9,]*)",
                r"Min(?:imum)?\s*SIP\s*[:\-]?\s*₹?\s*([0-9][0-9,]*)",
                r"SIP\s*starts?\s*at\s*₹?\s*([0-9][0-9,]*)",
            ]
        )
        out["nav_value"] = pick(
            [
                r"Latest NAV as of\s*[0-9]{1,2}\s+[A-Za-z]{3,}\s+[0-9]{4}\s*is\s*₹\s*([0-9,]+\.[0-9]+)",
                r"NAV of .*? is ₹\s*([0-9,]+\.[0-9]+)\s*as of",
                r"Current NAV[^₹0-9]{0,40}₹\s*([0-9]{1,4}\.[0-9]{1,4})",
                r"NAV as of[^₹0-9]{0,40}₹\s*([0-9]{1,4}\.[0-9]{1,4})",
                r"\bNAV\b\s*[:\-]?\s*₹\s*([0-9]{1,4}\.[0-9]{1,4})",
                r"\bNAV\b\s*[:\-]?\s*([0-9]{1,4}\.[0-9]{1,4})",
            ]
        )
        out["nav_date"] = pick(
            [
                r"Latest NAV as of\s*([0-9]{1,2}\s+[A-Za-z]{3,}\s+[0-9]{4})",
                r"NAV of .*? as of\s*([0-9]{1,2}\s+[A-Za-z]{3,}\s+[0-9]{4})",
                r"\bNAV\b.*?(?:As on|as of)\s*([0-9]{1,2}\s+[A-Za-z]{3,}\s+[0-9]{4})",
                r"NAV as of\s*([0-9]{1,2}\s+[A-Za-z]{3,}\s+[0-9]{4})",
            ]
        )
        out["aum_value"] = pick(
            [
                r"AUM of .*? is ₹\s*([0-9][0-9,\.]*\s*(?:Cr|Crore|L|Lakh)?)\s*as of",
                r"(?:Fund size|AUM)\s*[:\-]?\s*₹\s*([0-9][0-9,\.]*\s*(?:Cr|Crore|L|Lakh)?)",
                r"(?:Fund size|AUM)\s*[:\-]?\s*([0-9][0-9,\.]*\s*(?:Cr|Crore|L|Lakh)?)",
            ]
        )
        out["aum_date"] = pick(
            [
                r"AUM of .*? as of\s*([0-9]{1,2}\s+[A-Za-z]{3,}\s+[0-9]{4})",
                r"(?:Fund size|AUM).*?(?:As on|as of)\s*([0-9]{1,2}\s+[A-Za-z]{3,}\s+[0-9]{4})",
            ]
        )
        benchmark = pick([r"Benchmark\s*[:\-]?\s*([A-Za-z0-9 \-\(\)\/&]+)"])
        if benchmark:
            # Trim common trailing text from page sections.
            benchmark = re.split(r"Scheme Information Document|Fund house|Rank \(", benchmark, maxsplit=1)[0].strip()
        out["benchmark"] = benchmark
        out["riskometer"] = pick([r"Risk\s*(Low|Moderately Low|Moderate|Moderately High|High|Very High)"])
        exit_load = pick([r"Exit Load\s*[:\-]?\s*([A-Za-z0-9 \.\-\/%]+)"])
        if exit_load and "fee payable" in exit_load.lower():
            exit_load = None
        out["exit_load"] = exit_load
        manager = pick([r"Fund manager\s*[:\-]?\s*([A-Za-z\.\- ]{3,60})"])
        if manager:
            out["fund_managers"] = [manager.strip()]

        # Parse top holdings from the holdings table rows.
        holdings: list[dict[str, Any]] = []
        as_of_date = out.get("nav_date") or out.get("aum_date") or datetime.now(tz=timezone.utc).date().isoformat()
        for row in soup.select('[class*="hld236Row"]'):
            text_row = " ".join(row.stripped_strings)
            m = re.search(r"([A-Za-z0-9&\.\- ]+)\s+([0-9]+(?:\.[0-9]+)?)%", text_row)
            if not m:
                continue
            name = m.group(1).strip()
            if name.lower().startswith("top 10 holdings"):
                continue
            holdings.append(
                {
                    "security_name": name,
                    "sector": None,
                    "weight": m.group(2),
                    "as_of_date": as_of_date,
                }
            )
            if len(holdings) >= 10:
                break
        if holdings:
            out["portfolio_holdings"] = holdings

        # Remove empty entries
        return {k: v for k, v in out.items() if v not in (None, "")}

    def _fetch_live(self) -> list[dict[str, Any]]:
        req = request.Request(
            self.master_url,
            headers={
                "Accept": "text/html,application/xhtml+xml",
                "User-Agent": "mf-rag-chatbot/0.1 (+python urllib)",
            },
            method="GET",
        )
        with request.urlopen(req, timeout=30) as resp:
            html = resp.read().decode("utf-8", "replace")

        next_data = self._extract_next_data_json(html)
        if not next_data:
            return []

        candidates: list[dict[str, Any]] = []
        for node in self._iter_dicts(next_data):
            if not isinstance(node, dict):
                continue
            # Heuristic: treat dict as scheme-like if a scheme/fund name is present.
            if any(k in node for k in ("scheme_name", "fundName", "name")) and any(
                k in node for k in ("scheme_code", "schemeId", "id", "search_id")
            ):
                normalized = self._normalize_live_scheme(node)
                if normalized["scheme_name"]:
                    candidates.append(normalized)

        # Deduplicate by scheme_id
        dedup: dict[str, dict[str, Any]] = {}
        for item in candidates:
            dedup[item["scheme_id"]] = item
        return list(dedup.values())

    def fetch_scheme_master(self) -> list[dict[str, Any]]:
        if self.use_live:
            try:
                live = self._fetch_live_selected()
                if live:
                    return live
                live = self._fetch_live()
                if live:
                    return live
            except Exception:
                # Deterministic fallback keeps pipeline up if source blocks or changes.
                pass
        return self._fetch_sample()
