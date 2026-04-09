from __future__ import annotations

from dataclasses import dataclass
import re
from typing import Any


@dataclass
class VerificationResult:
    grounded: bool
    ungrounded_claims: list[str]


def verify_answer_grounding(answer: str, factual_context: dict[str, Any], required_fields: list[str]) -> VerificationResult:
    answer_l = answer.lower()
    ungrounded: list[str] = []

    def extract_numbers(text: str) -> list[float]:
        nums: list[float] = []
        for m in re.findall(r"[0-9][0-9,]*(?:\.[0-9]+)?", text):
            try:
                nums.append(float(m.replace(",", "")))
            except ValueError:
                continue
        return nums

    answer_nums = extract_numbers(answer)

    for field in required_fields:
        value = factual_context.get(field)
        if value in (None, "", []):
            ungrounded.append(field)
            continue
        if isinstance(value, list):
            # Holdings/managers: require at least one representative token in answer.
            if not value:
                ungrounded.append(field)
                continue
            if field == "portfolio_holdings":
                names: list[str] = []
                for item in value[:5]:
                    if isinstance(item, dict) and item.get("security_name"):
                        names.append(str(item["security_name"]).lower())
                if names and not any(n in answer_l for n in names):
                    ungrounded.append(field)
                continue
            item_tokens = [str(v).lower() for v in value[:5]]
            if item_tokens and not any(tok in answer_l for tok in item_tokens):
                ungrounded.append(field)
            continue
        if isinstance(value, (int, float)):
            val = float(value)
            if not any(abs(n - val) < 1e-6 for n in answer_nums):
                ungrounded.append(field)
            continue
        value_text = str(value).lower().strip()
        # Handle numeric text values with formatting differences (e.g., "1,000" vs "1000.0").
        value_nums = extract_numbers(value_text)
        if value_nums:
            val = value_nums[0]
            if not any(abs(n - val) < 1e-6 for n in answer_nums):
                ungrounded.append(field)
        elif value_text not in answer_l:
            ungrounded.append(field)

    return VerificationResult(grounded=len(ungrounded) == 0, ungrounded_claims=ungrounded)
