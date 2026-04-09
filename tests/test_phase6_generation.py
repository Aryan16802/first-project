from mf_rag.phases.phase6 import FALLBACK, generate_grounded_answer, verify_answer_grounding


class FakeLLM:
    def __init__(self, response: str) -> None:
        self.response = response

    def generate(self, system_prompt: str, user_prompt: str) -> str:
        return self.response


def test_verifier_accepts_answer_with_required_values() -> None:
    answer = "Expense ratio is 1.62. Source: https://groww.in/x. As of 2026-04-08T07:00:00+00:00."
    factual_context = {"expense_ratio": 1.62}
    result = verify_answer_grounding(answer, factual_context, ["expense_ratio"])
    assert result.grounded is True


def test_generator_blocks_ungrounded_answer() -> None:
    context_packet = {
        "required_fields": ["expense_ratio"],
        "factual_context": {"expense_ratio": 1.62},
        "citations": {"source_url": "https://groww.in/x", "source_timestamp": "2026-04-08T07:00:00+00:00"},
    }
    llm = FakeLLM("Expense ratio is 2.10.")
    out = generate_grounded_answer("What is expense ratio?", context_packet, llm)
    assert out["grounded"] is False
    assert out["answer"] == FALLBACK


def test_generator_returns_grounded_answer_with_citations() -> None:
    context_packet = {
        "required_fields": ["expense_ratio"],
        "factual_context": {"expense_ratio": 1.62},
        "citations": {"source_url": "https://groww.in/x", "source_timestamp": "2026-04-08T07:00:00+00:00"},
    }
    llm = FakeLLM("Expense ratio is 1.62. Source: https://groww.in/x. As of 2026-04-08T07:00:00+00:00.")
    out = generate_grounded_answer("What is expense ratio?", context_packet, llm)
    assert out["grounded"] is True
    assert out["reason"] == "ok"
    assert out["citations"]["source_url"] == "https://groww.in/x"


def test_verifier_accepts_holdings_when_answer_mentions_security() -> None:
    answer = "Top holding is HDFC Bank at 7.1%."
    factual_context = {"portfolio_holdings": [{"security_name": "HDFC Bank", "weight": 7.1}]}
    result = verify_answer_grounding(answer, factual_context, ["portfolio_holdings"])
    assert result.grounded is True


def test_generator_deterministic_aum_holdings_answer() -> None:
    context_packet = {
        "intent": "aum_holdings",
        "factual_context": {
            "scheme_name": "UTI Nifty 50 Index Fund Direct Growth",
            "aum_value": 244332400000.0,
            "aum_date": "2026-04-09",
            "portfolio_holdings": [
                {"security_name": "HDFC Bank Ltd.", "weight": 11.83},
                {"security_name": "ICICI Bank Ltd.", "weight": 8.58},
            ],
        },
        "citations": {"source_url": "https://groww.in/mutual-funds/uti-nifty-fund-direct-growth"},
    }
    out = generate_grounded_answer("AUM and holdings?", context_packet, FakeLLM("ignored"))
    assert out["grounded"] is True
    assert "Top holdings" in out["answer"]
