from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass
class GroqConfig:
    api_key: str
    model: str
    temperature: float
    max_tokens: int


def load_groq_config() -> GroqConfig:
    api_key = os.getenv("GROQ_API_KEY", "").strip()
    model = os.getenv("GROQ_MODEL", "").strip()
    temperature = float(os.getenv("GROQ_TEMPERATURE", "0.1"))
    max_tokens = int(os.getenv("GROQ_MAX_TOKENS", "300"))

    if not api_key:
        raise ValueError("GROQ_API_KEY is missing.")
    if not model:
        raise ValueError("GROQ_MODEL is missing.")

    return GroqConfig(
        api_key=api_key,
        model=model,
        temperature=temperature,
        max_tokens=max_tokens,
    )
