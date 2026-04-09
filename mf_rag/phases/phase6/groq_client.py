from __future__ import annotations

import json
from typing import Any
from urllib import error, request

from mf_rag.phases.phase6.config import GroqConfig


class GroqClient:
    endpoint = "https://api.groq.com/openai/v1/chat/completions"

    def __init__(self, config: GroqConfig) -> None:
        self.config = config

    def generate(self, system_prompt: str, user_prompt: str) -> str:
        payload: dict[str, Any] = {
            "model": self.config.model,
            "temperature": self.config.temperature,
            "max_tokens": self.config.max_tokens,
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
        }
        data = json.dumps(payload).encode("utf-8")
        req = request.Request(
            self.endpoint,
            data=data,
            headers={
                "Authorization": f"Bearer {self.config.api_key}",
                "Content-Type": "application/json",
                "Accept": "application/json",
                # Explicit UA avoids edge-network blocks on generic python clients.
                "User-Agent": "mf-rag-chatbot/0.1 (+python urllib)",
            },
            method="POST",
        )
        try:
            with request.urlopen(req, timeout=30) as resp:
                body = json.loads(resp.read().decode("utf-8"))
            return str(body["choices"][0]["message"]["content"]).strip()
        except error.HTTPError as exc:
            detail = exc.read().decode("utf-8", "replace")
            raise RuntimeError(f"Groq API request failed with HTTP {exc.code}: {detail}") from exc
