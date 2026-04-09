from __future__ import annotations

import re
import sys
from urllib import request
import os

from mf_rag.ingestion.browser_fetch import fetch_rendered_html


def main() -> int:
    url = sys.argv[1] if len(sys.argv) > 1 else "https://groww.in/mutual-funds/uti-nifty-50-index-fund"
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/125.0.0.0 Safari/537.36"
        ),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
    }
    req = request.Request(url, headers=headers)
    html = ""
    try:
        html = request.urlopen(req, timeout=30).read().decode("utf-8", "replace")
        print("HTTP_STATUS: 200")
    except Exception as exc:  # noqa: BLE001
        print("HTTP_ERROR:", repr(exc))

    if not html or "404" in html[:5000]:
        print("Trying browser fetch...")
        html = fetch_rendered_html(url, timeout_ms=45000)
        print("BROWSER_FETCH_LEN:", len(html))
    print("URL:", url)
    print("HTML_LEN:", len(html))

    patterns = [
        r"https?://[^\"]*api\.groww\.in[^\"]*",
        r"https?://[^\"]*groww\.in[^\"]*/api/[^\"]*",
        r"/v\d+/api/[^\"]+",
    ]
    for pat in patterns:
        matches = sorted(set(re.findall(pat, html)))
        print("\nPATTERN:", pat, "COUNT:", len(matches))
        for m in matches[:50]:
            print(m)

    # Print small surrounding snippets for "nav" occurrences
    for needle in ["nav", "aum", "expenseRatio", "exitLoad"]:
        idx = html.lower().find(needle.lower())
        if idx != -1:
            start = max(0, idx - 120)
            end = min(len(html), idx + 200)
            print(f"\nSNIP({needle}):")
            print(html[start:end].replace("\n", " ")[:350])

    print("\nKEYWORD POSITIONS:")
    lower = html.lower()
    for kw in ["holding", "top holding", "portfolio", "hdfc bank", "icici bank", "min sip", "nav as of", "aum of"]:
        print(kw, lower.find(kw))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

