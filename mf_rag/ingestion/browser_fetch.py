from __future__ import annotations

import os
from playwright.sync_api import sync_playwright


def fetch_rendered_html(url: str, timeout_ms: int = 30000) -> str:
    """
    Fetch HTML using a real browser context.
    This is used when direct HTTP fetches are blocked or incomplete.
    """
    """
    Modes:
    - Default: headless Chromium (may be blocked by some sites).
    - CDP mode: if MF_CDP_ENDPOINT is set (e.g. http://127.0.0.1:9222),
      connect to an existing user Chrome session for cookie-backed access.
    """
    cdp = os.getenv("MF_CDP_ENDPOINT", "").strip()
    with sync_playwright() as p:
        if cdp:
            browser = p.chromium.connect_over_cdp(cdp)
            context = browser.contexts[0] if browser.contexts else browser.new_context()
        else:
            browser = p.chromium.launch(
                headless=True,
                args=[
                    "--disable-blink-features=AutomationControlled",
                    "--no-sandbox",
                ],
            )
            context = browser.new_context(
                user_agent=(
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/125.0.0.0 Safari/537.36"
                ),
                locale="en-US",
                viewport={"width": 1366, "height": 768},
            )

        page = context.new_page()
        page.add_init_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined});")
        page.goto(url, wait_until="domcontentloaded", timeout=timeout_ms)
        page.wait_for_timeout(1500)
        html = page.content()
        page.close()
        if not cdp:
            context.close()
            browser.close()
        return html

