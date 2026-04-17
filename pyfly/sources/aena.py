"""AENA live scraper — current scheduled routes via playwright. CLI-only, never app-triggered."""
import asyncio
import json
from pathlib import Path

import polars as pl
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright

from .base import FlightSource, Scope
from ..enrich import enrich, load_airports, load_airlines
from ..exceptions import ScraperError

CONFIG_PATH = Path(__file__).parent.parent.parent / "config" / "airport_urls.json"
DEST_SUFFIX = "/aerolineas-y-destinos/destinos-aeropuerto.html"
REQUEST_DELAY_MS = 1500   # polite delay between airport requests
PAGE_TIMEOUT_MS = 15_000
ARTICLE_TIMEOUT_MS = 10_000


import re as _re

_DATE_ANNOTATION = _re.compile(
    r"^desde\s*el\b",  # "Desde el 01/04/2026" or typo "Desdel el ..."
    _re.IGNORECASE,
)


def _is_date_annotation(text: str) -> bool:
    """Return True for seasonal-start strings like 'Desde el 01/04/2026'."""
    return bool(_DATE_ANNOTATION.match(text.strip()))


def _dest_url(main_url: str) -> str:
    """Derive the destinations page URL from an airport main page URL."""
    return main_url.replace(".html", DEST_SUFFIX)


class AENASource(FlightSource):
    name = "AENA Live"
    requires_auth = False
    supports_scopes = [Scope.AENA]

    def is_available(self) -> bool:
        return CONFIG_PATH.exists()

    def fetch(self, scope: Scope) -> pl.DataFrame:
        airport_urls = self._load_config()
        raw_rows = asyncio.run(self._scrape_all(airport_urls))

        if not raw_rows:
            raise ScraperError(
                "AENA scraper returned 0 routes across all airports. "
                "The page structure may have changed — check CSS selectors "
                "against archive/aenadestinations.py."
            )

        raw_df = pl.DataFrame(raw_rows, schema={
            "origin_iata": pl.Utf8,
            "dest_raw": pl.Utf8,
            "dest_country": pl.Utf8,
            "airline_name": pl.Utf8,
        })

        airports = load_airports()
        airlines = load_airlines()
        return enrich(raw_df, airports, airlines, source="aena")

    def _load_config(self) -> dict[str, str]:
        data = json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
        return {k: v for k, v in data.items() if not k.startswith("_")}

    async def _scrape_all(self, airport_urls: dict[str, str]) -> list[dict]:
        results = []
        zero_result = []

        async with async_playwright() as pw:
            browser = await pw.chromium.launch(headless=True)
            page = await browser.new_page(
                user_agent=(
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/120.0.0.0 Safari/537.36"
                )
            )

            for iata, main_url in airport_urls.items():
                rows = await self._scrape_airport(page, iata, main_url)

                if rows:
                    results.extend(rows)
                    print(f"  {iata}: {len(rows)} routes")
                else:
                    zero_result.append(iata)

                await page.wait_for_timeout(REQUEST_DELAY_MS)

            await browser.close()

        if zero_result:
            print(f"\n  0 results for {len(zero_result)} airports: {', '.join(zero_result)}")

        return results

    async def _scrape_airport(self, page, iata: str, main_url: str) -> list[dict]:
        try:
            # Step 1: load the airport main page and extract the destinations URL
            # from the nav link — handles non-standard URL paths across all airports
            await page.goto(main_url, timeout=PAGE_TIMEOUT_MS, wait_until="domcontentloaded")
            main_html = await page.content()
            dest_url = self._extract_dest_url(main_html, main_url)

            # Step 2: navigate to the destinations page
            await page.goto(dest_url, timeout=PAGE_TIMEOUT_MS, wait_until="domcontentloaded")
            try:
                await page.wait_for_selector("article", timeout=ARTICLE_TIMEOUT_MS)
            except Exception:
                pass  # page loaded but no articles visible — handled in parser

            html = await page.content()
            return self._parse_destinations(html, iata)

        except Exception as e:
            print(f"  Error scraping {iata}: {type(e).__name__}: {e}")
            return []

    def _extract_dest_url(self, html: str, main_url: str) -> str:
        """Extract the destinations page URL from the airport main page nav."""
        soup = BeautifulSoup(html, "html.parser")
        base = "https://www.aena.es"

        # Match by link text — avoids false positives from href prefix /aerolineas-y-destinos/
        for link in soup.select("a.header-text.segmento"):
            text = link.get_text(strip=True)
            if "destinos" in text.lower() and link.get("href"):
                href = link["href"]
                return href if href.startswith("http") else base + href

        # Fallback: derive from main URL (works for most airports)
        return _dest_url(main_url)

    def _parse_destinations(self, html: str, origin_iata: str) -> list[dict]:
        soup = BeautifulSoup(html, "html.parser")

        # 2022 selector still valid as of 2026
        articles = soup.select("article.fila.resultado.regular.filtered")
        if not articles:
            articles = soup.select("article.resultado")
        if not articles:
            articles = soup.select("article[class*='resultado']")

        rows = []
        for article in articles:
            lines = [
                line.strip()
                for line in article.get_text("\n").split("\n")
                if line.strip()
            ]
            # 2026 structure (after stripping blanks):
            #   lines[0]  : destination e.g. "AMSTERDAM /SCHIPHOL (AMS)"
            #   lines[1]  : "País"  (label — skip)
            #   lines[2]  : country e.g. "HOLANDA"
            #   lines[3]  : "Aerolíneas" (label — skip)
            #   lines[4:] : one airline name per line
            if len(lines) < 3:
                continue

            dest_raw = lines[0]
            dest_country = lines[2] if len(lines) > 2 else ""
            # airlines start after the "Aerolíneas" label at index 3
            # filter out seasonal date annotations e.g. "Desde el 01/04/2026"
            airlines = [
                l for l in (lines[4:] if len(lines) > 4 else [""])
                if not _is_date_annotation(l)
            ] or [""]

            # one row per airline — allows filtering by carrier in the app
            for airline in airlines:
                rows.append({
                    "origin_iata": origin_iata,
                    "dest_raw": dest_raw,
                    "dest_country": dest_country,
                    "airline_name": airline,
                })

        return rows
