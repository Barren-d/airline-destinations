"""ANA Aeroportos de Portugal live scraper — current routes via playwright. CLI-only, never app-triggered."""
import asyncio
import json
from pathlib import Path

import polars as pl
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright

from .base import FlightSource, Scope
from ..enrich import enrich, load_airports, load_airlines
from ..exceptions import ScraperError

CONFIG_PATH = Path(__file__).parent.parent.parent / "config" / "ana_destinations.json"
DEST_PATH = "/en/{code}/flights-destinations/airlines/airlines-and-destinations"
REQUEST_DELAY_MS = 1500   # polite delay between airport requests
PAGE_TIMEOUT_MS = 45_000  # Imperva challenge runs before the page renders
CARD_TIMEOUT_MS = 15_000

# ANA airports with scheduled passenger service, from the footer nav (.navbar.footer-main-nav)
# on lisbonairport.pt. Each domain redirects to its English site (e.g. portoairport.pt) and
# the path code is the airport's IATA code.
#
# Beja (BYJ, https://www.aeroportobeja.pt/en/byj/home) is deliberately excluded: it is listed
# in ANA's nav, but its airlines-and-destinations page returns 404. The airport has no
# scheduled passenger flights — it handles occasional charters and is mainly used by Hi Fly
# for aircraft parking. Checked 2026-11-11.
AIRPORTS = {
    "LIS": "https://www.aeroportolisboa.pt",
    "OPO": "https://www.aeroportoporto.pt",
    "FAO": "https://www.aeroportofaro.pt",
    "PDL": "https://www.aeroportopontadelgada.pt",
    "SMA": "https://www.aeroportosantamaria.pt",
    "HOR": "https://www.aeroportohorta.pt",
    "FLW": "https://www.aeroportoflores.pt",
    "FNC": "https://www.aeroportomadeira.pt",
    "PXO": "https://www.aeroportoportosanto.pt",
}


class ANASource(FlightSource):
    name = "ANA Live"
    requires_auth = False
    supports_scopes = [Scope.PORTUGAL]

    def is_available(self) -> bool:
        return CONFIG_PATH.exists()

    def fetch(self, scope: Scope) -> pl.DataFrame:
        dest_map = self._load_config()
        raw_rows = asyncio.run(self._scrape_all())

        if not raw_rows:
            raise ScraperError(
                "ANA scraper returned 0 routes across all airports. "
                "The page structure may have changed — check the .filter-list and "
                "article.card selectors, or whether Imperva is blocking the browser."
            )

        rows, unmapped = [], {}
        for r in raw_rows:
            label = r.pop("dest_label")
            if label not in dest_map:
                unmapped.setdefault(label, set()).add(r["origin_iata"])
                continue
            if dest_map[label] is None:  # deliberately dropped in config
                continue
            rows.append({**r, "dest_iata": dest_map[label]})

        if unmapped:
            print(f"\n  {len(unmapped)} destination labels not in {CONFIG_PATH.name} — skipped:")
            for label, origins in sorted(unmapped.items()):
                print(f"    {label!r} (from {', '.join(sorted(origins))})")

        # Several labels can map to one airport (e.g. "Berlin, Tegel" and "Berlin Brandenburg"
        # both -> BER), so the same airline/route can appear twice.
        raw_df = pl.DataFrame(rows, schema={
            "origin_iata": pl.Utf8,
            "dest_iata": pl.Utf8,
            "airline_name": pl.Utf8,
        }).unique(maintain_order=True)

        airports = load_airports()
        airlines = load_airlines()
        return enrich(raw_df, airports, airlines, source="ana")

    def _load_config(self) -> dict[str, str | None]:
        data = json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
        return {k: v for k, v in data.items() if not k.startswith("_")}

    async def _scrape_all(self) -> list[dict]:
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

            for iata, base_url in AIRPORTS.items():
                rows = await self._scrape_airport(page, iata, base_url)

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

    async def _scrape_airport(self, page, iata: str, base_url: str) -> list[dict]:
        url = base_url + DEST_PATH.format(code=iata.lower())
        try:
            await page.goto(url, timeout=PAGE_TIMEOUT_MS, wait_until="domcontentloaded")
            try:
                await page.wait_for_selector("article.card", timeout=CARD_TIMEOUT_MS)
            except Exception:
                pass  # challenge page or no cards — handled in parser

            html = await page.content()
            return self._parse_destinations(html, iata)

        except Exception as e:
            print(f"  Error scraping {iata}: {type(e).__name__}: {e}")
            return []

    def _parse_destinations(self, html: str, origin_iata: str) -> list[dict]:
        soup = BeautifulSoup(html, "html.parser")

        # Dropdown buttons carry the readable label; data-filter is ".continent-<slug>"
        slug_to_label = {
            button["data-filter"].lstrip(".").removeprefix("continent-"): button.get_text(strip=True)
            for button in soup.select(".filter-list button[data-filter]")
            if button["data-filter"] != "*"
        }

        rows = []
        # Each airline card lists every destination it serves as a continent-<slug> class.
        # Cards with no continent-* class (e.g. Icelandair at FAO) contribute no routes.
        for card in soup.select("article.card"):
            title = card.select_one(".card-header-title")
            airline = title.get_text(strip=True) if title else ""
            for cls in card.get("class", []):
                label = slug_to_label.get(cls.removeprefix("continent-")) if cls.startswith("continent-") else None
                if label:
                    # one row per airline — allows filtering by carrier in the app
                    rows.append({
                        "origin_iata": origin_iata,
                        "dest_label": label,
                        "airline_name": airline,
                    })

        return rows
