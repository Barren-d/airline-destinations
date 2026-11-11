"""Avinor Live — direct routes published on avinor.no airport pages. CLI-only, never app-triggered.

Each /en/airport/<slug>/direct-routes/ page embeds its route list as JSON (the props of the
DirectFlightsBlock component), including destination IATA codes, so no name matching or
browser is needed. As of 2026-11-11 only 8 of Avinor's 43 airports have such a page (OSL, BGO,
TRD, SVG, TOS, AES, BOO, KRS); the rest return 404. Every airport is still tried each run so new
pages are picked up. The pages do not list airlines — see avinor_feed.py for a source that does.
"""
import json
import time
from urllib.parse import urljoin

import httpx
import polars as pl

from .base import FlightSource, Scope
from ..enrich import enrich, load_airports, load_airlines
from ..exceptions import ScraperError

LANDING_URL = "https://www.avinor.no/en/"
REQUEST_DELAY_S = 1.0  # polite delay between page requests
USER_AGENT = "PyFly route explorer (personal project)"
PROPS_MARKER = '"componentName":"DirectFlightsBlock"'


class AvinorSource(FlightSource):
    name = "Avinor Live"
    requires_auth = False
    supports_scopes = [Scope.NORWAY]

    def is_available(self) -> bool:
        return True

    def fetch(self, scope: Scope) -> pl.DataFrame:
        with httpx.Client(headers={"User-Agent": USER_AGENT}, follow_redirects=True, timeout=30) as client:
            raw_rows = self._scrape_all(client)

        if not raw_rows:
            raise ScraperError(
                "Avinor scraper returned 0 routes. The airport list (.start-page__content) or the "
                "DirectFlightsBlock JSON on /direct-routes/ pages may have changed."
            )

        raw_df = pl.DataFrame(raw_rows, schema={
            "origin_iata": pl.Utf8,
            "dest_iata": pl.Utf8,
            "airline_name": pl.Utf8,
        }).unique(maintain_order=True)

        airports = load_airports()
        airlines = load_airlines()
        return enrich(raw_df, airports, airlines, source="avinor")

    def _scrape_all(self, client: httpx.Client) -> list[dict]:
        airport_urls = self._airport_urls(client)
        results, no_page = [], []

        for url in airport_urls:
            time.sleep(REQUEST_DELAY_S)
            resp = client.get(url.rstrip("/") + "/direct-routes/")
            if resp.status_code == 404:
                no_page.append(url.rstrip("/").rsplit("/", 1)[-1])
                continue
            try:
                resp.raise_for_status()
                rows = self._parse_routes(resp.text)
            except Exception as e:
                print(f"  Error scraping {url}: {type(e).__name__}: {e}")
                continue
            if rows:
                results.extend(rows)
                print(f"  {rows[0]['origin_iata']}: {len(rows)} routes")

        print(f"\n  {len(no_page)} airports without a direct-routes page: {', '.join(no_page)}")
        return results

    def _airport_urls(self, client: httpx.Client) -> list[str]:
        """Airport base URLs from the landing page's airport list (it appears more than once)."""
        from bs4 import BeautifulSoup

        resp = client.get(LANDING_URL)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")
        urls = [
            urljoin(LANDING_URL, a["href"])
            for a in soup.select(".start-page__content a[href]")
            if "/airport/" in a["href"]
        ]
        # Dedupe case-insensitively but keep the original casing (e.g. /airport/Stavanger/)
        unique = {}
        for u in urls:
            unique.setdefault(u.lower().rstrip("/"), u.rstrip("/") + "/")
        return list(unique.values())

    def _parse_routes(self, html: str) -> list[dict]:
        props = self._extract_props(html)
        if props is None:
            return []
        origin = props["origin"]["iata"]
        rows = []
        for item in props.get("items") or []:
            # isRoute = scheduled, isCharter = charter; entries with neither look stale
            if not (item.get("isRoute") or item.get("isCharter")) or not item.get("iata"):
                continue
            if item["iata"] == origin:  # Bodø's page lists BOO itself
                continue
            rows.append({"origin_iata": origin, "dest_iata": item["iata"], "airline_name": None})
        return rows

    @staticmethod
    def _extract_props(html: str) -> dict | None:
        """Decode the DirectFlightsBlock props object: {blockHeading, origin, localization, items}."""
        marker = html.find(PROPS_MARKER)
        if marker < 0:
            return None
        key = html.rfind('"blockHeading"', 0, marker)
        start = html.rfind("{", 0, key)
        if key < 0 or start < 0:
            return None
        props, _ = json.JSONDecoder().raw_decode(html, start)
        return props
