"""Swedavia Live — destinations and airlines for Sweden's 10 Swedavia airports. CLI-only.

Every airport's destinations page (e.g. /landvetter/destinations/, /arlanda/destinations2/, or
Bromma's tile layout) is filled from the same JSON endpoint,
/services/destinations/alldestinations/<IATA>/en, which lists each destination with its country,
airlines and a charter flag. The big airports' pages only show a "top destinations" selection,
but the endpoint returns the full list, so this reads it directly — one request per airport,
plain HTTP. Destinations have names, not IATA codes; config/swedavia_destinations.json maps them.
"""
import json
import time
from pathlib import Path

import httpx
import polars as pl

from .base import FlightSource, Scope
from ..enrich import enrich, load_airports, load_airlines
from ..exceptions import ScraperError

CONFIG_PATH = Path(__file__).parent.parent.parent / "config" / "swedavia_destinations.json"
DESTINATIONS_URL = "https://www.swedavia.com/services/destinations/alldestinations/{iata}/en"
REQUEST_DELAY_S = 1.0
USER_AGENT = "PyFly route explorer (personal project)"

# Swedavia's 10 airports (the landing page's airport picker is built client-side)
AIRPORTS = ["ARN", "GOT", "BMA", "MMX", "LLA", "UME", "OSD", "VBY", "KRN", "RNB"]


class SwedaviaSource(FlightSource):
    name = "Swedavia Live"
    requires_auth = False
    supports_scopes = [Scope.SWEDEN]

    def is_available(self) -> bool:
        return CONFIG_PATH.exists()

    def fetch(self, scope: Scope) -> pl.DataFrame:
        dest_map = self._load_config()
        with httpx.Client(headers={"User-Agent": USER_AGENT}, timeout=30) as client:
            raw_rows = self._scrape_all(client)

        if not raw_rows:
            raise ScraperError(
                "Swedavia scraper returned 0 destinations. The alldestinations endpoint may have "
                "moved — check the XHR calls on an airport's destinations page."
            )

        rows, unmapped = [], {}
        for r in raw_rows:
            name = r.pop("dest_name")
            if name not in dest_map:
                unmapped.setdefault(name, set()).add(r["origin_iata"])
                continue
            dest = self._resolve(dest_map[name], r["airline_name"])
            if dest and dest != r["origin_iata"]:  # null = dropped; skip e.g. Stockholm from ARN
                rows.append({**r, "dest_iata": dest})

        if unmapped:
            print(f"\n  {len(unmapped)} destinations not in {CONFIG_PATH.name} — skipped:")
            for name, origins in sorted(unmapped.items()):
                print(f"    {name!r} (from {', '.join(sorted(origins))})")

        raw_df = pl.DataFrame(rows, schema={
            "origin_iata": pl.Utf8, "dest_iata": pl.Utf8, "airline_name": pl.Utf8,
        }).unique(maintain_order=True)

        airports = load_airports()
        airlines = load_airlines()
        return enrich(raw_df, airports, airlines, source="swedavia")

    def _load_config(self) -> dict:
        data = json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
        return {k: v for k, v in data.items() if not k.startswith("_")}

    @staticmethod
    def _resolve(value, airline: str | None) -> str | None:
        """A mapping value is one IATA code, null, or {airline name: code, "default": code}."""
        if isinstance(value, dict):
            return value.get(airline or "", value.get("default"))
        return value

    def _scrape_all(self, client: httpx.Client) -> list[dict]:
        results, empty = [], []
        for iata in AIRPORTS:
            time.sleep(REQUEST_DELAY_S)
            try:
                resp = client.get(DESTINATIONS_URL.format(iata=iata))
                resp.raise_for_status()
                rows = self._parse_destinations(resp.json(), iata)
            except Exception as e:
                print(f"  Error fetching {iata}: {type(e).__name__}: {e}")
                rows = []
            if rows:
                results.extend(rows)
                print(f"  {iata}: {len({r['dest_name'] for r in rows})} destinations, {len(rows)} routes")
            else:
                empty.append(iata)
        if empty:
            print(f"\n  0 destinations for {len(empty)} airports: {', '.join(empty)}")
        return results

    @staticmethod
    def _parse_destinations(items: list[dict], origin: str) -> list[dict]:
        rows = []
        for item in items:
            name = (item.get("name") or "").strip()
            if not name:
                continue
            # one row per airline; charter destinations sometimes list none
            for airline in [a.get("name") for a in item.get("airlines") or []] or [None]:
                rows.append({"origin_iata": origin, "dest_name": name, "airline_name": airline})
        return rows
