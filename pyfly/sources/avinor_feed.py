"""Avinor Feed — departures from Avinor's official open XML flight data. CLI-only.

https://partner.avinor.no/en/services/flight-data/ offers the timetable for all 43 Avinor
airports for reuse in other services, and asks users to cache rather than query on demand —
which a manual snapshot scrape does. The feed returns at most ~14 days ahead (asking for 30 or
90 days returns the same flights), so a route appears if it is flown at least once in the
next two weeks. One request per airport, plus one for airline names.
"""
import time
import xml.etree.ElementTree as ET

import httpx
import polars as pl

from .base import FlightSource, Scope
from ..enrich import enrich, load_airports, load_airlines
from ..exceptions import ScraperError

FEED_URL = "https://asrv.avinor.no/XmlFeed/v1.0"            # parameters are case-sensitive
AIRLINE_NAMES_URL = "https://asrv.avinor.no/airlineNames/v1.0"
WINDOW_HOURS = 336        # 14 days — the feed's maximum
REQUEST_DELAY_S = 1.0
USER_AGENT = "PyFly route explorer (personal project)"

# Avinor's 43 airports, as listed on avinor.no. The feed's default service types are scheduled
# (J) and charter (C) passenger flights; Værøy heliport's scheduled helicopter link to Bodø
# (Lufttransport) is type J, so it is included.
AIRPORTS = [
    "OSL", "BGO", "TRD", "SVG", "TOS", "AES", "ALF", "ANX", "BDU", "BJF", "BVG", "BOO", "BNN",
    "FRO", "FDE", "HFT", "EVE", "HAA", "HVG", "KKN", "KRS", "KSU", "LKL", "LKN", "MEH", "MQN",
    "MOL", "MJF", "OSY", "HOV", "RRS", "RVK", "RET", "SDN", "SSJ", "SOG", "SOJ", "SKN", "LYR",
    "SVJ", "VDS", "VRY", "VAW",
]


class AvinorFeedSource(FlightSource):
    name = "Avinor Feed"
    requires_auth = False
    supports_scopes = [Scope.NORWAY]

    def is_available(self) -> bool:
        return True

    def fetch(self, scope: Scope) -> pl.DataFrame:
        with httpx.Client(headers={"User-Agent": USER_AGENT}, timeout=120) as client:
            airline_names = self._airline_names(client)
            raw_rows, empty = [], []
            for iata in AIRPORTS:
                time.sleep(REQUEST_DELAY_S)
                try:
                    resp = client.get(FEED_URL, params={
                        "airport": iata, "direction": "D", "TimeFrom": "0", "TimeTo": str(WINDOW_HOURS),
                    })
                    resp.raise_for_status()
                    rows = self._parse_departures(resp.content, iata)
                except Exception as e:
                    print(f"  Error fetching {iata}: {type(e).__name__}: {e}")
                    rows = []
                if rows:
                    raw_rows.extend(rows)
                    routes = len({(r["dest_iata"], r["airline_iata"]) for r in rows})
                    print(f"  {iata}: {len(rows)} departures, {routes} routes")
                else:
                    empty.append(iata)

        if empty:
            print(f"\n  0 departures for {len(empty)} airports: {', '.join(empty)}")
        if not raw_rows:
            raise ScraperError("Avinor feed returned 0 departures across all airports.")

        # Many departures per route over 14 days — keep one row per origin/destination/airline
        raw_df = pl.DataFrame(raw_rows, schema={
            "origin_iata": pl.Utf8, "dest_iata": pl.Utf8, "airline_iata": pl.Utf8,
        }).unique(maintain_order=True).with_columns(
            pl.col("airline_iata").replace_strict(airline_names, default=None, return_dtype=pl.Utf8)
            .alias("airline_name")
        ).with_columns(
            pl.coalesce("airline_name", "airline_iata").alias("airline_name")
        # Some carriers use several codes under one name (Norwegian: DY and D8) — the app
        # filters by name, so keep one row per origin/destination/airline name.
        ).unique(subset=["origin_iata", "dest_iata", "airline_name"], maintain_order=True)

        airports = load_airports()
        airlines = load_airlines()
        return enrich(raw_df, airports, airlines, source="avinor_feed")

    @staticmethod
    def _airline_names(client: httpx.Client) -> dict[str, str]:
        resp = client.get(AIRLINE_NAMES_URL)
        resp.raise_for_status()
        root = ET.fromstring(resp.content)
        return {el.get("code"): el.get("name") for el in root.iter("airlineName") if el.get("code")}

    @staticmethod
    def _parse_departures(xml: bytes, origin: str) -> list[dict]:
        rows = []
        for flight in ET.fromstring(xml).iterfind("./flights/flight"):
            if flight.findtext("arr_dep") != "D":
                continue
            # A departure with stops lists them in via_airport ("SVG,BOO"); the direct route
            # from this airport is to the first stop, not the final destination.
            via = [v.strip() for v in (flight.findtext("via_airport") or "").split(",") if v.strip()]
            dest = via[0] if via else flight.findtext("airport")
            airline = flight.findtext("airline")
            if dest and airline and dest != origin:
                rows.append({"origin_iata": origin, "dest_iata": dest, "airline_iata": airline})
        return rows
