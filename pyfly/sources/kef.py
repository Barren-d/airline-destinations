"""Keflavík Airport (KEF) — international destinations from kefairport.com. CLI-only.

kefairport.com/destinations is a Next.js page: the destination cards (and the "type" dropdown,
which only filters them client-side) are built from props.pageProps.extraData in the page's
__NEXT_DATA__ JSON — 85 destinations as of 2026-11-13, each with its airlines, season
(All year / Summer / Winter) and sometimes the airport code. One plain-HTTP request.

Most flight entries leave "airport" empty, so destination titles map to IATA through
DESTINATIONS below (a given code takes precedence). Unknown titles are logged and skipped.
"""
import json

import httpx
import polars as pl
from bs4 import BeautifulSoup

from .base import FlightSource, Scope
from ..enrich import enrich, load_airports, load_airlines
from ..exceptions import ScraperError

PAGE_URL = "https://www.kefairport.com/destinations"
USER_AGENT = "PyFly route explorer (personal project)"
ORIGIN = "KEF"

# Card title -> IATA, or {airline: IATA, "default": IATA} for cities with several airports.
# Checked 2026-11-13. "Birmingham" is listed under country "Greece" on the site — it is BHX.
DESTINATIONS = {
    "Alicante": "ALC", "Amsterdam": "AMS", "Baltimore": "BWI", "Barcelona": "BCN", "Basel": "BSL",
    "Beijing": "PEK", "Bergen": "BGO", "Berlin": "BER", "Billund": "BLL", "Birmingham": "BHX",
    "Boston": "BOS", "Bristol": "BRS", "Brussels": "BRU", "Budapest": "BUD", "Calgary": "YYC",
    "Chicago": "ORD", "Copenhagen": "CPH", "Denver": "DEN", "Detroit": "DTW", "Dublin": "DUB",
    "Dusseldorf": "DUS", "Edinburgh": "EDI", "Edmonton": "YEG", "Faro": "FAO",
    "Faroe Islands - Vagar": "FAE", "Frankfurt": "FRA", "Gdansk": "GDN", "Geneva": "GVA",
    "Glasgow": "GLA", "Gothenburg": "GOT", "Gran Canaria": "LPA", "Halifax": "YHZ",
    "Helsinki": "HEL", "Ilulissat": "JAV", "Katowice": "KTW", "Kulusuk": "KUS", "Leeds": "LBA",
    "Lisbon": "LIS", "London Gatwick": "LGW", "London Heathrow": "LHR", "London Luton": "LTN",
    "London Stansted": "STN", "Lyon": "LYS", "Madrid": "MAD", "Malaga": "AGP", "Manchester": "MAN",
    "Miami": "MIA", "Milan": "MXP", "Minneapolis": "MSP", "Montreal": "YUL", "Munich": "MUC",
    "Nashville": "BNA", "New York JFK": "JFK", "New York Newark": "EWR", "Newcastle": "NCL",
    "Nice": "NCE", "Nuuk": "GOH", "Orlando": "MCO", "Oslo": "OSL", "Paris CDG": "CDG",
    "Paris Orly": "ORY", "Pittsburgh": "PIT", "Portland": "PDX", "Prague": "PRG", "Qaqortoq": "JJU",
    "Raleigh-Durham": "RDU", "Riga": "RIX", "Rome": "FCO", "Salzburg": "SZG", "Seattle": "SEA",
    "Stockholm": "ARN", "Stuttgart": "STR", "Tenerife": "TFS", "Toronto Pearson": "YYZ",
    "Tromso": "TOS", "Vancouver": "YVR", "Venice": "VCE", "Verona": "VRN", "Vienna": "VIE",
    "Vilnius": "VNO", "Warsaw": "WAW", "Washington DC": "IAD", "Winnipeg": "YWG",
    "Wroclaw": "WRO", "Zurich": "ZRH",
}
AIRLINE_ALIASES = {"EasyJet": "easyJet"}


class KEFSource(FlightSource):
    name = "Keflavík Airport"
    requires_auth = False
    supports_scopes = [Scope.ICELAND]

    def is_available(self) -> bool:
        return True

    def fetch(self, scope: Scope) -> pl.DataFrame:
        with httpx.Client(headers={"User-Agent": USER_AGENT}, follow_redirects=True, timeout=60) as client:
            resp = client.get(PAGE_URL)
            resp.raise_for_status()
        items = self._extract_destinations(resp.text)
        rows, unknown = self._routes(items)
        print(f"  KEF: {len(items)} destinations -> {len(rows)} routes")
        if unknown:
            print(f"  {len(unknown)} destinations without an airport code — skipped (add to DESTINATIONS): "
                  f"{', '.join(sorted(unknown))}")
        if not rows:
            raise ScraperError("Keflavík page had no destinations — the __NEXT_DATA__ layout may have changed.")
        raw_df = pl.DataFrame(rows, schema={
            "origin_iata": pl.Utf8, "dest_iata": pl.Utf8, "airline_name": pl.Utf8,
        }).unique(maintain_order=True)
        return enrich(raw_df, load_airports(), load_airlines(), source="kef")

    @staticmethod
    def _extract_destinations(html: str) -> list[dict]:
        script = BeautifulSoup(html, "html.parser").select_one("script#__NEXT_DATA__")
        if script is None:
            raise ScraperError("No __NEXT_DATA__ on the Keflavík destinations page.")
        data = json.loads(script.string)
        return data["props"]["pageProps"].get("extraData") or []

    @staticmethod
    def _routes(items: list[dict]) -> tuple[list[dict], set[str]]:
        rows, unknown = [], set()
        for item in items:
            title = (item.get("title") or "").strip()
            for flight in item.get("flights") or []:
                dest = (flight.get("airport") or "").strip().upper() or DESTINATIONS.get(title)
                if not dest:
                    unknown.add(title)
                    continue
                airline = (flight.get("airline") or "").strip() or None
                rows.append({"origin_iata": ORIGIN, "dest_iata": dest,
                             "airline_name": AIRLINE_ALIASES.get(airline, airline)})
        return rows, unknown
