"""Isavia Domestic — Iceland's domestic airports (innanlandsflugvellir.is). CLI-only.

Each airport's /en/<slug>-airport/flight-information/destinations page lists destinations as
cards under "Domestic flights" / "INTERNATIONAL FLIGHTS" headings: the card title is the
destination (h3, or h2 on older pages) and the first text line is the airline. Plain HTTP.

Coverage as of 2026-11-11: only RKV and AEY have full card lists; EGS, IFJ and HZK have one
older-style card each; VEY has prose only (no route); the other six return 404. Some older pages
still name Icelandair's retired domestic brand "Air Iceland Connect". Keflavík (KEF) is on a
separate site (kefairport.com) and not covered here.
"""
import time

import httpx
import polars as pl
from bs4 import BeautifulSoup

from .base import FlightSource, Scope
from ..enrich import enrich, load_airports, load_airlines
from ..exceptions import ScraperError

DEST_URL = "https://www.innanlandsflugvellir.is/en/{slug}-airport/flight-information/destinations"
REQUEST_DELAY_S = 1.0
USER_AGENT = "PyFly route explorer (personal project)"

# The 12 airports on the landing page, slug -> IATA
AIRPORTS = {
    "reykjavik": "RKV", "akureyri": "AEY", "egilsstadir": "EGS", "isafjordur": "IFJ",
    "vestmannaeyjar": "VEY", "husavik": "HZK", "hornafjordur": "HFN", "grimsey": "GRY",
    "bildudalur": "BIU", "vopnafjordur": "VPN", "thorshofn": "THO", "gjogur": "GJR",
}

# Card titles -> IATA. The network is small and fixed; unknown titles are logged and skipped.
DESTINATIONS = {
    "Reykjavík": "RKV", "Akureyri": "AEY", "Egilsstaðir": "EGS", "Ísafjörður": "IFJ",
    "Vestmannaeyjar": "VEY", "Húsavík": "HZK", "Hornafjörður": "HFN", "Grímsey": "GRY",
    "Bíldudalur": "BIU", "Vopnafjörður": "VPN", "Þórshöfn": "THO", "Gjögur": "GJR",
    "Nerlerit Inaat, Greenland": "CNP",
}

# Icelandair retired the "Air Iceland Connect" brand in 2021; keep one name per airline
AIRLINE_ALIASES = {"Air Iceland Connect": "Icelandair"}


class IsaviaSource(FlightSource):
    name = "Isavia Domestic"
    requires_auth = False
    supports_scopes = [Scope.ICELAND]

    def is_available(self) -> bool:
        return True

    def fetch(self, scope: Scope) -> pl.DataFrame:
        raw_rows, unknown, empty = [], {}, []
        with httpx.Client(headers={"User-Agent": USER_AGENT}, follow_redirects=True, timeout=30) as client:
            for slug, iata in AIRPORTS.items():
                time.sleep(REQUEST_DELAY_S)
                resp = client.get(DEST_URL.format(slug=slug))
                cards = self._parse_cards(resp.text) if resp.status_code == 200 else []
                rows = []
                for dest_name, airline in cards:
                    dest = DESTINATIONS.get(dest_name)
                    if dest is None:
                        unknown.setdefault(dest_name, set()).add(iata)
                    elif dest != iata:
                        rows.append({"origin_iata": iata, "dest_iata": dest, "airline_name": airline})
                if rows:
                    raw_rows.extend(rows)
                    print(f"  {iata}: {len(rows)} routes")
                else:
                    empty.append(f"{iata} ({resp.status_code})")

        print(f"\n  no destination cards for {len(empty)} airports: {', '.join(empty)}")
        if unknown:
            print(f"  {len(unknown)} unknown destinations — skipped (add to DESTINATIONS):")
            for name, origins in sorted(unknown.items()):
                print(f"    {name!r} (from {', '.join(sorted(origins))})")
        if not raw_rows:
            raise ScraperError("Isavia scraper found 0 destination cards — the page layout may have changed.")

        raw_df = pl.DataFrame(raw_rows, schema={
            "origin_iata": pl.Utf8, "dest_iata": pl.Utf8, "airline_name": pl.Utf8,
        }).unique(maintain_order=True)
        return enrich(raw_df, load_airports(), load_airlines(), source="isavia")

    @staticmethod
    def _parse_cards(html: str) -> list[tuple[str, str | None]]:
        """(destination, airline) per card; cards without a title (e.g. airline directory links) are skipped."""
        soup = BeautifulSoup(html, "html.parser")
        out = []
        for card in soup.select(".card"):
            title = card.select_one(".card-body h3, .card-body h2")
            if not title:
                continue
            p = card.select_one(".card-body p")
            lines = [s.strip() for s in p.get_text("\n").split("\n") if s.strip()] if p else []
            airline = AIRLINE_ALIASES.get(lines[0], lines[0]) if lines else None
            out.append((title.get_text(strip=True), airline))
        return out
