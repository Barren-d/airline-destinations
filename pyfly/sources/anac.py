"""ANAC VRA (Brazil) — routes flown, from Brazil's civil aviation agency open data. CLI-only.

Voo Regular Ativo (VRA) lists every scheduled flight to, from and within Brazil — Brazilian and
foreign airlines — with its status, one CSV per month (~12 MB), in a plain file directory:
sistemas.anac.gov.br/dadosabertos/Voos e operações aéreas/Voo Regular Ativo (VRA)/<year>/<MM - Mês>/.
Published with a ~1–2 month lag.

A route is an airline/origin/destination flown (Situação Voo = REALIZADO) at least
MIN_FLIGHTS times in the month, on passenger route types only. Código Tipo Linha, inferred
from which airlines use each code (Sep 2026; no official dictionary found):
N = domestic passenger, I = international passenger, C = domestic cargo, G = international cargo.
Airports and airlines are ICAO codes, converted to IATA via airports.csv / airlines.dat.
scraped_at is the first day of the data month, so history files are monthly.
"""
import csv
import io
import re
import time
from datetime import datetime
from pathlib import Path
from urllib.parse import quote, urljoin

import httpx
import polars as pl
from bs4 import BeautifulSoup

from .base import FlightSource, Scope
from ..enrich import enrich, load_airports, load_airlines
from ..exceptions import ScraperError

VRA_URL = ("https://sistemas.anac.gov.br/dadosabertos/" + quote("Voos e operações aéreas") + "/"
           + quote("Voo Regular Ativo (VRA)") + "/")
PASSENGER_LINE_TYPES = {"N", "I"}
MIN_FLIGHTS = 4             # ~weekly; drops one-off and extra flights
REQUEST_DELAY_S = 1.0
USER_AGENT = "PyFly route explorer (personal project)"
DATA_DIR = Path(__file__).parent.parent.parent / "data"

# airlines.dat (OpenFlights, ~2017) names some current ICAO codes wrongly, by an old name, or
# not at all. ICAO -> (IATA or None, current name), checked against the VRA routes 2025-10..2026-09.
AIRLINE_NAMES = {
    "TAM": ("JJ", "LATAM Brasil"), "LAN": ("LA", "LATAM Chile"), "LPE": ("LP", "LATAM Peru"),
    "LNE": ("XL", "LATAM Ecuador"), "LAP": ("PZ", "LATAM Paraguay"), "ARE": ("4C", "LATAM Colombia"),
    "ACN": (None, "Azul Conecta"), "GLO": ("G3", "Gol"), "ABJ": (None, "Abaeté"),
    "JAT": ("JA", "JetSMART"), "JES": ("WJ", "JetSMART Argentina"),
    "ITY": ("AZ", "ITA Airways"), "DWI": ("DM", "Arajet"), "SWR": ("LX", "Swiss"),
    "AVA": ("AV", "Avianca"),
}
# Codes airlines.dat gives to a different (often defunct) airline that we could not confirm for
# these routes — shown as the raw ICAO code rather than a wrong name.
UNVERIFIED_CODES = {"SKX", "EAL", "CMA"}  # "Skyways Express", "European Air Express", "Central European Airlines"


class ANACSource(FlightSource):
    name = "ANAC VRA"
    requires_auth = False
    supports_scopes = [Scope.BRAZIL]

    def is_available(self) -> bool:
        return True

    def fetch(self, scope: Scope) -> pl.DataFrame:
        with self._client() as client:
            year, month = self.latest_month(client)
            return self.fetch_month(client, year, month)

    @staticmethod
    def _client() -> httpx.Client:
        return httpx.Client(headers={"User-Agent": USER_AGENT}, follow_redirects=True, timeout=300)

    # -- directory listing ---------------------------------------------------------------------

    @staticmethod
    def _listing(client: httpx.Client, url: str) -> list[str]:
        time.sleep(REQUEST_DELAY_S)
        resp = client.get(url)
        resp.raise_for_status()
        return [urljoin(str(resp.url), a["href"]) for a in BeautifulSoup(resp.text, "html.parser").select("a[href]")
                if a.get_text(strip=True) != "../"]

    def _month_csv(self, client: httpx.Client, year: int, month: int) -> str | None:
        """URL of VRA_<year><month>.csv, or None if that month is not published yet."""
        try:
            folders = self._listing(client, f"{VRA_URL}{year}/")
        except httpx.HTTPStatusError:
            return None
        folder = next((f for f in folders if re.search(rf"/{month:02d}%20-%20", f)), None)
        if folder is None:
            return None
        return next((f for f in self._listing(client, folder) if f.lower().endswith(".csv")), None)

    def latest_month(self, client: httpx.Client) -> tuple[int, int]:
        """Newest month with a CSV — month folders for the rest of the year exist before data does."""
        today = datetime.now()
        year, month = today.year, today.month
        for _ in range(18):
            if self._month_csv(client, year, month):
                return year, month
            year, month = (year, month - 1) if month > 1 else (year - 1, 12)
        raise ScraperError("No VRA month with a CSV found in the last 18 months.")

    def recent_months(self, client: httpx.Client, n: int) -> list[tuple[int, int]]:
        """The n most recent available months, oldest first."""
        year, month = self.latest_month(client)
        months = []
        for _ in range(n):
            months.append((year, month))
            year, month = (year, month - 1) if month > 1 else (year - 1, 12)
        return months[::-1]

    # -- data ----------------------------------------------------------------------------------

    def fetch_month(self, client: httpx.Client, year: int, month: int) -> pl.DataFrame:
        url = self._month_csv(client, year, month)
        if url is None:
            raise ScraperError(f"VRA {year}-{month:02d} is not published.")
        time.sleep(REQUEST_DELAY_S)
        resp = client.get(url)
        resp.raise_for_status()
        flights = self._parse_csv(resp.content)
        routes = self._routes(flights)
        mapped = self._to_iata(routes)
        print(f"  {year}-{month:02d}: {len(flights)} flights -> {len(routes)} routes "
              f"({len(routes) - len(mapped)} dropped: airport without IATA code)")
        if mapped.is_empty():
            raise ScraperError(f"VRA {year}-{month:02d} produced no passenger routes.")
        df = enrich(mapped, load_airports(), load_airlines(), source="anac")
        return df.with_columns(pl.lit(datetime(year, month, 1)).alias("scraped_at"))

    @staticmethod
    def _parse_csv(content: bytes) -> pl.DataFrame:
        text = content.decode("utf-8-sig")
        if text.startswith("Atualizado em"):  # first line is "Atualizado em: <date>"
            text = text.split("\n", 1)[1]
        return pl.read_csv(io.StringIO(text), separator=";", infer_schema_length=0).rename({
            "ICAO Empresa Aérea": "airline_icao", "Código Tipo Linha": "line_type",
            "ICAO Aeródromo Origem": "origin_icao", "ICAO Aeródromo Destino": "dest_icao",
            "Situação Voo": "status",
        })

    @staticmethod
    def _routes(flights: pl.DataFrame) -> pl.DataFrame:
        """Passenger flights actually flown, counted per airline/origin/destination."""
        return (
            flights.filter((pl.col("status") == "REALIZADO") & pl.col("line_type").is_in(PASSENGER_LINE_TYPES))
            .filter(pl.col("origin_icao") != pl.col("dest_icao"))
            .group_by(["airline_icao", "origin_icao", "dest_icao"])
            .len("flights")
            .filter(pl.col("flights") >= MIN_FLIGHTS)
            .sort(["origin_icao", "dest_icao", "airline_icao"])
        )

    @staticmethod
    def _to_iata(routes: pl.DataFrame) -> pl.DataFrame:
        airport_iata, airline_info = _code_tables()
        return (
            routes.with_columns(
                pl.col("origin_icao").replace_strict(airport_iata, default=None, return_dtype=pl.Utf8).alias("origin_iata"),
                pl.col("dest_icao").replace_strict(airport_iata, default=None, return_dtype=pl.Utf8).alias("dest_iata"),
                pl.col("airline_icao").replace_strict({k: v[0] for k, v in airline_info.items()},
                                                      default=None, return_dtype=pl.Utf8).alias("airline_iata"),
                pl.col("airline_icao").replace_strict({k: v[1] for k, v in airline_info.items()},
                                                      default=None, return_dtype=pl.Utf8).alias("airline_name"),
            )
            .with_columns(pl.coalesce("airline_name", "airline_icao").alias("airline_name"))
            .drop_nulls(["origin_iata", "dest_iata"])
            .select(["origin_iata", "dest_iata", "airline_iata", "airline_name"])
        )


def _code_tables() -> tuple[dict[str, str], dict[str, tuple[str | None, str]]]:
    """ICAO -> IATA for airports (airports.csv) and ICAO -> (IATA, name) for airlines."""
    airports = {}
    with open(DATA_DIR / "airports.csv", encoding="utf-8") as f:
        for r in csv.DictReader(f):
            if r["iata_code"] and r["latitude_deg"]:
                for key in (r["icao_code"], r["gps_code"], r["ident"]):
                    if key:
                        airports.setdefault(key, r["iata_code"])
    airlines = {}
    with open(DATA_DIR / "airlines.dat", encoding="utf-8") as f:
        for r in csv.reader(f):
            if len(r) >= 8 and r[4] not in ("", "\\N", "N/A"):
                if r[4] not in airlines or r[7] == "Y":  # prefer the active carrier for reused codes
                    airlines[r[4]] = (r[3] if r[3] not in ("", "\\N", "-") else None, r[1])
    airlines.update(AIRLINE_NAMES)
    for icao in UNVERIFIED_CODES:
        airlines.pop(icao, None)  # falls back to the raw ICAO code as the name
    return airports, airlines
