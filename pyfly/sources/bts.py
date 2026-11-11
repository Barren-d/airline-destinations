"""BTS T-100 (USA) — routes flown, from the US DOT's T-100 Segment (All Carriers) table. CLI-only.

Every airline flying to, from or within the US reports each nonstop segment it operated per
month (Bureau of Transportation Statistics, TranStats). Public-domain US government data, with
airport and carrier codes, published with a ~3-month lag. Downloaded through the TranStats
ASP.NET form (one POST per month, ~0.5 MB zipped CSV), plain HTTP.

A route is an origin/destination/carrier with at least MIN_DEPARTURES scheduled-passenger
departures in the month (Class F on passenger aircraft; charters and cargo excluded). Carriers
are the *operating* airline, so regional partners (SkyWest, Republic, Envoy…) appear by name.
scraped_at is set to the first day of the data month, so history files are monthly.
"""
import io
import re
import time
import zipfile
from datetime import datetime

import httpx
import polars as pl
from bs4 import BeautifulSoup

from .base import FlightSource, Scope
from ..enrich import enrich, load_airports, load_airlines
from ..exceptions import ScraperError

FIELDS_URL = "https://www.transtats.bts.gov/Fields.asp?gnoyr_VQ=FMG"
FORM_URL = "https://www.transtats.bts.gov/DL_SelectFields.aspx?gnoyr_VQ=FMG&QO_fu146_anzr="
FIELDS = ["DEPARTURES_PERFORMED", "CARRIER", "CARRIER_NAME", "ORIGIN", "DEST",
          "AIRCRAFT_CONFIG", "CLASS", "YEAR", "MONTH"]
MIN_DEPARTURES = 4          # ~weekly; drops diversions and one-off flights
REQUEST_DELAY_S = 2.0
USER_AGENT = "PyFly route explorer (personal project)"
_MONTHS = ["January", "February", "March", "April", "May", "June", "July", "August",
           "September", "October", "November", "December"]


class BTSSource(FlightSource):
    name = "BTS T-100"
    requires_auth = False
    supports_scopes = [Scope.USA]

    def is_available(self) -> bool:
        return True

    def fetch(self, scope: Scope) -> pl.DataFrame:
        with self._client() as client:
            year, month = self.latest_month(client)
            return self.fetch_month(client, year, month)

    def recent_months(self, client: httpx.Client, n: int) -> list[tuple[int, int]]:
        """The n most recent available months, oldest first."""
        year, month = self.latest_month(client)
        months = []
        for _ in range(n):
            months.append((year, month))
            year, month = (year, month - 1) if month > 1 else (year - 1, 12)
        return months[::-1]

    @staticmethod
    def _client() -> httpx.Client:
        return httpx.Client(headers={"User-Agent": USER_AGENT}, follow_redirects=True, timeout=300)

    @staticmethod
    def latest_month(client: httpx.Client) -> tuple[int, int]:
        """Parse 'Latest Available Data: July 2026' from the table's field page."""
        text = BeautifulSoup(client.get(FIELDS_URL).text, "html.parser").get_text(" ", strip=True)
        m = re.search(r"Latest Available Data:\s*(\w+)\s+(\d{4})", text)
        if not m or m.group(1) not in _MONTHS:
            raise ScraperError("Could not find 'Latest Available Data' on the T-100 field page.")
        return int(m.group(2)), _MONTHS.index(m.group(1)) + 1

    def fetch_month(self, client: httpx.Client, year: int, month: int) -> pl.DataFrame:
        time.sleep(REQUEST_DELAY_S)
        raw = self._download(client, year, month)
        routes = self._routes(raw)
        if routes.is_empty():
            raise ScraperError(f"T-100 {year}-{month:02d} returned no scheduled passenger routes.")
        print(f"  {year}-{month:02d}: {len(raw)} segment rows -> {len(routes)} routes")
        df = enrich(routes, load_airports(), load_airlines(), source="bts")
        return df.with_columns(pl.lit(datetime(year, month, 1)).alias("scraped_at"))

    @staticmethod
    def _download(client: httpx.Client, year: int, month: int) -> pl.DataFrame:
        page = client.get(FORM_URL)
        form = BeautifulSoup(page.text, "html.parser").select_one("form#form1")
        if form is None:
            raise ScraperError("T-100 download form (form#form1) not found.")
        data = {i["name"]: i.get("value", "") for i in form.select("input[type=hidden]") if i.get("name")}
        data.update({"cboGeography": "All", "cboYear": str(year), "cboPeriod": str(month),
                     "btnDownload": "Download"})
        data.update({f: "on" for f in FIELDS})
        resp = client.post(page.url, data=data)
        resp.raise_for_status()
        content = resp.content
        if content[:2] == b"PK":
            z = zipfile.ZipFile(io.BytesIO(content))
            content = z.read(next(n for n in z.namelist() if n.lower().endswith(".csv")))
        return pl.read_csv(io.BytesIO(content), infer_schema_length=10000)

    @staticmethod
    def _routes(raw: pl.DataFrame) -> pl.DataFrame:
        """Scheduled passenger service, summed over aircraft types, kept if flown ~weekly."""
        return (
            raw.filter((pl.col("CLASS") == "F") & (pl.col("AIRCRAFT_CONFIG") == 1))
            # same-airport segments (ABQ-ABQ, ANC-ANC) are returns to origin, not routes
            .filter(pl.col("ORIGIN") != pl.col("DEST"))
            .group_by(["ORIGIN", "DEST", "CARRIER", "CARRIER_NAME"])
            .agg(pl.col("DEPARTURES_PERFORMED").sum().alias("departures"))
            .filter(pl.col("departures") >= MIN_DEPARTURES)
            .select(
                pl.col("ORIGIN").alias("origin_iata"),
                pl.col("DEST").alias("dest_iata"),
                pl.col("CARRIER").cast(pl.Utf8).alias("airline_iata"),
                pl.col("CARRIER_NAME").str.strip_chars().str.replace_all(r"\s+", " ").alias("airline_name"),
            )
            .sort(["origin_iata", "dest_iata", "airline_name"])
        )
