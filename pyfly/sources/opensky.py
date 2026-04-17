"""OpenSky live source — actual flights flown via OpenSky Network REST API."""
import os
import time
from pathlib import Path

import httpx
import polars as pl
from dotenv import load_dotenv

from .base import FlightSource, Scope, AENA_IATA
from ..enrich import load_airports, load_airlines

load_dotenv()

OPENSKY_API = "https://opensky-network.org/api/flights/departure"
WINDOW_DAYS = 7
REQUEST_TIMEOUT = 30
RETRY_DELAY = 5

# Scope → IATA set (reuse sets from openflights for consistency)
from .openflights import EUROPEAN_IATA, GLOBAL_TOP_100_IATA, SCOPE_MAP as _OF_SCOPE_MAP


class OpenSkySource(FlightSource):
    name = "OpenSky (Live)"
    requires_auth = True
    supports_scopes = [Scope.AENA, Scope.EUROPEAN, Scope.GLOBAL_TOP_100, Scope.CUSTOM]

    def is_available(self) -> bool:
        return bool(os.getenv("OPENSKY_USERNAME") and os.getenv("OPENSKY_PASSWORD"))

    def fetch(self, scope: Scope, custom_iata: set[str] | None = None) -> pl.DataFrame:
        if not self.is_available():
            raise RuntimeError(
                "OpenSky credentials not found. Set OPENSKY_USERNAME and "
                "OPENSKY_PASSWORD in .env"
            )

        airports_df = load_airports()
        airlines_df = load_airlines()

        iata_set = custom_iata if scope == Scope.CUSTOM else _OF_SCOPE_MAP[scope]
        icao_map = self._iata_to_icao(iata_set, airports_df)

        from ..db import init_db, check_cache, write_cache
        init_db()

        raw_records: list[dict] = []
        for iata, icao in icao_map.items():
            hit, cached = check_cache(icao)
            if hit and cached is not None and len(cached) > 0:
                raw_records.extend(cached.to_dicts())
                continue

            fetched = self._fetch_airport(icao)
            write_cache(icao, fetched)
            raw_records.extend(fetched)

        if not raw_records:
            return self._empty_df()

        raw_df = pl.DataFrame(raw_records, infer_schema_length=1000)
        return self._build_routes(raw_df, airports_df, airlines_df)

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _iata_to_icao(self, iata_set: set[str], airports_df: pl.DataFrame) -> dict[str, str]:
        mapping = (
            airports_df
            .filter(pl.col("iata_code").is_in(iata_set))
            .filter(pl.col("icao_code").is_not_null())
            .select(["iata_code", "icao_code"])
        )
        return {row["iata_code"]: row["icao_code"] for row in mapping.iter_rows(named=True)}

    def _fetch_airport(self, icao: str) -> list[dict]:
        end_ts = int(time.time())
        begin_ts = end_ts - WINDOW_DAYS * 86400
        username = os.getenv("OPENSKY_USERNAME")
        password = os.getenv("OPENSKY_PASSWORD")

        url = f"{OPENSKY_API}?airport={icao}&begin={begin_ts}&end={end_ts}"
        try:
            resp = httpx.get(url, auth=(username, password), timeout=REQUEST_TIMEOUT)
        except httpx.TimeoutException:
            print(f"  OpenSky timeout for {icao}, retrying...")
            time.sleep(RETRY_DELAY)
            try:
                resp = httpx.get(url, auth=(username, password), timeout=REQUEST_TIMEOUT)
            except httpx.TimeoutException:
                print(f"  OpenSky timeout again for {icao}, skipping")
                return []

        if resp.status_code == 401:
            raise RuntimeError(
                "OpenSky authentication failed. Check OPENSKY_USERNAME / "
                "OPENSKY_PASSWORD in .env"
            )
        if resp.status_code == 429:
            print(f"  OpenSky rate limit hit for {icao} — using stale cache or skipping")
            return []
        if not resp.is_success:
            print(f"  OpenSky {resp.status_code} for {icao}, skipping")
            return []

        flights = resp.json()
        if not flights:
            return []

        return [
            {
                "origin_icao": icao,
                "dest_icao": f.get("estArrivalAirport") or "",
                "callsign": (f.get("callsign") or "").strip(),
            }
            for f in flights
            if f.get("estArrivalAirport")
        ]

    def _build_routes(
        self,
        raw_df: pl.DataFrame,
        airports_df: pl.DataFrame,
        airlines_df: pl.DataFrame,
    ) -> pl.DataFrame:
        # ICAO → IATA for airports
        icao_to_iata = {
            row["icao_code"]: row["iata_code"]
            for row in airports_df
            .filter(pl.col("iata_code").is_not_null())
            .select(["icao_code", "iata_code"])
            .iter_rows(named=True)
        }

        # ICAO airline prefix (3 chars) → IATA airline code
        # airlines.dat has both icao and iata columns
        icao_airline_map: dict[str, tuple[str, str]] = {}
        for row in airlines_df.iter_rows(named=True):
            icao_code = row.get("icao") or ""
            iata_code = row.get("iata") or ""
            name = row.get("name") or ""
            if icao_code and len(icao_code) == 3:
                icao_airline_map[icao_code.upper()] = (iata_code, name)

        seen: set[tuple] = set()
        rows = []
        for rec in raw_df.iter_rows(named=True):
            origin_icao = rec["origin_icao"]
            dest_icao = rec["dest_icao"]
            callsign = rec["callsign"]

            origin_iata = icao_to_iata.get(origin_icao)
            dest_iata = icao_to_iata.get(dest_icao)
            if not origin_iata or not dest_iata:
                continue

            airline_icao = callsign[:3].upper() if len(callsign) >= 3 else ""
            airline_iata, airline_name = icao_airline_map.get(airline_icao, ("", ""))

            key = (origin_iata, dest_iata, airline_iata)
            if key in seen:
                continue
            seen.add(key)
            rows.append({
                "origin_iata": origin_iata,
                "dest_iata": dest_iata,
                "airline_iata": airline_iata,
                "airline_name": airline_name,
            })

        if not rows:
            return self._empty_df()

        routes_df = pl.DataFrame(rows)

        # Attach coordinates
        coord_cols = airports_df.select([
            pl.col("iata_code"),
            pl.col("latitude_deg").alias("lat"),
            pl.col("longitude_deg").alias("lon"),
        ])

        routes_df = (
            routes_df
            .join(
                coord_cols.rename({"iata_code": "origin_iata", "lat": "origin_lat", "lon": "origin_lon"}),
                on="origin_iata", how="left",
            )
            .join(
                coord_cols.rename({"iata_code": "dest_iata", "lat": "dest_lat", "lon": "dest_lon"}),
                on="dest_iata", how="left",
            )
            .drop_nulls(subset=["origin_lat", "dest_lat"])
            .with_columns(pl.lit("opensky").alias("source"))
            .select([
                "origin_iata", "origin_lat", "origin_lon",
                "dest_iata", "dest_lat", "dest_lon",
                "airline_iata", "airline_name", "source",
            ])
        )
        return routes_df

    def _empty_df(self) -> pl.DataFrame:
        import polars as pl
        from .base import SCHEMA
        return pl.DataFrame({col: pl.Series([], dtype=dtype) for col, dtype in SCHEMA.items()})
