"""OpenFlights historical source — routes.dat circa 2017, all scopes."""
from pathlib import Path
import polars as pl
from .base import FlightSource, Scope, AENA_IATA
from ..enrich import enrich, load_airports, load_airlines

DATA_DIR = Path(__file__).parent.parent.parent / "data"

# Curated scope lists — ICAO-adjacent major hubs per region
EUROPEAN_IATA = {
    "LHR", "CDG", "AMS", "FRA", "MAD", "BCN", "FCO", "MXP", "MUC", "ZRH",
    "VIE", "BRU", "CPH", "ARN", "OSL", "HEL", "LIS", "ATH", "WAW", "PRG",
    "BUD", "DUB", "MAN", "EDI", "LGW", "STN", "ORY", "LYS", "NCE", "TLS",
    "HAM", "DUS", "BER", "GVA", "BSL", "BTS", "OTP", "SOF", "ZAG", "LJU",
    "SKP", "TIA", "TXL", "CGN", "STR", "NUE", "BHX", "BRS",
}

GLOBAL_TOP_100_IATA = EUROPEAN_IATA | {
    "ATL", "LAX", "ORD", "DFW", "DEN", "JFK", "SFO", "SEA", "LAS", "MIA",
    "PHX", "IAH", "CLT", "MCO", "EWR", "MSP", "BOS", "DTW", "PHL", "LGA",
    "DXB", "DOH", "AUH", "SIN", "HKG", "PEK", "PVG", "NRT", "ICN", "BKK",
    "KUL", "CGK", "DEL", "BOM", "SYD", "MEL", "JNB", "CAI", "NBO", "LOS",
    "GRU", "BOG", "SCL", "LIM", "EZE", "MEX", "YYZ", "YVR", "YUL",
}

SCOPE_MAP = {
    Scope.AENA: AENA_IATA,
    Scope.EUROPEAN: EUROPEAN_IATA,
    Scope.GLOBAL_TOP_100: GLOBAL_TOP_100_IATA,
}


class OpenFlightsSource(FlightSource):
    name = "Historical (2017)"
    requires_auth = False
    supports_scopes = [Scope.AENA, Scope.EUROPEAN, Scope.GLOBAL_TOP_100, Scope.CUSTOM]

    def is_available(self) -> bool:
        return (DATA_DIR / "routes.dat").exists() and (DATA_DIR / "airlines.dat").exists()

    def fetch(self, scope: Scope, custom_iata: set[str] | None = None) -> pl.DataFrame:
        origin_filter = custom_iata if scope == Scope.CUSTOM else SCOPE_MAP[scope]

        routes = pl.read_csv(
            DATA_DIR / "routes.dat",
            has_header=False,
            null_values=[r"\N"],
            new_columns=[
                "airline_iata", "airline_id", "src_iata", "src_id",
                "dst_iata", "dst_id", "codeshare", "stops", "equipment",
            ],
        )

        # Filter to chosen scope and direct flights only
        routes = (
            routes.filter(pl.col("src_iata").is_in(origin_filter))
            .filter(pl.col("stops") == 0)
            .select(["airline_iata", "src_iata", "dst_iata"])
            .rename({"src_iata": "origin_iata", "dst_iata": "dest_iata"})
            .drop_nulls()
        )

        airports = load_airports()
        airlines = load_airlines()
        return enrich(routes, airports, airlines, source="openflights_2017")
