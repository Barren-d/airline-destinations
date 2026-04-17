from abc import ABC, abstractmethod
from enum import Enum
import polars as pl


class Scope(Enum):
    AENA = "aena"
    EUROPEAN = "european"
    GLOBAL_TOP_100 = "global_top_100"
    CUSTOM = "custom"


SCHEMA = {
    "origin_iata": pl.Utf8,
    "origin_lat": pl.Float64,
    "origin_lon": pl.Float64,
    "dest_iata": pl.Utf8,
    "dest_lat": pl.Float64,
    "dest_lon": pl.Float64,
    "airline_iata": pl.Utf8,
    "airline_name": pl.Utf8,
    "source": pl.Utf8,
}

AENA_IATA = {
    # Extracted from aena.es/es/pasajeros/nuestros-aeropuertos.html 2026-04-17
    # 43 commercial airports — excludes heliports (AEI, JCU) and GA fields (LECU, LESB, RGS)
    "LCG", "MAD", "ABC", "ALC", "LEI", "OVD", "BJZ", "BIO", "ACE", "ODB",
    "VDE", "GRX", "FUE", "GRO", "LPA", "HSK", "IBZ", "RMU", "XRY", "BCN",
    "GMZ", "SPC", "LEN", "RJL", "MLN", "MAH", "AGP", "PMI", "PNA", "REU",
    "QSA", "SLM", "EAS", "SCQ", "SDR", "SVQ", "TFN", "TFS", "VLC", "VLL",
    "VGO", "VIT", "ZAZ",
}


class FlightSource(ABC):
    name: str
    requires_auth: bool
    supports_scopes: list[Scope]

    @abstractmethod
    def fetch(self, scope: Scope) -> pl.DataFrame:
        """Return routes as a DataFrame matching SCHEMA."""
        ...

    @abstractmethod
    def is_available(self) -> bool:
        """Return False if required credentials or data files are absent."""
        ...
