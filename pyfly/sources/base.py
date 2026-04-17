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
    "MAD", "BCN", "AGP", "PMI", "ALC", "SVQ", "TFS", "LPA", "IBZ", "VLC",
    "BIO", "SDR", "VGO", "SCQ", "ACE", "FUE", "MAH", "GRX", "ZAZ", "LEI",
    "OVD", "XRY", "VIT", "PNA", "VLL", "RMU", "TFN", "VDE", "GMZ", "BJZ",
    "HSK", "ABC", "ODB", "RJL", "LEN", "QSA", "SLM", "MRS", "REU", "GRO",
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
