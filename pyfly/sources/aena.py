"""AENA live scraper — fetches current scheduled routes from aena.es via playwright."""
from .base import FlightSource, Scope
import polars as pl


class AENASource(FlightSource):
    name = "AENA Live"
    requires_auth = False
    supports_scopes = [Scope.AENA]

    def fetch(self, scope: Scope) -> pl.DataFrame:
        raise NotImplementedError

    def is_available(self) -> bool:
        raise NotImplementedError
