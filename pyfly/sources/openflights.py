"""OpenFlights historical source — routes.dat circa 2017, all scopes."""
from .base import FlightSource, Scope
import polars as pl


class OpenFlightsSource(FlightSource):
    name = "Historical (2017)"
    requires_auth = False
    supports_scopes = [Scope.AENA, Scope.EUROPEAN, Scope.GLOBAL_TOP_100, Scope.CUSTOM]

    def fetch(self, scope: Scope) -> pl.DataFrame:
        raise NotImplementedError

    def is_available(self) -> bool:
        raise NotImplementedError
