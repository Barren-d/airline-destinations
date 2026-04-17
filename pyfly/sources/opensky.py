"""OpenSky live source — actual flights flown via OpenSky Network REST API."""
from .base import FlightSource, Scope
import polars as pl


class OpenSkySource(FlightSource):
    name = "OpenSky"
    requires_auth = True
    supports_scopes = [Scope.AENA, Scope.EUROPEAN, Scope.GLOBAL_TOP_100, Scope.CUSTOM]

    def fetch(self, scope: Scope) -> pl.DataFrame:
        raise NotImplementedError

    def is_available(self) -> bool:
        raise NotImplementedError
