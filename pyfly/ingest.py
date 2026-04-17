"""Orchestrates fetch → enrich → write for a given source and scope."""
from .sources.base import Scope
from .sources.aena import AENASource
from .sources.openflights import OpenFlightsSource
from .sources.opensky import OpenSkySource

SOURCES = {
    "aena": AENASource,
    "openflights": OpenFlightsSource,
    "opensky": OpenSkySource,
}


def run(source_name: str, scope: Scope):
    raise NotImplementedError
