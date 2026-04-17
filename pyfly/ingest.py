"""Orchestrates fetch → write for a given source and scope."""
from .sources.base import Scope
from .sources.aena import AENASource
from .sources.openflights import OpenFlightsSource
from .sources.opensky import OpenSkySource
from .db import init_db, write_routes

SOURCES = {
    "aena": AENASource,
    "openflights": OpenFlightsSource,
    "opensky": OpenSkySource,
}


def run(source_name: str, scope: Scope, custom_iata: set[str] | None = None) -> None:
    source_cls = SOURCES.get(source_name)
    if source_cls is None:
        raise ValueError(f"Unknown source '{source_name}'. Choose from: {list(SOURCES)}")

    source = source_cls()

    if not source.is_available():
        raise RuntimeError(
            f"Source '{source_name}' is not available. "
            f"Check credentials or required data files."
        )

    if scope not in source.supports_scopes:
        raise ValueError(
            f"Source '{source_name}' does not support scope '{scope.value}'. "
            f"Supported: {[s.value for s in source.supports_scopes]}"
        )

    print(f"Fetching {source.name} / scope={scope.value} ...")
    init_db()

    kwargs = {}
    if custom_iata is not None:
        kwargs["custom_iata"] = custom_iata

    df = source.fetch(scope, **kwargs)

    print(f"Writing {len(df)} rows to DuckDB (source={source.name}) ...")
    write_routes(df, df["source"][0] if len(df) > 0 else source_name)

    print(f"Done. {len(df)} routes, "
          f"{df['dest_iata'].n_unique()} destinations, "
          f"{df['airline_iata'].n_unique()} airlines.")
