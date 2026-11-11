"""Orchestrates fetch → write for a given source and scope."""
from .sources.base import Scope
from .sources.aena import AENASource
from .sources.ana import ANASource
from .sources.avinor import AvinorSource
from .sources.avinor_feed import AvinorFeedSource
from .sources.swedavia import SwedaviaSource
from .sources.isavia import IsaviaSource
from .sources.bts import BTSSource
from .sources.openflights import OpenFlightsSource
from .sources.opensky import OpenSkySource
from .db import init_db, write_routes

SOURCES = {
    "aena": AENASource,
    "ana": ANASource,
    "avinor": AvinorSource,
    "avinor_feed": AvinorFeedSource,
    "swedavia": SwedaviaSource,
    "isavia": IsaviaSource,
    "bts": BTSSource,
    "openflights": OpenFlightsSource,
    "opensky": OpenSkySource,
}


def run(source_name: str, scope: Scope, custom_iata: set[str] | None = None, backfill: int = 0) -> None:
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

    if backfill > 1:
        _backfill(source, backfill)
        return

    kwargs = {}
    if custom_iata is not None:
        kwargs["custom_iata"] = custom_iata

    df = source.fetch(scope, **kwargs)

    print(f"Writing {len(df)} rows to DuckDB (source={source.name}) ...")
    write_routes(df, df["source"][0] if len(df) > 0 else source_name)

    print(f"Done. {len(df)} routes, "
          f"{df['dest_iata'].n_unique()} destinations, "
          # Scraped sources only have airline_name; airline_iata is null.
          f"{df['airline_name'].drop_nulls().n_unique()} airlines.")


def _backfill(source, months: int) -> None:
    """Archive the previous months as history, then write the latest as the snapshot.

    Only for sources with monthly data (BTS T-100: recent_months / fetch_month).
    """
    if not hasattr(source, "fetch_month"):
        raise ValueError(f"{source.name} has no monthly data to backfill.")
    with source._client() as client:
        periods = source.recent_months(client, months)
        for year, month in periods[:-1]:
            df = source.fetch_month(client, year, month)
            write_routes(df, df["source"][0], snapshot=False)
        df = source.fetch_month(client, *periods[-1])
    write_routes(df, df["source"][0])
    print(f"Done. Latest {periods[-1][0]}-{periods[-1][1]:02d}: {len(df)} routes, "
          f"{df['dest_iata'].n_unique()} destinations, {df['airline_name'].n_unique()} airlines; "
          f"{len(periods) - 1} earlier months archived.")
