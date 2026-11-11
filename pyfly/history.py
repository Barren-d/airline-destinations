"""Read the scrape archive written by db.write_routes and compare snapshots."""
from dataclasses import dataclass
from datetime import date
from pathlib import Path

import polars as pl

from . import db

ROUTE_KEY = ["origin_iata", "dest_iata"]


@dataclass
class RouteChanges:
    """Routes that appeared or disappeared between two scrapes of one source.

    added / dropped hold the standard route rows (one per airline), so the same filters as
    the main map apply to them. A route is an origin→destination pair: an airline joining
    or leaving a route that other airlines still fly is not counted as a change.
    """
    previous: date
    latest: date
    added: pl.DataFrame
    dropped: pl.DataFrame


def list_snapshots(source: str) -> list[Path]:
    """Archived snapshots for source, oldest first (filenames are YYYY-MM-DD)."""
    return sorted((db.HISTORY_DIR / source).glob("*.parquet"))


def route_changes(source: str) -> RouteChanges | None:
    """Compare the two most recent snapshots of source, or None if there are fewer than two."""
    snapshots = list_snapshots(source)
    if len(snapshots) < 2:
        return None
    prev_path, latest_path = snapshots[-2], snapshots[-1]
    prev = pl.read_parquet(prev_path).drop("scraped_at", strict=False)
    latest = pl.read_parquet(latest_path).drop("scraped_at", strict=False)

    prev_routes = prev.select(ROUTE_KEY).unique()
    latest_routes = latest.select(ROUTE_KEY).unique()
    return RouteChanges(
        previous=date.fromisoformat(prev_path.stem),
        latest=date.fromisoformat(latest_path.stem),
        added=latest.join(prev_routes, on=ROUTE_KEY, how="anti"),
        dropped=prev.join(latest_routes, on=ROUTE_KEY, how="anti"),
    )
