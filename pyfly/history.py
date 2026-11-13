"""Read the scrape archive written by db.write_routes and compare snapshots."""
from dataclasses import dataclass
from datetime import date, timedelta
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


@dataclass
class NewRoutes:
    """Routes in the latest snapshot that appear in none of the earlier snapshots in a window.

    Unlike RouteChanges.added (vs the previous snapshot only), seasonal routes that also ran
    earlier in the window are not "new". compared lists the snapshot dates actually checked, so
    a short history (e.g. two scrapes) is visible to the user.
    """
    latest: date
    compared: list[date]
    routes: pl.DataFrame


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


def new_routes_within(source: str, days: int = 365) -> NewRoutes | None:
    """Routes in the latest snapshot not seen in any snapshot from the preceding `days` days.

    None if there is no earlier snapshot in the window to compare against.
    """
    snapshots = list_snapshots(source)
    if len(snapshots) < 2:
        return None
    latest_path = snapshots[-1]
    latest_day = date.fromisoformat(latest_path.stem)
    window = [p for p in snapshots[:-1] if date.fromisoformat(p.stem) >= latest_day - timedelta(days=days)]
    if not window:
        return None
    seen = pl.concat([pl.read_parquet(p, columns=ROUTE_KEY) for p in window]).unique()
    latest = pl.read_parquet(latest_path).drop("scraped_at", strict=False)
    return NewRoutes(
        latest=latest_day,
        compared=[date.fromisoformat(p.stem) for p in window],
        routes=latest.join(seen, on=ROUTE_KEY, how="anti"),
    )
