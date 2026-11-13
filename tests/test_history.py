"""Tests for the scrape archive (db.write_routes) and snapshot comparison (pyfly.history)."""
from datetime import date

import polars as pl
import pytest

import pyfly.db as db
from pyfly.history import list_snapshots, new_routes_within, route_changes
from pyfly.sources.base import SCHEMA


def _routes(rows: list[tuple[str, str, str]], source: str = "ana") -> pl.DataFrame:
    return pl.DataFrame(
        [{"origin_iata": o, "origin_lat": 38.7, "origin_lon": -9.1,
          "dest_iata": d, "dest_lat": 41.3, "dest_lon": 2.1,
          "airline_iata": None, "airline_name": a, "source": source} for o, d, a in rows],
        schema=SCHEMA,
    )


@pytest.fixture
def tmp_data(tmp_path, monkeypatch):
    """Point the DB, parquet snapshots and history archive at a temp dir."""
    monkeypatch.setattr(db, "DB_PATH", tmp_path / "test.ddb")
    monkeypatch.setattr(db, "HISTORY_DIR", tmp_path / "history")
    monkeypatch.setitem(db.PARQUET_SOURCES, "ana", tmp_path / "routes_ana.parquet")
    db._init_tables()
    return tmp_path


def _write_snapshot(tmp_data, day: str, df: pl.DataFrame, source: str = "ana") -> None:
    out = tmp_data / "history" / source / f"{day}.parquet"
    out.parent.mkdir(parents=True, exist_ok=True)
    df.write_parquet(out)


def test_write_routes_archives_one_file_per_day(tmp_data):
    db.write_routes(_routes([("LIS", "BCN", "TAP")]), "ana")
    db.write_routes(_routes([("LIS", "BCN", "TAP"), ("LIS", "MAD", "Iberia")]), "ana")

    snapshots = list_snapshots("ana")
    assert len(snapshots) == 1, "a same-day rescrape should replace that day's file"
    archived = pl.read_parquet(snapshots[0])
    assert len(archived) == 2
    assert "scraped_at" in archived.columns
    assert snapshots[0].stem == f"{archived['scraped_at'].max():%Y-%m-%d}"


def test_static_sources_are_not_archived(tmp_data, monkeypatch):
    monkeypatch.setitem(db.PARQUET_SOURCES, "openflights_global", tmp_data / "of.parquet")
    db.write_routes(_routes([("LIS", "BCN", "TAP")], source="openflights_global"), "openflights_global")
    assert list_snapshots("openflights_global") == []


def test_route_changes_needs_two_snapshots(tmp_data):
    assert route_changes("ana") is None
    _write_snapshot(tmp_data, "2026-04-17", _routes([("LIS", "BCN", "TAP")]))
    assert route_changes("ana") is None


def test_route_changes_compares_latest_two(tmp_data):
    _write_snapshot(tmp_data, "2026-01-01", _routes([("LIS", "OPO", "TAP")]))  # ignored: older
    _write_snapshot(tmp_data, "2026-04-17", _routes([
        ("LIS", "BCN", "TAP"), ("LIS", "BCN", "Vueling"),
        ("LIS", "IBZ", "Ryanair"),
    ]))
    _write_snapshot(tmp_data, "2026-11-11", _routes([
        ("LIS", "BCN", "Vueling"),                      # TAP left, route still flown -> no change
        ("LIS", "LPA", "Ryanair"), ("LIS", "LPA", "Binter"),
    ]))

    changes = route_changes("ana")
    assert (changes.previous, changes.latest) == (date(2026, 4, 17), date(2026, 11, 11))
    assert set(zip(changes.added["dest_iata"], changes.added["airline_name"])) == {
        ("LPA", "Ryanair"), ("LPA", "Binter"),
    }
    assert changes.dropped["dest_iata"].to_list() == ["IBZ"]
    assert set(changes.added.columns) == set(SCHEMA)  # same columns as the map -> same filters


def test_new_routes_within_ignores_seasonal_returns(tmp_data):
    _write_snapshot(tmp_data, "2025-06-01", _routes([("LIS", "FAO", "TAP")]))    # outside 365 days
    _write_snapshot(tmp_data, "2025-12-01", _routes([("LIS", "IBZ", "Ryanair")]))  # summer route seen last year
    _write_snapshot(tmp_data, "2026-10-01", _routes([("LIS", "BCN", "TAP")]))
    _write_snapshot(tmp_data, "2026-11-01", _routes([
        ("LIS", "BCN", "TAP"),       # flown last month -> not new
        ("LIS", "IBZ", "Ryanair"),   # back after a gap -> new vs previous, NOT new in 12 months
        ("LIS", "FAO", "TAP"),       # last seen > 12 months ago -> new
        ("LIS", "LPA", "Binter"),    # never seen -> new
    ]))
    year = new_routes_within("ana", days=365)
    assert year.latest == date(2026, 11, 1)
    assert year.compared == [date(2025, 12, 1), date(2026, 10, 1)]
    assert set(year.routes["dest_iata"]) == {"FAO", "LPA"}
    assert set(route_changes("ana").added["dest_iata"]) == {"IBZ", "FAO", "LPA"}


def test_new_routes_within_needs_an_earlier_snapshot(tmp_data):
    assert new_routes_within("ana") is None
    _write_snapshot(tmp_data, "2026-11-01", _routes([("LIS", "BCN", "TAP")]))
    assert new_routes_within("ana") is None
