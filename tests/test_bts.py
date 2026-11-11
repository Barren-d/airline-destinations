"""Tests for BTSSource — route filtering runs offline; data checks use the committed parquet."""
from pathlib import Path

import polars as pl
import pytest

from pyfly.history import list_snapshots
from pyfly.sources.base import SCHEMA
from pyfly.sources.bts import MIN_DEPARTURES, BTSSource


def _segment(origin, dest, carrier, deps, cls="F", config=1, name=None):
    return {"ORIGIN": origin, "DEST": dest, "CARRIER": carrier, "CARRIER_NAME": name or carrier,
            "DEPARTURES_PERFORMED": float(deps), "CLASS": cls, "AIRCRAFT_CONFIG": config,
            "YEAR": 2026, "MONTH": 7}


def test_bts_routes_keep_weekly_scheduled_passenger_service():
    raw = pl.DataFrame([
        _segment("JFK", "LAX", "AA", 3), _segment("JFK", "LAX", "AA", 2),   # two aircraft types -> 5
        _segment("JFK", "BOS", "B6", MIN_DEPARTURES - 1),                    # too rare
        _segment("JFK", "MIA", "AA", 30, cls="L"),                           # charter
        _segment("JFK", "ORD", "5X", 30, cls="G"),                           # scheduled cargo
        _segment("JFK", "SFO", "AA", 30, config=2),                          # freighter
        _segment("LGA", "BOS", "YX", 579, name="Republic  Airline "),
        _segment("ANC", "ANC", "8V", 40),                                    # return to origin
    ])
    routes = BTSSource._routes(raw)
    assert routes.select(["origin_iata", "dest_iata", "airline_iata"]).rows() == [
        ("JFK", "LAX", "AA"), ("LGA", "BOS", "YX"),
    ]
    assert routes.filter(pl.col("airline_iata") == "YX")["airline_name"].item() == "Republic Airline"


def test_bts_snapshot_is_one_month():
    path = Path("data/routes_bts.parquet")
    if not path.exists():
        pytest.skip("not downloaded yet")
    df = pl.read_parquet(path)
    assert len(df) > 1000
    for col, dtype in SCHEMA.items():
        assert df[col].dtype == dtype, f"{col}: expected {dtype}, got {df[col].dtype}"
    assert df["source"].unique().to_list() == ["bts"]
    assert df["scraped_at"].n_unique() == 1 and df["scraped_at"].max().day == 1  # first of the month
    assert df["dest_lat"].null_count() == 0
    assert (df["origin_iata"] != df["dest_iata"]).all()
    assert df.select(["origin_iata", "dest_iata", "airline_iata"]).is_duplicated().sum() == 0


def test_bts_history_files_are_monthly():
    files = list_snapshots("bts")
    if not files:
        pytest.skip("no history yet")
    assert all(f.stem.endswith("-01") for f in files)
