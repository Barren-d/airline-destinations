"""Smoke tests for AENASource — will fail until Phase 4 is complete."""
import pytest
import polars as pl
from pyfly.sources.aena import AENASource
from pyfly.sources.base import Scope, SCHEMA, AENA_IATA


@pytest.fixture(scope="module")
def aena_df():
    parquet = pl.Path("data/routes_aena.parquet") if hasattr(pl, "Path") else None
    from pathlib import Path
    cached = Path("data/routes_aena.parquet")
    if cached.exists():
        # Fast path: validate already-scraped data without re-scraping
        return pl.read_parquet(cached)
    # Slow path: live scrape (only runs if parquet not present)
    return AENASource().fetch(Scope.AENA)


def test_aena_is_available():
    assert AENASource().is_available()


def test_aena_returns_rows(aena_df):
    assert len(aena_df) > 0, "AENA scraper returned 0 routes"


def test_aena_schema(aena_df):
    for col, dtype in SCHEMA.items():
        assert col in aena_df.columns, f"Missing column: {col}"
        assert aena_df[col].dtype == dtype, f"{col}: expected {dtype}, got {aena_df[col].dtype}"


def test_aena_bcn_present(aena_df):
    origins = set(aena_df["origin_iata"].to_list())
    assert "BCN" in origins, "BCN not found in scraped origins"


def test_aena_coordinates_valid(aena_df):
    assert aena_df["origin_lat"].is_null().sum() == 0
    assert aena_df["origin_lon"].is_null().sum() == 0
    assert aena_df["dest_lat"].is_null().sum() == 0
    assert aena_df["dest_lon"].is_null().sum() == 0
    assert aena_df["origin_lat"].is_between(-90, 90).all()
    assert aena_df["origin_lon"].is_between(-180, 180).all()


def test_aena_airline_names_resolved(aena_df):
    names = aena_df["airline_name"].drop_nulls().to_list()
    assert len(names) > 0
    for name in names[:20]:
        assert len(name) > 2, f"Airline name looks like a raw code: {name}"


def test_aena_source_tag(aena_df):
    assert aena_df["source"].unique().to_list() == ["aena"]
