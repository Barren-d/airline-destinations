"""Smoke tests for OpenFlightsSource — will fail until Phase 4b is complete."""
import pytest
import polars as pl
from pyfly.sources.openflights import OpenFlightsSource
from pyfly.sources.base import Scope, SCHEMA


@pytest.fixture(scope="module")
def of_aena_df():
    return OpenFlightsSource().fetch(Scope.AENA)


@pytest.fixture(scope="module")
def of_european_df():
    return OpenFlightsSource().fetch(Scope.EUROPEAN)


def test_openflights_is_available():
    assert OpenFlightsSource().is_available()


def test_openflights_aena_scope(of_aena_df):
    assert len(of_aena_df) > 0
    origins = set(of_aena_df["origin_iata"].to_list())
    assert "BCN" in origins
    assert "MAD" in origins


def test_openflights_european_scope(of_european_df):
    assert len(of_european_df) > 0


def test_openflights_schema(of_aena_df):
    for col, dtype in SCHEMA.items():
        assert col in of_aena_df.columns, f"Missing column: {col}"
        assert of_aena_df[col].dtype == dtype, f"{col}: expected {dtype}, got {of_aena_df[col].dtype}"


def test_openflights_no_null_coords(of_aena_df):
    for col in ["origin_lat", "origin_lon", "dest_lat", "dest_lon"]:
        assert of_aena_df[col].is_null().sum() == 0, f"Null values in {col}"


def test_openflights_airline_names_resolved(of_aena_df):
    names = of_aena_df["airline_name"].drop_nulls().to_list()
    assert len(names) > 0
    for name in names[:20]:
        assert len(name) > 2, f"Airline name looks like a raw code: {name}"


def test_openflights_source_tag(of_aena_df):
    assert of_aena_df["source"].unique().to_list() == ["openflights_2017"]
