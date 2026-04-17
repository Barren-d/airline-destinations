"""Smoke tests for OpenSkySource — will fail until Phase 5 is complete."""
import pytest
import polars as pl
from unittest.mock import patch
from pyfly.sources.opensky import OpenSkySource
from pyfly.sources.base import Scope, SCHEMA


def test_opensky_unavailable_without_credentials():
    with patch.dict("os.environ", {}, clear=True):
        source = OpenSkySource()
        assert not source.is_available(), "Should be unavailable without credentials"


def _opensky_available():
    try:
        return OpenSkySource().is_available()
    except NotImplementedError:
        return False


@pytest.mark.skipif(not _opensky_available(), reason="OpenSky credentials not configured in .env")
class TestOpenSkyWithCredentials:
    @pytest.fixture(scope="class")
    def sky_df(self):
        return OpenSkySource().fetch(Scope.AENA)

    def test_opensky_returns_rows(self, sky_df):
        assert len(sky_df) > 0

    def test_opensky_schema(self, sky_df):
        for col, dtype in SCHEMA.items():
            assert col in sky_df.columns, f"Missing column: {col}"
            assert sky_df[col].dtype == dtype

    def test_opensky_coordinates_valid(self, sky_df):
        for col in ["origin_lat", "origin_lon", "dest_lat", "dest_lon"]:
            assert sky_df[col].is_null().sum() == 0

    def test_opensky_source_tag(self, sky_df):
        assert sky_df["source"].unique().to_list() == ["opensky"]

    def test_opensky_cache_written(self, sky_df):
        from pyfly.db import check_cache
        hit, _ = check_cache("LEBL")
        assert hit, "DuckDB cache should be populated after OpenSky fetch"
