"""Tests for ANASource — parser and config run offline; data checks use the committed parquet."""
import csv
import json
from pathlib import Path

import pytest
import polars as pl
from pyfly.sources.ana import ANASource, AIRPORTS, CONFIG_PATH
from pyfly.sources.base import Scope, SCHEMA

# Trimmed copy of the live markup (lisbonairport.pt, 2026-11-11)
SAMPLE_HTML = """
<nav class="filter-list"><ul class="dropdown-menu filter-countries">
  <li class="filter"><button class="all selected" data-filter="*"><span>All</span></button></li>
  <li class="filter"><button data-filter=".continent-abu-dhabi"><span>Abu Dhabi</span></button></li>
  <li class="filter"><button data-filter=".continent-zurich"><span>Zurich</span></button></li>
  <li class="filter"><button data-filter=".continent-barcelona"><span>Barcelona</span></button></li>
</ul></nav>
<article class="card continent-abu-dhabi visible"><div class="card-header">
  <h4 class="card-header-title">Etihad Airways</h4></div></article>
<article class="card continent-barcelona continent-zurich visible"><div class="card-header">
  <h4 class="card-header-title">EasyJet Switzerland</h4></div></article>
<article class="card"><div class="card-header">
  <h4 class="card-header-title">Icelandair</h4></div></article>
"""


@pytest.fixture(scope="module")
def ana_df():
    cached = Path("data/routes_ana.parquet")
    if cached.exists():
        # Fast path: validate already-scraped data without re-scraping
        return pl.read_parquet(cached)
    # Slow path: live scrape (only runs if parquet not present)
    return ANASource().fetch(Scope.PORTUGAL)


def test_ana_parse_single_and_multi_airline_cards():
    rows = ANASource()._parse_destinations(SAMPLE_HTML, "LIS")
    got = {(r["dest_label"], r["airline_name"]) for r in rows}
    assert got == {
        ("Abu Dhabi", "Etihad Airways"),
        ("Barcelona", "EasyJet Switzerland"),
        ("Zurich", "EasyJet Switzerland"),
    }
    assert {r["origin_iata"] for r in rows} == {"LIS"}


def test_ana_config_codes_exist_in_airports_csv():
    mapping = {k: v for k, v in json.loads(CONFIG_PATH.read_text(encoding="utf-8")).items()
               if not k.startswith("_")}
    with open("data/airports.csv", encoding="utf-8") as f:
        known = {r["iata_code"] for r in csv.DictReader(f) if r["iata_code"] and r["latitude_deg"]}
    missing = {label: code for label, code in mapping.items() if code and code not in known}
    assert not missing, f"Mapped IATA codes without coordinates: {missing}"


def test_ana_is_available():
    assert ANASource().is_available()


def test_ana_returns_rows(ana_df):
    assert len(ana_df) > 0, "ANA scraper returned 0 routes"


def test_ana_schema(ana_df):
    for col, dtype in SCHEMA.items():
        assert col in ana_df.columns, f"Missing column: {col}"
        assert ana_df[col].dtype == dtype, f"{col}: expected {dtype}, got {ana_df[col].dtype}"


def test_ana_origins_are_ana_airports(ana_df):
    origins = set(ana_df["origin_iata"].to_list())
    assert "LIS" in origins and "OPO" in origins
    assert origins <= set(AIRPORTS), f"Unexpected origins: {origins - set(AIRPORTS)}"


def test_ana_coordinates_valid(ana_df):
    for col in ["origin_lat", "origin_lon", "dest_lat", "dest_lon"]:
        assert ana_df[col].is_null().sum() == 0
    assert ana_df["dest_lat"].is_between(-90, 90).all()
    assert ana_df["dest_lon"].is_between(-180, 180).all()


def test_ana_no_duplicate_routes(ana_df):
    key = ["origin_iata", "dest_iata", "airline_name"]
    assert ana_df.select(key).is_duplicated().sum() == 0


def test_ana_source_tag(ana_df):
    assert ana_df["source"].unique().to_list() == ["ana"]
