"""Tests for KEFSource — parsing runs offline; data checks use the committed parquet."""
import csv
import json
from pathlib import Path

import polars as pl
import pytest

from pyfly.sources.base import SCHEMA
from pyfly.sources.kef import DESTINATIONS, KEFSource

# Trimmed from kefairport.com/destinations __NEXT_DATA__ (2026-11-13)
SAMPLE = {"props": {"pageProps": {"extraData": [
    {"title": "Copenhagen", "country": "Denmark", "flights": [
        {"airline": "Icelandair", "airport": "CPH", "time_period": "All year"},
        {"airline": "SAS", "airport": "CPH", "time_period": "All year"}]},
    {"title": "Paris Orly", "country": "France", "flights": [
        {"airline": "Transavia", "airport": "", "time_period": "Summer"}]},
    {"title": "Newcastle", "country": "UK", "flights": [
        {"airline": "EasyJet", "airport": "NCL", "time_period": "Winter"}]},
    {"title": "Atlantis", "country": "Nowhere", "flights": [
        {"airline": "Myth Air", "airport": "", "time_period": "All year"}]},
]}}}
SAMPLE_HTML = f'<script id="__NEXT_DATA__" type="application/json">{json.dumps(SAMPLE)}</script>'


def test_kef_parses_flights_with_and_without_codes():
    items = KEFSource._extract_destinations(SAMPLE_HTML)
    rows, unknown = KEFSource._routes(items)
    assert {(r["dest_iata"], r["airline_name"]) for r in rows} == {
        ("CPH", "Icelandair"), ("CPH", "SAS"),
        ("ORY", "Transavia"),     # empty code -> title mapping
        ("NCL", "easyJet"),       # alias
    }
    assert unknown == {"Atlantis"}
    assert {r["origin_iata"] for r in rows} == {"KEF"}


def test_kef_mapping_codes_have_coordinates():
    with open("data/airports.csv", encoding="utf-8") as f:
        known = {r["iata_code"] for r in csv.DictReader(f) if r["iata_code"] and r["latitude_deg"]}
    assert set(DESTINATIONS.values()) <= known


def test_kef_snapshot():
    path = Path("data/routes_kef.parquet")
    if not path.exists():
        pytest.skip("not scraped yet")
    df = pl.read_parquet(path)
    assert len(df) > 50
    for col, dtype in SCHEMA.items():
        assert df[col].dtype == dtype, f"{col}: expected {dtype}, got {df[col].dtype}"
    assert df["source"].unique().to_list() == ["kef"]
    assert df["origin_iata"].unique().to_list() == ["KEF"]
    assert df["dest_lat"].null_count() == 0
    assert df.select(["dest_iata", "airline_name"]).is_duplicated().sum() == 0
