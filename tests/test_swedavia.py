"""Tests for SwedaviaSource — parser and config run offline; data checks use the committed parquet."""
import csv
import json
from pathlib import Path

import polars as pl
import pytest

from pyfly.sources.base import SCHEMA
from pyfly.sources.swedavia import AIRPORTS, CONFIG_PATH, SwedaviaSource

# Trimmed from /services/destinations/alldestinations/BMA/en and /GOT/en (2026-11-11)
SAMPLE_ITEMS = [
    {"name": "Gotland", "country": "Sweden", "isCharterDestination": False,
     "airlines": [{"name": "BROMMAFLYG", "url": "https://brommaflyg.se/"}]},
    {"name": "London", "country": "United Kingdom", "isCharterDestination": False,
     "airlines": [{"name": "BRITISH AIRWAYS"}, {"name": "RYANAIR"}]},
    {"name": "Hurghada", "country": "Egypt", "isCharterDestination": True, "airlines": []},
]


def test_swedavia_parse_one_row_per_airline():
    rows = SwedaviaSource._parse_destinations(SAMPLE_ITEMS, "GOT")
    assert {(r["dest_name"], r["airline_name"]) for r in rows} == {
        ("Gotland", "BROMMAFLYG"), ("London", "BRITISH AIRWAYS"), ("London", "RYANAIR"),
        ("Hurghada", None),  # charter destination without a listed airline
    }


def test_swedavia_resolve_per_airline_airport():
    london = {"default": "LHR", "NORWEGIAN": "LGW", "RYANAIR": "STN"}
    assert SwedaviaSource._resolve(london, "RYANAIR") == "STN"
    assert SwedaviaSource._resolve(london, "BRITISH AIRWAYS") == "LHR"
    assert SwedaviaSource._resolve(london, None) == "LHR"
    assert SwedaviaSource._resolve("VBY", "BROMMAFLYG") == "VBY"
    assert SwedaviaSource._resolve(None, "ANY") is None


def test_swedavia_config_codes_exist_in_airports_csv():
    mapping = {k: v for k, v in json.loads(CONFIG_PATH.read_text(encoding="utf-8")).items()
               if not k.startswith("_")}
    with open("data/airports.csv", encoding="utf-8") as f:
        known = {r["iata_code"] for r in csv.DictReader(f) if r["iata_code"] and r["latitude_deg"]}
    codes = {c for v in mapping.values() if v for c in ([v] if isinstance(v, str) else v.values())}
    assert codes <= known, f"Mapped IATA codes without coordinates: {codes - known}"
    for name, v in mapping.items():
        if isinstance(v, dict):
            assert "default" in v, f"{name}: per-airline mapping needs a default"


def test_swedavia_lists_10_airports():
    assert len(AIRPORTS) == 10 and len(set(AIRPORTS)) == 10


def test_swedavia_snapshot():
    path = Path("data/routes_swedavia.parquet")
    if not path.exists():
        pytest.skip("not scraped yet")
    df = pl.read_parquet(path)
    assert len(df) > 0
    for col, dtype in SCHEMA.items():
        assert df[col].dtype == dtype, f"{col}: expected {dtype}, got {df[col].dtype}"
    assert df["source"].unique().to_list() == ["swedavia"]
    assert set(df["origin_iata"]) <= set(AIRPORTS)
    assert df["dest_lat"].null_count() == 0
    assert (df["origin_iata"] != df["dest_iata"]).all()
    assert df.select(["origin_iata", "dest_iata", "airline_name"]).is_duplicated().sum() == 0
    assert "scraped_at" in df.columns
