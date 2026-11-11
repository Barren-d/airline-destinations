"""Tests for the Avinor sources — parsers run offline; data checks use the committed parquets."""
from pathlib import Path

import polars as pl
import pytest

from pyfly.sources.avinor import AvinorSource
from pyfly.sources.avinor_feed import AIRPORTS, AvinorFeedSource
from pyfly.sources.base import SCHEMA

# Trimmed from https://www.avinor.no/en/airport/bodo/direct-routes/ (2026-11-11)
SAMPLE_PAGE = (
    '<html><script>window.__data={"blocks":[{"customContent":{"blockHeading":"Direct routes",'
    '"origin":{"name":"Bodø","iata":"BOO","lat":67.27,"lon":14.36},"localization":{},"items":['
    '{"nameEn":"Oslo","iata":"OSL","isDomestic":true,"isCharter":false,"isRoute":true},'
    '{"nameEn":"Chania","iata":"CHQ","isDomestic":false,"isCharter":true,"isRoute":false},'
    '{"nameEn":"Molde","iata":"MOL","isDomestic":true,"isCharter":false,"isRoute":false},'
    '{"nameEn":"Bodø","iata":"BOO","isDomestic":true,"isCharter":false,"isRoute":true}'
    ']},"componentName":"DirectFlightsBlock"}]}</script></html>'
)

# Trimmed from https://asrv.avinor.no/XmlFeed/v1.0?airport=ALF&direction=D (2026-11-11)
SAMPLE_FEED = b"""<?xml version="1.0" encoding="iso-8859-1"?>
<airport name="ALF"><flights lastUpdate="2026-11-11T16:00:00Z">
  <flight uniqueID="1"><airline>WF</airline><flight_id>WF927</flight_id><dom_int>D</dom_int>
    <schedule_time>2026-11-11T17:05:00Z</schedule_time><arr_dep>D</arr_dep><airport>TOS</airport></flight>
  <flight uniqueID="2"><airline>WF</airline><flight_id>WF948</flight_id><dom_int>D</dom_int>
    <schedule_time>2026-11-11T18:05:00Z</schedule_time><arr_dep>D</arr_dep><airport>VDS</airport>
    <via_airport>KKN</via_airport></flight>
  <flight uniqueID="3"><airline>SK</airline><flight_id>SK4409</flight_id><dom_int>D</dom_int>
    <schedule_time>2026-11-11T19:05:00Z</schedule_time><arr_dep>D</arr_dep><airport>OSL</airport>
    <via_airport>TOS</via_airport></flight>
</flights></airport>"""


def test_avinor_parse_keeps_scheduled_and_charter_only():
    rows = AvinorSource()._parse_routes(SAMPLE_PAGE)
    assert {(r["origin_iata"], r["dest_iata"]) for r in rows} == {("BOO", "OSL"), ("BOO", "CHQ")}


def test_avinor_parse_page_without_routes():
    assert AvinorSource()._parse_routes("<html>no routes block</html>") == []


def test_avinor_feed_uses_first_stop_as_direct_route():
    rows = AvinorFeedSource._parse_departures(SAMPLE_FEED, "ALF")
    assert {(r["dest_iata"], r["airline_iata"]) for r in rows} == {
        ("TOS", "WF"),   # nonstop
        ("KKN", "WF"),   # ALF -> KKN -> VDS: the direct route is ALF-KKN
        ("TOS", "SK"),   # ALF -> TOS -> OSL
    }


def test_avinor_feed_lists_43_airports():
    assert len(AIRPORTS) == 43 and len(set(AIRPORTS)) == 43


@pytest.mark.parametrize("source", ["avinor", "avinor_feed"])
def test_avinor_snapshots(source):
    path = Path(f"data/routes_{source}.parquet")
    if not path.exists():
        pytest.skip(f"{path} not scraped yet")
    df = pl.read_parquet(path)
    assert len(df) > 0
    for col, dtype in SCHEMA.items():
        assert df[col].dtype == dtype, f"{col}: expected {dtype}, got {df[col].dtype}"
    assert df["source"].unique().to_list() == [source]
    assert df["dest_lat"].null_count() == 0 and df["origin_lat"].null_count() == 0
    assert (df["origin_iata"] != df["dest_iata"]).all(), "self-routes like BOO-BOO"
    assert df.select(["origin_iata", "dest_iata", "airline_name"]).is_duplicated().sum() == 0
    assert "scraped_at" in df.columns


def test_avinor_feed_covers_regional_airports():
    path = Path("data/routes_avinor_feed.parquet")
    if not path.exists():
        pytest.skip("not scraped yet")
    df = pl.read_parquet(path)
    origins = set(df["origin_iata"])
    assert {"OSL", "ALF", "LYR"} <= origins  # a hub, a regional airport, Svalbard
    assert df["airline_name"].null_count() == 0
