"""Tests for IsaviaSource — parser runs offline; data checks use the committed parquet."""
from pathlib import Path

import polars as pl
import pytest

from pyfly.sources.base import SCHEMA
from pyfly.sources.isavia import AIRPORTS, DESTINATIONS, IsaviaSource

# Trimmed from the Akureyri and Egilsstaðir destination pages (2026-11-11): new cards use h3,
# older ones h2; the last card is an airline-directory link without a title.
SAMPLE_PAGE = """
<h2>Domestic flights</h2>
<div class="card"><div class="card-body"><h3>Reykjavík</h3><p>Icelandair<br/>Daily flights</p></div></div>
<div class="card"><div class="card-body"><h3>Grímsey</h3><p>Norlandair<br/>Flights from three times a week.</p></div></div>
<h2>INTERNATIONAL FLIGHTS</h2>
<div class="card"><div class="card-body"><h3>Nerlerit Inaat, Greenland</h3><p>Norlandair</p></div></div>
<div class="card"><div class="card-body"><h2>Reykjavík</h2><p>Air Iceland Connect<br/>Daily flights</p></div></div>
<div class="card"><div class="card-body"><p>Flugfélög á Reykjavíkurflugvelli</p></div></div>
"""


def test_isavia_parse_cards_new_and_old_layout():
    assert IsaviaSource._parse_cards(SAMPLE_PAGE) == [
        ("Reykjavík", "Icelandair"),
        ("Grímsey", "Norlandair"),
        ("Nerlerit Inaat, Greenland", "Norlandair"),
        ("Reykjavík", "Icelandair"),  # "Air Iceland Connect" renamed
    ]


def test_isavia_every_airport_is_a_known_destination():
    assert set(AIRPORTS.values()) <= set(DESTINATIONS.values())


def test_isavia_snapshot():
    path = Path("data/routes_isavia.parquet")
    if not path.exists():
        pytest.skip("not scraped yet")
    df = pl.read_parquet(path)
    assert len(df) > 0
    for col, dtype in SCHEMA.items():
        assert df[col].dtype == dtype, f"{col}: expected {dtype}, got {df[col].dtype}"
    assert df["source"].unique().to_list() == ["isavia"]
    assert df["dest_lat"].null_count() == 0
    assert (df["origin_iata"] != df["dest_iata"]).all()
    assert "Air Iceland Connect" not in df["airline_name"].to_list()
    assert "scraped_at" in df.columns
