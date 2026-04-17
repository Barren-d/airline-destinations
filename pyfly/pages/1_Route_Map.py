"""Route map page — interactive great circle visualisation."""
import sys
from pathlib import Path

_ROOT = Path(__file__).parent.parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

import polars as pl
import pydeck as pdk
import streamlit as st

from pyfly.db import get_data_age as _db_age, init_db, read_routes
from pyfly.sources.opensky import OpenSkySource

st.set_page_config(
    page_title="Route Map — PyFly",
    page_icon="🗺️",
    layout="wide",
    initial_sidebar_state="expanded",
)

DATA_DIR = Path(__file__).parent.parent.parent / "data"

SOURCE_INFO = {
    "aena": "Scraped from aena.es — current scheduled routes.",
    "openflights_2017": "OpenFlights database circa 2017. Useful for pre-COVID comparison.",
    "opensky": "OpenSky Network — actual flights flown in the last 7 days.",
}


@st.cache_data(ttl=300)
def load_data(source: str) -> pl.DataFrame:
    init_db()
    return read_routes(source=source)


@st.cache_data(ttl=300)
def data_age_hours(source: str) -> float | None:
    return _db_age(source)


@st.cache_data
def country_lookup() -> dict[str, str]:
    path = DATA_DIR / "airports.csv"
    if not path.exists():
        return {}
    df = pl.read_csv(path).select(["iata_code", "iso_country"])
    return {r["iata_code"]: r["iso_country"] for r in df.iter_rows(named=True)}


def _age_label(source: str) -> str:
    age = data_age_hours(source)
    if age is None:
        return "no data"
    if age < 6:
        return f"✅ {age:.0f}h ago"
    if age < 24:
        return f"🟡 {age:.0f}h ago"
    return f"🔴 {age:.0f}h ago"


def _opensky_available() -> bool:
    try:
        return OpenSkySource().is_available()
    except Exception:
        return False


# ---------------------------------------------------------------------------
# Sidebar
# ---------------------------------------------------------------------------

with st.sidebar:
    st.subheader("Data source")
    sky_ok = _opensky_available()
    source_options = {
        "AENA Live": "aena",
        "Historical (2017)": "openflights_2017",
        f"OpenSky {'✓' if sky_ok else '(no credentials)'}": "opensky",
    }
    source_choice = st.radio("Source", list(source_options.keys()), label_visibility="collapsed")
    selected_source = source_options[source_choice]

    if selected_source == "opensky" and not sky_ok:
        st.warning("Set OPENSKY_USERNAME and OPENSKY_PASSWORD in .env to enable OpenSky.")

    st.caption(f"Data age: {_age_label(selected_source)}")
    if st.button("↻ Refresh", help="Clear cache and re-read from DuckDB"):
        st.cache_data.clear()
        st.rerun()

    st.markdown("---")

    df = load_data(selected_source)

    if df.is_empty():
        src_cli = selected_source.replace("_2017", "")
        st.warning("No data for this source. Run:")
        st.code(f"uv run python -m pyfly --source {src_cli} --scope aena")
        st.stop()

    st.subheader("Filters")

    all_origins = ["All"] + sorted(df["origin_iata"].unique().to_list())
    origin_filter = st.selectbox("Origin airport", all_origins)

    all_airlines = ["All"] + sorted(
        x for x in df["airline_name"].drop_nulls().unique().to_list() if x
    )
    airline_filter = st.selectbox("Airline", all_airlines)

    cmap = country_lookup()
    dest_countries = sorted({
        cmap.get(iata, "") for iata in df["dest_iata"].unique().to_list() if cmap.get(iata)
    })
    country_filter = st.selectbox("Dest country", ["All"] + dest_countries)

    st.markdown("---")
    st.subheader("Arc density")
    max_arcs = st.slider("Max routes", min_value=100, max_value=2000, value=500, step=100)

    st.markdown("---")
    stats_slot = st.empty()


# ---------------------------------------------------------------------------
# Filtering
# ---------------------------------------------------------------------------

filtered = df

if origin_filter != "All":
    filtered = filtered.filter(pl.col("origin_iata") == origin_filter)

if airline_filter != "All":
    filtered = filtered.filter(pl.col("airline_name") == airline_filter)

if country_filter != "All" and cmap:
    in_country = {k for k, v in cmap.items() if v == country_filter}
    filtered = filtered.filter(pl.col("dest_iata").is_in(in_country))

capped = False
if origin_filter == "All" and len(filtered) > max_arcs:
    filtered = filtered.head(max_arcs)
    capped = True

with stats_slot:
    st.markdown(
        f"**Routes:** {len(filtered)}  \n"
        f"**Destinations:** {filtered['dest_iata'].n_unique()}  \n"
        f"**Airlines:** {filtered['airline_iata'].n_unique()}"
    )


# ---------------------------------------------------------------------------
# Map
# ---------------------------------------------------------------------------

age = data_age_hours(selected_source)
if age is not None and age > 48:
    st.error(f"⚠️ Data is {age:.0f}h old — the scraper may need attention.")
elif age is not None and age > 6:
    st.warning(f"Data is {age:.0f}h old.")

if capped:
    st.info(f"Showing top {max_arcs} routes. Adjust with the sidebar slider.")

if filtered.is_empty():
    st.warning("No routes match the current filters.")
    st.stop()

arc_data = filtered.select([
    "origin_iata", "origin_lat", "origin_lon",
    "dest_iata", "dest_lat", "dest_lon",
    "airline_name", "source",
]).to_pandas()

arc_layer = pdk.Layer(
    "ArcLayer",
    data=arc_data,
    get_source_position=["origin_lon", "origin_lat"],
    get_target_position=["dest_lon", "dest_lat"],
    get_source_color=[255, 140, 0, 160],
    get_target_color=[0, 128, 255, 160],
    get_width=1.5,
    pickable=True,
    auto_highlight=True,
)

tooltip = {
    "html": (
        "<b>{dest_iata}</b><br/>"
        "Airline: {airline_name}<br/>"
        "Source: {source}"
    ),
    "style": {"backgroundColor": "#1a1a2e", "color": "white", "fontSize": "13px"},
}

st.pydeck_chart(
    pdk.Deck(
        layers=[arc_layer],
        initial_view_state=pdk.ViewState(
            latitude=38.5,   # centre lower to balance Canary Islands weight
            longitude=-5.0,
            zoom=4.8,
            pitch=45,        # steeper angle fills vertical space better
            bearing=-10,     # slight rotation breaks the rectangular feel
        ),
        tooltip=tooltip,
        map_style="https://basemaps.cartocdn.com/gl/dark-matter-gl-style/style.json",
    ),
    height=680,
    width="stretch",
)

st.caption(SOURCE_INFO.get(selected_source, ""))
