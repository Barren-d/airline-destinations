"""Route map page — interactive great circle visualisation."""
import sys
from pathlib import Path

_ROOT = Path(__file__).parent.parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

import pycountry
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
    "openflights_global": "All countries — OpenFlights database circa 2017. Filter by origin country to explore.",
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
def airport_lookup() -> dict[str, dict]:
    """iata_code → {iso_country, name}"""
    path = DATA_DIR / "airports.csv"
    if not path.exists():
        return {}
    df = pl.read_csv(path).select(["iata_code", "iso_country", "name"])
    return {
        r["iata_code"]: {"iso_country": r["iso_country"], "name": r["name"]}
        for r in df.iter_rows(named=True)
        if r["iata_code"]
    }


@st.cache_data
def iso_to_country_name(iso: str) -> str:
    c = pycountry.countries.get(alpha_2=iso)
    return c.name if c else iso


def _view_for_airports(lats: list[float], lons: list[float]) -> pdk.ViewState:
    """Return a ViewState centred on the bounding box of the given coordinates."""
    if not lats:
        return pdk.ViewState(latitude=40.0, longitude=-4.5, zoom=5.0, pitch=25)
    center_lat = (max(lats) + min(lats)) / 2
    center_lon = (max(lons) + min(lons)) / 2
    span = max(max(lats) - min(lats), max(lons) - min(lons))
    zoom = 7 if span < 5 else 5 if span < 15 else 4 if span < 40 else 3 if span < 80 else 2
    return pdk.ViewState(latitude=center_lat, longitude=center_lon, zoom=zoom, pitch=25)


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
        "Historical — Global (2017)": "openflights_global",
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
        cli_map = {
            "aena": "uv run python -m pyfly --source aena --scope aena",
            "openflights_global": "uv run python -m pyfly --source openflights --scope global_all",
            "opensky": "uv run python -m pyfly --source opensky --scope aena",
        }
        st.warning("No data for this source. Run:")
        st.code(cli_map.get(selected_source, f"uv run python -m pyfly --source {selected_source}"))
        st.stop()

    st.subheader("Filters")

    alookup = airport_lookup()
    # iata → iso_country
    cmap = {iata: info["iso_country"] for iata, info in alookup.items()}

    # Country dropdowns: ISO code → "Full Name"
    origin_iso_codes = sorted({
        cmap.get(iata, "") for iata in df["origin_iata"].unique().to_list() if cmap.get(iata)
    })
    origin_country_options = {iso_to_country_name(c): c for c in origin_iso_codes}

    if selected_source == "openflights_global":
        default_name = iso_to_country_name("ES")
        default_idx = list(origin_country_options).index(default_name) if default_name in origin_country_options else 0
        origin_country_name = st.selectbox("Origin country", list(origin_country_options), index=default_idx)
        origin_country_filter = origin_country_options[origin_country_name]
    else:
        origin_country_name = st.selectbox("Origin country", ["All"] + list(origin_country_options))
        origin_country_filter = origin_country_options.get(origin_country_name, "All")

    # Airport dropdown: "Name (IATA)" labels
    airport_iatas = sorted(df["origin_iata"].unique().to_list())
    airport_labels = {
        f"{alookup[i]['name']} ({i})" if i in alookup else i: i
        for i in airport_iatas
    }
    origin_label = st.selectbox("Origin airport", ["All"] + list(airport_labels), key=f"origin_airport_{origin_country_filter}")
    origin_filter = airport_labels.get(origin_label, "All") if origin_label != "All" else "All"

    all_airlines = ["All"] + sorted(
        x for x in df["airline_name"].drop_nulls().unique().to_list() if x
    )
    airline_filter = st.selectbox("Airline", all_airlines)

    dest_iso_codes = sorted({
        cmap.get(iata, "") for iata in df["dest_iata"].unique().to_list() if cmap.get(iata)
    })
    dest_country_options = {iso_to_country_name(c): c for c in dest_iso_codes}
    dest_country_name = st.selectbox("Dest country", ["All"] + list(dest_country_options))
    dest_country_filter = dest_country_options.get(dest_country_name, "All") if dest_country_name != "All" else "All"

    st.markdown("---")
    st.subheader("Arc density")
    max_arcs = st.slider("Max routes", min_value=100, max_value=2000, value=500, step=100)

    st.markdown("---")
    stats_slot = st.empty()


# ---------------------------------------------------------------------------
# Filtering
# ---------------------------------------------------------------------------

filtered = df

if origin_country_filter != "All" and cmap:
    in_origin_country = {k for k, v in cmap.items() if v == origin_country_filter}
    filtered = filtered.filter(pl.col("origin_iata").is_in(in_origin_country))

if origin_filter != "All":
    filtered = filtered.filter(pl.col("origin_iata") == origin_filter)

if airline_filter != "All":
    filtered = filtered.filter(pl.col("airline_name") == airline_filter)

if dest_country_filter != "All" and cmap:
    in_dest_country = {k for k, v in cmap.items() if v == dest_country_filter}
    filtered = filtered.filter(pl.col("dest_iata").is_in(in_dest_country))

capped = False
if origin_filter == "All" and origin_country_filter == "All" and len(filtered) > max_arcs:
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
if age is not None:
    st.caption(f"Data last retrieved {age:.0f}h ago.")

if capped:
    st.info(f"Showing top {max_arcs} routes. Adjust with the sidebar slider.")

if filtered.is_empty():
    st.warning("No routes match the current filters.")
    st.stop()

arc_data = filtered.select([
    "origin_iata", "origin_lat", "origin_lon",
    "dest_iata", "dest_lat", "dest_lon",
    "airline_name", "source",
]).with_columns(
    # Normalise dest_lon so the arc always takes the shorter path across the antimeridian.
    # ArcLayer interpolates longitude linearly, so keeping the difference within ±180°
    # ensures the arc goes the geographically correct direction.
    pl.when((pl.col("dest_lon") - pl.col("origin_lon")) > 180)
      .then(pl.col("dest_lon") - 360)
      .when((pl.col("dest_lon") - pl.col("origin_lon")) < -180)
      .then(pl.col("dest_lon") + 360)
      .otherwise(pl.col("dest_lon"))
      .alias("dest_lon")
).to_pandas()

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

origin_lats = filtered["origin_lat"].drop_nulls().to_list()
origin_lons = filtered["origin_lon"].drop_nulls().to_list()
view_state = _view_for_airports(origin_lats, origin_lons)

st.pydeck_chart(
    pdk.Deck(
        layers=[arc_layer],
        initial_view_state=view_state,
        tooltip=tooltip,
        map_style="https://basemaps.cartocdn.com/gl/dark-matter-gl-style/style.json",
    ),
    height=680,
    width="stretch",
)

st.caption(SOURCE_INFO.get(selected_source, ""))
