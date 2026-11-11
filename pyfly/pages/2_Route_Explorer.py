"""Route map page — interactive great circle visualisation."""
import math
import sys
from pathlib import Path

_ROOT = Path(__file__).parent.parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

import pycountry
import polars as pl
import pydeck as pdk
import streamlit as st

from datetime import datetime, timezone

from pyfly.db import HISTORY_SOURCES, get_last_scraped as _db_last_scraped, init_db, read_routes
from pyfly.history import RouteChanges, route_changes
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
    "ana": "Scraped from ANA Aeroportos de Portugal airport sites — current routes for 9 airports incl. Madeira and the Azores.",
    "avinor": "Scraped from avinor.no direct-routes pages — scheduled and charter routes for the 8 Avinor airports that publish them. No airline data.",
    "avinor_feed": "Avinor's open flight data feed — every route flown from Avinor's 43 airports in a 14-day window, with airlines.",
    "openflights_global": "All countries — OpenFlights database circa 2017. Filter by origin country to explore.",
    "opensky": "OpenSky Network — actual flights flown in the last 7 days.",
}


@st.cache_resource
def _ensure_db() -> None:
    """Create tables and hydrate from parquet before anything queries DuckDB."""
    init_db()


@st.cache_data(ttl=300)
def load_data(source: str) -> pl.DataFrame:
    return read_routes(source=source)


@st.cache_data(ttl=300)
def last_scraped(source: str) -> datetime | None:
    return _db_last_scraped(source)


@st.cache_data(ttl=300)
def changes_since_previous(source: str) -> RouteChanges | None:
    return route_changes(source)


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


MAP_HEIGHT_PX = 680

# Default framing per source when no origin country/airport is selected. Fitting every
# origin centres on the Atlantic between the mainland and the Canaries / Azores and makes
# the mainland small, so frame the mainland; the islands are an airport filter away.
DEFAULT_VIEWS = {
    "aena": {"latitude": 40.0, "longitude": -4.0, "zoom": 4.9},
    "ana": {"latitude": 38.0, "longitude": -14.5, "zoom": 4.3},
    # Mainland Norway; Svalbard (LYR, 78°N) is an airport filter away
    "avinor": {"latitude": 63.0, "longitude": 12.5, "zoom": 3.75},
    "avinor_feed": {"latitude": 63.0, "longitude": 12.5, "zoom": 3.75},
}
_FIT_WIDTH_PX = 900  # the map stretches to the page; assume a typical desktop width
_FIT_PADDING = 1.6   # leave room around the airports for the arcs leaving them


def _view_for_airports(lats: list[float], lons: list[float]) -> pdk.ViewState:
    """Return a ViewState that fits the given airports, in Web Mercator like the basemap."""
    if not lats:
        return pdk.ViewState(latitude=40.0, longitude=-4.5, zoom=5.0, pitch=25)

    def merc_y(lat: float) -> float:
        return math.log(math.tan(math.pi / 4 + math.radians(lat) / 2))

    ys = [merc_y(lat) for lat in lats]
    center_y = (max(ys) + min(ys)) / 2
    center_lat = math.degrees(2 * math.atan(math.exp(center_y)) - math.pi / 2)
    center_lon = (max(lons) + min(lons)) / 2

    # deck.gl uses 512px tiles: at zoom z the world is 512 * 2^z px wide.
    # Pick the largest z where both spans fit.
    lon_span = max(max(lons) - min(lons), 1e-6) * _FIT_PADDING
    y_span = max(math.degrees(max(ys) - min(ys)), 1e-6) * _FIT_PADDING
    zoom = min(
        math.log2(_FIT_WIDTH_PX * 360 / (512 * lon_span)),
        math.log2(MAP_HEIGHT_PX * 360 / (512 * y_span)),
    )
    zoom = max(1.5, min(zoom, 7.0))  # a single airport would otherwise zoom to street level
    return pdk.ViewState(latitude=center_lat, longitude=center_lon, zoom=round(zoom, 2), pitch=25)


# Static datasets have no scrape date — their scraped_at is just when they were loaded.
STATIC_SOURCES = {"openflights_global": "OpenFlights dataset, circa 2017 (static)"}


def _age_label(source: str) -> str:
    if source in STATIC_SOURCES:
        return STATIC_SOURCES[source]
    ts = last_scraped(source)
    if ts is None:
        return "no data"
    age = datetime.now(timezone.utc) - ts
    if source == "opensky":  # 24h API cache — freshness is measured in hours
        return f"fetched {age.total_seconds() / 3600:.0f}h ago"
    # Scrapes are triggered manually, so show the date rather than judging freshness
    days = age.days
    ago = "today" if days == 0 else "1 day ago" if days == 1 else f"{days} days ago"
    return f"scraped {ts:%d %b %Y} ({ago})"


def _opensky_available() -> bool:
    try:
        return OpenSkySource().is_available()
    except Exception:
        return False


# ---------------------------------------------------------------------------
# Sidebar
# ---------------------------------------------------------------------------

_ensure_db()

with st.sidebar:
    st.subheader("Data source")
    sky_ok = _opensky_available()
    source_options = {
        "AENA Live (Spain)": "aena",
        "ANA Live (Portugal)": "ana",
        "Avinor Live (Norway)": "avinor",
        "Avinor Feed (Norway)": "avinor_feed",
        "Historical — Global (2017)": "openflights_global",
        f"OpenSky {'✓' if sky_ok else '(no credentials)'}": "opensky",
    }
    source_choice = st.radio("Source", list(source_options.keys()), label_visibility="collapsed")
    selected_source = source_options[source_choice]

    if selected_source == "opensky" and not sky_ok:
        st.warning("Set OPENSKY_USERNAME and OPENSKY_PASSWORD in .env to enable OpenSky.")

    st.caption(f"Data: {_age_label(selected_source)}")

    st.markdown("---")

    df = load_data(selected_source)

    if df.is_empty():
        cli_map = {
            "aena": "uv run python -m pyfly --source aena --scope aena",
            "ana": "uv run python -m pyfly --source ana --scope portugal",
            "avinor": "uv run python -m pyfly --source avinor --scope norway",
            "avinor_feed": "uv run python -m pyfly --source avinor_feed --scope norway",
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

def apply_filters(frame: pl.DataFrame) -> pl.DataFrame:
    """Apply the sidebar filters — shared by the map and the changes-since-last-scrape view."""
    if origin_country_filter != "All" and cmap:
        in_origin_country = {k for k, v in cmap.items() if v == origin_country_filter}
        frame = frame.filter(pl.col("origin_iata").is_in(in_origin_country))

    if origin_filter != "All":
        frame = frame.filter(pl.col("origin_iata") == origin_filter)

    if airline_filter != "All":
        frame = frame.filter(pl.col("airline_name") == airline_filter)

    if dest_country_filter != "All" and cmap:
        in_dest_country = {k for k, v in cmap.items() if v == dest_country_filter}
        frame = frame.filter(pl.col("dest_iata").is_in(in_dest_country))

    return frame


filtered = apply_filters(df)
# Fit the map to every matching origin, not just those that survive the arc cap below
# (the cap keeps the first rows, e.g. only LIS/OPO/FAO for ANA, dropping Madeira and the Azores)
view_origins = filtered.select(["origin_lat", "origin_lon"]).drop_nulls()

capped = False
if origin_filter == "All" and origin_country_filter == "All" and len(filtered) > max_arcs:
    filtered = filtered.head(max_arcs)
    capped = True

with stats_slot:
    st.markdown(
        f"**Routes:** {len(filtered)}  \n"
        f"**Destinations:** {filtered['dest_iata'].n_unique()}  \n"
        # Scraped sources (AENA, ANA) only have airline_name; airline_iata is null.
        f"**Airlines:** {filtered['airline_name'].drop_nulls().n_unique()}"
    )


# ---------------------------------------------------------------------------
# Changes since previous scrape
# ---------------------------------------------------------------------------

def _route_summary(frame: pl.DataFrame) -> pl.DataFrame:
    """One row per origin→destination with readable airport names and its airlines."""
    def name(iata: str) -> str:
        return f"{alookup[iata]['name']} ({iata})" if iata in alookup else iata

    def country(iata: str) -> str:
        return iso_to_country_name(cmap[iata]) if iata in cmap else ""

    return (
        frame.group_by(["origin_iata", "dest_iata"])
        .agg(pl.col("airline_name").drop_nulls().unique().sort().str.join(", ").alias("Airlines"))
        .sort(["origin_iata", "dest_iata"])
        .with_columns(
            pl.col("origin_iata").map_elements(name, return_dtype=pl.Utf8).alias("From"),
            pl.col("dest_iata").map_elements(name, return_dtype=pl.Utf8).alias("To"),
            pl.col("dest_iata").map_elements(country, return_dtype=pl.Utf8).alias("Country"),
        )
        .select(["From", "To", "Country", "Airlines"])
    )


def render_changes() -> None:
    if selected_source not in HISTORY_SOURCES:
        return
    st.markdown("---")
    st.subheader("Changes since previous scrape")
    changes = changes_since_previous(selected_source)
    if changes is None:
        st.caption("History starts with the latest scrape — changes will appear after the next one.")
        return

    added = _route_summary(apply_filters(changes.added))
    dropped = _route_summary(apply_filters(changes.dropped))
    st.caption(
        f"{changes.previous:%d %b %Y} → {changes.latest:%d %b %Y}, for the current filters. "
        "A route counts as new or dropped when no airline flew it in the other scrape."
    )
    c1, c2 = st.columns(2)
    c1.metric("New routes", len(added))
    c2.metric("Dropped routes", len(dropped))
    tab_added, tab_dropped = st.tabs([f"New ({len(added)})", f"Dropped ({len(dropped)})"])
    with tab_added:
        st.dataframe(added, hide_index=True, width="stretch")
    with tab_dropped:
        st.dataframe(dropped, hide_index=True, width="stretch")


# ---------------------------------------------------------------------------
# Map
# ---------------------------------------------------------------------------

scraped = last_scraped(selected_source)
if scraped is not None and selected_source not in STATIC_SOURCES:
    st.caption(f"Routes last scraped on {scraped:%d %b %Y at %H:%M} UTC.")

if capped:
    st.info(f"Showing top {max_arcs} routes. Adjust with the sidebar slider.")

if filtered.is_empty():
    st.warning("No routes match the current filters.")
    render_changes()  # dropped routes can still match, e.g. an airline that left
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
        "<b>{origin_iata} → {dest_iata}</b><br/>"
        "Airline: {airline_name}<br/>"
        "Source: {source}"
    ),
    "style": {"backgroundColor": "#1a1a2e", "color": "white", "fontSize": "13px"},
}

if origin_filter == "All" and origin_country_filter == "All" and selected_source in DEFAULT_VIEWS:
    view_state = pdk.ViewState(**DEFAULT_VIEWS[selected_source], pitch=25)
else:
    view_state = _view_for_airports(view_origins["origin_lat"].to_list(), view_origins["origin_lon"].to_list())

st.pydeck_chart(
    pdk.Deck(
        layers=[arc_layer],
        initial_view_state=view_state,
        tooltip=tooltip,
        map_style="https://basemaps.cartocdn.com/gl/dark-matter-gl-style/style.json",
    ),
    height=MAP_HEIGHT_PX,
    width="stretch",
)

st.caption(SOURCE_INFO.get(selected_source, ""))

render_changes()
