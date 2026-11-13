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

from pyfly.db import (
    HISTORY_SOURCES, get_last_scraped as _db_last_scraped, init_db, read_routes, snapshot_fingerprint,
)
from pyfly.history import NewRoutes, RouteChanges, new_routes_within, route_changes
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
    "swedavia": "Scraped from swedavia.com destination lists — scheduled and charter routes with airlines for Swedavia's 10 airports.",
    "isavia": "Scraped from innanlandsflugvellir.is — Iceland's domestic network (and Akureyri–Greenland) as listed by Isavia's domestic airports. Excludes Keflavík.",
    "kef": "Scraped from kefairport.com — Keflavík's international destinations with airlines (all-year and seasonal) as published by the airport.",
    "anac": "Brazil ANAC Voo Regular Ativo (VRA) — every scheduled passenger route flown at least weekly to, from or within Brazil in the month, Brazilian and foreign airlines. Cargo excluded. Published with a ~1–2 month lag.",
    "bts": "US DOT BTS T-100 Segment — every scheduled passenger route flown at least weekly to, from or within the US in the month, by operating carrier (regional partners such as SkyWest appear by name). Published with a ~3-month lag.",
    "openflights_global": "All countries — OpenFlights database circa 2017. Filter by origin country to explore.",
    "opensky": "OpenSky Network — actual flights flown in the last 7 days.",
}


# All data caches take the snapshot fingerprint as an argument, so new or changed parquet
# files (a push applied in the same process) re-hydrate DuckDB and invalidate cached reads.
# (No leading underscore: Streamlit leaves _-prefixed arguments out of the cache key.)
@st.cache_resource
def _ensure_db(fingerprint: tuple) -> None:
    """Create tables and hydrate from parquet before anything queries DuckDB."""
    init_db()


@st.cache_data(ttl=300)
def load_data(source: str, fingerprint: tuple) -> pl.DataFrame:
    return read_routes(source=source)


@st.cache_data(ttl=300)
def last_scraped(source: str, fingerprint: tuple) -> datetime | None:
    return _db_last_scraped(source)


@st.cache_data(ttl=300)
def changes_since_previous(source: str, fingerprint: tuple) -> RouteChanges | None:
    return route_changes(source)


@st.cache_data(ttl=300)
def new_in_past_year(source: str, fingerprint: tuple) -> NewRoutes | None:
    return new_routes_within(source, days=365)


# Sidebar "Show" modes for sources with history, and the arc colours each one uses
SHOW_ALL = "All"
SHOW_NEW_PREV = "New since previous snapshot"
SHOW_NEW_YEAR = "New in past 12 months"
SHOW_DROPPED = "Dropped since previous snapshot"
ARC_COLORS = {
    SHOW_ALL: ([255, 140, 0, 160], [0, 128, 255, 160]),
    SHOW_NEW_PREV: ([40, 200, 110, 190], [170, 240, 120, 190]),
    SHOW_NEW_YEAR: ([40, 200, 110, 190], [170, 240, 120, 190]),
    SHOW_DROPPED: ([230, 60, 60, 180], [255, 160, 120, 180]),
}


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
    "swedavia": {"latitude": 61.0, "longitude": 16.0, "zoom": 3.9},
    # Iceland, with room to the west for Akureyri–Nerlerit Inaat (Greenland)
    "isavia": {"latitude": 66.6, "longitude": -20.0, "zoom": 4.5},
    # Keflavík's transatlantic network: North America to Europe
    "kef": {"latitude": 52.0, "longitude": -35.0, "zoom": 2.3},
    # Contiguous US; Alaska, Hawaii and the territories are an airport filter away
    "bts": {"latitude": 37.5, "longitude": -96.0, "zoom": 3.4},
    "anac": {"latitude": -14.0, "longitude": -50.0, "zoom": 3.4},
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
# Monthly datasets: scraped_at is the first day of the month the data covers
MONTHLY_SOURCES = {"bts", "anac"}


def _age_label(source: str) -> str:
    if source in STATIC_SOURCES:
        return STATIC_SOURCES[source]
    ts = last_scraped(source, SNAPSHOTS)
    if ts is None:
        return "no data"
    if source in MONTHLY_SOURCES:
        return f"routes flown in {ts:%B %Y}"
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

SNAPSHOTS = snapshot_fingerprint()
_ensure_db(SNAPSHOTS)

with st.sidebar:
    st.subheader("Data source")
    sky_ok = _opensky_available()
    source_options = {
        "AENA Live (Spain)": "aena",
        "ANA Live (Portugal)": "ana",
        "Avinor Live (Norway)": "avinor",
        "Avinor Feed (Norway)": "avinor_feed",
        "Swedavia Live (Sweden)": "swedavia",
        "Isavia Domestic (Iceland)": "isavia",
        "Keflavík Airport (Iceland)": "kef",
        "BTS T-100 (USA)": "bts",
        "ANAC VRA (Brazil)": "anac",
        "Historical — Global (2017)": "openflights_global",
        f"OpenSky {'✓' if sky_ok else '(no credentials)'}": "opensky",
    }
    source_labels = {v: k for k, v in source_options.items()}
    # A list of checkboxes keeps the look of the old single-choice radio list
    selected_sources = [
        src for label, src in source_options.items()
        if st.checkbox(label, value=(src == "aena"), key=f"source_{src}")
    ]
    if not selected_sources:
        st.info("Choose at least one source.")
        st.stop()

    if "opensky" in selected_sources and not sky_ok:
        st.warning("Set OPENSKY_USERNAME and OPENSKY_PASSWORD in .env to enable OpenSky.")

    for src in selected_sources:
        prefix = f"{source_labels[src]}: " if len(selected_sources) > 1 else ""
        st.caption(f"Data: {prefix}{_age_label(src)}")

    st.markdown("---")

    frames = {src: load_data(src, SNAPSHOTS) for src in selected_sources}
    empty_sources = [src for src, frame in frames.items() if frame.is_empty()]
    loaded = [frame for frame in frames.values() if not frame.is_empty()]
    df = pl.concat(loaded, how="vertical_relaxed") if loaded else pl.DataFrame()

    if empty_sources:
        cli_map = {
            "aena": "uv run python -m pyfly --source aena --scope aena",
            "ana": "uv run python -m pyfly --source ana --scope portugal",
            "avinor": "uv run python -m pyfly --source avinor --scope norway",
            "avinor_feed": "uv run python -m pyfly --source avinor_feed --scope norway",
            "swedavia": "uv run python -m pyfly --source swedavia --scope sweden",
            "isavia": "uv run python -m pyfly --source isavia --scope iceland",
            "kef": "uv run python -m pyfly --source kef --scope iceland",
            "bts": "uv run python -m pyfly --source bts --scope usa --backfill 12",
            "anac": "uv run python -m pyfly --source anac --scope brazil --backfill 12",
            "openflights_global": "uv run python -m pyfly --source openflights --scope global_all",
            "opensky": "uv run python -m pyfly --source opensky --scope aena",
        }
        for src in empty_sources:
            st.warning(f"No data for {source_labels[src]}. Run:")
            st.code(cli_map.get(src, f"uv run python -m pyfly --source {src}"))
        selected_sources = [src for src in selected_sources if src not in empty_sources]
        if not selected_sources:
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

    # The 2017 global set is too big to draw unfiltered, so it always has an origin country
    if "openflights_global" in selected_sources:
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

    # Each source lists departures from its own airports (AENA: Spain → Portugal, ANA: Portugal →
    # Spain), so origin/destination filters can match the reverse direction too.
    both_directions = st.checkbox(
        "Match both directions",
        help="Also include routes from the destination side to the origin side — e.g. with AENA and "
             "ANA selected, Spain → Portugal (AENA) and Portugal → Spain (ANA).",
    )

    # Route changes: swap the map's data for new or dropped routes; the filters above apply on top.
    # Each source is compared with its own history; sources without two snapshots are left out.
    show = SHOW_ALL
    with_history = {src: changes_since_previous(src, SNAPSHOTS)
                    for src in selected_sources if src in HISTORY_SOURCES}
    with_history = {src: ch for src, ch in with_history.items() if ch is not None}
    if not with_history:
        st.caption("New/dropped route views need at least two snapshots of a selected source.")
    else:
        show = st.selectbox("Show", [SHOW_ALL, SHOW_NEW_PREV, SHOW_NEW_YEAR, SHOW_DROPPED], key="show")
        if show != SHOW_ALL:
            parts, notes = [], []
            for src, changes in with_history.items():
                fmt = "%B %Y" if src in MONTHLY_SOURCES else "%d %b %Y"
                name = f"{source_labels[src]}: " if len(selected_sources) > 1 else ""
                if show == SHOW_NEW_PREV:
                    parts.append(changes.added)
                    notes.append(f"{name}routes in {changes.latest:{fmt}} not flown in {changes.previous:{fmt}}.")
                elif show == SHOW_DROPPED:
                    parts.append(changes.dropped)
                    notes.append(f"{name}routes in {changes.previous:{fmt}} gone by {changes.latest:{fmt}}.")
                else:
                    year = new_in_past_year(src, SNAPSHOTS)
                    parts.append(year.routes)
                    span = (f"{year.compared[0]:{fmt}} – {year.compared[-1]:{fmt}}" if len(year.compared) > 1
                            else f"{year.compared[0]:{fmt}}")
                    notes.append(
                        f"{name}routes in {year.latest:{fmt}} not seen in any of {len(year.compared)} earlier "
                        f"snapshot{'s' if len(year.compared) > 1 else ''} ({span})."
                        + (" Little history yet." if len(year.compared) < 3 else "")
                    )
            df = pl.concat(parts, how="vertical_relaxed")
            # only sources that keep history can be "not enough yet" (OpenFlights is static)
            skipped = [source_labels[s] for s in selected_sources
                       if s in HISTORY_SOURCES and s not in with_history]
            if skipped:
                notes.append(f"Not enough history yet: {', '.join(skipped)}.")
            st.caption("  \n".join(notes))

    st.markdown("---")
    st.subheader("Arc density")
    max_arcs = st.slider("Max routes", min_value=100, max_value=2000, value=500, step=100)

    st.markdown("---")
    stats_slot = st.empty()


# ---------------------------------------------------------------------------
# Filtering
# ---------------------------------------------------------------------------

def _ends_match(from_col: str, to_col: str) -> pl.Expr:
    """True where from_col satisfies the origin filters and to_col the destination filter."""
    expr = pl.lit(True)
    if origin_country_filter != "All" and cmap:
        in_origin_country = {k for k, v in cmap.items() if v == origin_country_filter}
        expr &= pl.col(from_col).is_in(in_origin_country)
    if origin_filter != "All":
        expr &= pl.col(from_col) == origin_filter
    if dest_country_filter != "All" and cmap:
        in_dest_country = {k for k, v in cmap.items() if v == dest_country_filter}
        expr &= pl.col(to_col).is_in(in_dest_country)
    return expr


def apply_filters(frame: pl.DataFrame) -> pl.DataFrame:
    """Apply the sidebar filters — shared by the map and the changes-since-last-scrape view."""
    ends = _ends_match("origin_iata", "dest_iata")
    if both_directions:
        ends |= _ends_match("dest_iata", "origin_iata")
    frame = frame.filter(ends)

    if airline_filter != "All":
        frame = frame.filter(pl.col("airline_name") == airline_filter)

    return frame


def route_count(frame: pl.DataFrame) -> int:
    """Routes are origin→destination airport pairs: one pair reported by several sources (AENA and
    BTS both list Madrid→JFK) or flown by several airlines counts once."""
    return frame.select(["origin_iata", "dest_iata"]).unique().height


filtered = apply_filters(df)
matching = filtered  # everything the filters match, before the arc cap
# Fit the map to every matching origin, not just those that survive the arc cap below
# (the cap keeps the first rows, e.g. only LIS/OPO/FAO for ANA, dropping Madeira and the Azores)
view_origins = filtered.select(["origin_lat", "origin_lon"]).drop_nulls()

capped = False
if origin_filter == "All" and origin_country_filter == "All" and len(filtered) > max_arcs:
    if len(selected_sources) > 1:
        # share the cap between sources, otherwise the first source would take every arc
        filtered = filtered.group_by("source", maintain_order=True).head(max_arcs // len(selected_sources))
    else:
        filtered = filtered.head(max_arcs)
    capped = True

with stats_slot:
    # Stats cover every matching route, not just the arcs drawn under the cap
    st.markdown(
        f"**Routes:** {route_count(matching)}  \n"
        f"**Destinations:** {matching['dest_iata'].n_unique()}  \n"
        # Scraped sources (AENA, ANA) only have airline_name; airline_iata is null.
        f"**Airlines:** {matching['airline_name'].drop_nulls().n_unique()}"
    )


# ---------------------------------------------------------------------------
# Changes since previous scrape
# ---------------------------------------------------------------------------

def _route_summary(frame: pl.DataFrame) -> pl.DataFrame:
    """One row per source and origin→destination with readable airport names and its airlines."""
    def name(iata: str) -> str:
        return f"{alookup[iata]['name']} ({iata})" if iata in alookup else iata

    def country(iata: str) -> str:
        return iso_to_country_name(cmap[iata]) if iata in cmap else ""

    columns = (["Source"] if len(selected_sources) > 1 else []) + ["From", "To", "Country", "Airlines"]
    return (
        frame.group_by(["source", "origin_iata", "dest_iata"])
        .agg(pl.col("airline_name").drop_nulls().unique().sort().str.join(", ").alias("Airlines"))
        .sort(["source", "origin_iata", "dest_iata"])
        .with_columns(
            pl.col("source").replace_strict(source_labels, default=pl.col("source")).alias("Source"),
            pl.col("origin_iata").map_elements(name, return_dtype=pl.Utf8).alias("From"),
            pl.col("dest_iata").map_elements(name, return_dtype=pl.Utf8).alias("To"),
            pl.col("dest_iata").map_elements(country, return_dtype=pl.Utf8).alias("Country"),
        )
        .select(columns)
    )


def render_changes() -> None:
    sources = [src for src in selected_sources if src in HISTORY_SOURCES]
    if not sources:
        return
    st.markdown("---")
    st.subheader("Changes since previous scrape")
    all_changes = {src: changes_since_previous(src, SNAPSHOTS) for src in sources}
    available = {src: ch for src, ch in all_changes.items() if ch is not None}
    if not available:
        st.caption("History starts with the latest scrape — changes will appear after the next one.")
        return

    added_rows = apply_filters(pl.concat([ch.added for ch in available.values()], how="vertical_relaxed"))
    dropped_rows = apply_filters(pl.concat([ch.dropped for ch in available.values()], how="vertical_relaxed"))
    added, dropped = _route_summary(added_rows), _route_summary(dropped_rows)
    periods = []
    for src, ch in available.items():
        fmt = "%B %Y" if src in MONTHLY_SOURCES else "%d %b %Y"
        name = f"{source_labels[src]}: " if len(selected_sources) > 1 else ""
        periods.append(f"{name}{ch.previous:{fmt}} → {ch.latest:{fmt}}")
    missing = [source_labels[s] for s, ch in all_changes.items() if ch is None]
    st.caption(
        "; ".join(periods) + ", for the current filters. "
        "A route counts as new or dropped when no airline flew it in the other scrape."
        + (f" No history yet: {', '.join(missing)}." if missing else "")
    )
    c1, c2 = st.columns(2)
    # Metrics count airport pairs; the tables keep one row per source so you can see who reports it
    n_added, n_dropped = route_count(added_rows), route_count(dropped_rows)
    c1.metric("New routes", n_added)
    c2.metric("Dropped routes", n_dropped)
    tab_added, tab_dropped = st.tabs([f"New ({n_added})", f"Dropped ({n_dropped})"])
    with tab_added:
        st.dataframe(added, hide_index=True, width="stretch")
    with tab_dropped:
        st.dataframe(dropped, hide_index=True, width="stretch")


# ---------------------------------------------------------------------------
# Map
# ---------------------------------------------------------------------------

for src in selected_sources:
    scraped = last_scraped(src, SNAPSHOTS)
    name = f"{source_labels[src]}: " if len(selected_sources) > 1 else ""
    if scraped is not None and src in MONTHLY_SOURCES:
        st.caption(f"{name}Routes flown in {scraped:%B %Y} (the latest month published).")
    elif scraped is not None and src not in STATIC_SOURCES:
        st.caption(f"{name}Routes last scraped on {scraped:%d %b %Y at %H:%M} UTC.")
if show != SHOW_ALL:
    st.info(f"Showing **{show.lower()}** only — switch back under **Show** in the sidebar.")

if capped:
    per_source = f", up to {max_arcs // len(selected_sources)} per source" if len(selected_sources) > 1 else ""
    st.info(f"Drawing {len(filtered)} of {len(matching)} route arcs{per_source}. "
            "Adjust with the Max routes slider, or pick an airport or country.")

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
    get_source_color=ARC_COLORS[show][0],  # green for new routes, red for dropped
    get_target_color=ARC_COLORS[show][1],
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

single = selected_sources[0] if len(selected_sources) == 1 else None
if origin_filter == "All" and origin_country_filter == "All" and single in DEFAULT_VIEWS:
    view_state = pdk.ViewState(**DEFAULT_VIEWS[single], pitch=25)
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

for src in selected_sources:
    st.caption(SOURCE_INFO.get(src, ""))

render_changes()
