"""Airport Explorer — find IATA codes on an interactive world map."""
import sys
import re
from pathlib import Path

_ROOT = Path(__file__).parent.parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

import pycountry
import polars as pl
import pydeck as pdk
import streamlit as st

st.set_page_config(
    page_title="Airport Explorer — PyFly",
    page_icon="🔍",
    layout="wide",
    initial_sidebar_state="expanded",
)

DATA_DIR = Path(__file__).parent.parent.parent / "data"

# ---------------------------------------------------------------------------
# Colours by airport type
# ---------------------------------------------------------------------------

_TYPE_COLOUR = {
    "large_airport":  [245, 158,  11, 220],   # amber
    "medium_airport": [ 16, 185, 129, 200],   # emerald
    "small_airport":  [ 99, 102, 241, 160],   # indigo, more transparent
    "heliport":       [236,  72, 153, 140],   # pink
    "seaplane_base":  [  6, 182, 212, 140],   # cyan
}
_TYPE_RADIUS = {
    "large_airport": 40_000,
    "medium_airport": 25_000,
    "small_airport": 12_000,
    "heliport": 10_000,
    "seaplane_base": 12_000,
}
_TYPE_LABELS = {
    "large_airport": "Large",
    "medium_airport": "Medium",
    "small_airport": "Small",
    "heliport": "Heliport",
    "seaplane_base": "Seaplane base",
}

# ---------------------------------------------------------------------------
# Data
# ---------------------------------------------------------------------------

@st.cache_data
def _load_airports() -> pl.DataFrame:
    path = DATA_DIR / "airports.csv"
    if not path.exists():
        return pl.DataFrame()
    keep = ["iata_code", "name", "type", "municipality", "iso_country",
            "latitude_deg", "longitude_deg", "scheduled_service"]
    df = (
        pl.read_csv(path, ignore_errors=True)
        .filter(pl.col("iata_code").is_not_null() & (pl.col("iata_code") != ""))
        .select([c for c in keep if c in pl.read_csv(path, n_rows=0).columns])
        .rename({"latitude_deg": "lat", "longitude_deg": "lon"})
    )
    # Strip parenthetical qualifiers from municipality for cleaner display
    if "municipality" in df.columns:
        df = df.with_columns(
            pl.col("municipality").map_elements(
                lambda m: re.sub(r"\s*\(.*?\)", "", m or "").strip(),
                return_dtype=pl.String,
            )
        )
    return df


@st.cache_data
def _country_options(df: pl.DataFrame) -> list[str]:
    codes = df["iso_country"].drop_nulls().unique().sort().to_list()
    names = []
    for c in codes:
        try:
            names.append(pycountry.countries.get(alpha_2=c).name)
        except Exception:
            names.append(c)
    return sorted(names)


@st.cache_data
def _country_name_to_code() -> dict[str, str]:
    mapping = {}
    for c in pycountry.countries:
        mapping[c.name] = c.alpha_2
    return mapping


# ---------------------------------------------------------------------------
# Sidebar filters
# ---------------------------------------------------------------------------

df_all = _load_airports()

with st.sidebar:
    st.subheader("Filter airports")

    search = st.text_input(
        "Search",
        placeholder="Name, IATA code, or city",
        help="Matches airport name, IATA code, or city.",
    )

    type_choice = st.multiselect(
        "Airport type",
        options=list(_TYPE_LABELS.keys()),
        default=["large_airport"],
        format_func=lambda t: _TYPE_LABELS.get(t, t),
    )

    country_names = _country_options(df_all)
    country_choice = st.selectbox("Country", ["All"] + country_names)

    scheduled_only = st.checkbox("Scheduled service only", value=False)

    st.markdown("---")
    st.caption(f"{len(df_all):,} airports total in database")

# ---------------------------------------------------------------------------
# Filter
# ---------------------------------------------------------------------------

df = df_all

if type_choice:
    df = df.filter(pl.col("type").is_in(type_choice))
else:
    df = df.filter(pl.lit(False))  # nothing selected → empty

if country_choice != "All":
    code = _country_name_to_code().get(country_choice, country_choice)
    df = df.filter(pl.col("iso_country") == code)

if scheduled_only and "scheduled_service" in df.columns:
    df = df.filter(pl.col("scheduled_service") == "yes")

if search.strip():
    q = search.strip().upper()
    q_lower = search.strip().lower()
    df = df.filter(
        pl.col("iata_code").str.contains(q)
        | pl.col("name").str.to_lowercase().str.contains(q_lower)
        | pl.col("municipality").str.to_lowercase().str.contains(q_lower)
    )

# ---------------------------------------------------------------------------
# Map
# ---------------------------------------------------------------------------

st.title("🔍 Airport Explorer")
st.caption(f"Showing {len(df):,} airports — hover any dot for details")

if df.is_empty():
    st.info("No airports match the current filters.")
    st.stop()

map_rows = df.to_dicts()

# Centre on filtered set
lats = [r["lat"] for r in map_rows if r["lat"] is not None]
lons = [r["lon"] for r in map_rows if r["lon"] is not None]
if lats:
    clat = (max(lats) + min(lats)) / 2
    clon = (max(lons) + min(lons)) / 2
    span = max(max(lats) - min(lats), max(lons) - min(lons))
    zoom = 7 if span < 5 else 5 if span < 20 else 4 if span < 60 else 2
else:
    clat, clon, zoom = 20.0, 0.0, 2

# One layer per type so each gets distinct min/max pixel sizes
_TYPE_PIX = {
    "large_airport":  (7, 18),
    "medium_airport": (5, 12),
    "small_airport":  (3,  8),
    "heliport":       (2,  6),
    "seaplane_base":  (2,  6),
}

layers = []
for airport_type, rows_for_type in {
    t: [r for r in map_rows if r.get("type") == t]
    for t in dict.fromkeys(r.get("type") for r in map_rows)
}.items():
    if not rows_for_type:
        continue
    colour = _TYPE_COLOUR.get(airport_type, [150, 150, 150, 140])
    min_px, max_px = _TYPE_PIX.get(airport_type, (3, 8))
    layers.append(pdk.Layer(
        "ScatterplotLayer",
        data=rows_for_type,
        get_position=["lon", "lat"],
        get_fill_color=colour,
        get_radius=_TYPE_RADIUS.get(airport_type, 12_000),
        radius_min_pixels=min_px,
        radius_max_pixels=max_px,
        pickable=True,
        auto_highlight=True,
    ))

st.pydeck_chart(
    pdk.Deck(
        layers=layers,
        initial_view_state=pdk.ViewState(latitude=clat, longitude=clon, zoom=zoom, pitch=0),
        tooltip={
            "html": (
                "<b>{iata_code}</b> — {name}<br/>"
                "{municipality}<br/>"
                "{iso_country} &nbsp;·&nbsp; {type}"
            ),
            "style": {"backgroundColor": "#1a1a2e", "color": "white", "fontSize": "13px"},
        },
        map_style="https://basemaps.cartocdn.com/gl/dark-matter-gl-style/style.json",
    ),
    key=f"airport_map_{search}_{country_choice}_{','.join(type_choice)}_{scheduled_only}",
    height=540,
    width="stretch",
)

# ---------------------------------------------------------------------------
# Results table
# ---------------------------------------------------------------------------

st.markdown(f"### Results ({len(df):,})")

display_cols = ["iata_code", "name", "municipality", "iso_country", "type"]
display_cols = [c for c in display_cols if c in df.columns]

rename_map = {
    "iata_code": "IATA",
    "name": "Airport",
    "municipality": "City",
    "iso_country": "Country",
    "type": "Type",
}

table = (
    df.select(display_cols)
    .rename({k: v for k, v in rename_map.items() if k in display_cols})
    .with_columns(pl.col("Type").map_elements(lambda t: _TYPE_LABELS.get(t, t), return_dtype=pl.String))
    .sort("IATA")
)

st.dataframe(table, use_container_width=True, height=400, hide_index=True)
