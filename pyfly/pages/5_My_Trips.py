"""My Trips — collection of saved trips, each a named group of routes."""
import sys
import json
from pathlib import Path

_ROOT = Path(__file__).parent.parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

import httpx
import pydeck as pdk
import streamlit as st

st.set_page_config(
    page_title="My Trips — PyFly",
    page_icon="📖",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# Keep 5-button trip card rows horizontal on mobile.
# Targets only stHorizontalBlock elements that have a 5th column child,
# leaving all other column layouts (map/list split etc.) unaffected.
st.markdown("""
<style>
@media (max-width: 640px) {
    div[data-testid="stHorizontalBlock"]:has(> div[data-testid="column"]:nth-child(5)) {
        flex-wrap: nowrap !important;
        gap: 0.25rem !important;
    }
    div[data-testid="stHorizontalBlock"]:has(> div[data-testid="column"]:nth-child(5)) > div[data-testid="column"] {
        min-width: 0 !important;
        flex: 1 1 0 !important;
        padding: 0 !important;
    }
    div[data-testid="stHorizontalBlock"]:has(> div[data-testid="column"]:nth-child(5)) button {
        padding: 0.15rem 0.1rem !important;
        font-size: 0.85rem !important;
    }
}
</style>
""", unsafe_allow_html=True)

MODE_ICONS = {"plane": "✈", "train": "🚂", "boat": "⛴", "car": "🚗"}
MODE_COLOUR = {
    "plane": [245, 158, 11, 200],
    "train": [16, 185, 129, 200],
    "boat":  [6, 182, 212, 200],
    "car":   [244, 63, 94, 200],
}

# ---------------------------------------------------------------------------
# Airport lookup (includes ISO codes for region coloring)
# ---------------------------------------------------------------------------

def _iata_map() -> dict:
    if "_trip_iata_map" not in st.session_state:
        path = _ROOT / "data" / "airports.csv"
        if path.exists():
            import polars as pl
            df = pl.read_csv(path, ignore_errors=True).filter(
                pl.col("iata_code").is_not_null() & (pl.col("iata_code") != "")
            )
            cols = ["iata_code", "latitude_deg", "longitude_deg", "iso_country"]
            for extra in ("iso_region",):
                if extra in df.columns:
                    cols.append(extra)
            st.session_state["_trip_iata_map"] = {
                r["iata_code"]: {k: v for k, v in r.items() if k != "iata_code"}
                for r in df.select(cols).iter_rows(named=True)
            }
        else:
            st.session_state["_trip_iata_map"] = {}
    return st.session_state["_trip_iata_map"]


# ---------------------------------------------------------------------------
# Region coloring helpers (mirrors My Routes)
# ---------------------------------------------------------------------------

_GEO_URLS = {
    "Country": "https://raw.githubusercontent.com/nvkelso/natural-earth-vector/master/geojson/ne_110m_admin_0_countries.geojson",
    "Region":  "https://raw.githubusercontent.com/nvkelso/natural-earth-vector/master/geojson/ne_10m_admin_1_states_provinces.geojson",
}


def _hex_to_rgb(h: str) -> tuple[int, int, int]:
    h = h.lstrip("#")
    return int(h[0:2], 16), int(h[2:4], 16), int(h[4:6], 16)


@st.cache_resource(show_spinner="Loading region data…")
def _fetch_geojson(url: str) -> dict:
    resp = httpx.get(url, timeout=90)
    resp.raise_for_status()
    raw = resp.json()
    keep = {"ISO_A2", "iso_3166_2", "code_hasc"}
    return {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "geometry": f["geometry"],
                "properties": {k: v for k, v in (f.get("properties") or {}).items() if k in keep},
            }
            for f in raw.get("features", [])
        ],
    }


def _pip_rings(px: float, py: float, rings: list) -> bool:
    inside = False
    for ring in rings:
        n = len(ring)
        j = n - 1
        for i in range(n):
            xi, yi = ring[i][0], ring[i][1]
            xj, yj = ring[j][0], ring[j][1]
            if ((yi > py) != (yj > py)) and px < (xj - xi) * (py - yi) / (yj - yi) + xi:
                inside = not inside
            j = i
    return inside


def _pip_feature(lon: float, lat: float, feature: dict) -> bool:
    geom = feature.get("geometry") or {}
    gtype, coords = geom.get("type"), geom.get("coordinates", [])
    if gtype == "Polygon":
        return _pip_rings(lon, lat, coords)
    if gtype == "MultiPolygon":
        return any(_pip_rings(lon, lat, rings) for rings in coords)
    return False


def _collect_visited_iso(routes, geojson: dict, level: str) -> set[str]:
    iata_data = _iata_map()
    visited: set[str] = set()
    geocoded: list[dict] = []
    for entry in routes:
        for node in (entry.get("nodes") or []):
            if not node:
                continue
            iata = node.get("iata")
            if iata and iata in iata_data:
                r = iata_data[iata]
                iso = r.get("iso_country") if level == "Country" else r.get("iso_region")
                if iso:
                    visited.add(iso)
            elif node.get("lat") is not None:
                geocoded.append(node)
    for node in geocoded:
        for f in geojson.get("features", []):
            if _pip_feature(node["lon"], node["lat"], f):
                props = f.get("properties") or {}
                if level == "Country":
                    iso = props.get("ISO_A2")
                else:
                    iso = props.get("iso_3166_2") or props.get("code_hasc", "").replace(".", "-", 1) or None
                if iso:
                    visited.add(iso)
                break
    return visited


def _filter_geojson(geojson: dict, visited: set[str], level: str) -> dict:
    if level == "Country":
        return {
            "type": "FeatureCollection",
            "features": [
                f for f in geojson["features"]
                if (f.get("properties") or {}).get("ISO_A2") in visited
            ],
        }
    visited_hasc = {v.replace("-", ".", 1) for v in visited}
    return {
        "type": "FeatureCollection",
        "features": [
            f for f in geojson["features"]
            if (
                (f.get("properties") or {}).get("iso_3166_2") in visited
                or (f.get("properties") or {}).get("code_hasc") in visited_hasc
            )
        ],
    }


# ---------------------------------------------------------------------------
# Road routing (OSRM)
# ---------------------------------------------------------------------------

def _road_geometry(lat1: float, lon1: float, lat2: float, lon2: float) -> list[list[float]] | None:
    """Return [[lon, lat], ...] road polyline from OSRM, or None on failure."""
    key = (round(lat1, 4), round(lon1, 4), round(lat2, 4), round(lon2, 4))
    if "road_cache" not in st.session_state:
        st.session_state.road_cache = {}
    cache = st.session_state.road_cache
    if key in cache:
        return cache[key]
    try:
        url = f"https://router.project-osrm.org/route/v1/driving/{lon1},{lat1};{lon2},{lat2}"
        resp = httpx.get(url, params={"overview": "full", "geometries": "geojson"}, timeout=8)
        resp.raise_for_status()
        coords = resp.json()["routes"][0]["geometry"]["coordinates"]
        cache[key] = coords
        return coords
    except Exception:
        cache[key] = None
        return None


# ---------------------------------------------------------------------------
# Map building
# ---------------------------------------------------------------------------

def _build_map(routes: list[dict], region_layer=None) -> tuple[list, list]:
    """Return (pydeck layers, list of valid coords) for a list of route entries."""
    iata = _iata_map()
    arc_rows, line_rows, road_rows, node_rows, all_coords = [], [], [], [], []

    for entry in routes:
        mode = entry.get("mode", "plane")
        colour = MODE_COLOUR.get(mode, [200, 200, 200, 200])
        nodes = [n for n in (entry.get("nodes") or []) if n]
        coords = []
        for n in nodes:
            if n.get("iata") and n["iata"] in iata:
                c = iata[n["iata"]]
                coords.append((c["latitude_deg"], c["longitude_deg"]))
            elif n.get("lat") is not None:
                coords.append((n["lat"], n["lon"]))
            else:
                coords.append(None)

        all_coords.extend(c for c in coords if c)

        for i in range(len(coords) - 1):
            c0, c1 = coords[i], coords[i + 1]
            if not c0 or not c1:
                continue
            if mode == "plane":
                arc_rows.append({"origin_lat": c0[0], "origin_lon": c0[1],
                                  "dest_lat": c1[0], "dest_lon": c1[1], "colour": colour})
            elif mode == "car":
                geom = _road_geometry(c0[0], c0[1], c1[0], c1[1])
                if geom:
                    road_rows.append({"path": geom, "colour": colour})
                else:
                    line_rows.append({"origin_lat": c0[0], "origin_lon": c0[1],
                                      "dest_lat": c1[0], "dest_lon": c1[1], "colour": colour})
            else:
                line_rows.append({"origin_lat": c0[0], "origin_lon": c0[1],
                                  "dest_lat": c1[0], "dest_lon": c1[1], "colour": colour})

        n_coords = len(coords)
        for i, c in enumerate(coords):
            if c:
                is_endpoint = (i == 0 or i == n_coords - 1)
                node_rows.append({
                    "lat": c[0], "lon": c[1],
                    "radius": 8000 if is_endpoint else 4000,
                    "opacity": 210 if is_endpoint else 120,
                })

    layers = []
    if region_layer:
        layers.append(region_layer)
    if arc_rows:
        layers.append(pdk.Layer("ArcLayer", data=arc_rows,
            get_source_position=["origin_lon", "origin_lat"],
            get_target_position=["dest_lon", "dest_lat"],
            get_source_color="colour", get_target_color="colour",
            get_width=2, pickable=True))
    if line_rows:
        layers.append(pdk.Layer("LineLayer", data=line_rows,
            get_source_position=["origin_lon", "origin_lat"],
            get_target_position=["dest_lon", "dest_lat"],
            get_color="colour", get_width=2, pickable=True))
    if road_rows:
        layers.append(pdk.Layer("PathLayer", data=road_rows,
            get_path="path", get_color="colour",
            get_width=2, width_min_pixels=2, pickable=True))
    if node_rows:
        layers.append(pdk.Layer("ScatterplotLayer", data=node_rows,
            get_position=["lon", "lat"],
            get_fill_color=[255, 255, 255, "opacity"],
            get_radius="radius", pickable=True))
    return layers, all_coords


def _map_view(all_coords):
    if not all_coords:
        return 30.0, 0.0, 2
    lats = [c[0] for c in all_coords]
    lons = [c[1] for c in all_coords]
    clat = (max(lats) + min(lats)) / 2
    clon = (max(lons) + min(lons)) / 2
    span = max(max(lats) - min(lats), max(lons) - min(lons))
    zoom = 7 if span < 5 else 5 if span < 15 else 4 if span < 40 else 3 if span < 80 else 2
    return clat, clon, zoom


def _mode_summary(routes: list[dict]) -> str:
    modes = {r.get("mode") for r in routes} - {None}
    return "  ".join(MODE_ICONS.get(m, "") for m in sorted(modes))


def _route_label(entry: dict) -> str:
    return " → ".join(entry.get("legs") or [])


# ---------------------------------------------------------------------------
# Guard
# ---------------------------------------------------------------------------

for _k, _v in [("trips", []), ("routes", []), ("routes_selection_open", False), ("routes_selected", set())]:
    if _k not in st.session_state:
        st.session_state[_k] = _v

# ---------------------------------------------------------------------------
# Sidebar
# ---------------------------------------------------------------------------

with st.sidebar:
    # ── Region coloring ───────────────────────────────────────────────────────
    st.subheader("Region coloring")
    region_enabled = st.toggle("Color visited regions", value=False, key="trip_region_enabled")
    if region_enabled:
        st.selectbox("Scale", list(_GEO_URLS.keys()), key="trip_region_level")
        col_a, col_b = st.columns([1, 2])
        with col_a:
            st.color_picker("Colour", value="#3B82F6", key="trip_region_color")
        with col_b:
            st.slider("Opacity", 5, 80, 35, key="trip_region_opacity")

    if st.session_state.get("trips"):
        st.markdown("---")
        st.subheader("Share")
        st.download_button(
            "⬇ Download JSON",
            data=json.dumps(st.session_state.trips, indent=2),
            file_name="my_trips.json",
            mime="application/json",
            use_container_width=True,
        )
        _up = st.file_uploader("⬆ Upload JSON", type="json", label_visibility="collapsed")
        if _up:
            if _up.size > 512_000:
                st.error("File too large — expected a small PyFly trips export.")
            else:
                try:
                    _data = json.loads(_up.read().decode())
                    if isinstance(_data, list) and all(isinstance(t, dict) and "routes" in t for t in _data):
                        st.session_state.trips = _data
                        st.success(f"Loaded {len(_data)} trip{'s' if len(_data) != 1 else ''}.")
                        st.rerun()
                    else:
                        st.error("Invalid format — expected a PyFly trips JSON export.")
                except Exception:
                    st.error("Invalid JSON file.")

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

st.title("📖 My Trips")

if not st.session_state.trips:
    st.info("No trips saved yet. Go to **My Routes**, select some routes, and click **Save trip**.")
    if st.button("← Go to My Routes"):
        st.switch_page("pages/1_My_Routes.py")
    st.stop()

# ---------------------------------------------------------------------------
# Trip cards
# ---------------------------------------------------------------------------

if "collapsed_trips" not in st.session_state:
    st.session_state.collapsed_trips = set()
if "editing_trip" not in st.session_state:
    st.session_state.editing_trip = None

_to_delete = None
_to_move = None  # (from_index, to_index)
_n_trips = len(st.session_state.trips)

for _ti, _trip in enumerate(st.session_state.trips):
    _routes = _trip.get("routes") or []
    _title = _trip.get("title") or f"Trip {_ti + 1}"
    _date = _trip.get("created_at", "")
    _modes = _mode_summary(_routes)
    _is_editing = st.session_state.editing_trip == _ti

    with st.container(border=True):
        _hd_left, _hd_right = st.columns([4, 2])

        with _hd_left:
            if _is_editing:
                _trip["title"] = st.text_input("Title", value=_title, key=f"edit_title_{_ti}",
                                               label_visibility="collapsed", placeholder="Trip name…")
                st.caption(f"{_modes}  ·  {len(_routes)} route{'s' if len(_routes) != 1 else ''}  ·  {_date}")
                _trip["notes"] = st.text_area("Notes", value=_trip.get("notes", ""),
                                              key=f"edit_notes_{_ti}",
                                              placeholder="Add notes… (markdown supported)",
                                              height=68, label_visibility="collapsed")
            else:
                st.markdown(f"### {_title}")
                st.caption(f"{_modes}  ·  {len(_routes)} route{'s' if len(_routes) != 1 else ''}  ·  {_date}")
                if _trip.get("notes"):
                    st.markdown(_trip["notes"])

        with _hd_right:
            _is_open = _ti not in st.session_state.collapsed_trips
            _b1, _b2, _b3, _b4, _b5 = st.columns(5)
            with _b1:
                if st.button("▼" if _is_open else "▶", key=f"tog_{_ti}",
                             use_container_width=True, help="Collapse" if _is_open else "Expand"):
                    if _is_open:
                        st.session_state.collapsed_trips.add(_ti)
                    else:
                        st.session_state.collapsed_trips.discard(_ti)
                    st.rerun()
            with _b2:
                if st.button("💾" if _is_editing else "✏", key=f"edit_{_ti}", use_container_width=True,
                             help="Save" if _is_editing else "Edit"):
                    if _is_editing:
                        st.session_state.editing_trip = None
                    else:
                        st.session_state.editing_trip = _ti
                    st.rerun()
            with _b3:
                st.button("↑", key=f"up_{_ti}", use_container_width=True, help="Move up",
                          disabled=_is_open or _ti == 0,
                          on_click=lambda i=_ti: st.session_state.update(_move=(i, i - 1)))
            with _b4:
                st.button("↓", key=f"dn_{_ti}", use_container_width=True, help="Move down",
                          disabled=_is_open or _ti == _n_trips - 1,
                          on_click=lambda i=_ti: st.session_state.update(_move=(i, i + 1)))
            with _b5:
                if st.button("🗑", key=f"del_{_ti}", use_container_width=True, help="Delete trip"):
                    _to_delete = _ti

        if _ti not in st.session_state.collapsed_trips:
            st.markdown("---")
            _map_col, _list_col = st.columns([4, 3])

            with _map_col:
                try:
                    # Build region layer for this trip's routes
                    _region_layer = None
                    if st.session_state.get("trip_region_enabled") and _routes:
                        try:
                            _geo_url = _GEO_URLS[st.session_state.get("trip_region_level", "Country")]
                            _geojson = _fetch_geojson(_geo_url)
                            _visited = _collect_visited_iso(_routes, _geojson, st.session_state.get("trip_region_level", "Country"))
                            _filtered = _filter_geojson(_geojson, _visited, st.session_state.get("trip_region_level", "Country"))
                            if _filtered["features"]:
                                _r, _g, _b = _hex_to_rgb(st.session_state.get("trip_region_color", "#3B82F6"))
                                _fill_a = int(st.session_state.get("trip_region_opacity", 35) * 255 / 100)
                                _line_a = min(255, _fill_a * 3)
                                _region_layer = pdk.Layer(
                                    "GeoJsonLayer", data=_filtered,
                                    get_fill_color=[_r, _g, _b, _fill_a],
                                    get_line_color=[_r, _g, _b, _line_a],
                                    get_line_width=1, pickable=False,
                                )
                        except Exception:
                            pass

                    _layers, _coords = _build_map(_routes, _region_layer)
                    _clat, _clon, _zoom = _map_view(_coords)
                    st.pydeck_chart(pdk.Deck(
                        layers=_layers,
                        initial_view_state=pdk.ViewState(latitude=_clat, longitude=_clon, zoom=_zoom, pitch=20),
                        map_style="https://basemaps.cartocdn.com/gl/dark-matter-gl-style/style.json",
                    ), height=500)
                except Exception as _e:
                    st.caption(f"Map unavailable: {_e}")

            with _list_col:
                for _r in _routes:
                    _icon = MODE_ICONS.get(_r.get("mode", "plane"), "✈")
                    _lbl = _route_label(_r)
                    _r_date = _r.get("date", "")
                    _r_tag = _r.get("tag", "")
                    _meta = "  ·  ".join(x for x in [_r_date, _r_tag] if x)
                    st.markdown(f"{_icon} **{_lbl}**" + (f"  \n_{_meta}_" if _meta else ""))

if _to_delete is not None:
    st.session_state.trips.pop(_to_delete)
    st.session_state.collapsed_trips = {
        i if i < _to_delete else i - 1
        for i in st.session_state.collapsed_trips if i != _to_delete
    }
    if st.session_state.editing_trip == _to_delete:
        st.session_state.editing_trip = None
    st.rerun()

if "_move" in st.session_state:
    _a, _b = st.session_state.pop("_move")
    trips = st.session_state.trips
    trips[_a], trips[_b] = trips[_b], trips[_a]
    def _remap(idx):
        if idx == _a: return _b
        if idx == _b: return _a
        return idx
    st.session_state.collapsed_trips = {_remap(i) for i in st.session_state.collapsed_trips}
    if st.session_state.editing_trip in (_a, _b):
        st.session_state.editing_trip = _remap(st.session_state.editing_trip)
    st.rerun()

# ---------------------------------------------------------------------------
# Convert to Routes
# ---------------------------------------------------------------------------

_btn_col, _ = st.columns([1, 5])
with _btn_col:
    _btn_label = "✕ Cancel" if st.session_state.routes_selection_open else "🧳 Convert to Routes →"
    if st.button(_btn_label, type="primary", use_container_width=True):
        st.session_state.routes_selection_open = not st.session_state.routes_selection_open
        if st.session_state.routes_selection_open and not st.session_state.routes_selected:
            st.session_state.routes_selected = set(range(len(st.session_state.trips)))
        st.rerun()

if st.session_state.routes_selection_open:
    st.session_state.routes_selected = {
        i for i in st.session_state.routes_selected if i < len(st.session_state.trips)
    }

    _all_checked = len(st.session_state.routes_selected) >= len(st.session_state.trips)
    if st.checkbox("Select all", value=_all_checked, key="routes_sel_all_chk"):
        st.session_state.routes_selected = set(range(len(st.session_state.trips)))
    else:
        if _all_checked:
            st.session_state.routes_selected = set()

    _sel_rows = []
    for _i, _trip in enumerate(st.session_state.trips):
        _r_list = _trip.get("routes") or []
        _sel_rows.append({
            "Select": _i in st.session_state.routes_selected,
            "Trip":   _trip.get("title") or f"Trip {_i + 1}",
            "Modes":  _mode_summary(_r_list),
            "Routes": len(_r_list),
            "Date":   _trip.get("created_at", ""),
        })

    _edited = st.data_editor(
        _sel_rows,
        column_config={
            "Select": st.column_config.CheckboxColumn("Select", width="small"),
            "Trip":   st.column_config.TextColumn("Trip"),
            "Modes":  st.column_config.TextColumn("Modes", width="small"),
            "Routes": st.column_config.NumberColumn("Routes", width="small"),
            "Date":   st.column_config.TextColumn("Date", width="small"),
        },
        disabled=["Trip", "Modes", "Routes", "Date"],
        hide_index=True,
        key="routes_sel_editor",
        use_container_width=True,
    )
    st.session_state.routes_selected = {_i for _i, _r in enumerate(_edited) if _r["Select"]}

    _n_sel = len(st.session_state.routes_selected)
    if _n_sel > 0:
        _n_routes = sum(len((st.session_state.trips[_i].get("routes") or [])) for _i in st.session_state.routes_selected)
        if st.button(f"🧳 Add {_n_routes} route{'s' if _n_routes != 1 else ''} to My Routes",
                     type="primary", use_container_width=True, key="convert_to_routes_btn"):
            for _i in sorted(st.session_state.routes_selected):
                for _route in (st.session_state.trips[_i].get("routes") or []):
                    st.session_state.routes.append(_route)
            st.session_state.routes_selection_open = False
            st.session_state.routes_selected = set()
            st.switch_page("pages/1_My_Routes.py")
