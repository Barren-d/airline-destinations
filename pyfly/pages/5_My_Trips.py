"""My Trips — collection of saved trips rendered as blog entries."""
import sys
import json
from pathlib import Path

_ROOT = Path(__file__).parent.parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

import pydeck as pdk
import streamlit as st
from pyfly.trip_utils import generate_markdown

st.set_page_config(
    page_title="My Trips — PyFly",
    page_icon="🗺",
    layout="wide",
    initial_sidebar_state="collapsed",
)

MODE_ICONS = {"plane": "✈", "train": "🚂", "boat": "⛴", "car": "🚗"}


def _stop_display(stop: dict) -> str:
    return stop.get("title") or (stop.get("node") or {}).get("iata") or "Stop"


def _all_stops(trip: dict) -> list[dict]:
    """Yield all stops regardless of new grouped or legacy flat format."""
    if trip.get("routes") is not None:
        return [s for g in trip["routes"] for s in (g.get("stops") or [])]
    return trip.get("stops") or []


def _trip_summary(trip: dict) -> str:
    stops = [s for s in _all_stops(trip) if not s.get("transited")]
    cities = " → ".join(_stop_display(s) for s in stops[:4])
    if len(stops) > 4:
        cities += f" +{len(stops) - 4} more"
    return cities


def _mode_icons(trip: dict) -> str:
    if trip.get("routes") is not None:
        modes = {g.get("mode") for g in trip["routes"]} - {None}
    else:
        modes = {
            (s.get("transit_out") or {}).get("mode")
            for s in (trip.get("stops") or [])
            if s.get("transit_out")
        } - {None}
    return "  ".join(MODE_ICONS.get(m, "") for m in sorted(modes))


# ---------------------------------------------------------------------------
# Guard
# ---------------------------------------------------------------------------

if "trips" not in st.session_state:
    st.session_state.trips = []
if "trip_draft" not in st.session_state:
    st.session_state.trip_draft = None

st.title("🗺 My Trips")

if not st.session_state.trips:
    st.info("No trips saved yet. Go to **My Routes**, select some routes, and click **Convert to Trip →**.")
    if st.button("← Go to My Routes"):
        st.switch_page("pages/1_My_Routes.py")
    st.stop()

# ---------------------------------------------------------------------------
# Download / Upload
# ---------------------------------------------------------------------------

with st.expander("Import / Export", expanded=False):
    _dl_col, _ul_col = st.columns(2)
    with _dl_col:
        st.download_button(
            "⬇ Download all trips",
            data=json.dumps(st.session_state.trips, indent=2),
            file_name="my_trips.json",
            mime="application/json",
            use_container_width=True,
        )
    with _ul_col:
        _uploaded = st.file_uploader("⬆ Upload trips JSON", type="json", label_visibility="collapsed")
        if _uploaded:
            try:
                _data = json.loads(_uploaded.read().decode())
                if isinstance(_data, list) and all(isinstance(t, dict) and ("routes" in t or "stops" in t) for t in _data):
                    st.session_state.trips = _data
                    st.success(f"Loaded {len(_data)} trip{'s' if len(_data) != 1 else ''}.")
                    st.rerun()
                else:
                    st.error("Invalid format — expected a PyFly trips JSON export.")
            except Exception:
                st.error("Invalid JSON file.")

st.markdown("---")

# ---------------------------------------------------------------------------
# Trip cards + expanded blog view
# ---------------------------------------------------------------------------

if "expanded_trip" not in st.session_state:
    st.session_state.expanded_trip = None

_to_delete = None

for _ti, _trip in enumerate(st.session_state.trips):
    _title = _trip.get("title") or f"Trip {_ti + 1}"
    _n_stops = len([s for s in _all_stops(_trip) if not s.get("transited")])
    _summary = _trip_summary(_trip)
    _icons = _mode_icons(_trip)
    _date = _trip.get("created_at", "")

    with st.container(border=True):
        _card_left, _card_right = st.columns([5, 1])
        with _card_left:
            st.markdown(f"### {_title}")
            st.caption(f"{_icons}  {_summary}")
            if _date:
                st.caption(f"Created {_date} · {_n_stops} stop{'s' if _n_stops != 1 else ''}")
        with _card_right:
            _is_open = st.session_state.expanded_trip == _ti
            if st.button("▲ Close" if _is_open else "▼ Open", key=f"tog_{_ti}", use_container_width=True):
                st.session_state.expanded_trip = None if _is_open else _ti
                st.rerun()
            if st.button("✏ Edit", key=f"edit_{_ti}", use_container_width=True):
                st.session_state.trip_draft = json.loads(json.dumps(_trip))
                st.switch_page("pages/4_Trip_Creator.py")
            if st.button("🗑", key=f"del_{_ti}", use_container_width=True, help="Delete trip"):
                _to_delete = _ti

        if st.session_state.expanded_trip == _ti:
            st.markdown("---")
            _blog_col, _map_col = st.columns([1, 1])

            with _blog_col:
                _iata_m = st.session_state.get("_trip_iata_map", {})
                _md = generate_markdown(_trip, _iata_m)
                st.markdown(_md if _md.strip() else "_No content yet._")

            with _map_col:
                # Build map
                try:
                    import math as _math
                    _data_dir = _ROOT / "data"
                    import polars as pl
                    _airports_path = _data_dir / "airports.csv"
                    if "_trip_iata_map" not in st.session_state and _airports_path.exists():
                        _df = pl.read_csv(_airports_path, ignore_errors=True).filter(
                            pl.col("iata_code").is_not_null() & (pl.col("iata_code") != "")
                        )
                        st.session_state["_trip_iata_map"] = {
                            r["iata_code"]: {"lat": r["latitude_deg"], "lon": r["longitude_deg"]}
                            for r in _df.select(["iata_code", "latitude_deg", "longitude_deg"]).iter_rows(named=True)
                        }
                    _iata_m = st.session_state.get("_trip_iata_map", {})

                    _COLOUR = {"plane": [245, 158, 11, 200], "train": [16, 185, 129, 200],
                               "boat": [6, 182, 212, 200], "car": [244, 63, 94, 200]}
                    _arc_rows, _line_rows, _node_rows = [], [], []
                    _all_coords = []

                    # Handle new grouped format (routes) and legacy flat format (stops)
                    _route_groups = _trip.get("routes")
                    if _route_groups is None:
                        _route_groups = [{"mode": "plane", "stops": _trip.get("stops") or []}]

                    for _group in _route_groups:
                        _stops = _group.get("stops") or []
                        _coords = []
                        for _s in _stops:
                            _nd = _s.get("node") or {}
                            if _nd.get("iata") and _nd["iata"] in _iata_m:
                                _c2 = _iata_m[_nd["iata"]]
                                _coords.append((_c2["lat"], _c2["lon"]))
                            elif _nd.get("lat") is not None:
                                _coords.append((_nd["lat"], _nd["lon"]))
                            else:
                                _coords.append(None)
                        _all_coords.extend(c for c in _coords if c)

                        # Departure leg
                        _dep2 = _group.get("departure")
                        if _dep2 and _coords:
                            _dc = None
                            _dn = _dep2.get("node", {})
                            if _dn.get("iata") and _dn["iata"] in _iata_m:
                                _r = _iata_m[_dn["iata"]]
                                _dc = (_r["lat"], _r["lon"])
                            elif _dn.get("lat") is not None:
                                _dc = (_dn["lat"], _dn["lon"])
                            if _dc and _coords[0]:
                                _gm = _group.get("mode", "plane")
                                _colour = _COLOUR.get(_gm, [200, 200, 200, 200])
                                _row = {"origin_lat": _dc[0], "origin_lon": _dc[1],
                                        "dest_lat": _coords[0][0], "dest_lon": _coords[0][1], "colour": _colour}
                                (_arc_rows if _gm == "plane" else _line_rows).append(_row)
                                _node_rows.append({"lat": _dc[0], "lon": _dc[1]})
                                _all_coords.append(_dc)

                        for _i in range(len(_stops) - 1):
                            _t = (_stops[_i].get("transit_out") or {})
                            _mode = _t.get("mode", _group.get("mode", "plane"))
                            _c0, _c1 = _coords[_i], _coords[_i + 1]
                            if not _c0 or not _c1:
                                continue
                            _colour = _COLOUR.get(_mode, [200, 200, 200, 200])
                            _row = {"origin_lat": _c0[0], "origin_lon": _c0[1],
                                    "dest_lat": _c1[0], "dest_lon": _c1[1], "colour": _colour}
                            (_arc_rows if _mode == "plane" else _line_rows).append(_row)

                        for _c in _coords:
                            if _c:
                                _node_rows.append({"lat": _c[0], "lon": _c[1]})

                    _coords = _all_coords

                    _layers = []
                    if _arc_rows:
                        _layers.append(pdk.Layer("ArcLayer", data=_arc_rows,
                            get_source_position=["origin_lon", "origin_lat"],
                            get_target_position=["dest_lon", "dest_lat"],
                            get_source_color="colour", get_target_color="colour", get_width=2))
                    if _line_rows:
                        _layers.append(pdk.Layer("LineLayer", data=_line_rows,
                            get_source_position=["origin_lon", "origin_lat"],
                            get_target_position=["dest_lon", "dest_lat"],
                            get_color="colour", get_width=2))
                    if _node_rows:
                        _layers.append(pdk.Layer("ScatterplotLayer", data=_node_rows,
                            get_position=["lon", "lat"],
                            get_fill_color=[255, 255, 255, 200], get_radius=50000))

                    _valid = [c for c in _coords if c]
                    if _valid:
                        _clat = (max(c[0] for c in _valid) + min(c[0] for c in _valid)) / 2
                        _clon = (max(c[1] for c in _valid) + min(c[1] for c in _valid)) / 2
                        _span = max(
                            max(c[0] for c in _valid) - min(c[0] for c in _valid),
                            max(c[1] for c in _valid) - min(c[1] for c in _valid),
                        )
                        _zoom = 7 if _span < 5 else 5 if _span < 15 else 4 if _span < 40 else 3 if _span < 80 else 2
                    else:
                        _clat, _clon, _zoom = 30.0, 0.0, 2

                    st.pydeck_chart(pdk.Deck(
                        layers=_layers,
                        initial_view_state=pdk.ViewState(latitude=_clat, longitude=_clon, zoom=_zoom, pitch=20),
                        map_style="https://basemaps.cartocdn.com/gl/dark-matter-gl-style/style.json",
                    ), height=400)
                except Exception as _e:
                    st.caption(f"Map unavailable: {_e}")

if _to_delete is not None:
    st.session_state.trips.pop(_to_delete)
    if st.session_state.expanded_trip == _to_delete:
        st.session_state.expanded_trip = None
    st.rerun()
