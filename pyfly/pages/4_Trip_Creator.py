"""Trip Creator — build a blog-style trip from selected routes."""
import sys
import json
import time as _time
from datetime import date as _date
from pathlib import Path

_ROOT = Path(__file__).parent.parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

import pydeck as pdk
import streamlit as st
from streamlit_sortables import sort_items
from pyfly.trip_utils import generate_markdown, node_coords, stop_display

st.set_page_config(
    page_title="Trip Creator — PyFly",
    page_icon="✈",
    layout="wide",
    initial_sidebar_state="collapsed",
)

MODE_REF_LABEL = {"plane": "Flight number", "train": "Train number", "boat": "Vessel", "car": "Bus / service no."}
MODE_COLOUR = {
    "plane": [245, 158, 11, 200],
    "train": [16, 185, 129, 200],
    "boat":  [6, 182, 212, 200],
    "car":   [244, 63, 94, 200],
}
MODE_ICONS = {"plane": "✈", "train": "🚂", "boat": "⛴", "car": "🚗"}


def _iata_map_cached() -> dict:
    """Lazy import to avoid circular dependency with My Routes page."""
    data_dir = _ROOT / "data"
    path = data_dir / "airports.csv"
    if not path.exists():
        return {}
    if "_trip_iata_map" not in st.session_state:
        import polars as pl
        df = pl.read_csv(path, ignore_errors=True).filter(
            pl.col("iata_code").is_not_null() & (pl.col("iata_code") != "")
        )
        cols = ["iata_code", "latitude_deg", "longitude_deg"]
        st.session_state["_trip_iata_map"] = {
            r["iata_code"]: {"lat": r["latitude_deg"], "lon": r["longitude_deg"]}
            for r in df.select(cols).iter_rows(named=True)
        }
    return st.session_state["_trip_iata_map"]


def _build_trip_map_layers(stops: list[dict], iata_map: dict) -> list:
    arc_rows, line_rows, node_rows = [], [], []
    coords_list = []
    for stop in stops:
        c = node_coords(stop["node"], iata_map)
        coords_list.append(c)

    for i in range(len(stops) - 1):
        t_out = stops[i].get("transit_out") or {}
        mode = t_out.get("mode", "plane")
        c0, c1 = coords_list[i], coords_list[i + 1]
        if not c0 or not c1:
            continue
        colour = MODE_COLOUR.get(mode, MODE_COLOUR["plane"])
        row = {"origin_lat": c0[0], "origin_lon": c0[1],
               "dest_lat": c1[0], "dest_lon": c1[1],
               "colour": colour, "label": f"{stop_display(stops[i])} → {stop_display(stops[i+1])}"}
        (arc_rows if mode == "plane" else line_rows).append(row)

    for i, stop in enumerate(stops):
        c = coords_list[i]
        if c:
            node_rows.append({"lat": c[0], "lon": c[1], "label": stop_display(stop)})

    layers = []
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
    if node_rows:
        layers.append(pdk.Layer("ScatterplotLayer", data=node_rows,
            get_position=["lon", "lat"], get_fill_color=[255, 255, 255, 200],
            get_radius=50000, pickable=True))
    return layers


# ---------------------------------------------------------------------------
# Guard — must arrive from My Routes with a draft
# ---------------------------------------------------------------------------

if "trip_draft" not in st.session_state or not st.session_state.trip_draft:
    st.warning("No trip draft found. Go to My Routes and select routes to convert.")
    if st.button("← Back to My Routes"):
        st.switch_page("pages/1_My_Routes.py")
    st.stop()

draft = st.session_state.trip_draft
iata_map = _iata_map_cached()

# ---------------------------------------------------------------------------
# Top bar
# ---------------------------------------------------------------------------

_top_left, _top_right = st.columns([1, 5])
with _top_left:
    if st.button("← Change selection"):
        st.session_state.trip_selection_open = True
        st.switch_page("pages/1_My_Routes.py")

st.title("✈ Trip Creator")

# ---------------------------------------------------------------------------
# Trip header
# ---------------------------------------------------------------------------

draft["title"] = st.text_input("Trip title", value=draft.get("title", ""), placeholder="Summer 2026")
draft["description"] = st.text_area(
    "Description", value=draft.get("description", ""),
    placeholder="A short intro — rendered as the opening paragraph of your trip.",
    height=80,
)

st.markdown("---")

# ---------------------------------------------------------------------------
# Stop list
# ---------------------------------------------------------------------------

stops = draft.get("stops") or []

st.subheader("Stops")
st.caption("Drag to reorder stops · use ↑↓ to reorder sections within a stop.")

# Build sortable labels (index encoded so we can re-map after sort)
_sort_labels = [f"{i}::{_stop_display(s)}" for i, s in enumerate(stops)]
_sorted_labels = sort_items(_sort_labels, direction="vertical", key="stop_sort")

# Re-order stops based on drag result
_new_order = []
for lbl in _sorted_labels:
    try:
        _idx = int(lbl.split("::")[0])
        _new_order.append(stops[_idx])
    except (ValueError, IndexError):
        pass
if _new_order and len(_new_order) == len(stops):
    stops = _new_order
    draft["stops"] = stops

_need_rerun = False

for _si, stop in enumerate(stops):
    with st.expander(f"{'~~' if stop.get('transited') else ''}**{_stop_display(stop)}**{'~~' if stop.get('transited') else ''}", expanded=not stop.get("transited")):
        _c1, _c2 = st.columns([3, 1])
        with _c1:
            stop["title"] = st.text_input(
                "Stop title", value=stop.get("title", ""),
                key=f"stop_title_{_si}", label_visibility="collapsed",
                placeholder="City or place name",
            )
        with _c2:
            stop["transited"] = st.checkbox(
                "Just transited", value=stop.get("transited", False),
                key=f"stop_transit_{_si}",
            )

        if not stop.get("transited"):
            sections = stop.get("sections") or []

            for _seci, sec in enumerate(sections):
                _sc1, _sc2, _sc3 = st.columns([3, 0.3, 0.3])
                with _sc1:
                    sec["title"] = st.text_input(
                        "Section title", value=sec.get("title", ""),
                        key=f"sec_title_{_si}_{_seci}", label_visibility="collapsed",
                        placeholder="Day 1 / Arrival / Morning hike…",
                    )
                with _sc2:
                    if _seci > 0 and st.button("↑", key=f"sec_up_{_si}_{_seci}", help="Move up"):
                        sections[_seci - 1], sections[_seci] = sections[_seci], sections[_seci - 1]
                        _need_rerun = True
                with _sc3:
                    if _seci < len(sections) - 1 and st.button("↓", key=f"sec_dn_{_si}_{_seci}", help="Move down"):
                        sections[_seci], sections[_seci + 1] = sections[_seci + 1], sections[_seci]
                        _need_rerun = True

                sec["notes"] = st.text_area(
                    "Notes", value=sec.get("notes", ""),
                    key=f"sec_notes_{_si}_{_seci}", label_visibility="collapsed",
                    placeholder="Write about this section… markdown supported.",
                    height=100,
                )

            if st.button("+ Add section", key=f"add_sec_{_si}"):
                sections.append({"title": "", "notes": "", "photos": []})
                _need_rerun = True

            stop["sections"] = sections

        # Transit out
        t_out = stop.get("transit_out")
        if t_out is not None:
            st.markdown("---")
            _t1, _t2, _t3 = st.columns([1, 2, 3])
            with _t1:
                st.markdown(f"**{MODE_ICONS.get(t_out.get('mode','plane'))} {t_out.get('mode','').capitalize()}**")
            with _t2:
                t_out["ref"] = st.text_input(
                    MODE_REF_LABEL.get(t_out.get("mode", "plane"), "Reference"),
                    value=t_out.get("ref", ""),
                    key=f"tref_{_si}", placeholder="e.g. IB3105",
                )
            with _t3:
                t_out["notes"] = st.text_input(
                    "Transit notes", value=t_out.get("notes", ""),
                    key=f"tnotes_{_si}", placeholder="Optional — overnight, rough sea…",
                )
            stop["transit_out"] = t_out

if _need_rerun:
    st.rerun()

st.markdown("---")

# ---------------------------------------------------------------------------
# Preview + Save
# ---------------------------------------------------------------------------

_prev_col, _map_col = st.columns([1, 1])

with _prev_col:
    st.subheader("Preview")
    _md = generate_markdown(draft, iata_map)
    st.markdown(_md if _md.strip() else "_Add a title and some notes to see a preview._")

with _map_col:
    st.subheader("Map")
    _layers = _build_trip_map_layers(stops, iata_map)
    _all_coords = [node_coords(s["node"], iata_map) for s in stops]
    _all_coords = [c for c in _all_coords if c]
    if _all_coords:
        _clat = (max(c[0] for c in _all_coords) + min(c[0] for c in _all_coords)) / 2
        _clon = (max(c[1] for c in _all_coords) + min(c[1] for c in _all_coords)) / 2
        _span = max(
            max(c[0] for c in _all_coords) - min(c[0] for c in _all_coords),
            max(c[1] for c in _all_coords) - min(c[1] for c in _all_coords),
        )
        _zoom = 7 if _span < 5 else 5 if _span < 15 else 4 if _span < 40 else 3 if _span < 80 else 2
    else:
        _clat, _clon, _zoom = 30.0, 0.0, 2

    st.pydeck_chart(pdk.Deck(
        layers=_layers,
        initial_view_state=pdk.ViewState(latitude=_clat, longitude=_clon, zoom=_zoom, pitch=20),
        map_style="https://basemaps.cartocdn.com/gl/dark-matter-gl-style/style.json",
    ), height=500)

st.markdown("---")

_save_col, _discard_col, _ = st.columns([1, 1, 3])

with _save_col:
    if st.button("💾 Save Trip", type="primary", use_container_width=True):
        # Strip empty photos/ref/notes before saving
        _to_save = json.loads(json.dumps(draft))  # deep copy via JSON
        for _stop in _to_save.get("stops") or []:
            if not _stop.get("photos"):
                _stop.pop("photos", None)
            for _sec in _stop.get("sections") or []:
                if not _sec.get("photos"):
                    _sec.pop("photos", None)
            _t = _stop.get("transit_out")
            if _t:
                if not _t.get("ref"):
                    _t.pop("ref", None)
                if not _t.get("notes"):
                    _t.pop("notes", None)
        st.session_state.trips.append(_to_save)
        st.session_state.trip_draft = None
        st.success("Trip saved!")
        st.switch_page("pages/5_My_Trips.py")

with _discard_col:
    if st.button("🗑 Discard", use_container_width=True):
        st.session_state.trip_draft = None
        st.switch_page("pages/1_My_Routes.py")
