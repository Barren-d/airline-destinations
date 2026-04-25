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

MODE_REF_LABEL = {"plane": "Flight no.", "train": "Train no.", "boat": "Vessel", "car": "Service no."}
MODE_COLOUR = {
    "plane": [245, 158, 11, 200],
    "train": [16, 185, 129, 200],
    "boat":  [6, 182, 212, 200],
    "car":   [244, 63, 94, 200],
}
MODE_ICONS = {"plane": "✈", "train": "🚂", "boat": "⛴", "car": "🚗"}


def _iata_map_cached() -> dict:
    data_dir = _ROOT / "data"
    path = data_dir / "airports.csv"
    if not path.exists():
        return {}
    if "_trip_iata_map" not in st.session_state:
        import polars as pl
        df = pl.read_csv(path, ignore_errors=True).filter(
            pl.col("iata_code").is_not_null() & (pl.col("iata_code") != "")
        )
        st.session_state["_trip_iata_map"] = {
            r["iata_code"]: {"lat": r["latitude_deg"], "lon": r["longitude_deg"]}
            for r in df.select(["iata_code", "latitude_deg", "longitude_deg"]).iter_rows(named=True)
        }
    return st.session_state["_trip_iata_map"]


def _build_trip_map_layers(route_groups: list[dict], iata_map: dict) -> tuple[list, list]:
    """Return (pydeck layers, list of valid coords) for the trip map."""
    arc_rows, line_rows, node_rows = [], [], []
    all_coords = []

    for group in route_groups:
        stops = group.get("stops") or []
        mode = group.get("mode", "plane")
        coords = [node_coords(s["node"], iata_map) for s in stops]

        # Departure leg: origin → first stop
        _dep = group.get("departure")
        if _dep and stops:
            _dep_c = node_coords(_dep.get("node", {}), iata_map)
            if _dep_c and coords[0]:
                colour = MODE_COLOUR.get(mode, MODE_COLOUR["plane"])
                row = {"origin_lat": _dep_c[0], "origin_lon": _dep_c[1],
                       "dest_lat": coords[0][0], "dest_lon": coords[0][1], "colour": colour}
                (arc_rows if mode == "plane" else line_rows).append(row)
                all_coords.append(_dep_c)

        all_coords.extend(c for c in coords if c)

        for i in range(len(stops) - 1):
            t_out = stops[i].get("transit_out") or {}
            mode = t_out.get("mode", group.get("mode", "plane"))
            c0, c1 = coords[i], coords[i + 1]
            if not c0 or not c1:
                continue
            colour = MODE_COLOUR.get(mode, MODE_COLOUR["plane"])
            row = {"origin_lat": c0[0], "origin_lon": c0[1],
                   "dest_lat": c1[0], "dest_lon": c1[1], "colour": colour}
            (arc_rows if mode == "plane" else line_rows).append(row)

        for i, stop in enumerate(stops):
            c = coords[i]
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
    return layers, all_coords


# ---------------------------------------------------------------------------
# Guard
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

_top_left, _ = st.columns([1, 5])
with _top_left:
    if st.button("← Change selection"):
        st.session_state.trip_selection_open = True
        st.switch_page("pages/1_My_Routes.py")

st.title("✏️ Trip Creator")

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
# Route list — drag to reorder routes, nodes are fixed within each route
# ---------------------------------------------------------------------------

routes = draft.get("routes") or []

st.subheader("Routes")
st.caption("Drag to reorder routes. Nodes within each route are fixed.")

_sort_labels = [f"{i}::{r.get('label', f'Route {i+1}')}" for i, r in enumerate(routes)]
_sorted_labels = sort_items(_sort_labels, direction="vertical", key="route_sort")

_new_order = []
for lbl in _sorted_labels:
    try:
        _idx = int(lbl.split("::")[0])
        _new_order.append(routes[_idx])
    except (ValueError, IndexError):
        pass
if _new_order and len(_new_order) == len(routes):
    routes = _new_order
    draft["routes"] = routes

_need_rerun = False

for _ri, route in enumerate(routes):
    _route_stops = route.get("stops") or []
    _mode_icon = MODE_ICONS.get(route.get("mode", "plane"), "✈")
    _auto_label = " → ".join(stop_display(s) for s in _route_stops) or route.get("label", f"Route {_ri + 1}")
    _display_name = route.get("name") or _auto_label

    route["name"] = st.text_input(
        f"route_name_{_ri}", value=route.get("name", ""),
        key=f"route_name_{_ri}",
        placeholder=_auto_label,
        label_visibility="collapsed",
    )

    with st.expander(f"{_mode_icon} **{route.get('name') or _auto_label}**", expanded=True):
        # Departure segment — first leg from origin to first stop
        _dep = route.get("departure")
        if _dep:
            _first_stop = _route_stops[0] if _route_stops else None
            _dep_from = _dep.get("node", {}).get("iata") or _dep.get("title", "?")
            _dep_to = (_first_stop["node"].get("iata") or stop_display(_first_stop)) if _first_stop else "?"
            _pad0, _dep_col = st.columns([0.04, 1])
            with _dep_col:
                st.caption(f"{_mode_icon} {_dep_from} → {_dep_to}")
                _d1, _d2, _d3, _d4 = st.columns([0.4, 1, 1.5, 2.5])
                with _d1:
                    st.markdown(f"**{_mode_icon}**")
                with _d2:
                    _dep["date"] = st.text_input("Date", value=_dep.get("date", ""),
                        key=f"dep_date_{_ri}", placeholder="25 Apr")
                with _d3:
                    _dep["ref"] = st.text_input(
                        MODE_REF_LABEL.get(route.get("mode", "plane"), "Ref"),
                        value=_dep.get("ref", ""),
                        key=f"dep_ref_{_ri}", placeholder="IB3105")
                with _d4:
                    _dep["notes"] = st.text_input("Notes", value=_dep.get("notes", ""),
                        key=f"dep_notes_{_ri}", placeholder="Optional — overnight, rough seas…")
            route["departure"] = _dep

        for _si, stop in enumerate(_route_stops):
            _pad, _node_col = st.columns([0.04, 1])
            with _node_col:
                with st.container(border=True):
                    _c1, _c2 = st.columns([4, 1])
                    with _c1:
                        stop["title"] = st.text_input(
                            "Stop name", value=stop.get("title", ""),
                            key=f"stop_title_{_ri}_{_si}",
                            label_visibility="collapsed",
                            placeholder="City or place name",
                        )
                    with _c2:
                        stop["transited"] = st.checkbox(
                            "Ignore", value=stop.get("transited", False),
                            key=f"stop_ignore_{_ri}_{_si}",
                        )

                    if not stop.get("transited"):
                        sections = stop.get("sections") or [{"title": "", "notes": "", "photos": []}]

                        for _seci, sec in enumerate(sections):
                            _sc1, _sc2, _sc3 = st.columns([3, 0.3, 0.3])
                            with _sc1:
                                sec["title"] = st.text_input(
                                    "Section title", value=sec.get("title", ""),
                                    key=f"sec_title_{_ri}_{_si}_{_seci}",
                                    label_visibility="collapsed",
                                    placeholder="Day 1 / Arrival / Morning hike…",
                                )
                            with _sc2:
                                if _seci > 0 and st.button("↑", key=f"sec_up_{_ri}_{_si}_{_seci}", help="Move up"):
                                    sections[_seci - 1], sections[_seci] = sections[_seci], sections[_seci - 1]
                                    _need_rerun = True
                            with _sc3:
                                if _seci < len(sections) - 1 and st.button("↓", key=f"sec_dn_{_ri}_{_si}_{_seci}", help="Move down"):
                                    sections[_seci], sections[_seci + 1] = sections[_seci + 1], sections[_seci]
                                    _need_rerun = True

                            sec["notes"] = st.text_area(
                                "Notes", value=sec.get("notes", ""),
                                key=f"sec_notes_{_ri}_{_si}_{_seci}",
                                label_visibility="collapsed",
                                placeholder="Write about this section… markdown supported.",
                                height=100,
                            )

                        if st.button("+ Add section", key=f"add_sec_{_ri}_{_si}"):
                            sections.append({"title": "", "notes": "", "photos": []})
                            _need_rerun = True

                        stop["sections"] = sections

            # Transit segment sits between nodes, outside the node card
            t_out = stop.get("transit_out")
            if t_out is not None:
                _next_stop = _route_stops[_si + 1] if _si + 1 < len(_route_stops) else None
                _seg_icon = MODE_ICONS.get(t_out.get("mode", "plane"), "✈")
                _from_code = stop["node"].get("iata") or stop_display(stop)
                _to_code = (_next_stop["node"].get("iata") or stop_display(_next_stop)) if _next_stop else "?"
                _pad2, _transit_col = st.columns([0.04, 1])
                with _transit_col:
                    st.caption(f"{_seg_icon} {_from_code} → {_to_code}")
                    _t1, _t2, _t3, _t4 = st.columns([0.4, 1, 1.5, 2.5])
                    with _t1:
                        st.markdown(f"**{_seg_icon}**")
                    with _t2:
                        t_out["date"] = st.text_input(
                            "Date", value=t_out.get("date", ""),
                            key=f"tdate_{_ri}_{_si}", placeholder="25 Apr",
                        )
                    with _t3:
                        t_out["ref"] = st.text_input(
                            MODE_REF_LABEL.get(t_out.get("mode", "plane"), "Ref"),
                            value=t_out.get("ref", ""),
                            key=f"tref_{_ri}_{_si}", placeholder="IB3105",
                        )
                    with _t4:
                        t_out["notes"] = st.text_input(
                            "Notes", value=t_out.get("notes", ""),
                            key=f"tnotes_{_ri}_{_si}", placeholder="Optional — overnight, rough seas…",
                        )
                    stop["transit_out"] = t_out

if _need_rerun:
    st.rerun()

st.markdown("---")

# ---------------------------------------------------------------------------
# Preview + Map
# ---------------------------------------------------------------------------

_prev_col, _map_col = st.columns([1, 1])

with _prev_col:
    st.subheader("Preview")
    _md = generate_markdown(draft, iata_map)
    st.markdown(_md if _md.strip() else "_Add a title and some notes to see a preview._")

with _map_col:
    st.subheader("Map")
    _layers, _all_coords = _build_trip_map_layers(routes, iata_map)
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

# ---------------------------------------------------------------------------
# Save / Discard
# ---------------------------------------------------------------------------

_save_col, _discard_col, _ = st.columns([1, 1, 3])

with _save_col:
    if st.button("💾 Save Trip", type="primary", use_container_width=True):
        _to_save = json.loads(json.dumps(draft))
        for _group in _to_save.get("routes") or []:
            for _stop in _group.get("stops") or []:
                if not _stop.get("photos"):
                    _stop.pop("photos", None)
                for _sec in _stop.get("sections") or []:
                    if not _sec.get("photos"):
                        _sec.pop("photos", None)
                _t = _stop.get("transit_out")
                if _t:
                    for _k in ("ref", "date", "notes"):
                        if not _t.get(_k):
                            _t.pop(_k, None)
        st.session_state.trips.append(_to_save)
        st.session_state.trip_draft = None
        st.session_state._goto_my_trips = True
        st.rerun()

with _discard_col:
    if st.button("🗑 Discard", use_container_width=True):
        st.session_state.trip_draft = None
        st.switch_page("pages/1_My_Routes.py")
