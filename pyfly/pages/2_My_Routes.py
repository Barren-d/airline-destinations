"""My Routes — personal travel history map."""
import sys
import json
import gzip
import base64
import math
from pathlib import Path

_ROOT = Path(__file__).parent.parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

import polars as pl
import pydeck as pdk
import streamlit as st
from rapidfuzz import process as rf_process, fuzz
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut

st.set_page_config(
    page_title="My Routes — PyFly",
    page_icon="🧳",
    layout="wide",
    initial_sidebar_state="expanded",
)

DATA_DIR = Path(__file__).parent.parent.parent / "data"

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

MODES = {"✈ Plane": "plane", "🚂 Train": "train", "⛴ Boat": "boat", "🚗 Car": "car"}
MODE_ICONS = {"plane": "✈", "train": "🚂", "boat": "⛴", "car": "🚗"}
MODE_TRIP_LABEL = {"plane": ("flight", "flights"), "train": ("train", "trains"), "boat": ("voyage", "voyages"), "car": ("ride", "rides")}

MODE_COLOUR = {
    "plane": [245, 158, 11, 200],
    "train": [16, 185, 129, 200],
    "boat":  [6, 182, 212, 200],
    "car":   [244, 63, 94, 200],
}

GROUND_MODES = {"train", "boat", "car"}


def _trip_width(n: int) -> float:
    if n == 1: return 2.0
    if n == 2: return 3.0
    if n == 3: return 4.5
    if n == 4: return 6.0
    if n == 5: return 7.5
    if n <= 8:  return 9.0
    return 12.0


# ---------------------------------------------------------------------------
# Airport data
# ---------------------------------------------------------------------------

@st.cache_data
def _airport_df() -> pl.DataFrame:
    path = DATA_DIR / "airports.csv"
    if not path.exists():
        return pl.DataFrame()
    return (
        pl.read_csv(path)
        .filter(pl.col("iata_code").is_not_null() & (pl.col("iata_code") != ""))
        .select(["iata_code", "name", "latitude_deg", "longitude_deg", "iso_country"])
        .rename({"latitude_deg": "lat", "longitude_deg": "lon"})
    )


@st.cache_data
def _iata_map() -> dict:
    return {r["iata_code"]: r for r in _airport_df().iter_rows(named=True)}


@st.cache_data
def _fuzzy_corpus() -> list[str]:
    return [
        f"{r['name']} ({r['iata_code']})"
        for r in _airport_df().iter_rows(named=True)
    ]


# ---------------------------------------------------------------------------
# Token resolution
# ---------------------------------------------------------------------------

def _node_from_iata(iata: str) -> dict:
    r = _iata_map()[iata]
    return {"label": f"{r['name']} ({iata})", "iata": iata, "lat": r["lat"], "lon": r["lon"]}


def _fuzzy_candidates(query: str, limit: int = 6) -> list[dict]:
    matches = rf_process.extract(query, _fuzzy_corpus(), scorer=fuzz.WRatio, limit=limit)
    results = []
    iata_map = _iata_map()
    for label, score, _ in matches:
        if score < 45:
            continue
        iata = label.split("(")[-1].rstrip(")")
        if iata in iata_map:
            r = iata_map[iata]
            results.append({"label": label, "iata": iata, "lat": r["lat"], "lon": r["lon"], "score": score})
    return results


def _geocode(query: str) -> dict | None:
    cache = st.session_state.geocode_cache
    if query in cache:
        return cache[query]
    try:
        geo = Nominatim(user_agent="pyfly-routes/1.0")
        loc = geo.geocode(query, timeout=5)
        if loc:
            result = {
                "label": loc.address.split(",")[0].strip(),
                "iata": None,
                "lat": loc.latitude,
                "lon": loc.longitude,
            }
            cache[query] = result
            return result
    except (GeocoderTimedOut, Exception):
        pass
    cache[query] = None
    return None


def _resolve(token: str, mode: str) -> tuple[dict | None, list[dict]]:
    """Return (auto_resolved_node, candidates_if_ambiguous)."""
    upper = token.strip().upper()

    # Exact IATA always wins regardless of mode
    if upper in _iata_map():
        return _node_from_iata(upper), []

    if mode == "plane":
        candidates = _fuzzy_candidates(token)
        if not candidates:
            return None, []
        if len(candidates) == 1 or candidates[0]["score"] >= 88:
            return candidates[0], []
        return None, candidates

    # Ground / sea — geocode
    result = _geocode(token)
    return result, []


# ---------------------------------------------------------------------------
# State management
# ---------------------------------------------------------------------------

def _init_state():
    defaults = {"routes": [], "geocode_cache": {}, "pending": None, "_url_loaded": False, "_date_clear": 0}
    for k, v in defaults.items():
        if k not in st.session_state:
            st.session_state[k] = v


def _load_url_once():
    if st.session_state._url_loaded:
        return
    st.session_state._url_loaded = True
    r = st.query_params.get("r")
    if r:
        try:
            raw = base64.urlsafe_b64decode(r.encode())
            data = json.loads(gzip.decompress(raw).decode())
            st.session_state.routes = [_expand(e) for e in data]
        except Exception:
            pass


def _slim(entry: dict) -> dict:
    """Strip node data down to just what's needed for storage."""
    return {
        "legs": entry["legs"],
        "mode": entry["mode"],
        "tag": entry.get("tag", ""),
        "date": entry.get("date", ""),
        "nodes": [
            {"iata": n["iata"]} if n.get("iata")
            else {"label": n["label"], "lat": n["lat"], "lon": n["lon"]}
            for n in (entry.get("nodes") or []) if n
        ],
    }


def _expand(entry: dict) -> dict:
    """Re-hydrate slim node entries from airports.csv."""
    iata_map = _iata_map()
    nodes = []
    for n in entry.get("nodes") or []:
        if n.get("iata") and n["iata"] in iata_map:
            r = iata_map[n["iata"]]
            nodes.append({"label": f"{r['name']} ({n['iata']})", "iata": n["iata"], "lat": r["lat"], "lon": r["lon"]})
        else:
            nodes.append(n)
    return {**entry, "nodes": nodes}


def _sync_url():
    if st.session_state.routes:
        slim = [_slim(r) for r in st.session_state.routes]
        compressed = gzip.compress(json.dumps(slim, separators=(",", ":")).encode())
        st.query_params["r"] = base64.urlsafe_b64encode(compressed).decode()
    else:
        st.query_params.clear()


# ---------------------------------------------------------------------------
# Render data
# ---------------------------------------------------------------------------

def _haversine(lat1, lon1, lat2, lon2) -> float:
    R = 6371
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp = math.radians(lat2 - lat1)
    dl = math.radians(lon2 - lon1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def _norm_lon(origin_lon, dest_lon):
    diff = dest_lon - origin_lon
    if diff > 180:
        return dest_lon - 360
    if diff < -180:
        return dest_lon + 360
    return dest_lon


def _build_render_data(routes):
    # Count trips per normalised pair+mode
    pair_counts: dict[tuple, int] = {}
    pair_meta: dict[tuple, tuple] = {}
    total_km = 0.0

    for entry in routes:
        nodes = entry.get("nodes") or []
        mode = entry["mode"]
        for i in range(len(nodes) - 1):
            a, b = nodes[i], nodes[i + 1]
            if not a or not b:
                continue
            ak = a.get("iata") or a["label"]
            bk = b.get("iata") or b["label"]
            key = (min(ak, bk), max(ak, bk), mode)
            pair_counts[key] = pair_counts.get(key, 0) + 1
            pair_meta[key] = (a, b)
            total_km += _haversine(a["lat"], a["lon"], b["lat"], b["lon"])

    # Collect endpoint keys for node sizing
    endpoint_keys: set[str] = set()
    for entry in routes:
        nodes = entry.get("nodes") or []
        if nodes:
            n0, n1 = nodes[0], nodes[-1]
            if n0:
                endpoint_keys.add(n0.get("iata") or n0["label"])
            if n1:
                endpoint_keys.add(n1.get("iata") or n1["label"])

    arc_rows, line_rows, node_dict = [], [], {}

    for key, count in pair_counts.items():
        _, _, mode = key
        a, b = pair_meta[key]
        colour = MODE_COLOUR[mode]
        row = {
            "origin_lat": a["lat"],
            "origin_lon": a["lon"],
            "dest_lat": b["lat"],
            "dest_lon": _norm_lon(a["lon"], b["lon"]),
            "colour": colour,
            "width": _trip_width(count),
            "label": f"{a['label']} → {b['label']}",
            "trips": count,
        }
        (line_rows if mode in GROUND_MODES else arc_rows).append(row)

        for node in (a, b):
            nk = node.get("iata") or node["label"]
            if nk not in node_dict:
                is_endpoint = nk in endpoint_keys
                node_dict[nk] = {
                    "lat": node["lat"],
                    "lon": node["lon"],
                    "label": node["label"],
                    "radius": 8000 if is_endpoint else 4000,
                    "opacity": 210 if is_endpoint else 120,
                }

    return arc_rows, line_rows, list(node_dict.values()), total_km


def _stats(routes):
    counts = {m: 0 for m in MODES.values()}
    airports, cities, countries = set(), set(), set()
    iata_map = _iata_map()
    for entry in routes:
        legs = max(1, len(entry.get("nodes") or []) - 1)
        counts[entry["mode"]] += legs
        for node in entry.get("nodes") or []:
            if not node:
                continue
            if node.get("iata"):
                airports.add(node["iata"])
                iso = iata_map.get(node["iata"], {}).get("iso_country")
                if iso:
                    countries.add(iso)
            else:
                cities.add(node["label"])
    return counts, len(airports), len(cities), len(countries)


# ---------------------------------------------------------------------------
# App
# ---------------------------------------------------------------------------

_init_state()
_load_url_once()

with st.sidebar:
    st.subheader("Log a route")

    mode_label = st.radio("Mode", list(MODES), horizontal=True, label_visibility="collapsed")
    mode = MODES[mode_label]

    with st.form("route_form", clear_on_submit=True):
        route_text = st.text_input(
            "Route",
            placeholder="FRA-CDG  or  Paris to London",
            help="Separate stops with  -  or  'to'. Use 'City, Country' for ambiguous places.",
        )
        add_clicked = st.form_submit_button("Add route", type="primary", use_container_width=True)

    st.text_input(
        "Tag (optional)",
        placeholder="Summer 2025",
        help="Stays set as you add multiple stops — useful for grouping a whole trip.",
        key="persistent_tag",
    )
    st.text_input(
        "Date (optional)",
        placeholder="2025-07-14",
        help="Specific date for this route — clears after each addition.",
        key=f"route_date_{st.session_state._date_clear}",
    )
    tag_text = st.session_state.get("persistent_tag", "")
    date_text = st.session_state.get(f"route_date_{st.session_state._date_clear}", "")

    mode = MODES[mode_label]

    if add_clicked and route_text.strip():
        import re as _re
        raw = route_text.strip()
        # Replace "to" keyword with a safe internal separator
        normalised = _re.sub(r"\s+to\s+", " | ", raw, flags=_re.IGNORECASE)
        if " - " in normalised or " | " in normalised:
            # Spaced delimiters present — split on those only, leaving unspaced
            # dashes intact so place names like Bora-Bora are preserved.
            tokens = [t.strip() for t in _re.split(r" - | \| ", normalised) if t.strip()]
        else:
            # No spaces around dashes — treat as pure IATA shorthand: BCN-FRA-SIN
            tokens = [t.strip() for t in normalised.split("-") if t.strip()]
        if len(tokens) < 2:
            st.error("Enter at least two stops, e.g. BCN-FRA")
        else:
            resolutions = []
            all_auto = True
            for tok in tokens:
                resolved, candidates = _resolve(tok, mode)
                resolutions.append({"token": tok, "resolved": resolved, "candidates": candidates})
                if resolved is None:
                    all_auto = False

            if all_auto:
                st.session_state.routes.append({
                    "legs": tokens,
                    "mode": mode,
                    "tag": tag_text.strip(),
                    "date": date_text.strip(),
                    "nodes": [r["resolved"] for r in resolutions],
                })
                st.session_state.pending = None
                st.session_state._date_clear += 1
                _sync_url()
                st.rerun()
            else:
                st.session_state.pending = {
                    "resolutions": resolutions,
                    "mode": mode,
                    "tag": tag_text.strip(),
                    "date": date_text.strip(),
                    "tokens": tokens,
                }

    # Disambiguation panel
    if st.session_state.pending:
        p = st.session_state.pending
        st.markdown("**Confirm stops:**")
        updated, all_ok = [], True

        for res in p["resolutions"]:
            if res["resolved"]:
                st.caption(f"✓ {res['token']} → {res['resolved']['label']}")
                updated.append(res)
            elif res["candidates"]:
                labels = [c["label"] for c in res["candidates"]]
                chosen_label = st.selectbox(f"Which '{res['token']}'?", labels, key=f"dis_{res['token']}")
                chosen = next(c for c in res["candidates"] if c["label"] == chosen_label)
                updated.append({**res, "resolved": chosen})
            else:
                st.error(f"Could not resolve '{res['token']}'")
                all_ok = False
                updated.append(res)

        p["resolutions"] = updated

        if all_ok and st.button("✓ Confirm & add", type="primary", use_container_width=True):
            st.session_state.routes.append({
                "legs": p["tokens"],
                "mode": p["mode"],
                "tag": p.get("tag", ""),
                "date": p.get("date", ""),
                "nodes": [r["resolved"] for r in updated],
            })
            st.session_state.pending = None
            st.session_state._date_clear += 1
            _sync_url()
            st.rerun()

    st.markdown("---")

    if st.session_state.routes:
        if st.button("🗑 Delete all routes", use_container_width=True):
            st.session_state.routes = []
            st.session_state.pending = None
            _sync_url()
            st.rerun()

        st.subheader("Your routes")
        routes_display = list(enumerate(st.session_state.routes))[::-1]
        for i, entry in routes_display:
            icon = MODE_ICONS[entry["mode"]]
            label = " – ".join(entry["legs"])
            meta = "  ".join(filter(None, [entry.get("tag"), entry.get("date")]))
            col1, col2 = st.columns([5, 1])
            with col1:
                st.caption(f"{icon} {label}")
                if meta:
                    st.caption(f"   {meta}")
            with col2:
                if st.button("🗑", key=f"del_{i}", help="Remove this route"):
                    st.session_state.routes.pop(i)
                    _sync_url()
                    st.rerun()

        st.markdown("---")
        st.subheader("Share")

        url_val = f"https://pyfly-routes.streamlit.app/My_Routes?r={st.query_params.get('r', '')}"
        st.text_input("URL", value=url_val, label_visibility="collapsed")
        if len(url_val) > 800:
            st.caption("URL is getting long — use Download JSON for reliable sharing.")

        st.download_button(
            "⬇ Download JSON",
            data=json.dumps(st.session_state.routes, indent=2),
            file_name="my_routes.json",
            mime="application/json",
            use_container_width=True,
        )

        uploaded = st.file_uploader("⬆ Upload JSON", type="json", label_visibility="collapsed")
        if uploaded:
            if uploaded.size > 512_000:
                st.error("File too large — expected a small PyFly routes export.")
            else:
                try:
                    data = json.loads(uploaded.read().decode())
                    if not isinstance(data, list) or not all(isinstance(r, dict) and "legs" in r for r in data):
                        st.error("Invalid format — expected a PyFly routes JSON export.")
                    else:
                        st.session_state.routes = data
                        _sync_url()
                        st.rerun()
                except Exception:
                    st.error("Invalid JSON file.")
    else:
        st.caption("No routes logged yet.")


# ---------------------------------------------------------------------------
# Main panel
# ---------------------------------------------------------------------------

if not st.session_state.routes:
    st.title("🧳 My Routes")
    st.info("Log your first route in the sidebar — try **BCN-LHR-JFK** with mode ✈ Plane.")
    st.stop()

arc_rows, line_rows, node_rows, total_km = _build_render_data(st.session_state.routes)
counts, n_airports, n_cities, n_countries = _stats(st.session_state.routes)

# Stats bar
parts = [
    f"{MODE_ICONS[m]} {c} {MODE_TRIP_LABEL[m][1 if c > 1 else 0]}"
    for m, c in counts.items() if c
] + [
    f"🌍 {n_countries} countr{'ies' if n_countries != 1 else 'y'}",
]
if n_airports:
    parts.append(f"🛬 {n_airports} airport{'s' if n_airports != 1 else ''}")
if n_cities:
    parts.append(f"🏙 {n_cities} {'cities' if n_cities != 1 else 'city'}")
parts.append(f"📏 {total_km:,.0f} km")
st.markdown(f"### {('  ·  ').join(parts)}")

# Map view centred on logged routes
if node_rows:
    lats = [n["lat"] for n in node_rows]
    lons = [n["lon"] for n in node_rows]
    clat = (max(lats) + min(lats)) / 2
    clon = (max(lons) + min(lons)) / 2
    span = max(max(lats) - min(lats), max(lons) - min(lons))
    zoom = 7 if span < 5 else 5 if span < 15 else 4 if span < 40 else 3 if span < 80 else 2
else:
    clat, clon, zoom = 30.0, 0.0, 2

layers = []

if arc_rows:
    layers.append(pdk.Layer(
        "ArcLayer",
        data=arc_rows,
        get_source_position=["origin_lon", "origin_lat"],
        get_target_position=["dest_lon", "dest_lat"],
        get_source_color="colour",
        get_target_color="colour",
        get_width="width",
        pickable=True,
        auto_highlight=True,
    ))

if line_rows:
    layers.append(pdk.Layer(
        "LineLayer",
        data=line_rows,
        get_source_position=["origin_lon", "origin_lat"],
        get_target_position=["dest_lon", "dest_lat"],
        get_color="colour",
        get_width="width",
        pickable=True,
        auto_highlight=True,
    ))

if node_rows:
    layers.append(pdk.Layer(
        "ScatterplotLayer",
        data=node_rows,
        get_position=["lon", "lat"],
        get_fill_color=[255, 255, 255, "opacity"],
        get_radius="radius",
        pickable=True,
    ))

st.pydeck_chart(
    pdk.Deck(
        layers=layers,
        initial_view_state=pdk.ViewState(latitude=clat, longitude=clon, zoom=zoom, pitch=25),
        tooltip={
            "html": "<b>{label}</b><br/>Trips on this route: {trips}",
            "style": {"backgroundColor": "#1a1a2e", "color": "white", "fontSize": "13px"},
        },
        map_style="https://basemaps.cartocdn.com/gl/dark-matter-gl-style/style.json",
    ),
    height=680,
    width="stretch",
)
