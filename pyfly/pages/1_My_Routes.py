"""My Routes — personal travel history map."""
import sys
import re as _re
import json
import gzip
import base64
import math
import time as _time
from datetime import date as _date
from pathlib import Path

_ROOT = Path(__file__).parent.parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

import httpx
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

MODES = {"✈ Plane": "plane", "🚂 Train": "train", "⛴ Boat": "boat", "🚗 Car / Bus": "car"}
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

_TYPE_PENALTY = {
    "large_airport": 0,
    "medium_airport": 0,
    "small_airport": -10,
    "heliport": -30,
    "seaplane_base": -25,
    "balloonport": -30,
    "closed": -50,
}


@st.cache_data
def _airport_df() -> pl.DataFrame:
    path = DATA_DIR / "airports.csv"
    if not path.exists():
        return pl.DataFrame()
    df = pl.read_csv(path, ignore_errors=True).filter(
        pl.col("iata_code").is_not_null() & (pl.col("iata_code") != "")
    )
    cols = ["iata_code", "name", "latitude_deg", "longitude_deg", "iso_country"]
    for extra in ("municipality", "type", "iso_region"):
        if extra in df.columns:
            cols.append(extra)
    return df.select(cols).rename({"latitude_deg": "lat", "longitude_deg": "lon"})


@st.cache_data
def _iata_map() -> dict:
    return {r["iata_code"]: r for r in _airport_df().iter_rows(named=True)}




@st.cache_data
def _fuzzy_data() -> tuple[list[str], list[str], list[dict]]:
    """Returns (name_corpus, muni_corpus, info_dicts) — parallel lists.

    Dual-corpus approach: score the query against airport names AND municipality
    separately, then take the best. This handles airports named after people
    (MNL=Ninoy Aquino, HAN=Noi Bai) where the city name only appears in
    municipality — and strips parenthetical qualifiers like "Hanoi (Soc Son)".
    """
    name_corpus, muni_corpus, info = [], [], []
    for r in _airport_df().iter_rows(named=True):
        muni_raw = (r.get("municipality") or "").strip()
        muni_clean = _re.sub(r"\s*\(.*?\)", "", muni_raw).strip()
        name = r["name"]
        iata = r["iata_code"]
        country = r["iso_country"]
        airport_type = r.get("type") or "small_airport"

        name_corpus.append(f"{name} {iata}")
        muni_corpus.append(muni_clean)

        label = f"{name} ({iata})"
        if muni_clean and muni_clean.lower() not in name.lower():
            label += f" — {muni_clean}, {country}"
        info.append({
            "label": label, "iata": iata, "lat": r["lat"], "lon": r["lon"],
            "type": airport_type,
        })
    return name_corpus, muni_corpus, info


# ---------------------------------------------------------------------------
# Token resolution
# ---------------------------------------------------------------------------

def _node_from_iata(iata: str) -> dict:
    r = _iata_map()[iata]
    return {"label": f"{r['name']} ({iata})", "iata": iata, "lat": r["lat"], "lon": r["lon"]}


def _fuzzy_candidates(query: str, limit: int = 6) -> list[dict]:
    name_corpus, muni_corpus, info = _fuzzy_data()

    name_scores = {idx: s for _, s, idx in rf_process.extract(query, name_corpus, scorer=fuzz.WRatio, limit=100)}
    muni_scores = {idx: s for _, s, idx in rf_process.extract(query, muni_corpus, scorer=fuzz.token_set_ratio, limit=200)}

    combined: dict[int, int] = {}
    for idx in set(name_scores) | set(muni_scores):
        raw = max(name_scores.get(idx, 0), muni_scores.get(idx, 0))
        penalty = _TYPE_PENALTY.get(info[idx]["type"], -15)
        combined[idx] = raw + penalty

    # Sort by (adjusted_score DESC, name_score DESC) so that when two airports
    # share the same city name, the one whose airport name contains the query
    # (e.g. "Barcelona-El Prat" for BCN) ranks above a municipality-only match
    # (e.g. BLA Venezuela whose name is "General Anzoategui...").
    top = sorted(
        combined.items(),
        key=lambda x: (-x[1], -name_scores.get(x[0], 0)),
    )[:limit]
    return [
        {**info[idx], "score": score, "name_score": name_scores.get(idx, 0)}
        for idx, score in top
        if score >= 40
    ]


def _nearest_airports(lat: float, lon: float, limit: int = 5, max_km: float = 150.0) -> list[dict]:
    """Return up to `limit` airports within `max_km` of the given coordinates."""
    R = 6371.0
    _, _, info = _fuzzy_data()
    results = []
    for a in info:
        dlat = math.radians(a["lat"] - lat)
        dlon = math.radians(a["lon"] - lon)
        p1, p2 = math.radians(lat), math.radians(a["lat"])
        hav = math.sin(dlat / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dlon / 2) ** 2
        km = R * 2 * math.atan2(math.sqrt(hav), math.sqrt(1 - hav))
        if km <= max_km:
            results.append((km, a))
    results.sort(key=lambda x: x[0])
    return [{**a, "score": 72} for _, a in results[:limit]]


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


def _road_geometry(lat1: float, lon1: float, lat2: float, lon2: float) -> list[list[float]] | None:
    """Return [[lon, lat], ...] road polyline from OSRM, or None on failure.

    Uses the public OSRM API (OpenStreetMap data, no key required).
    Results cached in session_state.road_cache to avoid repeat calls.
    """
    key = (round(lat1, 4), round(lon1, 4), round(lat2, 4), round(lon2, 4))
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
# Region coloring
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
    # Keep only the properties we match against — reduces memory and speeds PiP
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
    """Return OurAirports-format ISO codes for all visited nodes (e.g. 'ES', 'ES-CT')."""
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
    # Region: Natural Earth iso_3166_2 is sparsely populated; also try code_hasc
    # OurAirports uses "ES-CT", Natural Earth code_hasc uses "ES.CT"
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


def _stop_title(node: dict) -> str:
    iata = node.get("iata")
    if iata:
        r = _iata_map().get(iata, {})
        return r.get("municipality") or r.get("name", iata)
    return node.get("label", "")


def _routes_to_stops(routes: list[dict]) -> list[dict]:
    stops = []
    for entry in routes:
        mode = entry["mode"]
        for node in (entry.get("nodes") or []):
            if not node:
                continue
            slim = ({"iata": node["iata"]} if node.get("iata")
                    else {"label": node["label"], "lat": round(node["lat"], 4), "lon": round(node["lon"], 4)})
            stops.append({
                "node": slim,
                "title": _stop_title(node),
                "transited": False,
                "photos": [],
                "sections": [],
                "transit_out": {"mode": mode, "ref": "", "notes": ""},
            })
    if stops:
        stops[-1]["transit_out"] = None
    return stops


def _resolve(token: str, mode: str) -> tuple[dict | None, list[dict]]:
    """Return (auto_resolved_node, candidates_if_ambiguous)."""
    upper = token.strip().upper()

    # Exact IATA always wins regardless of mode
    if upper in _iata_map():
        return _node_from_iata(upper), []

    if mode == "plane":
        candidates = _fuzzy_candidates(token)
        top_score = candidates[0]["score"] if candidates else 0
        runner_up = candidates[1]["score"] if len(candidates) > 1 else 0
        top_name = candidates[0].get("name_score", 0)
        runner_name = candidates[1].get("name_score", 0) if len(candidates) > 1 else 0

        # Auto-resolve when there is a clear winner by adjusted score, OR when
        # the adjusted scores are tied but one airport's name contains the query
        # while the other's doesn't (e.g. "Barcelona" → BCN wins over BLA/VE).
        name_gap = top_name - runner_name
        if top_score >= 90 and ((top_score - runner_up) >= 8 or (top_score == runner_up and name_gap >= 30)):
            return candidates[0], []

        # Low-confidence fuzzy: geocode the token and merge nearest airports.
        # This handles historical city names (Saigon), transliterations (Hanoi),
        # and any city whose name doesn't appear in the airport name/municipality.
        if top_score < 65:
            geo = _geocode(token)
            if geo:
                near = _nearest_airports(geo["lat"], geo["lon"])
                seen_iata = {c["iata"] for c in candidates}
                for a in near:
                    if a["iata"] not in seen_iata:
                        candidates.append(a)
                        seen_iata.add(a["iata"])

        if not candidates:
            return None, []
        return None, candidates

    # Ground / sea — geocode
    result = _geocode(token)
    return result, []


# ---------------------------------------------------------------------------
# State management
# ---------------------------------------------------------------------------

def _init_state():
    defaults = {
        "routes": [], "geocode_cache": {}, "road_cache": {}, "pending": None,
        "_url_loaded": False, "_date_clear": 0, "_route_clear": 0, "_focus_nodes": None,
        "trip_selection_open": False, "trip_selected": set(), "trip_draft": None, "trips": [],
    }
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
    d: dict = {
        "legs": entry["legs"],
        "mode": entry["mode"],
        "nodes": [
            {"iata": n["iata"]} if n.get("iata")
            else {"label": n["label"], "lat": round(n["lat"], 4), "lon": round(n["lon"], 4)}
            for n in (entry.get("nodes") or []) if n
        ],
    }
    if entry.get("tag"):
        d["tag"] = entry["tag"]
    if entry.get("date"):
        d["date"] = entry["date"]
    return d


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
    return {**entry, "nodes": nodes, "tag": entry.get("tag", ""), "date": entry.get("date", "")}


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


_PARALLEL_OFFSET_DEG = 0.05  # ~5 km separation for overlapping ground-mode lines


def _perpendicular_offset(a: dict, b_lon: float, b_lat: float, idx: int, n: int) -> tuple:
    """Return (olat, olon, dlat, dlon) shifted perpendicular to the segment."""
    dlat = b_lat - a["lat"]
    dlon = b_lon - a["lon"]
    length = math.sqrt(dlat ** 2 + dlon ** 2) or 1
    perp_lat = -dlon / length
    perp_lon = dlat / length
    shift = (idx - (n - 1) / 2.0) * _PARALLEL_OFFSET_DEG
    return (
        a["lat"] + perp_lat * shift,
        a["lon"] + perp_lon * shift,
        b_lat + perp_lat * shift,
        b_lon + perp_lon * shift,
    )


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

    # For each undirected geo-pair, collect ground modes in a stable order
    geo_pair_ground_keys: dict[tuple, list[tuple]] = {}
    for key in pair_counts:
        ak, bk, mode = key
        if mode in GROUND_MODES:
            gk = (ak, bk)
            geo_pair_ground_keys.setdefault(gk, []).append(key)

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

    arc_rows, line_rows, road_rows, node_dict = [], [], [], {}

    for key, count in pair_counts.items():
        ak, bk, mode = key
        a, b = pair_meta[key]
        colour = MODE_COLOUR[mode]
        width = _trip_width(count)
        label = f"{a['label']} → {b['label']}"

        if mode == "car":
            # Try to get road-following geometry from OSRM
            geom = _road_geometry(a["lat"], a["lon"], b["lat"], b["lon"])
            if geom:
                road_rows.append({
                    "path": geom,
                    "colour": colour,
                    "width": width,
                    "label": label,
                    "trips": count,
                })
                for node in (a, b):
                    nk = node.get("iata") or node["label"]
                    if nk not in node_dict:
                        is_endpoint = nk in endpoint_keys
                        node_dict[nk] = {
                            "lat": node["lat"], "lon": node["lon"],
                            "label": node["label"],
                            "radius": 8000 if is_endpoint else 4000,
                            "opacity": 210 if is_endpoint else 120,
                        }
                continue  # skip straight-line fallback

        norm_dest_lon = _norm_lon(a["lon"], b["lon"])
        olat, olon, dlat, dlon = a["lat"], a["lon"], b["lat"], norm_dest_lon

        if mode in GROUND_MODES:
            gk = (ak, bk)
            siblings = geo_pair_ground_keys.get(gk, [key])
            if len(siblings) > 1:
                idx = siblings.index(key)
                olat, olon, dlat, dlon = _perpendicular_offset(a, norm_dest_lon, b["lat"], idx, len(siblings))

        row = {
            "origin_lat": olat, "origin_lon": olon,
            "dest_lat": dlat, "dest_lon": dlon,
            "colour": colour, "width": width,
            "label": label, "trips": count,
        }
        (line_rows if mode in GROUND_MODES else arc_rows).append(row)

        for node in (a, b):
            nk = node.get("iata") or node["label"]
            if nk not in node_dict:
                is_endpoint = nk in endpoint_keys
                node_dict[nk] = {
                    "lat": node["lat"], "lon": node["lon"],
                    "label": node["label"],
                    "radius": 8000 if is_endpoint else 4000,
                    "opacity": 210 if is_endpoint else 120,
                }

    return arc_rows, line_rows, road_rows, list(node_dict.values()), total_km


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

# Add route is inside st.form — target it specifically so it stays red
# while Convert to Trip (outside any form) inherits the blue primaryColor.
st.markdown("""<style>
button[data-testid="baseButton-primaryFormSubmit"] {
    background-color: #FF4B4B !important;
    border-color: #FF4B4B !important;
    color: white !important;
}
button[data-testid="baseButton-primaryFormSubmit"]:hover {
    background-color: #E03333 !important;
    border-color: #E03333 !important;
}
</style>""", unsafe_allow_html=True)

_need_rerun = False

with st.sidebar:
    # ── 1. Log a route ───────────────────────────────────────────────────────
    st.subheader("Log a route")

    mode_label = st.radio("Mode", list(MODES), horizontal=True, label_visibility="collapsed")
    mode = MODES[mode_label]

    with st.form("route_form", clear_on_submit=False):
        route_text = st.text_input(
            "Route",
            placeholder="BCN-FRA  or  London to Paris",
            help="Separate stops with  -  or  'to'. Use 'City, Country' for ambiguous places.",
            key=f"route_input_{st.session_state._route_clear}",
        )
        add_clicked = st.form_submit_button("Add route", type="primary", use_container_width=True)

    st.text_input(
        "Tag (optional)",
        placeholder="Summer 2026",
        help="Stays set as you add multiple stops — useful for grouping a whole trip.",
        key="persistent_tag",
    )
    st.text_input(
        "Date (optional)",
        placeholder="2026-06-21",
        help="Specific date for this route — clears after each addition.",
        key=f"route_date_{st.session_state._date_clear}",
    )
    tag_text = st.session_state.get("persistent_tag", "")
    date_text = st.session_state.get(f"route_date_{st.session_state._date_clear}", "")

    if add_clicked and route_text.strip():
        raw = route_text.strip()
        normalised = _re.sub(r"\s+to\s+", " | ", raw, flags=_re.IGNORECASE)
        normalised = _re.sub(r"\s*–\s*", " | ", normalised)  # en dash
        if " - " in normalised or " | " in normalised:
            tokens = [t.strip() for t in _re.split(r" - | \| ", normalised) if t.strip()]
        else:
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
                new_nodes = [r["resolved"] for r in resolutions]
                st.session_state.routes.append({
                    "legs": tokens,
                    "mode": mode,
                    "tag": tag_text.strip(),
                    "date": date_text.strip(),
                    "nodes": new_nodes,
                })
                st.session_state._focus_nodes = new_nodes
                st.session_state.pending = None
                st.session_state._date_clear += 1
                st.session_state._route_clear += 1
                _sync_url()
                _need_rerun = True
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
        all_ok = True
        selectbox_choices: dict[str, dict] = {}

        for res in p["resolutions"]:
            if res["resolved"]:
                st.caption(f"✓ {res['token']} → {res['resolved']['label']}")
            elif res["candidates"]:
                labels = [c["label"] for c in res["candidates"]]
                chosen_label = st.selectbox(f"Which '{res['token']}'?", labels, key=f"dis_{res['token']}")
                chosen = next(c for c in res["candidates"] if c["label"] == chosen_label)
                selectbox_choices[res["token"]] = chosen
            else:
                search_url = f"https://www.google.com/search?q={res['token'].replace(' ', '+')}+airport+IATA+code"
                st.error(f"Could not resolve **{res['token']}**. Try the IATA code — [look it up]({search_url}).")
                all_ok = False

        if all_ok and st.button("✓ Confirm & add", type="primary", use_container_width=True):
            confirmed_nodes = [
                res["resolved"] if res["resolved"] else selectbox_choices[res["token"]]
                for res in p["resolutions"]
            ]
            st.session_state.routes.append({
                "legs": p["tokens"],
                "mode": p["mode"],
                "tag": p.get("tag", ""),
                "date": p.get("date", ""),
                "nodes": confirmed_nodes,
            })
            st.session_state._focus_nodes = confirmed_nodes
            st.session_state.pending = None
            st.session_state._date_clear += 1
            st.session_state._route_clear += 1
            _sync_url()
            _need_rerun = True

    st.markdown("---")

    # ── 2. Region coloring ───────────────────────────────────────────────────
    st.subheader("Region coloring")
    region_enabled = st.toggle("Color visited regions", value=False, key="region_enabled")
    if region_enabled:
        st.selectbox("Scale", list(_GEO_URLS.keys()), key="region_level")
        col_a, col_b = st.columns([1, 2])
        with col_a:
            st.color_picker("Colour", value="#3B82F6", key="region_color")
        with col_b:
            st.slider("Opacity", 5, 80, 35, key="region_opacity")

    st.markdown("---")

    # ── 3. My routes ─────────────────────────────────────────────────────────
    if st.session_state.routes:
        if st.button("🗑 Delete all routes", use_container_width=True):
            st.session_state.routes = []
            st.session_state.pending = None
            _sync_url()
            _need_rerun = True

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
                    _need_rerun = True
    else:
        st.caption("No routes logged yet.")

    st.markdown("---")

    # ── 4. Share ──────────────────────────────────────────────────────────────
    if st.session_state.routes:
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
                        _need_rerun = True
                except Exception:
                    st.error("Invalid JSON file.")

# Defer rerun until after the full sidebar has rendered so no widget state is lost
if _need_rerun:
    st.rerun()



# ---------------------------------------------------------------------------
# Main panel
# ---------------------------------------------------------------------------

if not st.session_state.routes:
    st.title("🧳 My Routes")
    st.info("Log your first route in the sidebar — try **BCN-LHR-JFK** with mode ✈ Plane.")
    st.stop()

arc_rows, line_rows, road_rows, node_rows, total_km = _build_render_data(st.session_state.routes)
counts, n_airports, n_cities, n_countries = _stats(st.session_state.routes)

# Stats bar
n_routes = len(st.session_state.routes)
st.title(f"🧳 {n_routes} {'route' if n_routes == 1 else 'routes'}")

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
st.subheader(("  ·  ").join(parts))

# Map view: centre on the most recently added route, fall back to all nodes
focus = st.session_state.get("_focus_nodes") or node_rows
if focus:
    lats = [n["lat"] for n in focus if n]
    lons = [n["lon"] for n in focus if n]
    clat = (max(lats) + min(lats)) / 2
    clon = (max(lons) + min(lons)) / 2
    span = max(max(lats) - min(lats), max(lons) - min(lons))
    zoom = 7 if span < 5 else 5 if span < 15 else 4 if span < 40 else 3 if span < 80 else 2
else:
    clat, clon, zoom = 30.0, 0.0, 2

layers = []

region_enabled = st.session_state.get("region_enabled", False)
region_level = st.session_state.get("region_level", "Country")
if region_enabled and st.session_state.routes:
    try:
        geo_url = _GEO_URLS[region_level]
        geojson = _fetch_geojson(geo_url)
        visited = _collect_visited_iso(st.session_state.routes, geojson, region_level)
        filtered_geo = _filter_geojson(geojson, visited, region_level)
        if filtered_geo["features"]:
            r, g, b = _hex_to_rgb(st.session_state.get("region_color", "#3B82F6"))
            opacity_pct = st.session_state.get("region_opacity", 35)
            fill_a = int(opacity_pct * 255 / 100)
            line_a = min(255, fill_a * 3)
            layers.append(pdk.Layer(
                "GeoJsonLayer",
                data=filtered_geo,
                get_fill_color=[r, g, b, fill_a],
                get_line_color=[r, g, b, line_a],
                get_line_width=1,
                line_width_min_pixels=1,
                pickable=False,
            ))
    except Exception as _e:
        st.warning(f"Region coloring unavailable: {_e}")

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

if road_rows:
    layers.append(pdk.Layer(
        "PathLayer",
        data=road_rows,
        get_path="path",
        get_color="colour",
        get_width="width",
        width_units="pixels",
        width_min_pixels=2,
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
    key=f"map_{len(st.session_state.routes)}_{hash(tuple(n['label'] for r in st.session_state.routes for n in (r.get('nodes') or []) if n))}",
    height=680,
    width="stretch",
)

# ---------------------------------------------------------------------------
# Convert to Trip
# ---------------------------------------------------------------------------

_btn_col, _ = st.columns([1, 5])
with _btn_col:
    _btn_label = "✕ Cancel" if st.session_state.trip_selection_open else "🗺 Convert to Trip →"
    if st.button(_btn_label, type="primary", use_container_width=True):
        st.session_state.trip_selection_open = not st.session_state.trip_selection_open
        if st.session_state.trip_selection_open and not st.session_state.trip_selected:
            st.session_state.trip_selected = set(range(len(st.session_state.routes)))
        st.rerun()

if st.session_state.trip_selection_open:
    # Clean up stale indices if routes were removed
    st.session_state.trip_selected = {
        i for i in st.session_state.trip_selected if i < len(st.session_state.routes)
    }

    # Collect available tags for filter
    _all_tags = sorted({e.get("tag", "").strip() for e in st.session_state.routes if e.get("tag", "").strip()})

    _all_checked = len(st.session_state.trip_selected) >= len(st.session_state.routes)
    _new_all = st.checkbox("Select all", value=_all_checked, key="trip_sel_all_chk")
    if _new_all and not _all_checked:
        st.session_state.trip_selected = set(range(len(st.session_state.routes)))
        st.rerun()
    elif not _new_all and _all_checked:
        st.session_state.trip_selected = set()
        st.rerun()

    if _all_tags:
        _tag_filter = st.pills(
            "Filter by tag", _all_tags,
            selection_mode="single", default=None,
            key="trip_tag_filter", label_visibility="collapsed",
        )
        if _tag_filter:
            _tag_indices = {i for i, e in enumerate(st.session_state.routes) if e.get("tag", "").strip() == _tag_filter}
            if st.session_state.trip_selected != _tag_indices:
                st.session_state.trip_selected = _tag_indices
                st.rerun()

    _sel_rows = []
    for _i, _entry in enumerate(st.session_state.routes):
        _nodes = _entry.get("nodes") or []
        _dist = int(sum(
            _haversine(_nodes[_j]["lat"], _nodes[_j]["lon"], _nodes[_j + 1]["lat"], _nodes[_j + 1]["lon"])
            for _j in range(len(_nodes) - 1) if _nodes[_j] and _nodes[_j + 1]
        ))
        _sel_rows.append({
            "Select": _i in st.session_state.trip_selected,
            "Mode":   MODE_ICONS[_entry["mode"]],
            "Route":  " → ".join(_entry["legs"]),
            "Tag":    _entry.get("tag", ""),
            "Date":   _entry.get("date", ""),
            "km":     _dist,
        })

    _edited = st.data_editor(
        _sel_rows,
        column_config={
            "Select": st.column_config.CheckboxColumn("", width="small"),
            "Mode":   st.column_config.TextColumn("Mode", width="small"),
            "Route":  st.column_config.TextColumn("Route"),
            "Tag":    st.column_config.TextColumn("Tag", width="medium"),
            "Date":   st.column_config.TextColumn("Date", width="medium"),
            "km":     st.column_config.NumberColumn("km", width="small", format="%d"),
        },
        disabled=["Mode", "Route", "Tag", "Date", "km"],
        hide_index=True,
        key=f"trip_sel_{len(st.session_state.routes)}",
        use_container_width=True,
    )
    _new_sel = {_i for _i, _r in enumerate(_edited) if _r["Select"]}
    if _new_sel != st.session_state.trip_selected:
        st.session_state.trip_selected = _new_sel
        st.rerun()

    _n_sel = len(st.session_state.trip_selected)
    if _n_sel > 0 and st.button(
        f"Create trip with {_n_sel} route{'s' if _n_sel != 1 else ''} →",
        type="primary",
        key="create_trip_btn",
    ):
        _selected_routes = [st.session_state.routes[_i] for _i in sorted(st.session_state.trip_selected)]
        st.session_state.trip_draft = {
            "version": 1,
            "id": str(int(_time.time())),
            "title": "",
            "description": "",
            "created_at": _date.today().isoformat(),
            "stops": _routes_to_stops(_selected_routes),
        }
        st.session_state.trip_selection_open = False
        st.switch_page("pages/4_Trip_Creator.py")
