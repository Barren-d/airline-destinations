"""Shared utilities for the Trip Creator and My Trips pages."""
import math

MODE_ICONS = {"plane": "✈", "train": "🚂", "boat": "⛴", "car": "🚗"}


def stop_display(stop: dict) -> str:
    return stop.get("title") or (stop.get("node") or {}).get("iata") or "Stop"


def node_coords(node: dict, iata_map: dict) -> tuple[float, float] | None:
    if node.get("iata"):
        r = iata_map.get(node["iata"])
        if r:
            return r["lat"], r["lon"]
    if node.get("lat") is not None:
        return node["lat"], node["lon"]
    return None


def haversine(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    R = 6371
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp, dl = math.radians(lat2 - lat1), math.radians(lon2 - lon1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def generate_markdown(draft: dict, iata_map: dict) -> str:
    lines = []
    title = draft.get("title", "").strip()
    if title:
        lines.append(f"# {title}")
    desc = draft.get("description", "").strip()
    if desc:
        lines.append(f"\n{desc}")

    stops = draft.get("stops") or []
    transit_nodes: list[str] = []
    transit_km = 0.0
    prev_coords: tuple | None = None
    prev_mode: str | None = None
    prev_ref: str = ""

    def _flush(dest_title: str):
        nonlocal transit_nodes, transit_km, prev_mode, prev_ref
        if prev_mode:
            icon = MODE_ICONS.get(prev_mode, "✈")
            via = f" · via {', '.join(transit_nodes)}" if transit_nodes else ""
            ref = f" {prev_ref}" if prev_ref else ""
            km = f" · {transit_km:,.0f} km" if transit_km else ""
            lines.append(f"\n{icon}{ref}{via} → {dest_title}{km}")
        transit_nodes.clear()
        transit_km = 0.0
        prev_mode = None
        prev_ref = ""

    for stop in stops:
        coords = node_coords(stop["node"], iata_map)
        if coords and prev_coords:
            transit_km += haversine(prev_coords[0], prev_coords[1], coords[0], coords[1])
        prev_coords = coords

        t_out = stop.get("transit_out") or {}
        if stop.get("transited"):
            transit_nodes.append(stop_display(stop))
            if not prev_mode:
                prev_mode = t_out.get("mode")
                prev_ref = t_out.get("ref", "")
            continue

        _flush(stop_display(stop))
        lines.append(f"\n## {stop_display(stop)}")

        for sec in (stop.get("sections") or []):
            sec_title = sec.get("title", "").strip()
            if sec_title:
                lines.append(f"\n### {sec_title}")
            notes = sec.get("notes", "").strip()
            if notes:
                lines.append(f"\n{notes}")

        prev_mode = t_out.get("mode")
        prev_ref = t_out.get("ref", "")
        transit_km = 0.0

    return "\n".join(lines)
