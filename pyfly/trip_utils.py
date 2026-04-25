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


def _iter_stops(draft: dict):
    """Yield every stop across all route groups (new format) or flat stops (legacy)."""
    route_groups = draft.get("routes")
    if route_groups is not None:
        for group in route_groups:
            yield from (group.get("stops") or [])
    else:
        yield from (draft.get("stops") or [])


def generate_markdown(draft: dict, iata_map: dict) -> str:
    lines = []
    title = draft.get("title", "").strip()
    if title:
        lines.append(f"# {title}")
    desc = draft.get("description", "").strip()
    if desc:
        lines.append(f"\n{desc}")

    # Support both new grouped format (routes) and legacy flat format (stops)
    route_groups = draft.get("routes")
    if route_groups is None:
        route_groups = [{"stops": draft.get("stops") or []}]

    for group in route_groups:
        stops = group.get("stops") or []
        mode = group.get("mode", "plane")
        icon = MODE_ICONS.get(mode, "✈")

        # Route-level heading — custom name takes priority over IATA label
        route_label = group.get("name") or group.get("label", "")
        if route_label:
            lines.append(f"\n## {icon} {route_label}")

        # Seed pending_transit with the departure leg so the first stop gets its arrow
        _dep = group.get("departure")
        pending_transit: dict | None = {"mode": mode, **{k: _dep.get(k, "") for k in ("date", "ref", "notes")}} if _dep else None
        _dep_node = (_dep or {}).get("node", {})
        pending_coords: tuple | None = node_coords(_dep_node, iata_map) if _dep_node else None

        for stop in stops:
            coords = node_coords(stop["node"], iata_map)

            if stop.get("transited"):
                if coords:
                    pending_coords = coords
                continue

            # Transit arrow before this city (from the previous stop's transit_out)
            if pending_transit is not None:
                t_icon = MODE_ICONS.get(pending_transit.get("mode", "plane"), "✈")
                parts = []
                d = pending_transit.get("date", "").strip()
                r = pending_transit.get("ref", "").strip()
                n = pending_transit.get("notes", "").strip()
                if d:
                    parts.append(d)
                if r:
                    parts.append(r)
                arrow = f"\n{t_icon}"
                if parts:
                    arrow += " " + " · ".join(parts)
                arrow += f" → {stop_display(stop)}"
                if coords and pending_coords:
                    dist = haversine(pending_coords[0], pending_coords[1], coords[0], coords[1])
                    arrow += f" · {dist:,.0f} km"
                if n:
                    arrow += f" · _{n}_"
                lines.append(arrow)

            lines.append(f"\n### {stop_display(stop)}")

            for sec in (stop.get("sections") or []):
                sec_title = sec.get("title", "").strip()
                if sec_title:
                    lines.append(f"\n#### {sec_title}")
                notes_text = sec.get("notes", "").strip()
                if notes_text:
                    lines.append(f"\n{notes_text}")

            pending_transit = stop.get("transit_out")
            pending_coords = coords

    return "\n".join(lines)
