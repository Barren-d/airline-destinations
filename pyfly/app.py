"""PyFly — landing page."""
import sys
from pathlib import Path

_ROOT = Path(__file__).parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

import streamlit as st

st.set_page_config(
    page_title="PyFly",
    page_icon="✈",
    layout="centered",
    initial_sidebar_state="expanded",
)

st.title("✈ PyFly")
st.subheader("Map your travels. Explore the world's flight network.")

st.markdown("""
PyFly is a personal flight and travel tracker combined with a global route explorer.
Log the routes you've flown, trained, sailed, or driven — then dive into scheduled
and historical flight data from Spain's AENA network, the 2017 global baseline,
or real-time OpenSky records.
""")

st.markdown("---")

col1, col2 = st.columns(2)

with col1:
    st.markdown("### 🧳 My Routes")
    st.markdown(
        "Log every route you've ever taken — flights, trains, boats, and car trips. "
        "Each leg is mapped as a great circle arc or ground line, thickened by how "
        "many times you've done it. Filter by mode, share via URL, or export as JSON."
    )

with col2:
    st.markdown("### 🗺 Route Explorer")
    st.markdown(
        "Browse global scheduled and historical flight routes. Explore the live AENA "
        "Spanish network, the 2017 OpenFlights global baseline (3,400+ airports), or "
        "actual flights from the OpenSky Network. Filter by country, airport, and airline."
    )

st.markdown("---")

st.markdown("### Data sources")

c1, c2, c3 = st.columns(3)

with c1:
    st.markdown("**🛫 AENA Live**")
    st.markdown(
        "Scheduled routes scraped monthly from **aena.es**. "
        "Covers all 43 commercial airports in the AENA network. "
        "Auto-updated via GitHub Actions."
    )

with c2:
    st.markdown("**📅 Historical (2017)**")
    st.markdown(
        "Pre-COVID baseline from the **OpenFlights** open dataset. "
        "Available for Spain, Portugal, or the full global network "
        "covering 3,400+ airports."
    )

with c3:
    st.markdown("**🔴 OpenSky**")
    st.markdown(
        "Actual flights flown in the last 7 days via the **OpenSky Network** API. "
        "Requires free credentials. Cached per-airport for 24 hours."
    )

st.markdown("---")

try:
    from pyfly.db import init_db, read_routes
    init_db()
    df = read_routes(source="aena")
    if not df.is_empty():
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("AENA Airports", df["origin_iata"].n_unique())
        c2.metric("Routes", len(df))
        c3.metric("Destinations", df["dest_iata"].n_unique())
        c4.metric("Airlines", df["airline_iata"].n_unique())
except Exception:
    pass

st.markdown("---")

st.info("👈 Open **My Routes** from the sidebar to start logging your travels, or **Route Explorer** to browse the global network.")

st.markdown(
    "<br><sub>Data sources: aena.es · OpenFlights · OpenSky Network · OurAirports · © OpenStreetMap contributors · © CARTO</sub>"
    "<br><sub>© 2025 Barren-d — personal and educational use only · "
    "[Source](https://github.com/Barren-d/airline-destinations)</sub>",
    unsafe_allow_html=True,
)
