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
    initial_sidebar_state="collapsed",
)

st.title("✈ PyFly")
st.subheader("Interactive great circle route visualisation for the AENA Spanish airport network")

st.markdown("""
PyFly maps every scheduled flight route in Spain's AENA network as a geodesic arc —
origin to destination, coloured by direction, hoverable for airline detail.
Built as a revival of a 2022 scraping project, modernised with a pluggable
multi-source architecture and a live Streamlit interface.
""")

st.markdown("---")

col1, col2, col3 = st.columns(3)

with col1:
    st.markdown("### 🛫 AENA Live")
    st.markdown(
        "Current scheduled routes scraped daily from **aena.es** using a headless "
        "Chromium browser. Covers all 43 commercial airports in the AENA network. "
        "Refreshed automatically via GitHub Actions."
    )

with col2:
    st.markdown("### 📅 Historical (2017)")
    st.markdown(
        "Pre-COVID baseline from the **OpenFlights** open dataset (~2017 vintage). "
        "Useful for spotting routes that existed before the pandemic and never "
        "came back, or new routes that didn't exist then."
    )

with col3:
    st.markdown("### 🔴 OpenSky")
    st.markdown(
        "Actual flights flown in the last 7 days from the **OpenSky Network** REST API. "
        "Requires free API credentials. Results are cached per-airport for 24 hours "
        "to stay within the free-tier rate limit."
    )

st.markdown("---")

st.markdown("### How it works")

st.markdown("""
**Scraper** (CLI / GitHub Actions cron)
`python -m pyfly --source aena --scope aena`
Playwright scrapes the AENA destinations page for each of the 43 airports,
parses routes and airlines, enriches with coordinates from OurAirports,
and writes a parquet snapshot committed back to the repo.

**App** (Streamlit — this page)
Reads exclusively from DuckDB, which is hydrated from the committed parquet
files on cold start. The app never triggers a scrape. Data age is shown
next to the source selector so you always know how fresh the data is.

**Adding a new source**
One new file implementing `FlightSource`, one line in `ingest.py`. Nothing else changes.
""")

st.markdown("---")

st.markdown("### Network at a glance")

try:
    from pyfly.db import init_db, read_routes
    init_db()
    df = read_routes(source="aena")
    if not df.is_empty():
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Airports", df["origin_iata"].n_unique())
        c2.metric("Routes", len(df))
        c3.metric("Destinations", df["dest_iata"].n_unique())
        c4.metric("Airlines", df["airline_iata"].n_unique())
    else:
        st.info("No AENA data loaded yet. Run the ingestion pipeline to populate.")
except Exception:
    st.info("Run `uv run python -m pyfly --source aena --scope aena` to load data.")

st.markdown("---")

st.page_link("pages/1_Route_Map.py", label="Open the Route Map →", icon="🗺️")

st.markdown(
    "<br><sub>Data sources: aena.es · OpenFlights · OpenSky Network · OurAirports</sub>",
    unsafe_allow_html=True,
)
