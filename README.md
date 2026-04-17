# ✈ PyFly

Interactive great circle route visualisation for the **AENA Spanish airport network**.

PyFly maps every scheduled flight route across Spain's 43 commercial airports as
geodesic arcs — origin to destination, coloured by direction, hoverable for airline
detail. Toggle between live scraped data, a 2017 historical baseline, and real-time
OpenSky flight records.

---

## What it looks like

Each arc represents a direct route. Orange end = origin, blue end = destination.
Hover any arc for the destination code, airline name, and data source.
Filter by origin airport, airline, or destination country. Adjust arc density with
the slider (default 500 — raise it or pick a single airport to see everything).

---

## Data sources

| Source | What | Freshness | Auth |
|---|---|---|---|
| **AENA Live** | Current scheduled routes scraped from aena.es | Daily (GitHub Actions) | None |
| **Historical (2017)** | OpenFlights open dataset, pre-COVID baseline | Static | None |
| **OpenSky** | Actual flights flown in the last 7 days | On demand, 24h cache | Free account |

---

## Quick start

**Requirements:** Python 3.12+, [uv](https://docs.astral.sh/uv/)

```bash
# Clone and install
git clone <repo-url>
cd PyFly
uv sync --extra scraper      # includes Playwright for the AENA scraper
uv run playwright install chromium

# Populate the database (uses committed parquet snapshots — no scrape needed)
uv run python -m pyfly --source openflights --scope aena

# Or run the live AENA scraper (~4 min, scrapes all 43 airports)
uv run python -m pyfly --source aena --scope aena

# Launch the app
uv run streamlit run pyfly/app.py
```

Open [http://localhost:8501](http://localhost:8501).

---

## Project structure

```
PyFly/
├── pyfly/
│   ├── app.py                  ← Streamlit landing page
│   ├── pages/
│   │   └── 1_Route_Map.py      ← Interactive map
│   ├── sources/
│   │   ├── base.py             ← FlightSource ABC + Scope enum + schema
│   │   ├── aena.py             ← Live Playwright scraper (CLI only)
│   │   ├── openflights.py      ← Historical 2017 source
│   │   └── opensky.py          ← OpenSky REST API + DuckDB cache
│   ├── enrich.py               ← Coordinate + airline name enrichment
│   ├── ingest.py               ← Fetch → enrich → write pipeline
│   ├── db.py                   ← DuckDB read/write + OpenSky cache
│   └── exceptions.py           ← ScraperError, AuthError
├── config/
│   └── airport_urls.json       ← All 43 AENA airports + their page URLs
├── data/
│   ├── airports.csv            ← OurAirports (coordinates + IATA/ICAO mapping)
│   ├── routes.dat              ← OpenFlights routes (~2017)
│   ├── airlines.dat            ← OpenFlights airline names
│   ├── routes_aena.parquet     ← Committed live snapshot (auto-updated by CI)
│   └── routes_openflights.parquet
├── tests/
│   ├── test_aena.py
│   ├── test_openflights.py
│   └── test_opensky.py
├── archive/                    ← Original 2022 scripts (reference only)
└── .github/workflows/
    └── scrape.yml              ← Daily AENA scrape + parquet commit
```

---

## CLI reference

```bash
# AENA live scrape (requires --extra scraper + Playwright)
uv run python -m pyfly --source aena --scope aena

# Historical data (no scraping, reads local files)
uv run python -m pyfly --source openflights --scope aena
uv run python -m pyfly --source openflights --scope european
uv run python -m pyfly --source openflights --scope global_top_100

# OpenSky (requires .env credentials)
uv run python -m pyfly --source opensky --scope aena

# Run tests
uv run pytest tests/ -v
```

---

## OpenSky setup (optional)

Register a free account at [opensky-network.org](https://opensky-network.org), then:

```bash
cp .env.example .env
# Edit .env and add your credentials
OPENSKY_USERNAME=your_username
OPENSKY_PASSWORD=your_password
```

Free tier: 400 API credits/day. Each airport query costs 1 credit.
Results are cached in DuckDB for 24 hours per airport.

---

## Deploying to Streamlit Cloud

1. Push the repo to GitHub — `data/pyfly.ddb` and `.env` are gitignored
2. Connect at [share.streamlit.io](https://share.streamlit.io), set main file: `pyfly/app.py`
3. Add OpenSky credentials as Streamlit Secrets (optional)

The app reads from the committed parquet snapshots on cold start — no scraping
needed in the cloud environment, and no Playwright dependency required.

The GitHub Actions workflow (`.github/workflows/scrape.yml`) runs the AENA scraper
daily at 04:00 UTC and commits the updated parquet back to the repo. Streamlit Cloud
picks up the new commit automatically.

---

## Adding a new source

1. Create `pyfly/sources/newsource.py` implementing `FlightSource`
2. Add one line to `SOURCES` in `ingest.py`
3. Add the source to the toggle in `pages/1_Route_Map.py`
4. Add `tests/test_newsource.py` following the existing pattern

Nothing else changes.

---

## Tech stack

| | |
|---|---|
| Scraping | Playwright (headless Chromium) + BeautifulSoup4 |
| Data processing | Polars |
| Storage | DuckDB (local) + Parquet (committed snapshots) |
| Visualisation | pydeck ArcLayer + CartoDB basemap |
| App | Streamlit |
| Package management | uv |
| CI/CD | GitHub Actions |

---

*Data sources: [aena.es](https://www.aena.es) · [OpenFlights](https://openflights.org/data.html) · [OpenSky Network](https://opensky-network.org) · [OurAirports](https://ourairports.com/data/)*
