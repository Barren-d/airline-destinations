"""DuckDB read/write operations and cache management."""
from pathlib import Path
import json
import duckdb
import polars as pl

DB_PATH = Path(__file__).parent.parent / "data" / "pyfly.ddb"
DATA_DIR = Path(__file__).parent.parent / "data"
CACHE_TTL_HOURS = 24
# DuckDB's NOW() is session-local; store naive UTC so get_data_age can compare against UTC.
_UTC_NOW = "(NOW() AT TIME ZONE 'UTC')"

PARQUET_SOURCES = {
    "aena": DATA_DIR / "routes_aena.parquet",
    "ana": DATA_DIR / "routes_ana.parquet",
    "openflights_2017": DATA_DIR / "routes_openflights.parquet",
    "openflights_global": DATA_DIR / "routes_openflights_global.parquet",
}


def _conn() -> duckdb.DuckDBPyConnection:
    return duckdb.connect(str(DB_PATH))


def init_db() -> None:
    try:
        _init_tables()
        _hydrate_from_parquet()
    except Exception as exc:
        print(f"  db: init failed ({exc}), recreating database")
        if DB_PATH.exists():
            DB_PATH.unlink()
        _init_tables()
        _hydrate_from_parquet()


def _init_tables() -> None:
    con = _conn()
    con.execute(f"""
        CREATE TABLE IF NOT EXISTS routes (
            origin_iata VARCHAR, origin_lat DOUBLE, origin_lon DOUBLE,
            dest_iata   VARCHAR, dest_lat   DOUBLE, dest_lon   DOUBLE,
            airline_iata VARCHAR, airline_name VARCHAR,
            source       VARCHAR, scraped_at TIMESTAMP DEFAULT {_UTC_NOW}
        )
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS opensky_cache (
            airport_icao VARCHAR PRIMARY KEY,
            fetched_at   TIMESTAMP,
            data         TEXT
        )
    """)
    con.close()


def _hydrate_from_parquet() -> None:
    """Load each committed parquet snapshot whose source is missing or older than its file.

    Checked per source, not only on an empty table: Streamlit Cloud can keep pyfly.ddb
    across redeploys, so a newly added source or a CI-refreshed snapshot must still load.
    """
    from datetime import datetime, timezone

    con = _conn()
    for source, path in PARQUET_SOURCES.items():
        if not path.exists():
            continue

        loaded_at = con.execute(
            "SELECT MAX(scraped_at) FROM routes WHERE source = ?", [source]
        ).fetchone()[0]
        file_time = datetime.fromtimestamp(path.stat().st_mtime, timezone.utc).replace(tzinfo=None)
        if loaded_at is not None and loaded_at >= file_time:
            continue

        con.execute("DELETE FROM routes WHERE source = ?", [source])
        # Force source to the key so the DELETE above always matches what is inserted
        con.execute(f"""
            INSERT INTO routes
            SELECT * REPLACE (?::VARCHAR AS source), {_UTC_NOW} AS scraped_at
            FROM read_parquet('{path.as_posix()}')
        """, [source])
        loaded = con.execute(
            "SELECT COUNT(*) FROM routes WHERE source = ?", [source]
        ).fetchone()[0]
        print(f"  db: hydrated {loaded} rows from {path.name}")

    con.close()


def write_routes(df: pl.DataFrame, source: str) -> None:
    con = _conn()
    con.execute("DELETE FROM routes WHERE source = ?", [source])
    # Register the polars df as a DuckDB view so we can INSERT from it
    con.register("_incoming", df.to_arrow())
    con.execute(f"INSERT INTO routes SELECT *, {_UTC_NOW} FROM _incoming")
    con.unregister("_incoming")
    con.close()

    # Export parquet snapshot after every successful write
    parquet_path = PARQUET_SOURCES.get(source)
    if parquet_path:
        df.write_parquet(parquet_path)
        print(f"  db: wrote {len(df)} rows to {parquet_path.name}")


def read_routes(source: str | None = None) -> pl.DataFrame:
    con = _conn()
    if source:
        result = con.execute(
            "SELECT * EXCLUDE scraped_at FROM routes WHERE source = ?", [source]
        ).pl()
    else:
        result = con.execute("SELECT * EXCLUDE scraped_at FROM routes").pl()
    con.close()
    return result


def get_data_age(source: str) -> float | None:
    """Return age in hours of the most recent write for source, or None."""
    con = _conn()
    row = con.execute(
        "SELECT MAX(scraped_at) FROM routes WHERE source = ?", [source]
    ).fetchone()
    con.close()
    if not row or row[0] is None:
        return None
    from datetime import datetime, timezone
    ts = row[0]
    if hasattr(ts, "tzinfo") and ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    now = datetime.now(timezone.utc)
    return (now - ts).total_seconds() / 3600


def check_cache(airport_icao: str) -> tuple[bool, pl.DataFrame | None]:
    """Return (hit, df). hit=True if cache exists and is < 24h old."""
    con = _conn()
    try:
        row = con.execute(
            "SELECT data, fetched_at FROM opensky_cache WHERE airport_icao = ?",
            [airport_icao],
        ).fetchone()
    except Exception:
        con.close()
        return False, None
    con.close()
    if not row:
        return False, None

    data_json, fetched_at = row
    from datetime import datetime, timezone
    if hasattr(fetched_at, "tzinfo") and fetched_at.tzinfo is None:
        fetched_at = fetched_at.replace(tzinfo=timezone.utc)
    age_h = (datetime.now(timezone.utc) - fetched_at).total_seconds() / 3600
    if age_h > CACHE_TTL_HOURS:
        return False, _records_to_df(json.loads(data_json))  # return stale for fallback

    return True, _records_to_df(json.loads(data_json))


def _records_to_df(records: list[dict]) -> pl.DataFrame | None:
    if not records:
        return pl.DataFrame()
    return pl.DataFrame(records)


def write_cache(airport_icao: str, records: list[dict]) -> None:
    con = _conn()
    con.execute(f"""
        INSERT INTO opensky_cache (airport_icao, fetched_at, data)
        VALUES (?, {_UTC_NOW}, ?)
        ON CONFLICT (airport_icao) DO UPDATE
            SET fetched_at = {_UTC_NOW}, data = EXCLUDED.data
    """, [airport_icao, json.dumps(records)])
    con.close()
