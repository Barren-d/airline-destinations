"""DuckDB read/write operations and cache management."""
from pathlib import Path
import json
import duckdb
import polars as pl

DB_PATH = Path(__file__).parent.parent / "data" / "pyfly.ddb"
CACHE_TTL_HOURS = 24


def _conn() -> duckdb.DuckDBPyConnection:
    return duckdb.connect(str(DB_PATH))


def init_db() -> None:
    try:
        _init_tables()
    except Exception as exc:
        print(f"  db: init failed ({exc}), recreating database")
        if DB_PATH.exists():
            DB_PATH.unlink()
        _init_tables()


def _init_tables() -> None:
    con = _conn()
    con.execute("""
        CREATE TABLE IF NOT EXISTS routes (
            origin_iata VARCHAR, origin_lat DOUBLE, origin_lon DOUBLE,
            dest_iata   VARCHAR, dest_lat   DOUBLE, dest_lon   DOUBLE,
            airline_iata VARCHAR, airline_name VARCHAR,
            source       VARCHAR, scraped_at TIMESTAMP DEFAULT NOW()
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


def write_routes(df: pl.DataFrame, source: str) -> None:
    con = _conn()
    con.execute("DELETE FROM routes WHERE source = ?", [source])
    con.execute("INSERT INTO routes SELECT *, NOW() FROM df")
    con.close()


def read_routes(source: str | None = None, scope=None) -> pl.DataFrame:
    con = _conn()
    query = "SELECT * FROM routes"
    if source:
        query += f" WHERE source = '{source}'"
    result = con.execute(query).pl()
    con.close()
    return result


def get_data_age(source: str) -> float | None:
    """Return age in hours of the most recent scrape for source, or None."""
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
    row = con.execute(
        "SELECT data, fetched_at FROM opensky_cache WHERE airport_icao = ?",
        [airport_icao],
    ).fetchone()
    con.close()
    if not row:
        return False, None

    data_json, fetched_at = row
    from datetime import datetime, timezone
    if hasattr(fetched_at, "tzinfo") and fetched_at.tzinfo is None:
        fetched_at = fetched_at.replace(tzinfo=timezone.utc)
    age_h = (datetime.now(timezone.utc) - fetched_at).total_seconds() / 3600
    if age_h > CACHE_TTL_HOURS:
        return False, None

    records = json.loads(data_json)
    if not records:
        return True, pl.DataFrame()
    return True, pl.DataFrame(records)


def write_cache(airport_icao: str, records: list[dict]) -> None:
    con = _conn()
    con.execute("""
        INSERT INTO opensky_cache (airport_icao, fetched_at, data)
        VALUES (?, NOW(), ?)
        ON CONFLICT (airport_icao) DO UPDATE
            SET fetched_at = NOW(), data = EXCLUDED.data
    """, [airport_icao, json.dumps(records)])
    con.close()
