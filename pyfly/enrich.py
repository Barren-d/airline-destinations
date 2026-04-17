"""Join raw routes with airport coordinates and airline full names."""
import re
from pathlib import Path
import polars as pl

DATA_DIR = Path(__file__).parent.parent / "data"


def load_airports() -> pl.DataFrame:
    return (
        pl.read_csv(DATA_DIR / "airports.csv")
        .select(["iata_code", "icao_code", "name", "latitude_deg", "longitude_deg", "iso_country"])
        .filter(pl.col("iata_code").is_not_null() & (pl.col("iata_code") != ""))
    )


def load_airlines() -> pl.DataFrame:
    df = pl.read_csv(
        DATA_DIR / "airlines.dat",
        has_header=False,
        null_values=[r"\N"],
        new_columns=["airline_id", "name", "alias", "iata", "icao", "callsign", "country", "active"],
    )
    return (
        df.filter(pl.col("active") == "Y")
        .filter(pl.col("iata").is_not_null())
        .unique(subset=["iata"], keep="first")
        .select(["iata", "name"])
    )


def extract_iata(text: str) -> str | None:
    """Extract the last 3-letter IATA code from a string like 'LONDON HEATHROW (LHR)'."""
    if not text:
        return None
    matches = re.findall(r"\(([A-Z]{3})\)", str(text))
    return matches[-1] if matches else None


def enrich(
    routes_df: pl.DataFrame,
    airports_df: pl.DataFrame,
    airlines_df: pl.DataFrame,
    source: str,
) -> pl.DataFrame:
    """
    Enrich a raw routes DataFrame with coordinates and airline names.

    Input columns expected (AENA path):
        origin_iata, dest_raw, dest_country, airline_name

    Input columns expected (OpenFlights path):
        origin_iata, dest_iata, airline_iata

    Returns the standard schema DataFrame.
    """
    df = routes_df

    # Extract dest_iata from dest_raw if not already present
    if "dest_raw" in df.columns and "dest_iata" not in df.columns:
        df = df.with_columns(
            pl.col("dest_raw")
            .map_elements(extract_iata, return_dtype=pl.Utf8)
            .alias("dest_iata")
        )

    coords = airports_df.select([
        pl.col("iata_code"),
        pl.col("latitude_deg").alias("lat"),
        pl.col("longitude_deg").alias("lon"),
    ])

    # Join origin coordinates
    df = df.join(
        coords.rename({"iata_code": "origin_iata", "lat": "origin_lat", "lon": "origin_lon"}),
        on="origin_iata",
        how="left",
    )

    # Join destination coordinates
    df = df.join(
        coords.rename({"iata_code": "dest_iata", "lat": "dest_lat", "lon": "dest_lon"}),
        on="dest_iata",
        how="left",
    )

    # Resolve airline_iata → airline_name (OpenFlights path)
    if "airline_iata" in df.columns and "airline_name" not in df.columns:
        df = df.join(
            airlines_df.rename({"iata": "airline_iata", "name": "airline_name"}),
            on="airline_iata",
            how="left",
        )

    # Ensure all standard columns exist
    for col in ["airline_iata", "airline_name"]:
        if col not in df.columns:
            df = df.with_columns(pl.lit(None).cast(pl.Utf8).alias(col))

    df = df.with_columns(pl.lit(source).alias("source"))

    # Drop rows missing coordinates — log the count and warn if > 5%
    before = len(df)
    df = df.filter(
        pl.col("origin_lat").is_not_null()
        & pl.col("dest_lat").is_not_null()
    )
    dropped = before - len(df)
    if dropped:
        pct = dropped / before * 100 if before else 0
        msg = f"  enrich: dropped {dropped}/{before} rows with missing coordinates ({pct:.1f}%)"
        if pct > 5:
            print(f"WARNING: {msg} — airports.csv may need refreshing")
        else:
            print(msg)

    return df.select([
        "origin_iata", "origin_lat", "origin_lon",
        "dest_iata", "dest_lat", "dest_lon",
        "airline_iata", "airline_name", "source",
    ])
