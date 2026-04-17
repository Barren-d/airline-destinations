"""Join raw routes with airport coordinates and airline full names."""
import polars as pl


def enrich(routes_df: pl.DataFrame, airports_df: pl.DataFrame, airlines_df: pl.DataFrame) -> pl.DataFrame:
    raise NotImplementedError
