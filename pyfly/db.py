"""DuckDB read/write operations and cache management."""


def init_db():
    raise NotImplementedError


def write_routes(df, source: str):
    raise NotImplementedError


def read_routes(source=None, scope=None):
    raise NotImplementedError


def get_data_age(source: str):
    raise NotImplementedError


def check_cache(airport_icao: str):
    raise NotImplementedError


def write_cache(airport_icao: str, df):
    raise NotImplementedError
