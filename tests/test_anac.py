"""Tests for ANACSource — parsing/filtering run offline; data checks use the committed parquet."""
from pathlib import Path

import polars as pl
import pytest

from pyfly.history import list_snapshots
from pyfly.sources.anac import MIN_FLIGHTS, ANACSource
from pyfly.sources.base import SCHEMA

HEADER = ('"ICAO Empresa Aérea";"Número Voo";"Código Autorização (DI)";"Código Tipo Linha";'
          '"ICAO Aeródromo Origem";"ICAO Aeródromo Destino";"Partida Prevista";"Partida Real";'
          '"Chegada Prevista";"Chegada Real";"Situação Voo";"Código Justificativa"')


def _csv(rows):
    lines = ["﻿Atualizado em: 2026-10-28", HEADER]
    for airline, line_type, orig, dest, status, n in rows:
        lines += [f'"{airline}";"1";"0";"{line_type}";"{orig}";"{dest}";"x";"x";"x";"x";"{status}";"N/A"'] * n
    return "\n".join(lines).encode("utf-8")


def test_anac_keeps_flown_passenger_routes_only():
    flights = ANACSource._parse_csv(_csv([
        ("GLO", "N", "SBFL", "SBGR", "REALIZADO", MIN_FLIGHTS),      # domestic passenger
        ("TAP", "I", "LPPT", "SBGR", "REALIZADO", MIN_FLIGHTS + 3),  # international passenger
        ("ETH", "G", "DNMM", "SBGR", "REALIZADO", 20),               # international cargo
        ("GLO", "C", "SBGR", "SBEG", "REALIZADO", 20),               # domestic cargo
        ("AZU", "N", "SBKP", "SBRF", "CANCELADO", 20),               # cancelled
        ("AZU", "N", "SBKP", "SBPA", "REALIZADO", MIN_FLIGHTS - 1),  # too rare
    ]))
    routes = ANACSource._routes(flights)
    assert set(routes.select(["airline_icao", "origin_icao", "dest_icao"]).rows()) == {
        ("GLO", "SBFL", "SBGR"), ("TAP", "LPPT", "SBGR"),
    }


def test_anac_converts_icao_to_iata_and_names():
    flights = ANACSource._parse_csv(_csv([("TAM", "N", "SBGR", "SBRJ", "REALIZADO", 10)]))
    mapped = ANACSource._to_iata(ANACSource._routes(flights))
    assert mapped.rows() == [("GRU", "SDU", "JJ", "LATAM Brasil")]


def test_anac_snapshot_is_one_month():
    path = Path("data/routes_anac.parquet")
    if not path.exists():
        pytest.skip("not downloaded yet")
    df = pl.read_parquet(path)
    assert len(df) > 500
    for col, dtype in SCHEMA.items():
        assert df[col].dtype == dtype, f"{col}: expected {dtype}, got {df[col].dtype}"
    assert df["source"].unique().to_list() == ["anac"]
    assert df["scraped_at"].n_unique() == 1 and df["scraped_at"].max().day == 1
    assert df["dest_lat"].null_count() == 0
    assert (df["origin_iata"] != df["dest_iata"]).all()
    assert {"GRU", "GIG", "BSB"} <= set(df["origin_iata"])


def test_anac_history_files_are_monthly():
    files = list_snapshots("anac")
    if not files:
        pytest.skip("no history yet")
    assert all(f.stem.endswith("-01") for f in files)
