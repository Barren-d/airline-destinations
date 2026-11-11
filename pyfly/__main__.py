"""CLI entry point: python -m pyfly --source aena --scope aena"""
import argparse
from .sources.base import Scope
from .ingest import run, SOURCES


def main():
    parser = argparse.ArgumentParser(description="PyFly data ingestion pipeline")
    parser.add_argument("--source", required=True, choices=list(SOURCES.keys()))
    parser.add_argument("--scope", default="aena", choices=[s.value for s in Scope])
    parser.add_argument("--backfill", type=int, default=0, metavar="N",
                        help="monthly sources (bts): also archive the N-1 months before the latest")
    args = parser.parse_args()
    scope = Scope(args.scope)
    run(args.source, scope, backfill=args.backfill)


if __name__ == "__main__":
    main()
