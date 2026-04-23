from __future__ import annotations

import logging
from pathlib import Path

import structlog

from etl.extract import extract_matches
from etl.load import connect, create_indexes, create_schema, load_dimensions, load_fact_ball
from etl.quality_checks import run_all_checks
from etl.transform import build_dim_match, build_dim_player, build_dim_season, build_dim_team, build_dim_venue
from etl.transform_facts import build_fact_ball


logger = structlog.get_logger(__name__)


def configure_logging() -> None:
    """Configure stdlib logging and structlog for CLI execution."""
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    structlog.configure(
        processors=[
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.stdlib.add_log_level,
            structlog.dev.ConsoleRenderer(),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(logging.INFO),
        logger_factory=structlog.PrintLoggerFactory(),
        cache_logger_on_first_use=True,
    )


def main() -> None:
    """Run the ETL pipeline from extraction through verification.

    Running the command multiple times produces the same database state because
    the load step recreates the schema on every run.
    """
    configure_logging()
    logger.info("Starting ETL run")

    matches = extract_matches(Path("data/raw"))

    players = build_dim_player(matches)
    teams = build_dim_team(matches)
    venues = build_dim_venue(matches)
    venue_id_map = {venue["venue_name"]: venue["venue_id"] for venue in venues}
    match_records = build_dim_match(matches, venue_id_map)
    seasons = build_dim_season(match_records)
    fact_rows = build_fact_ball(matches)

    conn = connect(Path("data/db/genbi.duckdb"))
    try:
        create_schema(conn)
        load_dimensions(conn, players, teams, venues, match_records, seasons)
        load_fact_ball(conn, fact_rows)
        create_indexes(conn)

        expected_counts = {
            "dim_player": len(players),
            "dim_team": len(teams),
            "dim_venue": len(venues),
            "dim_match": len(match_records),
            "dim_season": len(seasons),
            "fact_ball": len(fact_rows),
        }
        run_all_checks(conn, expected_counts)
    finally:
        conn.close()

    logger.info(
        "ETL complete",
        dim_player=len(players),
        dim_team=len(teams),
        dim_venue=len(venues),
        dim_match=len(match_records),
        dim_season=len(seasons),
        fact_ball=len(fact_rows),
    )


if __name__ == "__main__":
    main()
