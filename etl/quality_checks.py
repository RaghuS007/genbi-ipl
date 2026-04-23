from __future__ import annotations

from time import perf_counter

import duckdb
import structlog


logger = structlog.get_logger(__name__)


def verify_row_counts(
    conn: duckdb.DuckDBPyConnection,
    expected: dict[str, int],
) -> None:
    """Assert SELECT COUNT(*) matches expected for each table.

    Args:
        conn: Open DuckDB connection.
        expected: Expected row counts by table name.
    """
    for table_name, expected_count in expected.items():
        actual_count = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
        assert actual_count == expected_count, (
            f"Row count mismatch for {table_name}: expected {expected_count}, got {actual_count}"
        )

    logger.info("Verified row counts", table_count=len(expected))


def verify_referential_integrity(conn: duckdb.DuckDBPyConnection) -> None:
    """Assert no orphan foreign keys in fact_ball.

    Args:
        conn: Open DuckDB connection.
    """
    checks = {
        "batter_id": """
            SELECT COUNT(*)
            FROM fact_ball fb
            LEFT JOIN dim_player dp ON fb.batter_id = dp.player_id
            WHERE dp.player_id IS NULL
        """,
        "bowler_id": """
            SELECT COUNT(*)
            FROM fact_ball fb
            LEFT JOIN dim_player dp ON fb.bowler_id = dp.player_id
            WHERE dp.player_id IS NULL
        """,
        "non_striker_id": """
            SELECT COUNT(*)
            FROM fact_ball fb
            LEFT JOIN dim_player dp ON fb.non_striker_id = dp.player_id
            WHERE dp.player_id IS NULL
        """,
        "match_id": """
            SELECT COUNT(*)
            FROM fact_ball fb
            LEFT JOIN dim_match dm ON fb.match_id = dm.match_id
            WHERE dm.match_id IS NULL
        """,
    }

    for field_name, sql in checks.items():
        orphan_count = conn.execute(sql).fetchone()[0]
        assert orphan_count == 0, f"Referential integrity failed for {field_name}: {orphan_count} orphan rows"

    logger.info("Verified referential integrity")


def verify_phase_distribution(conn: duckdb.DuckDBPyConnection) -> None:
    """Assert match_phase distribution is in expected ranges.

    Args:
        conn: Open DuckDB connection.
    """
    total_rows = conn.execute("SELECT COUNT(*) FROM fact_ball").fetchone()[0]
    if total_rows == 0:
        logger.warning("Skipping phase distribution check for empty fact_ball")
        return

    rows = conn.execute(
        """
        SELECT match_phase, COUNT(*) AS row_count
        FROM fact_ball
        GROUP BY match_phase
        """
    ).fetchall()
    counts = {phase: count for phase, count in rows}
    ranges = {
        "powerplay": (0.25, 0.40),
        "middle": (0.35, 0.55),
        "death": (0.15, 0.30),
    }

    for phase, (minimum, maximum) in ranges.items():
        share = counts.get(phase, 0) / total_rows
        if not minimum <= share <= maximum:
            logger.warning(
                "Match phase distribution outside expected range",
                match_phase=phase,
                share=round(share, 4),
                expected_minimum=minimum,
                expected_maximum=maximum,
            )

    logger.info("Verified phase distribution", total_rows=total_rows)


def verify_no_duplicate_deliveries(conn: duckdb.DuckDBPyConnection) -> None:
    """Assert fact_ball has no duplicate delivery keys.

    Args:
        conn: Open DuckDB connection.
    """
    duplicate_count = conn.execute(
        """
        SELECT COUNT(*)
        FROM (
            SELECT match_id, innings_number, delivery_sequence
            FROM fact_ball
            GROUP BY match_id, innings_number, delivery_sequence
            HAVING COUNT(*) > 1
        ) duplicate_keys
        """
    ).fetchone()[0]
    assert duplicate_count == 0, f"Duplicate delivery keys found: {duplicate_count}"

    logger.info("Verified duplicate delivery absence")


def run_all_checks(
    conn: duckdb.DuckDBPyConnection,
    expected_counts: dict[str, int],
) -> None:
    """Run all quality checks in sequence. Raises on any failure.

    Args:
        conn: Open DuckDB connection.
        expected_counts: Expected row counts by table name.
    """
    started_at = perf_counter()
    verify_row_counts(conn, expected_counts)
    verify_referential_integrity(conn)
    verify_phase_distribution(conn)
    verify_no_duplicate_deliveries(conn)
    logger.info("Completed quality checks", duration_seconds=round(perf_counter() - started_at, 4))
