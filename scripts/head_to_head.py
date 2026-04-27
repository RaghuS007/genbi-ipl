"""Head-to-head record between two teams.

Usage:
    python scripts/head_to_head.py --team-a RCB --team-b DC
    python scripts/head_to_head.py --team-a "Royal Challengers Bengaluru" --team-b "Delhi Capitals"
    python scripts/head_to_head.py --team-a RCB --team-b DC --since-year 2020
    python scripts/head_to_head.py --team-a RCB --team-b DC --venue "M Chinnaswamy Stadium"
    python scripts/head_to_head.py --team-a RCB --team-b DC --format json
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import duckdb


DB_PATH = Path("data/db/genbi.duckdb")

TEAM_ALIASES: dict[str, str] = {
    "RCB": "Royal Challengers Bengaluru",
    "GT": "Gujarat Titans",
    "CSK": "Chennai Super Kings",
    "MI": "Mumbai Indians",
    "KKR": "Kolkata Knight Riders",
    "DC": "Delhi Capitals",
    "PBKS": "Punjab Kings",
    "SRH": "Sunrisers Hyderabad",
    "RR": "Rajasthan Royals",
    "LSG": "Lucknow Super Giants",
    "GL": "Gujarat Lions",
    "RPS": "Rising Pune Supergiant",
}


def resolve_team_name(conn: duckdb.DuckDBPyConnection, team_input: str) -> str | None:
    if team_input.upper() in TEAM_ALIASES:
        return TEAM_ALIASES[team_input.upper()]

    row = conn.execute(
        "SELECT team_name FROM dim_team WHERE team_name = ?", [team_input]
    ).fetchone()
    if row:
        return row[0]

    rows = conn.execute(
        "SELECT team_name FROM dim_team WHERE LOWER(team_name) LIKE LOWER(?) LIMIT 5",
        [f"%{team_input}%"],
    ).fetchall()
    if not rows:
        return None
    if len(rows) == 1:
        return rows[0][0]

    print(f"Ambiguous team '{team_input}'. Matches:", file=sys.stderr)
    for (name,) in rows:
        print(f"  - {name}", file=sys.stderr)
    return None


def fetch_overall_record(
    conn: duckdb.DuckDBPyConnection,
    team_a: str,
    team_b: str,
    since_year: int,
    venue: str | None,
) -> dict:
    """Aggregate H2H record across all matches between the two teams."""
    venue_filter = "AND v.venue_name = ?" if venue else ""
    params = [team_a, team_b, team_b, team_a, since_year]
    if venue:
        params.append(venue)

    row = conn.execute(
        f"""
        SELECT
            COUNT(*) AS total_matches,
            SUM(CASE WHEN tw.team_name = ? THEN 1 ELSE 0 END) AS team_a_wins,
            SUM(CASE WHEN tw.team_name = ? THEN 1 ELSE 0 END) AS team_b_wins,
            SUM(CASE WHEN m.result IN ('no result', 'tie') OR m.winner_team_id IS NULL THEN 1 ELSE 0 END) AS no_result_or_tie
        FROM dim_match m
        JOIN dim_team t1 ON m.team1_id = t1.team_id
        JOIN dim_team t2 ON m.team2_id = t2.team_id
        LEFT JOIN dim_team tw ON m.winner_team_id = tw.team_id
        JOIN dim_venue v ON m.venue_id = v.venue_id
        WHERE m.tournament = 'IPL'
          AND ((t1.team_name = ? AND t2.team_name = ?)
               OR (t1.team_name = ? AND t2.team_name = ?))
          AND m.season_year >= ?
          {venue_filter}
        """,
        [team_a, team_b, team_a, team_b, team_b, team_a, since_year]
        + ([venue] if venue else []),
    ).fetchone()

    total, a_wins, b_wins, nr_tie = row
    return {
        "total_matches": total or 0,
        "team_a_wins": a_wins or 0,
        "team_b_wins": b_wins or 0,
        "no_result_or_tie": nr_tie or 0,
    }


def fetch_per_season(
    conn: duckdb.DuckDBPyConnection,
    team_a: str,
    team_b: str,
    since_year: int,
    venue: str | None,
) -> list[tuple]:
    """Per-season breakdown."""
    venue_filter = "AND v.venue_name = ?" if venue else ""
    params = [team_a, team_b, team_a, team_b, team_b, team_a, since_year]
    if venue:
        params.append(venue)

    return conn.execute(
        f"""
        SELECT
            m.season_year,
            COUNT(*) AS matches,
            SUM(CASE WHEN tw.team_name = ? THEN 1 ELSE 0 END) AS team_a_wins,
            SUM(CASE WHEN tw.team_name = ? THEN 1 ELSE 0 END) AS team_b_wins
        FROM dim_match m
        JOIN dim_team t1 ON m.team1_id = t1.team_id
        JOIN dim_team t2 ON m.team2_id = t2.team_id
        LEFT JOIN dim_team tw ON m.winner_team_id = tw.team_id
        JOIN dim_venue v ON m.venue_id = v.venue_id
        WHERE m.tournament = 'IPL'
          AND ((t1.team_name = ? AND t2.team_name = ?)
               OR (t1.team_name = ? AND t2.team_name = ?))
          AND m.season_year >= ?
          {venue_filter}
        GROUP BY m.season_year
        ORDER BY m.season_year
        """,
        params,
    ).fetchall()


def fetch_by_venue(
    conn: duckdb.DuckDBPyConnection,
    team_a: str,
    team_b: str,
    since_year: int,
) -> list[tuple]:
    """Win record split by venue."""
    return conn.execute(
        """
        SELECT
            v.venue_name,
            v.city,
            COUNT(*) AS matches,
            SUM(CASE WHEN tw.team_name = ? THEN 1 ELSE 0 END) AS team_a_wins,
            SUM(CASE WHEN tw.team_name = ? THEN 1 ELSE 0 END) AS team_b_wins
        FROM dim_match m
        JOIN dim_team t1 ON m.team1_id = t1.team_id
        JOIN dim_team t2 ON m.team2_id = t2.team_id
        LEFT JOIN dim_team tw ON m.winner_team_id = tw.team_id
        JOIN dim_venue v ON m.venue_id = v.venue_id
        WHERE m.tournament = 'IPL'
          AND ((t1.team_name = ? AND t2.team_name = ?)
               OR (t1.team_name = ? AND t2.team_name = ?))
          AND m.season_year >= ?
        GROUP BY v.venue_name, v.city
        HAVING COUNT(*) >= 2
        ORDER BY matches DESC, v.venue_name
        """,
        [team_a, team_b, team_a, team_b, team_b, team_a, since_year],
    ).fetchall()


def fetch_recent_matches(
    conn: duckdb.DuckDBPyConnection,
    team_a: str,
    team_b: str,
    since_year: int,
    venue: str | None,
    limit: int,
) -> list[tuple]:
    """Last N matches with details."""
    venue_filter = "AND v.venue_name = ?" if venue else ""
    params = [team_a, team_b, team_b, team_a, since_year]
    if venue:
        params.append(venue)
    params.append(limit)

    return conn.execute(
        f"""
        SELECT
            m.match_date,
            m.season_year,
            t1.team_name AS team1,
            t2.team_name AS team2,
            tw.team_name AS winner,
            m.win_by_runs,
            m.win_by_wickets,
            m.result,
            v.venue_name,
            v.city
        FROM dim_match m
        JOIN dim_team t1 ON m.team1_id = t1.team_id
        JOIN dim_team t2 ON m.team2_id = t2.team_id
        LEFT JOIN dim_team tw ON m.winner_team_id = tw.team_id
        JOIN dim_venue v ON m.venue_id = v.venue_id
        WHERE m.tournament = 'IPL'
          AND ((t1.team_name = ? AND t2.team_name = ?)
               OR (t1.team_name = ? AND t2.team_name = ?))
          AND m.season_year >= ?
          {venue_filter}
        ORDER BY m.match_date DESC
        LIMIT ?
        """,
        params,
    ).fetchall()


def fetch_top_performers(
    conn: duckdb.DuckDBPyConnection,
    team_a: str,
    team_b: str,
    since_year: int,
    venue: str | None,
) -> dict:
    """Top run-scorers and wicket-takers in this fixture."""
    venue_filter_match = "AND v.venue_name = ?" if venue else ""
    base_params = [team_a, team_b, team_b, team_a, since_year]
    if venue:
        base_params.append(venue)

    top_batters = conn.execute(
        f"""
        SELECT
            f.batter_name,
            f.batting_team,
            SUM(f.batter_runs) AS runs,
            COUNT(DISTINCT f.match_id) AS matches
        FROM fact_ball f
        JOIN dim_match m ON f.match_id = m.match_id
        JOIN dim_team t1 ON m.team1_id = t1.team_id
        JOIN dim_team t2 ON m.team2_id = t2.team_id
        JOIN dim_venue v ON m.venue_id = v.venue_id
        WHERE m.tournament = 'IPL'
          AND ((t1.team_name = ? AND t2.team_name = ?)
               OR (t1.team_name = ? AND t2.team_name = ?))
          AND m.season_year >= ?
          {venue_filter_match}
        GROUP BY f.batter_name, f.batting_team
        ORDER BY runs DESC
        LIMIT 5
        """,
        base_params,
    ).fetchall()

    top_bowlers = conn.execute(
        f"""
        SELECT
            f.bowler_name,
            f.bowling_team,
            SUM(CASE WHEN f.is_bowler_wicket THEN 1 ELSE 0 END) AS wickets,
            COUNT(DISTINCT f.match_id) AS matches
        FROM fact_ball f
        JOIN dim_match m ON f.match_id = m.match_id
        JOIN dim_team t1 ON m.team1_id = t1.team_id
        JOIN dim_team t2 ON m.team2_id = t2.team_id
        JOIN dim_venue v ON m.venue_id = v.venue_id
        WHERE m.tournament = 'IPL'
          AND ((t1.team_name = ? AND t2.team_name = ?)
               OR (t1.team_name = ? AND t2.team_name = ?))
          AND m.season_year >= ?
          {venue_filter_match}
        GROUP BY f.bowler_name, f.bowling_team
        ORDER BY wickets DESC
        LIMIT 5
        """,
        base_params,
    ).fetchall()

    return {"top_batters": top_batters, "top_bowlers": top_bowlers}


def format_margin(runs: int | None, wickets: int | None, result: str | None) -> str:
    if result in ("no result", "tie"):
        return result
    if runs:
        return f"by {runs} runs"
    if wickets:
        return f"by {wickets} wickets"
    return "n/a"


def print_text(
    team_a: str,
    team_b: str,
    since_year: int,
    venue: str | None,
    overall: dict,
    per_season: list[tuple],
    by_venue: list[tuple],
    recent: list[tuple],
    performers: dict,
) -> None:
    venue_label = f" at {venue}" if venue else ""
    print("=" * 78)
    print(f"Head-to-Head: {team_a} vs {team_b}")
    print(f"Window: IPL since {since_year}{venue_label}")
    print("=" * 78)

    if overall["total_matches"] == 0:
        print("\nNo matches found in this window.")
        return

    print(f"\nOverall record ({overall['total_matches']} matches)")
    print("-" * 78)
    print(f"  {team_a:<35} {overall['team_a_wins']} wins")
    print(f"  {team_b:<35} {overall['team_b_wins']} wins")
    if overall["no_result_or_tie"]:
        print(f"  No result / tie:                    {overall['no_result_or_tie']}")

    if per_season:
        print(f"\nPer-season")
        print("-" * 78)
        print(f"  {'Season':<8} {'Matches':>8}  {team_a[:20]:<20} {team_b[:20]:<20}")
        for season, matches, a_wins, b_wins in per_season:
            print(f"  {season:<8} {matches:>8}  {a_wins:>20} {b_wins:>20}")

    if by_venue and not venue:
        print(f"\nBy venue (2+ matches)")
        print("-" * 78)
        print(f"  {'Venue':<35} {'Mat':>4}  {team_a[:14]:<14} {team_b[:14]:<14}")
        for v_name, city, matches, a_wins, b_wins in by_venue:
            display = v_name[:32] + ".." if len(v_name) > 34 else v_name
            print(f"  {display:<35} {matches:>4}  {a_wins:>14} {b_wins:>14}")

    if recent:
        print(f"\nLast {len(recent)} encounters")
        print("-" * 78)
        for d, season, t1, t2, w, r, wk, result, v_name, city in recent:
            margin = format_margin(r, wk, result)
            winner = w or "—"
            print(f"  {d}  {t1[:3].upper()} vs {t2[:3].upper()}  →  {winner[:25]:<25} {margin}")
            print(f"              {v_name}, {city}" if city else f"              {v_name}")

    if performers["top_batters"]:
        print(f"\nTop run-scorers in this fixture")
        print("-" * 78)
        for name, team, runs, matches in performers["top_batters"]:
            print(f"  {name:<25} ({team[:25]:<25})  {runs:>5} runs  {matches:>2} matches")

    if performers["top_bowlers"]:
        print(f"\nTop wicket-takers in this fixture")
        print("-" * 78)
        for name, team, wickets, matches in performers["top_bowlers"]:
            print(f"  {name:<25} ({team[:25]:<25})  {wickets:>3} wkts  {matches:>2} matches")
    print()


def print_json(
    team_a: str,
    team_b: str,
    since_year: int,
    venue: str | None,
    overall: dict,
    per_season: list[tuple],
    by_venue: list[tuple],
    recent: list[tuple],
    performers: dict,
) -> None:
    output = {
        "team_a": team_a,
        "team_b": team_b,
        "since_year": since_year,
        "venue": venue,
        "overall": overall,
        "per_season": [
            {"season": s, "matches": m, "team_a_wins": a, "team_b_wins": b}
            for s, m, a, b in per_season
        ],
        "by_venue": [
            {"venue": v, "city": c, "matches": m, "team_a_wins": a, "team_b_wins": b}
            for v, c, m, a, b in by_venue
        ],
        "recent_matches": [
            {
                "date": str(d),
                "season": s,
                "team1": t1,
                "team2": t2,
                "winner": w,
                "margin": format_margin(r, wk, res),
                "venue": v_name,
                "city": city,
            }
            for d, s, t1, t2, w, r, wk, res, v_name, city in recent
        ],
        "top_batters": [
            {"name": n, "team": t, "runs": r, "matches": m}
            for n, t, r, m in performers["top_batters"]
        ],
        "top_bowlers": [
            {"name": n, "team": t, "wickets": w, "matches": m}
            for n, t, w, m in performers["top_bowlers"]
        ],
    }
    print(json.dumps(output, indent=2, default=str))


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Head-to-head record between two teams.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument("--team-a", required=True, help="First team (name or alias)")
    parser.add_argument("--team-b", required=True, help="Second team (name or alias)")
    parser.add_argument(
        "--since-year", type=int, default=2008,
        help="Only include matches from this year onward (default: 2008, all-time)"
    )
    parser.add_argument(
        "--venue", help="Restrict to a specific venue (exact name from dim_venue)"
    )
    parser.add_argument(
        "--recent", type=int, default=5, help="How many recent matches to show (default: 5)"
    )
    parser.add_argument(
        "--format", choices=["text", "json"], default="text", help="Output format"
    )
    args = parser.parse_args()

    if not DB_PATH.exists():
        print(f"Database not found at {DB_PATH}. Run the ETL first.", file=sys.stderr)
        return 1

    conn = duckdb.connect(str(DB_PATH), read_only=True)
    try:
        team_a = resolve_team_name(conn, args.team_a)
        team_b = resolve_team_name(conn, args.team_b)
        if team_a is None:
            print(f"Team '{args.team_a}' not found.", file=sys.stderr)
            return 1
        if team_b is None:
            print(f"Team '{args.team_b}' not found.", file=sys.stderr)
            return 1
        if team_a == team_b:
            print("Team A and Team B are the same.", file=sys.stderr)
            return 1

        overall = fetch_overall_record(conn, team_a, team_b, args.since_year, args.venue)
        per_season = fetch_per_season(conn, team_a, team_b, args.since_year, args.venue)
        by_venue = fetch_by_venue(conn, team_a, team_b, args.since_year) if not args.venue else []
        recent = fetch_recent_matches(
            conn, team_a, team_b, args.since_year, args.venue, args.recent
        )
        performers = fetch_top_performers(conn, team_a, team_b, args.since_year, args.venue)

        if args.format == "json":
            print_json(team_a, team_b, args.since_year, args.venue, overall, per_season, by_venue, recent, performers)
        else:
            print_text(team_a, team_b, args.since_year, args.venue, overall, per_season, by_venue, recent, performers)

        return 0
    finally:
        conn.close()


if __name__ == "__main__":
    sys.exit(main())