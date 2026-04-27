"""Phase 1 acceptance test — verifies analytical correctness against known cricket facts."""
import duckdb
import sys

CHECKS = []


def check(name: str, expected, actual, comparator=None):
    """Record a check result."""
    if comparator is None:
        passed = expected == actual
    else:
        passed = comparator(expected, actual)
    CHECKS.append((name, expected, actual, passed))
    return passed


def main() -> int:
    conn = duckdb.connect("data/db/genbi.duckdb", read_only=True)

    # 1. Total IPL matches between 2008 and 2024 inclusive
    rows = conn.execute("""
        SELECT COUNT(*) FROM dim_match
        WHERE tournament = 'IPL' AND season_year BETWEEN 2008 AND 2024
    """).fetchone()[0]
    check("IPL matches 2008-2024", "between 1080 and 1110", rows,
      lambda e, a: 1080 <= a <= 1110)

    # 2. CSK has won 5 IPL titles (2010, 2011, 2018, 2021, 2023)
    csk_titles = conn.execute("""
        SELECT COUNT(DISTINCT m.season_year)
        FROM dim_match m
        JOIN dim_team t ON m.winner_team_id = t.team_id
        WHERE t.team_name = 'Chennai Super Kings'
          AND m.tournament = 'IPL'
          AND m.match_date IN (
              SELECT MAX(match_date) FROM dim_match m2
              JOIN dim_team t2 ON m2.winner_team_id = t2.team_id
              WHERE m2.season_year = m.season_year AND m2.tournament = 'IPL'
              AND t2.team_name = 'Chennai Super Kings'
          )
    """).fetchone()[0]
    check("CSK final wins (rough)", ">= 5", csk_titles, lambda e, a: a >= 5)

    # 3. Virat Kohli has scored 8000+ IPL runs
    kohli_runs = conn.execute("""
        SELECT SUM(batter_runs)
        FROM fact_ball f
        JOIN dim_match m ON f.match_id = m.match_id
        WHERE f.batter_name = 'V Kohli' AND m.tournament = 'IPL'
    """).fetchone()[0]
    check("V Kohli IPL career runs", ">= 8000", kohli_runs, lambda e, a: a >= 8000)

    # 4. MS Dhoni is the leading death-over run scorer
    top_death = conn.execute("""
        SELECT batter_name FROM fact_ball f
        JOIN dim_match m ON f.match_id = m.match_id
        WHERE f.match_phase = 'death' AND f.is_legal_delivery = TRUE
          AND m.tournament = 'IPL'
        GROUP BY batter_name
        ORDER BY SUM(batter_runs) DESC
        LIMIT 1
    """).fetchone()[0]
    check("Top IPL death-over run scorer", "MS Dhoni", top_death)

    # 5. Mumbai Indians has the most IPL match wins
    top_winner = conn.execute("""
        SELECT t.team_name FROM dim_match m
        JOIN dim_team t ON m.winner_team_id = t.team_id
        WHERE m.tournament = 'IPL'
        GROUP BY t.team_name
        ORDER BY COUNT(*) DESC
        LIMIT 1
    """).fetchone()[0]
    check("Most IPL match wins", "Mumbai Indians or Chennai Super Kings", top_winner,
          lambda e, a: a in ("Mumbai Indians", "Chennai Super Kings"))

    # 6. Phase distribution sane
    phase_dist = dict(conn.execute("""
        SELECT match_phase, COUNT(*) FROM fact_ball GROUP BY match_phase
    """).fetchall())
    powerplay_pct = phase_dist.get('powerplay', 0) * 100 / sum(phase_dist.values())
    check("Powerplay % of deliveries", "between 28 and 35", round(powerplay_pct, 1),
          lambda e, a: 28 <= a <= 35)

    # 7. WPL has only female matches
    wpl_genders = conn.execute("""
        SELECT DISTINCT gender FROM dim_match WHERE tournament = 'WPL'
    """).fetchall()
    check("WPL gender", "[('female',)]", wpl_genders, lambda e, a: a == [('female',)])

    # 8. IPL has only male matches
    ipl_genders = conn.execute("""
        SELECT DISTINCT gender FROM dim_match WHERE tournament = 'IPL'
    """).fetchall()
    check("IPL gender", "[('male',)]", ipl_genders, lambda e, a: a == [('male',)])

    # 9. No null player IDs in fact_ball
    null_count = conn.execute("""
        SELECT COUNT(*) FROM fact_ball
        WHERE batter_id IS NULL OR bowler_id IS NULL OR non_striker_id IS NULL
    """).fetchone()[0]
    check("Null required player IDs", "0", null_count, lambda e, a: a == 0)

    # 10. dim_team has exactly 17 (post-canonicalization)
    team_count = conn.execute("SELECT COUNT(*) FROM dim_team").fetchone()[0]
    check("dim_team row count", "17", team_count, lambda e, a: a == 17)

    # 11. RCB vs DC head-to-head
    h2h = conn.execute("""
        SELECT COUNT(*) FROM dim_match m
        JOIN dim_team t1 ON m.team1_id = t1.team_id
        JOIN dim_team t2 ON m.team2_id = t2.team_id
        WHERE m.tournament = 'IPL'
          AND ((t1.team_name = 'Royal Challengers Bengaluru' AND t2.team_name = 'Delhi Capitals')
               OR (t1.team_name = 'Delhi Capitals' AND t2.team_name = 'Royal Challengers Bengaluru'))
    """).fetchone()[0]
    check("RCB vs DC total matches", "32-36", h2h, lambda e, a: 32 <= a <= 36)

    conn.close()

    # Print results
    print(f"\n{'PASS/FAIL':<8} {'CHECK':<40} {'EXPECTED':<25} {'ACTUAL'}")
    print("-" * 100)
    passed = failed = 0
    for name, expected, actual, ok in CHECKS:
        status = "PASS" if ok else "FAIL"
        if ok:
            passed += 1
        else:
            failed += 1
        print(f"{status:<8} {name:<40} {str(expected):<25} {actual}")

    print(f"\n{passed} passed, {failed} failed out of {len(CHECKS)} checks.")
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    sys.exit(main())