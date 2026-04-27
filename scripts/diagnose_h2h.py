"""Diagnose RCB vs DC head-to-head data discrepancy."""
import duckdb

conn = duckdb.connect("data/db/genbi.duckdb", read_only=True)

# 1. How many RCB matches total in the database?
rcb = conn.execute("""
    SELECT COUNT(*) FROM dim_match m
    JOIN dim_team t1 ON m.team1_id = t1.team_id
    JOIN dim_team t2 ON m.team2_id = t2.team_id
    WHERE t1.team_name = 'Royal Challengers Bengaluru'
       OR t2.team_name = 'Royal Challengers Bengaluru'
""").fetchone()[0]
print(f"Total matches involving 'Royal Challengers Bengaluru': {rcb}")

# 2. How about old "Royal Challengers Bangalore" name?
rcb_old = conn.execute("""
    SELECT COUNT(*) FROM dim_match m
    JOIN dim_team t1 ON m.team1_id = t1.team_id
    JOIN dim_team t2 ON m.team2_id = t2.team_id
    WHERE t1.team_name = 'Royal Challengers Bangalore'
       OR t2.team_name = 'Royal Challengers Bangalore'
""").fetchone()[0]
print(f"Total matches involving 'Royal Challengers Bangalore': {rcb_old}")

# 3. What ARE all the team names in dim_team?
print("\nAll teams in dim_team:")
for (name,) in conn.execute("SELECT team_name FROM dim_team ORDER BY team_name").fetchall():
    print(f"  {name}")

# 4. RCB vs DC by individual team name combos
print("\nRCB vs DC matches by team name combo (since 2008):")
for (t1_name, t2_name, count) in conn.execute("""
    SELECT t1.team_name, t2.team_name, COUNT(*) AS c
    FROM dim_match m
    JOIN dim_team t1 ON m.team1_id = t1.team_id
    JOIN dim_team t2 ON m.team2_id = t2.team_id
    WHERE m.tournament = 'IPL'
      AND (t1.team_name LIKE '%Royal Challengers%' OR t1.team_name LIKE '%Delhi%')
      AND (t2.team_name LIKE '%Royal Challengers%' OR t2.team_name LIKE '%Delhi%')
      AND t1.team_name != t2.team_name
    GROUP BY t1.team_name, t2.team_name
    ORDER BY c DESC
""").fetchall():
    print(f"  {t1_name:<35} vs {t2_name:<35}  {count}")

conn.close()