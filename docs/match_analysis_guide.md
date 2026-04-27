Good idea — institutional knowledge is more valuable than the analysis itself. A guide you can follow blindly six months from now is what makes a tool actually useful.
Let me create this as a proper file in your repo.
Create docs/match_analysis_guide.md
markdown# Pre-Match Analysis Guide

A repeatable workflow for using `genbi-ipl` to analyse upcoming IPL or WPL fixtures. Designed to be followed in 10–15 minutes before a match starts.

---
## Tool Reference

The repo ships three reusable analysis scripts under `scripts/`:

| Script | Purpose | Key flags |
|---|---|---|
| `team_squad.py` | Players who appeared for a team in a given season | `--team`, `--season`, `--format` |
| `head_to_head.py` | Full H2H between two teams: record, venues, top performers | `--team-a`, `--team-b`, `--since-year`, `--venue`, `--recent` |
| `matchups.py` | Batter-vs-bowler grid for any roster combination | `--batters`, `--bowlers`, `--since-year`, `--phase` |

All three accept short team aliases (`RCB`, `GT`, `CSK`, etc.) and support both text and JSON output.

For database health verification:

| Script | Purpose |
|---|---|
| `verify_etl.py` | Post-ETL sanity queries (top scorers, season summary) |
| `acceptance_test.py` | 11 checks against known cricket facts |
| `diagnose_h2h.py` | Diagnose team canonicalization issues |
| `diagnose_seasons.py` | List raw Cricsheet season values |
## Prerequisites

- Repo cloned and Phase 1 ETL complete (you have `data/db/genbi.duckdb`)
- Docker services running:
```powershell
  docker compose up -d
```
- Latest data — refresh if your last ETL run was more than 24 hours ago:
```powershell
  docker compose exec intelligence python scripts/download_data.py
  docker compose exec intelligence python -m etl.run_etl
```

Cricsheet typically updates within 24 hours of each match. If today's analysis is for a match starting in a few hours, refreshing first ensures the previous match's data is included.

---

## Step 1 — Identify the Two Teams

Pull each team's current-season squad to refresh your memory on who's been playing.

```powershell
docker compose exec intelligence python scripts/team_squad.py --team RCB --season 2026
docker compose exec intelligence python scripts/team_squad.py --team GT  --season 2026
```

Available short aliases: `RCB, GT, CSK, MI, KKR, DC, PBKS, SRH, RR, LSG`. For older seasons add `GL` (Gujarat Lions), `RPS` (Rising Pune Supergiant). Use `--list-teams` if you forget.

What to look for in the output:
- **Top of the batters list:** core run-scorers — these are who you're worried about as the opposition
- **Top of the bowlers list:** wicket-takers — the strike bowlers
- **Match count:** anyone with ≥ 60% of season matches is a regular starter; below 40% is a bench player

Older seasons work too — `--season 2023` for retro analysis or trend questions.

---

## Step 2 — Get the Likely Playing XIs

Cricsheet doesn't predict lineups. You need to source the probable XIs from elsewhere:

- **ESPNCricinfo's match preview** (most reliable, usually published 2–3 hours before toss)
- **Cricbuzz live page**
- **Team's official social media** (toss announcements)

Write the names down in **Cricsheet format** — initials + surname. Examples:

| Common name | Cricsheet form |
|---|---|
| Virat Kohli | `V Kohli` |
| Phil Salt | `PD Salt` |
| Rajat Patidar | `RM Patidar` |
| Mohammed Siraj | `Mohammed Siraj` (full first name when no obvious initial) |
| Rashid Khan | `Rashid Khan` |

Don't stress about getting these perfect — `scripts/matchups.py` does fuzzy matching against `dim_player.name_variants`, so `Kohli` will resolve to `V Kohli`. But matching the canonical form gives cleaner output. The `team_squad.py` output from Step 1 shows the exact spellings.
### Step 2.5 — Head-to-Head Context

Before drilling into individual player matchups, get the team-level history. The `head_to_head.py` script summarises every encounter between two teams and surfaces patterns invisible in current-season data alone.

**Basic usage:**

```bash
docker compose exec intelligence python scripts/head_to_head.py --team-a RCB --team-b DC
```

This prints six sections:

1. **Overall record** — total matches, wins per team, no-result/tie count
2. **Per-season breakdown** — year-by-year W/L with match counts
3. **By venue** — split with at least 2 matches per venue
4. **Last N encounters** — most recent matches with date, winner, margin
5. **Top run-scorers in this fixture** — players who score most against this opposition
6. **Top wicket-takers in this fixture** — players who take most wickets

**Useful flags:**

| Flag | Purpose | Example |
|---|---|---|
| `--since-year` | Only matches from this year onward | `--since-year 2020` for recent form only |
| `--venue` | Restrict to one venue (exact name from `dim_venue`) | `--venue "M Chinnaswamy Stadium"` |
| `--recent` | How many recent matches to show (default 5) | `--recent 10` |
| `--format` | `text` (default) or `json` | `--format json` |

**Reading the per-season breakdown:**

Look for **trend reversals**. A team that dominated 2010–2015 but has lost 4 of the last 5 meetings is in a different rivalry now than the overall record suggests. Always weight recent seasons more.

**Reading the venue split:**

| Pattern | What it suggests |
|---|---|
| Team A wins 4/5 at their home venue | Strong home dominance — toss matters less if Team A bats first |
| Both teams roughly even at both venues | Toss-decided fixture — listen for what captain chose |
| One team wins consistently away | Travel/conditions advantage — note the away team's strength |

**Caveat — venue name fragmentation:**

Cricsheet records venue strings inconsistently across seasons. You may see:
- `M Chinnaswamy Stadium`
- `M Chinnaswamy Stadium, Bengaluru`
- `M.Chinnaswamy Stadium`

These are the same physical venue. The current data treats them as separate `dim_venue` rows. When reading the by-venue split, mentally combine variants of the same ground. (A future ETL improvement will canonicalize these the way teams already are.)

**Reading the top performers section:**

This is the most underrated part of the H2H output. Some players raise their game against specific opposition. If you see a name with disproportionate runs or wickets in this fixture compared to their overall career profile, **note them for the pre-match narrative** — they're the player most likely to have a big game tonight.

**Worked example (RCB vs DC, since 2008):**

- Overall: 34 matches, RCB 19, DC 13, 2 NR/tie — RCB has historically owned this fixture
- Recent: DC won 3 of the last 5 — momentum has shifted
- Top scorer in fixture: V Kohli, 1,030 runs in 27 matches — averages 38 against DC specifically, well above his career baseline
- Top wicket-taker: YS Chahal, 15 wickets in 15 matches — owns this matchup, but he's no longer with RCB
- Modern threat from DC: K Rabada, 13 wickets in 6 matches — over 2 wickets per match, the most likely RCB-killer

Combining all of this: the headline takeaway isn't "RCB has the historical edge"; it's "Kohli has owned this rivalry, but DC's current bowling unit has the recent psychological advantage."

**When to skip H2H:**

- **First-ever meeting** (e.g., GT vs LSG in 2022's first round) — no data, skip
- **Less than 5 historical meetings** — sample too small to draw conclusions; rely on player matchups instead
- **Both teams have radically different personnel** from past meetings — historical team-level patterns may not transfer

In those cases, lean harder on Step 3 (player matchups), which is personnel-specific.
---

## Step 3 — Run the Matchup Analysis

The core analysis is two queries — one per innings direction.

### 3a. Team A's batters vs Team B's bowlers

```powershell
docker compose exec intelligence python scripts/matchups.py `
    --batters "Comma,separated,batter,names" `
    --bowlers "Comma,separated,bowler,names" `
    --since-year 2022
```

### 3b. Team B's batters vs Team A's bowlers

Same command with the rosters swapped.

### Choosing `--since-year`

| Scenario | Use |
|---|---|
| Match between two teams that have both existed since IPL 2008 | `--since-year 2020` (recent form, 5–6 seasons of data) |
| One team is new (GT entered 2022, LSG entered 2022) | `--since-year` matching the newer team's debut year |
| You want a player's career-long history | `--since-year 2008` |
| You want only this season's form | `--since-year` matching the current year |

Default is `2020`. Going further back than `2020` is usually noise — players' games change too much over 6+ years.

---

## Step 4 — Reading the Matchup Grid

Each cell looks like `48b/62r(129) 7/2 w:1`:

| Symbol | Meaning |
|---|---|
| `48b` | 48 legal balls faced (excludes wides and no-balls) |
| `62r` | 62 runs scored by the batter (excludes extras) |
| `(129)` | Strike rate — 129 runs per 100 balls |
| `7/2` | 7 fours, 2 sixes |
| `w:1` | Dismissed once by this bowler (bowler-credited only — no run-outs) |
| `—` | Never faced each other in this window |

### Sample size rules

| Balls faced | Reliability |
|---|---|
| < 12 | Ignore — too small to read into |
| 12–24 | Suggestive — note but don't over-weight |
| 25–60 | Meaningful — real signal |
| 60+ | Strong evidence — this is a known pattern |

A 200 strike rate over 6 balls is meaningless. A 130 strike rate over 60 balls is a real story.

### What to flag

| Pattern | What it tells you |
|---|---|
| **High SR (>140), zero dismissals, decent sample** | Batter's comfort zone. Bowling captain will avoid this matchup. |
| **Low SR (<110) + dismissals + decent sample** | Bowler's dominant matchup. Expect bowling captain to force this in key overs. |
| **High dot ball % (>50)** | Bowler builds pressure even without taking wickets — useful in middle overs. |
| **High SR but high dismissals** | High-risk-high-reward matchup. Could go either way. |
| **Dash `—`** | First-time matchup. No data. Watch how the first 6 balls go. |

---

## Step 5 — The Phase-Specific Look (Optional but Useful)

The general matchup query merges all match phases. For deeper analysis, slice by phase:

```powershell
docker compose exec intelligence python scripts/matchups.py `
    --batters "V Kohli,RM Patidar,TH David,R Shepherd" `
    --bowlers "Rashid Khan,Mohammed Siraj,K Rabada" `
    --since-year 2022 `
    --phase death
```

Phase options: `powerplay` (overs 1–6), `middle` (7–15), `death` (16+).

**When to look at each phase:**
- **Powerplay:** focus on openers vs new-ball bowlers
- **Middle overs:** focus on spinners vs settled batters; this is where most matches are won/lost tactically
- **Death:** focus on finishers vs death specialists — Bumrah-type bowlers vs Dhoni/Pollard-type batters

In a Phase 2 RAG world, a question like "How does Kohli play Rashid Khan in the middle overs?" will get translated to this query automatically. For now, it's manual.

---

## Step 6 — Build the Pre-Match Narrative

Once you have the two grids and (optionally) phase splits, distil into 4–6 bullet points. The structure that works:

### Template
[Team A] vs [Team B] — [Date] — [Venue]
KEY MATCHUPS FOR [TEAM A]:

[Batter X] thrives against [Team B Bowler Y]: [stats]
[Batter Z] has been troubled by [Team B Bowler W]: [stats]

KEY MATCHUPS FOR [TEAM B]:

(same structure)

DEATH OVER WATCH:

[most concerning death matchup]

WILDCARDS:

[first-time matchup worth watching]
[recent form vs historical form mismatch]


### What "good" looks like

- Specific numbers, not vibes ("Kohli SR 142 vs Rabada in 38 balls" not "Kohli is good against Rabada")
- 4–6 bullets total, not 20 — anything longer doesn't get used
- Mention the bowler **and** the suggested counter — "expect GT to bowl Rashid in overs 11–14 against Patidar to dry up runs"

---

## Step 7 — Save Insights for Later

If you want to track how predictions match reality, save the JSON output:

```powershell
docker compose exec intelligence python scripts/matchups.py `
    --batters "..." --bowlers "..." `
    --since-year 2022 --format json > analysis/2026-04-23-rcb-vs-gt.json
```

Create the `analysis/` directory once and add it to `.gitignore` if you don't want it tracked. Or commit it as a record of your reasoning over the season — interesting later for retrospective accuracy checks.

---

## Common Pitfalls

**Pitfall: Reading into tiny samples.** A 250 strike rate over 4 balls is statistical noise. Stick to the sample-size rules above.

**Pitfall: Ignoring phase.** Bumrah's overall economy is meaningless if he only bowls death overs. Always check phase-specific numbers for specialists.

**Pitfall: Trusting old data for new contexts.** A batter's 2018 numbers against a bowler may not predict 2026 form — bodies change, techniques evolve. `--since-year 2022` is usually safer than 2008 for matchups.

**Pitfall: Forgetting franchise renames.** Punjab Kings = Kings XI Punjab. Delhi Capitals = Delhi Daredevils. RCB = Royal Challengers Bangalore. Your data already handles these via team alias canonicalisation, but if you ever query `dim_team` directly, the canonical name is the post-rename version.

**Pitfall: Sunrisers Hyderabad ≠ Deccan Chargers.** Even though same city. They're legally separate franchises with separate squads. Your data treats them as separate teams. Don't merge them.

---



## Cheat Sheet — Full Workflow

```bash
# 1. Refresh data (skip if recent)
docker compose exec intelligence python scripts/download_data.py
docker compose exec intelligence python -m etl.run_etl

# 2. Squads
docker compose exec intelligence python scripts/team_squad.py --team RCB --season 2026
docker compose exec intelligence python scripts/team_squad.py --team DC  --season 2026

# 3. Head-to-head context
docker compose exec intelligence python scripts/head_to_head.py --team-a RCB --team-b DC

# 4. Recent form (last 5 seasons)
docker compose exec intelligence python scripts/head_to_head.py --team-a RCB --team-b DC --since-year 2020

# 5. Player matchups (one per innings direction)
docker compose exec intelligence python scripts/matchups.py `
    --batters "<Team A XI batters>" `
    --bowlers "<Team B XI bowlers>" `
    --since-year 2020

docker compose exec intelligence python scripts/matchups.py `
    --batters "<Team B XI batters>" `
    --bowlers "<Team A XI bowlers>" `
    --since-year 2020

# 6. Death-over deep dive (optional)
docker compose exec intelligence python scripts/matchups.py `
    --batters "..." --bowlers "..." `
    --since-year 2020 --phase death

# 7. Sanity-check the database state (occasional)
docker compose exec intelligence python scripts/acceptance_test.py
```

## Where This Goes Next

Once Phase 2 (text-to-SQL pipeline) is complete, this entire workflow becomes a single natural-language conversation:

> "Give me the RCB vs GT matchup analysis for tonight's match. Phil Salt and Kohli open for RCB. Rashid Khan and Siraj are GT's main bowlers. Focus on death overs."

The system handles team-name resolution, player ID lookup, query construction, phase filtering, and narrative generation. The work in this guide is what the LLM will be doing under the hood — encoded in prompts and few-shot examples rather than executed manually by you.