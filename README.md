# genbi-ipl

**AI-powered generative BI for IPL and WPL cricket analytics.**

Ask questions in plain English. Get SQL, results, and a natural language explanation — grounded in ball-by-ball data from every IPL match since 2008 and every WPL match since 2023.

> *"Who had the best economy rate in powerplay overs across IPL 2023?"*
> *"Show me Virat Kohli's strike rate in death overs by season."*
> *"Which team hits the most sixes at Wankhede Stadium?"*

---

## What This Is

genbi-ipl is a **text-to-SQL system** built specifically for cricket analytics. It combines a RAG (Retrieval-Augmented Generation) pipeline with a semantic layer grounded in cricket domain knowledge, converting natural-language questions into accurate DuckDB SQL against a ball-by-ball fact table.

This is a portfolio project demonstrating real-world LLM application engineering — not a tutorial or toy. Every architectural decision is documented, every module is tested, and the system is designed to run end-to-end on a laptop with zero cloud costs.

---

## Architecture

```
Browser
   │
   ▼
┌──────────────────────┐
│   Go API Gateway     │  Request validation, rate limiting,
│   (Gin, port 8080)   │  structured logging, request IDs
└──────────┬───────────┘
           │ HTTP
           ▼
┌──────────────────────┐
│  Python Intelligence │  Text-to-SQL pipeline:
│  Service (FastAPI,   │  semantic rewrite → schema injection →
│  port 8000)          │  few-shot retrieval → SQL generation →
│                      │  validation → execution → explanation
└──────┬───────┬───────┘
       │       │
       ▼       ▼
  DuckDB    ChromaDB
  (fact +   (embeddings:
  dim       few-shot examples
  tables)   + entity resolution)
```

**Key design decisions:**

- Go gateway owns operational concerns (validation, rate limiting, observability). Python owns intelligence (LLM, RAG, SQL). Clean separation.
- Intelligence lives in prompt engineering and orchestration, not a self-hosted model. LLM calls go to Groq's free tier (Llama 4 Scout for SQL, Llama 3.1 8B for explanations).
- Schema design encodes cricket domain knowledge — `match_phase`, `is_bowler_wicket`, `is_legal_delivery` — so the LLM generates simpler, more accurate SQL.

---

## Tech Stack

| Layer                 | Technology                |           Why             |
|                    ---|                        ---|                       --- |
| API Gateway           | Go 1.22 + Gin             | Production-style operational shell |
| Intelligence Service  | Python 3.12 + FastAPI     | LLM orchestration, RAG pipeline |
| Database              | DuckDB                    | Embedded analytical DB, fast aggregations |
| Vector Store          | ChromaDB                  | Embedded, zero-cost, Python-native |
| Embeddings            | bge-small-en-v1.5 (CPU)   | Few-shot retrieval + entity resolution |
| LLM                   | Groq free tier (Llama 4 Scout) | Zero cost, strong SQL benchmarks |
| ETL                   | Python + pandas           | Cricsheet JSON → DuckDB, 303k rows in ~5s |
| Orchestration         | Docker Compose            | Single command runs everything |
| Logging               | structlog                 | Structured JSON logs with request IDs |
| SQL Validation        | sqlglot                   | Catches hallucinated tables, unsafe mutations |

---

## Data

**Source:** [Cricsheet](https://cricsheet.org/) — ball-by-ball JSON for every IPL and WPL match.

**Coverage:**

- IPL: 2008–2026 (1,193 matches)
- WPL: 2023–2026 (88 matches)
- Total: 1,281 matches, 303,846 deliveries

**Star schema:**

```
fact_ball          ← one row per delivery (303,846 rows)
  ├── dim_player   ← 1,099 unique players with UUID-based entity resolution
  ├── dim_match    ← 1,281 matches with outcome details
  ├── dim_team     ← 21 teams (handles franchise renames)
  ├── dim_venue    ← 61 venues
  └── dim_season   ← 23 seasons
```

**Pre-computed columns on `fact_ball`:**

- `match_phase` — powerplay / middle / death
- `is_legal_delivery` — excludes wides and no-balls
- `is_bowler_wicket` — excludes run-outs from bowling figures
- `is_boundary_four`, `is_boundary_six` — respects the `non_boundary` flag
- `is_dot_ball` — legal delivery, zero runs, no wicket

---

## Getting Started

### Prerequisites

- [Docker Desktop](https://www.docker.com/products/docker-desktop/) with at least 6 GB RAM allocated
- [Git](https://git-scm.com/)
- A free [Groq API key](https://console.groq.com/keys)

### Quick Start

```bash
# 1. Clone
git clone https://github.com/RaghuS007/genbi-ipl.git
cd genbi-ipl

# 2. Set up your Groq API key
cp .env.example .env       # macOS/Linux
copy .env.example .env     # Windows PowerShell
# Edit .env and set GROQ_API_KEY=your_key_here

# 3. Build and start both services
docker compose up --build -d

# 4. Verify both services are healthy
curl http://localhost:8080/health
curl http://localhost:8000/health

# 5. Download Cricsheet data (~150 MB)
docker compose exec intelligence python scripts/download_data.py

# 6. Run the ETL pipeline (~5 minutes first run)
docker compose exec intelligence python -m etl.run_etl

# 7. Verify the data loaded correctly
docker compose exec intelligence python scripts/verify_etl.py

# 8. Open the UI
# macOS:   open http://localhost:8080
# Linux:   xdg-open http://localhost:8080
# Windows: start http://localhost:8080
```

The ETL is fully idempotent — drop-and-recreate on every run, so running it twice gives you one clean database, not doubled rows.

---

## Useful Commands

```bash
# Tail logs
docker compose logs -f

# Stop services
docker compose down

# Rebuild and restart
docker compose up --build -d

# Run the ETL test suite (19 tests)
docker compose exec intelligence pytest etl/tests/ -v

# Re-run the ETL (after code changes or to refresh data)
docker compose exec intelligence python -m etl.run_etl

# Query the database via verification script
docker compose exec intelligence python scripts/verify_etl.py
```

---

## Project Structure

```
genbi-ipl/
├── gateway/                    # Go API gateway (Gin)
│   ├── main.go                 # Validation, proxy, middleware
│   └── Dockerfile
│
├── intelligence/               # Python intelligence service (FastAPI)
│   ├── app/
│   │   ├── main.py             # FastAPI app, /health + /query endpoints
│   │   └── api/                # Request/response schemas (Pydantic)
│   ├── requirements.txt
│   └── Dockerfile
│
├── etl/                        # ETL pipeline (Phase 1 — complete)
│   ├── extract.py              # Cricsheet JSON → Python dicts
│   ├── transform.py            # Dimension table builders
│   ├── transform_facts.py      # fact_ball with cricket logic
│   ├── load.py                 # DuckDB schema + bulk insert
│   ├── quality_checks.py       # Post-load assertions
│   ├── run_etl.py              # Orchestrator
│   └── tests/                  # 19 unit tests, all passing
│
├── config/                     # YAML configuration (semantic layer)
├── scripts/                    # Utility scripts
│   ├── download_data.py        # Fetch Cricsheet zips
│   └── verify_etl.py           # Post-ETL sanity queries
│
├── frontend/static/            # Minimal HTML/JS UI
├── data/                       # Gitignored: raw JSON, DuckDB, ChromaDB
├── evaluation/                 # Golden query set (Phase 6)
├── docker-compose.yaml
└── README.md
```

---

## Delivery Status

| Phase | Description | Status |
|---|---|---|
| Phase 0 | Docker containerization, service scaffold | ✅ Complete |
| Phase 1 | ETL pipeline (extract, transform, load, verify) | ✅ Complete |
| Phase 2 | Text-to-SQL pipeline (LLM + semantic layer) | 🔄 In progress |
| Phase 3 | RAG layer (ChromaDB, embeddings, retrieval) | ⏳ Planned |
| Phase 4 | Go gateway (full implementation) | ⏳ Planned |
| Phase 5 | Frontend UI | ⏳ Planned |
| Phase 6 | Evaluation suite + accuracy measurement | ⏳ Planned |

**Current capabilities:**

- ✅ Full ETL from Cricsheet to queryable DuckDB (1,281 matches, 303,846 deliveries)
- ✅ Full referential integrity (DuckDB-enforced FKs, CHECK constraints)
- ✅ Idempotent re-runs (drop-and-recreate)
- ✅ Automated post-load quality checks
- ✅ 19 unit tests passing
- 🔄 Text-to-SQL stub endpoint (real LLM integration in Phase 2)

---

## Troubleshooting

**`docker compose up` takes a long time on first build.**
First build downloads Python dependencies including a CPU-only PyTorch build (~200 MB). Subsequent builds use Docker layer cache and are much faster.

**`ModuleNotFoundError: No module named 'etl'` when running scripts.**
Make sure you're running inside the container (`docker compose exec intelligence ...`), not on your host. The `etl` package is at `/app/etl` inside the container.

**ETL fails with `Could not find file data/raw/...`.**
Run the download step first: `docker compose exec intelligence python scripts/download_data.py`

**Health check on `http://localhost:8080/health` fails.**
Give the services 10–15 seconds to start up after `docker compose up`. The Go gateway waits for the Python service to report healthy.

**First embedding call is slow.**
The `bge-small-en-v1.5` model downloads from HuggingFace on first use (~133 MB). Cached for all subsequent calls.

---

## Data Attribution

Ball-by-ball data from [Cricsheet](https://cricsheet.org/), made available under the [CC BY-SA 4.0](https://creativecommons.org/licenses/by-sa/4.0/) licence.

---

## License

MIT — see [LICENSE](LICENSE).