# GenBI — Natural Language Analytics for IPL Data

> Work in progress. See docs/ for architecture and design documentation.
markdown# genbi-ipl

**AI-powered generative BI for IPL and WPL cricket analytics.**

Ask questions in plain English. Get SQL, results, and a natural language explanation — grounded in ball-by-ball data from every IPL and WPL match since 2008.

> "Who had the best economy rate in powerplay overs across IPL 2023?"
> "Show me Virat Kohli's strike rate in death overs by season."
> "Which team hits the most sixes at Wankhede Stadium?"

---

## What This Is

genbi-ipl is a production-grade **text-to-SQL system** built specifically for cricket analytics. It combines a RAG (Retrieval-Augmented Generation) pipeline with a semantic layer grounded in cricket domain knowledge to convert natural language queries into accurate DuckDB SQL against a ball-by-ball fact table.

This is a portfolio project demonstrating real-world LLM application engineering — not a tutorial or toy. Every architectural decision is documented, every module is tested, and the system is designed to run end-to-end on a laptop with zero cloud costs.

---

## Architecture
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
│  Python Intelligence │  10-step query pipeline:
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

**Key design decisions:**
- Go gateway handles all operational concerns (auth, rate limiting, observability). Python handles all intelligence concerns (LLM, RAG, SQL). Clean separation of responsibilities.
- Intelligence lives in prompt engineering and orchestration, not a self-hosted model. LLM calls go to Groq's free tier (Llama 4 Scout for SQL, Llama 3.1 8B for explanations).
- Schema design encodes cricket domain knowledge — `match_phase`, `is_bowler_wicket`, `is_legal_delivery` — so the LLM generates simpler, more accurate SQL.

Full architecture documentation: [`docs/02_architecture.md`](docs/02_architecture.md)

---

## Tech Stack

| Layer | Technology | Why |
|---|---|---|
| API Gateway | Go 1.22 + Gin | Production-style operational shell — rate limiting, logging, timeouts |
| Intelligence Service | Python 3.12 + FastAPI | LLM orchestration, RAG pipeline |
| Database | DuckDB | Embedded analytical DB — no server, fast aggregations, great Python API |
| Vector Store | ChromaDB | Embedded, zero-cost, Python-native |
| Embeddings | bge-small-en-v1.5 (CPU) | Few-shot retrieval + entity resolution |
| LLM | Groq free tier (Llama 4 Scout) | Zero cost, strong SQL generation benchmarks |
| ETL | Python + pandas | Cricsheet JSON → DuckDB, 303k rows in ~5 seconds |
| Orchestration | Docker Compose | Single `docker compose up` runs everything |
| Logging | structlog | Structured JSON logs with request IDs |
| SQL Validation | sqlglot | Catches hallucinated tables, unsafe mutations, missing LIMITs |

---

## Data

**Source:** [Cricsheet](https://cricsheet.org/) — ball-by-ball JSON for every IPL and WPL match.

**Coverage:**
- IPL: 2008–2026 (1,193 matches)
- WPL: 2023–2026 (88 matches)
- Total: 1,281 matches, 303,846 deliveries

**Schema (star schema):**
fact_ball          ← one row per delivery (303,846 rows)
├── dim_player   ← 1,099 unique players with UUID-based entity resolution
├── dim_match    ← 1,281 matches with outcome details
├── dim_team     ← 21 teams (handles franchise renames)
├── dim_venue    ← 61 venues
└── dim_season   ← 23 seasons (IPL 2008–2026, WPL 2023–2026)

**Pre-computed columns on `fact_ball`:**
- `match_phase` — powerplay / middle / death (encoded at ETL time)
- `is_legal_delivery` — excludes wides and no-balls
- `is_bowler_wicket` — excludes run-outs from bowling figures
- `is_boundary_four`, `is_boundary_six` — respects `non_boundary` flag
- `is_dot_ball` — legal delivery, zero runs, no wicket

---

## Getting Started

### Prerequisites

- [Docker Desktop](https://www.docker.com/products/docker-desktop/) with at least 6GB RAM allocated
- [Git](https://git-scm.com/)
- A free [Groq API key](https://console.groq.com/keys)

### Setup

```bash
# 1. Clone the repo
git clone https://github.com/YOUR_USERNAME/genbi-ipl.git
cd genbi-ipl

# 2. Create your environment file
cp .env.example .env
# Edit .env and add your Groq API key: GROQ_API_KEY=gsk_...

# 3. Start both services
docker compose up --build -d

# 4. Download IPL + WPL data from Cricsheet (~150MB)
docker compose exec intelligence python scripts/download_data.py

# 5. Run the ETL pipeline (builds DuckDB from raw JSON, ~5 minutes)
docker compose exec intelligence python -m etl.run_etl

# 6. Verify the data loaded correctly
docker compose exec intelligence python scripts/verify_etl.py

# 7. Open the UI
open http://localhost:8080   # macOS
start http://localhost:8080  # Windows
```

The ETL run produces a ~80MB DuckDB file in `data/db/genbi.duckdb` with full referential integrity and automated quality checks. First run takes ~5 minutes; subsequent runs are idempotent.

### Verify Services Are Running

```bash
curl http://localhost:8080/health   # Go gateway
curl http://localhost:8000/health   # Python service
```

---

## Project Structure
genbi-ipl/
├── gateway/                    # Go API gateway (Gin)
│   ├── main.go                 # Request validation, proxy, middleware
│   └── Dockerfile
│
├── intelligence/               # Python intelligence service (FastAPI)
│   ├── app/
│   │   ├── main.py             # FastAPI app, health + query endpoints
│   │   ├── api/                # Request/response schemas (Pydantic)
│   │   └── orchestration/      # 10-step query pipeline (Phase 2)
│   ├── requirements.txt
│   └── Dockerfile
│
├── etl/                        # ETL pipeline
│   ├── extract.py              # Cricsheet JSON → Python dicts
│   ├── transform.py            # Dimension table builders
│   ├── transform_facts.py      # fact_ball builder with cricket logic
│   ├── load.py                 # DuckDB schema + bulk insert
│   ├── quality_checks.py       # Post-load assertions
│   ├── run_etl.py              # Orchestrator (extract→transform→load→verify)
│   └── tests/                  # 19 unit tests, all passing
│
├── config/                     # YAML configuration
│   └── semantic_layer/         # Cricket term definitions, metric formulas
│
├── scripts/                    # Utility scripts
│   ├── download_data.py        # Fetch Cricsheet zips
│   └── verify_etl.py           # Post-ETL sanity queries
│
├── frontend/static/            # Minimal HTML/JS UI
├── data/                       # Gitignored: raw JSON, DuckDB file, ChromaDB
├── evaluation/                 # Golden query set (Phase 6)
└── docs/                       # Planning documents
├── 01_product_and_scope.md
├── 02_architecture.md
├── 03_adrs.md              # 15 Architecture Decision Records
└── 04_delivery_plan.md

---

## Development

### Running Tests

```bash
# All ETL tests
docker compose exec intelligence pytest etl/tests/ -v

# Specific suite
docker compose exec intelligence pytest etl/tests/test_transform_facts.py -v
```

### Useful Make Targets

```bash
make up          # Start services (docker compose up --build -d)
make down        # Stop services
make logs        # Tail logs
make health      # Curl both health endpoints
```

### Re-running the ETL

The ETL is fully idempotent — safe to run multiple times:

```bash
docker compose exec intelligence python -m etl.run_etl
```

To refresh from the latest Cricsheet data:

```bash
docker compose exec intelligence python scripts/download_data.py
docker compose exec intelligence python -m etl.run_etl
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

---

## Architecture Decision Records

15 ADRs document every significant technical decision — why Go was chosen for the gateway, why DuckDB over PostgreSQL, why generated text corpus for RAG, why CPU-only embeddings, why the LLM-friendly schema design. See [`docs/03_adrs.md`](docs/03_adrs.md).

---

## Data Attribution

Ball-by-ball data from [Cricsheet](https://cricsheet.org/), made available under the [CC BY-SA 4.0](https://creativecommons.org/licenses/by-sa/4.0/) licence.

---

## License

MIT — see [LICENSE](LICENSE).
