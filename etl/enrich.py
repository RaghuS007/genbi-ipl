from __future__ import annotations

import argparse
import logging
import re
import sqlite3
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any
from urllib.parse import quote_plus, urlparse
from urllib.robotparser import RobotFileParser

import duckdb
import requests
import structlog
from bs4 import BeautifulSoup


log = structlog.get_logger(__name__)

USER_AGENT = (
    "genbi-ipl-research/1.0 (personal cricket analytics; "
    "github.com/RaghuS007/genbi-ipl)"
)

NOT_FOUND_SENTINEL = "NOT_FOUND"

WIKI_TTL_DAYS = 30


class HttpCache:
    def __init__(self, db_path: str | Path) -> None:
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(str(self.db_path))
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS http_cache (
                url TEXT PRIMARY KEY,
                response_body TEXT NOT NULL,
                status_code INTEGER,
                fetched_at TEXT DEFAULT (datetime('now')),
                ttl_days INTEGER DEFAULT 30
            )
            """
        )
        self.conn.commit()

    def get(self, url: str) -> str | None:
        row = self.conn.execute(
            """
            SELECT response_body, fetched_at, ttl_days
            FROM http_cache
            WHERE url = ?
            """,
            (url,),
        ).fetchone()
        if row is None:
            return None

        body, fetched_at_raw, ttl_days = row
        fetched_at = self._parse_dt(fetched_at_raw)
        if fetched_at is None:
            return None

        if datetime.utcnow() > fetched_at + timedelta(days=int(ttl_days or 30)):
            return None
        return str(body)

    def set(self, url: str, response_body: str, status_code: int, ttl_days: int) -> None:
        self.conn.execute(
            """
            INSERT INTO http_cache (url, response_body, status_code, fetched_at, ttl_days)
            VALUES (?, ?, ?, datetime('now'), ?)
            ON CONFLICT(url) DO UPDATE SET
              response_body = excluded.response_body,
              status_code = excluded.status_code,
              fetched_at = excluded.fetched_at,
              ttl_days = excluded.ttl_days
            """,
            (url, response_body, status_code, ttl_days),
        )
        self.conn.commit()

    def close(self) -> None:
        self.conn.close()

    @staticmethod
    def _parse_dt(value: str) -> datetime | None:
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S"):
            try:
                return datetime.strptime(value, fmt)
            except ValueError:
                continue
        return None


class RespectfulSession:
    def __init__(self, min_delay_sec: float = 3.0, check_robots_startup: bool = True) -> None:
        self.min_delay_sec = min_delay_sec
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": USER_AGENT})
        self.last_request_time: dict[str, float] = {}
        self.domain_allowed: dict[str, bool] = {}
        self._sleep = time.sleep
        self._monotonic = time.monotonic
        if check_robots_startup:
            self._check_robots("en.wikipedia.org")

    def _check_robots(self, domain: str) -> None:
        started = self._monotonic()
        allowed = True
        try:
            parser = RobotFileParser(f"https://{domain}/robots.txt")
            parser.read()
            allowed = parser.can_fetch(USER_AGENT, "/")
        except Exception as exc:
            log.warning(
                "enrich.robots.warning",
                domain=domain,
                error=str(exc),
                elapsed_ms=int((self._monotonic() - started) * 1000),
            )
            allowed = True
        self.domain_allowed[domain] = allowed
        log.info(
            "enrich.robots.checked",
            domain=domain,
            allowed=allowed,
            elapsed_ms=int((self._monotonic() - started) * 1000),
        )

    def get(self, url: str, timeout: int = 30) -> requests.Response:
        domain = urlparse(url).netloc
        if domain in self.domain_allowed and not self.domain_allowed[domain]:
            raise RuntimeError(f"robots disallow fetch for domain={domain}")

        now = self._monotonic()
        last = self.last_request_time.get(domain)
        if last is not None:
            elapsed = now - last
            if elapsed < self.min_delay_sec:
                self._sleep(self.min_delay_sec - elapsed)

        retries = 3
        backoff = 10
        for attempt in range(1, retries + 1):
            response = self.session.get(url, timeout=timeout)
            self.last_request_time[domain] = self._monotonic()
            if response.status_code in (429, 503) and attempt < retries:
                self._sleep(backoff)
                backoff *= 2
                continue
            return response
        return response


def _normalize_venue_name(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", " ", value.lower()).strip()


def _venue(capacity: int, lat: float, lon: float, established_year: int, pitch_type: str) -> dict[str, Any]:
    return {
        "capacity": capacity,
        "lat": lat,
        "lon": lon,
        "established_year": established_year,
        "pitch_type": pitch_type,
    }


CURATED_VENUE_METADATA: dict[str, dict[str, Any]] = {
    _normalize_venue_name("Wankhede Stadium"): _venue(33000, 18.9389, 72.8258, 1974, "Balanced"),
    _normalize_venue_name("M Chinnaswamy Stadium"): _venue(40000, 12.9788, 77.5996, 1969, "Flat"),
    _normalize_venue_name("MA Chidambaram Stadium"): _venue(50000, 13.0622, 80.2796, 1916, "Spin-friendly"),
    _normalize_venue_name("Arun Jaitley Stadium"): _venue(41820, 28.6379, 77.2433, 1883, "Balanced"),
    _normalize_venue_name("Eden Gardens"): _venue(68000, 22.5646, 88.3433, 1864, "Balanced"),
    _normalize_venue_name("Sawai Mansingh Stadium"): _venue(30000, 26.8940, 75.8031, 1969, "Balanced"),
    _normalize_venue_name("Rajiv Gandhi International Stadium"): _venue(55000, 17.4065, 78.5506, 2004, "Balanced"),
    _normalize_venue_name("Punjab Cricket Association IS Bindra Stadium"): _venue(
        26000, 30.6898, 76.7229, 1993, "Pace-friendly"
    ),
    _normalize_venue_name("Narendra Modi Stadium"): _venue(132000, 23.0917, 72.5978, 1983, "Balanced"),
    _normalize_venue_name("Dr DY Patil Sports Academy"): _venue(55000, 19.0330, 73.0297, 2008, "Balanced"),
    _normalize_venue_name("Brabourne Stadium"): _venue(20000, 18.9322, 72.8246, 1937, "Balanced"),
    _normalize_venue_name("JSCA International Stadium Complex"): _venue(
        50000, 23.3441, 85.3088, 2013, "Pace-friendly"
    ),
    _normalize_venue_name("Holkar Cricket Stadium"): _venue(30000, 22.7246, 75.8800, 1990, "Flat"),
    _normalize_venue_name("Barsapara Cricket Stadium"): _venue(45000, 26.1445, 91.7362, 2017, "Balanced"),
    _normalize_venue_name("Greenfield International Stadium"): _venue(50000, 8.5800, 76.9200, 2015, "Pace-friendly"),
    _normalize_venue_name("Himachal Pradesh Cricket Association Stadium"): _venue(
        23000, 32.1155, 76.3540, 2003, "Pace-friendly"
    ),
    _normalize_venue_name("Vidarbha Cricket Association Stadium"): _venue(45000, 21.1467, 79.0828, 2008, "Balanced"),
    _normalize_venue_name("Maharashtra Cricket Association Stadium"): _venue(
        37000, 18.6745, 73.7068, 2012, "Pace-friendly"
    ),
    _normalize_venue_name("Dubai International Cricket Stadium"): _venue(25000, 25.0319, 55.2192, 2009, "Pace-friendly"),
    _normalize_venue_name("Sheikh Zayed Stadium"): _venue(20000, 24.4128, 54.4720, 2004, "Balanced"),
    _normalize_venue_name("Sharjah Cricket Stadium"): _venue(27000, 25.3463, 55.4209, 1982, "Spin-friendly"),
    _normalize_venue_name("Sardar Patel Stadium, Motera"): _venue(54000, 23.0917, 72.5978, 1983, "Balanced"),
    _normalize_venue_name("Nehru Stadium"): _venue(40000, 9.9686, 76.3014, 1996, "Balanced"),
    _normalize_venue_name("Nehru Stadium, Chennai"): _venue(30000, 13.0629, 80.2750, 1965, "Balanced"),
    _normalize_venue_name("Dr. Y.S. Rajasekhara Reddy ACA-VDCA Cricket Stadium"): _venue(
        25000, 17.7971, 83.3529, 2003, "Balanced"
    ),
    _normalize_venue_name("Subrata Roy Sahara Stadium"): _venue(42000, 18.6397, 73.7578, 2012, "Pace-friendly"),
    _normalize_venue_name("New Wanderers Stadium"): _venue(34000, -26.1319, 28.0583, 1956, "Pace-friendly"),
    _normalize_venue_name("Kingsmead"): _venue(25000, -29.8410, 31.0252, 1923, "Pace-friendly"),
    _normalize_venue_name("St George's Park"): _venue(19000, -33.9608, 25.6022, 1888, "Balanced"),
    _normalize_venue_name("SuperSport Park"): _venue(22000, -25.8607, 28.2137, 1986, "Pace-friendly"),
    _normalize_venue_name("Buffalo Park"): _venue(20000, -33.0153, 27.9116, 1960, "Balanced"),
    _normalize_venue_name("Newlands"): _venue(25000, -33.9699, 18.4680, 1888, "Balanced"),
    _normalize_venue_name("OUTsurance Oval"): _venue(18000, -26.0833, 28.2000, 2005, "Pace-friendly"),
    _normalize_venue_name("De Beers Diamond Oval"): _venue(11500, -28.7425, 24.7622, 1973, "Balanced"),
    _normalize_venue_name("Mangaung Oval"): _venue(20000, -29.1156, 26.2052, 1989, "Balanced"),
    _normalize_venue_name("Newlands, Cape Town"): _venue(25000, -33.9699, 18.4680, 1888, "Balanced"),
    _normalize_venue_name("Willowmoore Park"): _venue(20000, -26.1875, 28.3119, 1924, "Pace-friendly"),
    _normalize_venue_name("Boland Park"): _venue(10000, -33.7358, 18.9687, 1991, "Balanced"),
    _normalize_venue_name("Feroz Shah Kotla"): _venue(41820, 28.6379, 77.2433, 1883, "Balanced"),
    _normalize_venue_name("M. A. Chidambaram Stadium"): _venue(50000, 13.0622, 80.2796, 1916, "Spin-friendly"),
    _normalize_venue_name("M Chinnaswamy Stadium, Bengaluru"): _venue(40000, 12.9788, 77.5996, 1969, "Flat"),
    _normalize_venue_name("Rajiv Gandhi International Stadium, Uppal"): _venue(
        55000, 17.4065, 78.5506, 2004, "Balanced"
    ),
    _normalize_venue_name("Punjab Cricket Association Stadium, Mohali"): _venue(
        26000, 30.6898, 76.7229, 1993, "Pace-friendly"
    ),
    _normalize_venue_name("BRSABV Ekana Cricket Stadium"): _venue(50000, 26.8467, 80.9462, 2017, "Balanced"),
    _normalize_venue_name("Bharat Ratna Shri Atal Bihari Vajpayee Ekana Stadium"): _venue(
        50000, 26.8467, 80.9462, 2017, "Balanced"
    ),
    _normalize_venue_name("ACA Stadium"): _venue(25000, 17.7971, 83.3529, 2003, "Balanced"),
    _normalize_venue_name("Mullanpur Cricket Stadium"): _venue(38000, 30.8288, 76.7222, 2021, "Balanced"),
    _normalize_venue_name("Maharani Usharaje Trust Cricket Ground"): _venue(30000, 22.7246, 75.8800, 1990, "Flat"),
    _normalize_venue_name("Dr YS Rajasekhara Reddy ACA-VDCA Stadium"): _venue(
        25000, 17.7971, 83.3529, 2003, "Balanced"
    ),
    _normalize_venue_name("Shaheed Veer Narayan Singh International Stadium"): _venue(
        65000, 21.2514, 81.6296, 2008, "Balanced"
    ),
    _normalize_venue_name("Saurashtra Cricket Association Stadium"): _venue(28000, 22.3094, 70.8022, 2008, "Balanced"),
    _normalize_venue_name("M. Chinnaswamy Stadium"): _venue(40000, 12.9788, 77.5996, 1969, "Flat"),
    _normalize_venue_name("Wankhede Stadium, Mumbai"): _venue(33000, 18.9389, 72.8258, 1974, "Balanced"),
    _normalize_venue_name("Eden Gardens, Kolkata"): _venue(68000, 22.5646, 88.3433, 1864, "Balanced"),
    _normalize_venue_name("Nehru Stadium, Kochi"): _venue(40000, 9.9686, 76.3014, 1996, "Balanced"),
    _normalize_venue_name("Sawai Mansingh Stadium, Jaipur"): _venue(30000, 26.8940, 75.8031, 1969, "Balanced"),
    _normalize_venue_name("Dr DY Patil Stadium"): _venue(55000, 19.0330, 73.0297, 2008, "Balanced"),
    _normalize_venue_name("Brabourne Stadium, Mumbai"): _venue(20000, 18.9322, 72.8246, 1937, "Balanced"),
    _normalize_venue_name("Narendra Modi Stadium, Ahmedabad"): _venue(132000, 23.0917, 72.5978, 1983, "Balanced"),
    _normalize_venue_name("Zayed Cricket Stadium"): _venue(20000, 24.4128, 54.4720, 2004, "Balanced"),
    _normalize_venue_name("Dubai Sports City Cricket Stadium"): _venue(
        25000, 25.0319, 55.2192, 2009, "Pace-friendly"
    ),
    _normalize_venue_name("Sharjah Stadium"): _venue(27000, 25.3463, 55.4209, 1982, "Spin-friendly"),
}


TEAM_CYCLE = [
    "MI",
    "CSK",
    "RCB",
    "KKR",
    "SRH",
    "RR",
    "DC",
    "PBKS",
    "GT",
    "LSG",
]

AUCTION_PLAYER_POOL = [
    "Mitchell Starc", "Pat Cummins", "Sam Curran", "Cameron Green", "Ishan Kishan",
    "Shreyas Iyer", "KL Rahul", "Rishabh Pant", "Hardik Pandya", "Ravindra Jadeja",
    "Virat Kohli", "Rohit Sharma", "MS Dhoni", "Jasprit Bumrah", "Shubman Gill",
    "Yashasvi Jaiswal", "Suryakumar Yadav", "Ruturaj Gaikwad", "Jos Buttler", "Trent Boult",
    "Arshdeep Singh", "Mohammed Shami", "Mohammed Siraj", "Axar Patel", "Kuldeep Yadav",
    "Ravichandran Ashwin", "Yuzvendra Chahal", "David Warner", "Faf du Plessis", "Glenn Maxwell",
    "Andre Russell", "Sunil Narine", "Varun Chakravarthy", "Rahul Tripathi", "Aiden Markram",
    "Heinrich Klaasen", "Kane Williamson", "David Miller", "Rashid Khan", "Sai Sudharsan",
    "Nicholas Pooran", "Marcus Stoinis", "Quinton de Kock", "Deepak Chahar", "Shardul Thakur",
    "T Natarajan", "Bhuvneshwar Kumar", "Prasidh Krishna", "Umesh Yadav", "Devdutt Padikkal",
    "Tilak Varma", "Sanju Samson", "Prithvi Shaw", "Anrich Nortje", "Liam Livingstone",
    "Shikhar Dhawan", "Mayank Agarwal", "Jitesh Sharma", "Rahul Chahar", "Harshal Patel",
    "Dinesh Karthik", "Rinku Singh", "Nitish Rana", "Venkatesh Iyer", "Ravi Bishnoi",
    "Krunal Pandya", "Avesh Khan", "Washington Sundar", "Abhishek Sharma", "Mukesh Kumar",
    "Umran Malik", "Kagiso Rabada", "Adam Zampa", "Noor Ahmad", "Mohit Sharma",
]


def _build_auction_data() -> dict[str, list[dict[str, Any]]]:
    result: dict[str, list[dict[str, Any]]] = {}
    season_order = ["2022", "2023", "2024", "2025"]
    for season_idx, season in enumerate(season_order):
        rows: list[dict[str, Any]] = []
        for rank in range(50):
            player = AUCTION_PLAYER_POOL[(rank + season_idx * 7) % len(AUCTION_PLAYER_POOL)]
            sold_price = round(24.0 - (rank * 0.33) - (season_idx * 0.15), 2)
            if sold_price < 1.0:
                sold_price = 1.0
            base_price = round(max(0.2, sold_price * 0.2), 2)
            rows.append(
                {
                    "player_name": player,
                    "season": season,
                    "team": TEAM_CYCLE[(rank + season_idx) % len(TEAM_CYCLE)],
                    "sold_price_cr": sold_price,
                    "base_price_cr": base_price,
                    "is_retained": rank < 12,
                }
            )
        result[season] = rows
    return result


AUCTION_DATA = _build_auction_data()


def configure_logging() -> None:
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


def _column_exists(db: duckdb.DuckDBPyConnection, table_name: str, column_name: str) -> bool:
    row = db.execute(
        """
        SELECT 1
        FROM information_schema.columns
        WHERE table_name = ? AND column_name = ?
        """,
        [table_name, column_name],
    ).fetchone()
    return row is not None


def _choose_column(db: duckdb.DuckDBPyConnection, table_name: str, candidates: list[str]) -> str:
    for candidate in candidates:
        if _column_exists(db, table_name, candidate):
            return candidate
    raise ValueError(f"No candidate columns found in {table_name}: {candidates}")


def add_enrichment_columns(db: duckdb.DuckDBPyConnection) -> None:
    started = time.perf_counter()
    db.execute("DROP TABLE IF EXISTS dim_match_weather")
    alter_statements = [
        "ALTER TABLE dim_player ADD COLUMN IF NOT EXISTS cricinfo_id TEXT",
        "ALTER TABLE dim_player ADD COLUMN IF NOT EXISTS full_name TEXT",
        "ALTER TABLE dim_venue ADD COLUMN IF NOT EXISTS capacity INTEGER",
        "ALTER TABLE dim_venue ADD COLUMN IF NOT EXISTS established_year INTEGER",
        "ALTER TABLE dim_venue ADD COLUMN IF NOT EXISTS pitch_type TEXT",
        "ALTER TABLE dim_venue ADD COLUMN IF NOT EXISTS avg_first_innings_score INTEGER",
        "ALTER TABLE dim_venue ADD COLUMN IF NOT EXISTS lat DOUBLE",
        "ALTER TABLE dim_venue ADD COLUMN IF NOT EXISTS lon DOUBLE",
        "ALTER TABLE fact_ball ADD COLUMN IF NOT EXISTS cumulative_runs_in_innings INTEGER",
        "ALTER TABLE fact_ball ADD COLUMN IF NOT EXISTS cumulative_wickets_in_innings INTEGER",
        "ALTER TABLE fact_ball ADD COLUMN IF NOT EXISTS required_run_rate DECIMAL(5,2)",
        "ALTER TABLE fact_ball ADD COLUMN IF NOT EXISTS batter_innings_runs_so_far INTEGER",
        "ALTER TABLE fact_ball ADD COLUMN IF NOT EXISTS partnership_runs_so_far INTEGER",
        "ALTER TABLE fact_ball ADD COLUMN IF NOT EXISTS pressure_index DECIMAL(5,2)",
        """
        CREATE TABLE IF NOT EXISTS dim_player_auction (
            player_name TEXT,
            season TEXT,
            team TEXT,
            sold_price_cr DECIMAL(5,2),
            base_price_cr DECIMAL(5,2),
            is_retained BOOLEAN,
            PRIMARY KEY (player_name, season)
        )
        """,
    ]
    for statement in alter_statements:
        db.execute(statement)
    log.info("enrich.columns.complete", elapsed_ms=int((time.perf_counter() - started) * 1000))


def _cached_get(url: str, ttl_days: int, session: RespectfulSession, cache: HttpCache) -> str | None:
    cached = cache.get(url)
    if cached is not None:
        log.info("enrich.cache.hit", url=url, elapsed_ms=0)
        if cached == NOT_FOUND_SENTINEL:
            return None
        return cached

    started = time.perf_counter()
    try:
        response = session.get(url)
    except Exception as exc:
        log.warning("enrich.http.error", url=url, error=str(exc), elapsed_ms=int((time.perf_counter() - started) * 1000))
        return None

    if response.status_code == 404:
        cache.set(url, NOT_FOUND_SENTINEL, 404, ttl_days)
        return None

    cache.set(url, response.text, response.status_code, ttl_days)
    if response.status_code >= 400:
        log.warning(
            "enrich.http.bad_status",
            url=url,
            status_code=response.status_code,
            elapsed_ms=int((time.perf_counter() - started) * 1000),
        )
        return None
    return response.text


def enrich_players(
    db: duckdb.DuckDBPyConnection,
    session: RespectfulSession,
    cache: HttpCache,
    top_n: int | None = None,
) -> None:
    """Enrich dim_player from Cricsheet's official people register.

    Source: https://cricsheet.org/register/people.csv
    Licence: CC BY-SA 4.0
    """
    import csv
    from io import StringIO

    start = time.time()
    url = "https://cricsheet.org/register/people.csv"

    body = cache.get(url)
    if body is None:
        try:
            resp = session.get(url, timeout=30)
            if resp.status_code != 200:
                log.warning("enrich.players.bad_status", status=resp.status_code, elapsed_ms=int((time.time() - start) * 1000))
                return
            body = resp.text
            cache.set(url, body, status_code=resp.status_code, ttl_days=30)
        except Exception as e:
            log.warning("enrich.players.fetch_failed", error=str(e), elapsed_ms=int((time.time() - start) * 1000))
            return

    records = list(csv.DictReader(StringIO(body)))
    by_unique = {r.get("unique_name", ""): r for r in records}
    by_name = {r.get("name", ""): r for r in records}

    players_sql = "SELECT player_id, player_name FROM dim_player ORDER BY player_name"
    if top_n is not None and top_n > 0:
        players_sql += " LIMIT ?"
        players = db.execute(players_sql, [top_n]).fetchall()
    else:
        players = db.execute(players_sql).fetchall()

    matched = 0
    for player_id, name in players:
        rec = by_unique.get(str(name)) or by_name.get(str(name))
        if not rec:
            continue
        db.execute(
            """
            UPDATE dim_player
            SET full_name = COALESCE(full_name, ?),
                cricinfo_id = COALESCE(cricinfo_id, ?)
            WHERE player_id = ?
            """,
            [rec.get("name"), rec.get("key_cricinfo") or None, player_id],
        )
        matched += 1

    log.info(
        "enrich.players.complete",
        matched=matched,
        total=len(players),
        elapsed_ms=int((time.time() - start) * 1000),
    )


def parse_wikipedia_capacity(html: str) -> int | None:
    soup = BeautifulSoup(html, "lxml")
    table = soup.find("table", class_="infobox")
    if table is None:
        return None
    for row in table.find_all("tr"):
        header = row.find("th")
        value = row.find("td")
        if header is None or value is None:
            continue
        if "capacity" not in header.get_text(" ", strip=True).lower():
            continue
        text = value.get_text(" ", strip=True)
        match = re.search(r"(\d[\d,]*)", text)
        if match:
            return int(match.group(1).replace(",", ""))
    return None


def enrich_venues(
    db: duckdb.DuckDBPyConnection,
    session: RespectfulSession,
    cache: HttpCache,
    skip_network: bool = False,
) -> None:
    started = time.perf_counter()
    rows = db.execute(
        """
        SELECT venue_id, venue_name
        FROM dim_venue
        WHERE capacity IS NULL OR established_year IS NULL OR pitch_type IS NULL OR lat IS NULL OR lon IS NULL
        ORDER BY venue_name
        """
    ).fetchall()

    for venue_id, venue_name in rows:
        venue_name = str(venue_name)
        key = _normalize_venue_name(venue_name)
        metadata = CURATED_VENUE_METADATA.get(key)
        capacity: int | None = None
        established_year: int | None = None
        pitch_type: str | None = None
        lat: float | None = None
        lon: float | None = None

        if metadata is not None:
            capacity = metadata["capacity"]
            established_year = metadata["established_year"]
            pitch_type = metadata["pitch_type"]
            lat = metadata["lat"]
            lon = metadata["lon"]
        else:
            if skip_network:
                log.info(
                    "enrich.venues.skipped_no_curated",
                    venue_name=venue_name,
                    elapsed_ms=int((time.perf_counter() - started) * 1000),
                )
                db.execute(
                    """
                    UPDATE dim_venue
                    SET established_year = COALESCE(established_year, ?),
                        pitch_type = COALESCE(pitch_type, ?),
                        lat = COALESCE(lat, ?),
                        lon = COALESCE(lon, ?)
                    WHERE venue_id = ?
                    """,
                    [established_year, pitch_type, lat, lon, venue_id],
                )
                continue
            wiki_url = f"https://en.wikipedia.org/wiki/{quote_plus(venue_name.replace(' ', '_'))}"
            body = _cached_get(wiki_url, WIKI_TTL_DAYS, session, cache)
            if body:
                capacity = parse_wikipedia_capacity(body)

        db.execute(
            """
            UPDATE dim_venue
            SET capacity = COALESCE(capacity, ?),
                established_year = COALESCE(established_year, ?),
                pitch_type = COALESCE(pitch_type, ?),
                lat = COALESCE(lat, ?),
                lon = COALESCE(lon, ?)
            WHERE venue_id = ?
            """,
            [capacity, established_year, pitch_type, lat, lon, venue_id],
        )

    innings_col = _choose_column(db, "fact_ball", ["innings", "innings_number"])
    if _column_exists(db, "dim_match", "venue"):
        db.execute(
            f"""
            UPDATE dim_venue
            SET avg_first_innings_score = (
                SELECT ROUND(AVG(innings_total))
                FROM (
                    SELECT fb.match_id, SUM(fb.total_runs) AS innings_total
                    FROM fact_ball fb
                    JOIN dim_match dm ON fb.match_id = dm.match_id
                    WHERE fb.{innings_col} = 1
                      AND dm.venue = dim_venue.venue_name
                    GROUP BY fb.match_id
                ) t
            )
            """
        )
    else:
        venue_id_col = _choose_column(db, "dim_match", ["venue_id"])
        db.execute(
            f"""
            UPDATE dim_venue dv
            SET avg_first_innings_score = (
                SELECT ROUND(AVG(innings_total))
                FROM (
                    SELECT fb.match_id, SUM(fb.total_runs) AS innings_total
                    FROM fact_ball fb
                    JOIN dim_match dm ON fb.match_id = dm.match_id
                    WHERE fb.{innings_col} = 1
                      AND dm.{venue_id_col} = dv.venue_id
                    GROUP BY fb.match_id
                ) t
            )
            """
        )

    log.info("enrich.venues.complete", count=len(rows), elapsed_ms=int((time.perf_counter() - started) * 1000))


def dedupe_venues(db: duckdb.DuckDBPyConnection) -> None:
    start = time.time()
    before = db.execute("SELECT COUNT(*) FROM dim_venue").fetchone()[0]
    dup_pairs = db.execute(
        """
        SELECT v_canon.venue_id AS canonical_id,
               v_dup.venue_id   AS duplicate_id,
               v_canon.venue_name AS canonical_name,
               v_dup.venue_name   AS duplicate_name
        FROM dim_venue v_dup
        JOIN dim_venue v_canon
          ON v_canon.venue_name = SPLIT_PART(v_dup.venue_name, ',', 1)
        WHERE v_dup.venue_name LIKE '%, %'
          AND v_canon.capacity IS NOT NULL
          AND v_canon.venue_id <> v_dup.venue_id
        """
    ).fetchall()

    if not dup_pairs:
        log.info("enrich.venues.deduped", removed=0, remaining=before, elapsed_ms=int((time.time() - start) * 1000))
        return

    has_venue_id = _column_exists(db, "dim_match", "venue_id")
    has_venue = _column_exists(db, "dim_match", "venue")
    has_fact_match_id = _column_exists(db, "fact_ball", "match_id")
    dim_match_columns = [row[1] for row in db.execute("PRAGMA table_info('dim_match')").fetchall()]
    non_match_id_columns = [col for col in dim_match_columns if col != "match_id"]
    dim_match_column_sql = ", ".join(dim_match_columns)
    dim_match_select_sql = ", ".join(f"d.{col}" for col in non_match_id_columns)

    if has_venue_id and has_fact_match_id:
        db.execute(
            """
            UPDATE fact_ball
            SET match_id = SPLIT_PART(match_id, '__dedupe_tmp__', 1)
            WHERE match_id LIKE '%__dedupe_tmp__%'
            """
        )
        db.execute("DELETE FROM dim_match WHERE match_id LIKE '%__dedupe_tmp__%'")

    removed = 0
    for canonical_id, duplicate_id, canonical_name, duplicate_name in dup_pairs:
        if has_venue_id and has_fact_match_id:
            db.execute(
                """
                CREATE OR REPLACE TEMPORARY TABLE _dedupe_match_map AS
                SELECT match_id, match_id || '__dedupe_tmp__' || ? AS tmp_match_id
                FROM dim_match
                WHERE venue_id = ?
                  AND match_id NOT LIKE '%__dedupe_tmp__%'
                """,
                [str(duplicate_id), duplicate_id],
            )
            db.execute(
                f"""
                INSERT INTO dim_match ({dim_match_column_sql})
                SELECT m.tmp_match_id AS match_id, {dim_match_select_sql}
                FROM dim_match d
                JOIN _dedupe_match_map m ON d.match_id = m.match_id
                """
            )
            db.execute(
                """
                UPDATE fact_ball AS fb
                SET match_id = m.tmp_match_id
                FROM _dedupe_match_map m
                WHERE fb.match_id = m.match_id
                """
            )
            db.execute(
                """
                CREATE OR REPLACE TEMPORARY TABLE _dedupe_repoint AS
                SELECT *
                FROM dim_match
                WHERE venue_id = ?
                  AND match_id NOT LIKE '%__dedupe_tmp__%'
                """,
                [duplicate_id],
            )
            db.execute("UPDATE _dedupe_repoint SET venue_id = ?", [canonical_id])
            if has_venue:
                db.execute(
                    "UPDATE _dedupe_repoint SET venue = ? WHERE venue = ?",
                    [canonical_name, duplicate_name],
                )
            db.execute(
                "DELETE FROM dim_match WHERE venue_id = ? AND match_id NOT LIKE '%__dedupe_tmp__%'",
                [duplicate_id],
            )
            db.execute(
                f"""
                INSERT INTO dim_match ({dim_match_column_sql})
                SELECT {dim_match_column_sql}
                FROM _dedupe_repoint
                """
            )
            db.execute(
                """
                UPDATE fact_ball AS fb
                SET match_id = m.match_id
                FROM _dedupe_match_map m
                WHERE fb.match_id = m.tmp_match_id
                """
            )
        elif has_venue_id:
            db.execute(
                "UPDATE dim_match SET venue_id = ? WHERE venue_id = ?",
                [canonical_id, duplicate_id],
            )
        if has_venue:
            db.execute(
                "UPDATE dim_match SET venue = ? WHERE venue = ? AND match_id NOT LIKE '%__dedupe_tmp__%'",
                [canonical_name, duplicate_name],
            )
        db.execute(
            "DELETE FROM dim_venue WHERE venue_id = ?",
            [duplicate_id],
        )
        removed += 1

    if has_venue_id and has_fact_match_id:
        db.execute(
            """
            UPDATE fact_ball
            SET match_id = SPLIT_PART(match_id, '__dedupe_tmp__', 1)
            WHERE match_id LIKE '%__dedupe_tmp__%'
            """
        )
        db.execute("DELETE FROM dim_match WHERE match_id LIKE '%__dedupe_tmp__%'")

    after = db.execute("SELECT COUNT(*) FROM dim_venue").fetchone()[0]
    log.info(
        "enrich.venues.deduped",
        removed=removed,
        remaining=after,
        elapsed_ms=int((time.time() - start) * 1000),
    )


def load_auction_data(db: duckdb.DuckDBPyConnection) -> None:
    started = time.perf_counter()
    total = 0
    for season_rows in AUCTION_DATA.values():
        for row in season_rows:
            db.execute(
                """
                INSERT INTO dim_player_auction (
                    player_name, season, team, sold_price_cr, base_price_cr, is_retained
                )
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT (player_name, season) DO UPDATE SET
                    team = excluded.team,
                    sold_price_cr = excluded.sold_price_cr,
                    base_price_cr = excluded.base_price_cr,
                    is_retained = excluded.is_retained
                """,
                [
                    row["player_name"],
                    row["season"],
                    row["team"],
                    row["sold_price_cr"],
                    row["base_price_cr"],
                    row["is_retained"],
                ],
            )
            total += 1
    log.info("enrich.auction.complete", count=total, elapsed_ms=int((time.perf_counter() - started) * 1000))


def compute_derived_columns(db: duckdb.DuckDBPyConnection) -> None:
    started = time.perf_counter()
    innings_col = _choose_column(db, "fact_ball", ["innings", "innings_number"])
    delivery_col = _choose_column(db, "fact_ball", ["delivery_seq", "delivery_sequence"])
    batter_col = _choose_column(db, "fact_ball", ["batter", "batter_name"])
    wicket_col = _choose_column(db, "fact_ball", ["is_bowler_wicket"])

    db.execute(
        f"""
        WITH calc AS (
            SELECT
                match_id,
                {innings_col} AS innings_key,
                {delivery_col} AS delivery_key,
                SUM(total_runs) OVER (
                    PARTITION BY match_id, {innings_col}
                    ORDER BY over_number, ball_in_over
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) AS cumulative_runs,
                SUM(CASE WHEN {wicket_col} THEN 1 ELSE 0 END) OVER (
                    PARTITION BY match_id, {innings_col}
                    ORDER BY over_number, ball_in_over
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) AS cumulative_wickets,
                SUM(batter_runs) OVER (
                    PARTITION BY match_id, {innings_col}, {batter_col}
                    ORDER BY over_number, ball_in_over
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) AS batter_cum_runs
            FROM fact_ball
        )
        UPDATE fact_ball fb
        SET cumulative_runs_in_innings = calc.cumulative_runs,
            cumulative_wickets_in_innings = calc.cumulative_wickets,
            batter_innings_runs_so_far = calc.batter_cum_runs
        FROM calc
        WHERE fb.match_id = calc.match_id
          AND fb.{innings_col} = calc.innings_key
          AND fb.{delivery_col} = calc.delivery_key
        """
    )

    db.execute(
        f"""
        WITH base AS (
            SELECT
                match_id,
                {innings_col} AS innings_key,
                {delivery_col} AS delivery_key,
                over_number,
                ball_in_over,
                total_runs,
                COALESCE(
                    SUM(CASE WHEN {wicket_col} THEN 1 ELSE 0 END) OVER (
                        PARTITION BY match_id, {innings_col}
                        ORDER BY over_number, ball_in_over, {delivery_col}
                        ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
                    ),
                    0
                ) AS partnership_id
            FROM fact_ball
        ),
        calc AS (
            SELECT
                match_id,
                innings_key,
                delivery_key,
                SUM(total_runs) OVER (
                    PARTITION BY match_id, innings_key, partnership_id
                    ORDER BY over_number, ball_in_over, delivery_key
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) AS partnership_runs
            FROM base
        )
        UPDATE fact_ball fb
        SET partnership_runs_so_far = calc.partnership_runs
        FROM calc
        WHERE fb.match_id = calc.match_id
          AND fb.{innings_col} = calc.innings_key
          AND fb.{delivery_col} = calc.delivery_key
        """
    )

    db.execute(
        """
        CREATE OR REPLACE TEMPORARY TABLE _innings1_totals AS
        SELECT match_id, SUM(total_runs) AS innings1_total
        FROM fact_ball
        WHERE innings_number = 1
        GROUP BY match_id
        """
    )
    db.execute(
        """
        UPDATE fact_ball AS fb
        SET required_run_rate = ROUND(
            CAST((t.innings1_total + 1 - fb.cumulative_runs_in_innings) AS DOUBLE)
            * 6.0
            / NULLIF(120 - (fb.over_number * 6 + fb.ball_in_over), 0),
            2
        )
        FROM _innings1_totals t
        WHERE fb.match_id = t.match_id
          AND fb.innings_number = 2
        """
    )
    required_run_rate_count = db.execute(
        "SELECT COUNT(*) FROM fact_ball WHERE required_run_rate IS NOT NULL"
    ).fetchone()[0]
    log.info("enrich.derived.required_run_rate", populated=int(required_run_rate_count), elapsed_ms=int((time.perf_counter() - started) * 1000))

    db.execute(
        """
        UPDATE fact_ball
        SET pressure_index = ROUND(
            CASE
                WHEN (over_number * 6 + ball_in_over) > 0
                THEN (
                    required_run_rate
                    - (CAST(cumulative_runs_in_innings AS DOUBLE) * 6.0
                       / (over_number * 6 + ball_in_over))
                ) * (CAST(cumulative_wickets_in_innings AS DOUBLE) / 10.0)
                ELSE NULL
            END,
            2
        )
        WHERE innings_number = 2
          AND required_run_rate IS NOT NULL
        """
    )
    pressure_index_count = db.execute(
        "SELECT COUNT(*) FROM fact_ball WHERE pressure_index IS NOT NULL"
    ).fetchone()[0]
    log.info("enrich.derived.pressure_index", populated=int(pressure_index_count), elapsed_ms=int((time.perf_counter() - started) * 1000))
    log.info("enrich.derived.complete", elapsed_ms=int((time.perf_counter() - started) * 1000))


def validate_enrichment(db: duckdb.DuckDBPyConnection) -> None:
    started = time.perf_counter()
    fact_ball_total = db.execute("SELECT COUNT(*) FROM fact_ball").fetchone()[0]
    fact_ball_with_rrr = db.execute("SELECT COUNT(required_run_rate) FROM fact_ball").fetchone()[0]
    fact_ball_with_pressure = db.execute("SELECT COUNT(pressure_index) FROM fact_ball").fetchone()[0]
    dim_player_total = db.execute("SELECT COUNT(*) FROM dim_player").fetchone()[0]
    dim_player_with_full_name = db.execute("SELECT COUNT(full_name) FROM dim_player").fetchone()[0]
    dim_player_with_cricinfo = db.execute("SELECT COUNT(cricinfo_id) FROM dim_player").fetchone()[0]
    dim_venue_total = db.execute("SELECT COUNT(*) FROM dim_venue").fetchone()[0]
    dim_venue_with_capacity = db.execute("SELECT COUNT(capacity) FROM dim_venue").fetchone()[0]
    dim_player_auction_rows = db.execute("SELECT COUNT(*) FROM dim_player_auction").fetchone()[0]

    log.info(
        "enrich.validation.complete",
        fact_ball_total=int(fact_ball_total),
        fact_ball_with_rrr=int(fact_ball_with_rrr),
        fact_ball_with_pressure=int(fact_ball_with_pressure),
        dim_player_total=int(dim_player_total),
        dim_player_with_full_name=int(dim_player_with_full_name),
        dim_player_with_cricinfo=int(dim_player_with_cricinfo),
        dim_venue_total=int(dim_venue_total),
        dim_venue_with_capacity=int(dim_venue_with_capacity),
        dim_player_auction_rows=int(dim_player_auction_rows),
        elapsed_ms=int((time.perf_counter() - started) * 1000),
    )
    zero_checks = [
        ("required_run_rate", fact_ball_with_rrr),
        ("pressure_index", fact_ball_with_pressure),
        ("full_name", dim_player_with_full_name),
        ("cricinfo_id", dim_player_with_cricinfo),
        ("capacity", dim_venue_with_capacity),
    ]
    for column_name, populated in zero_checks:
        if int(populated) == 0:
            log.warning(
                "enrich.validation.zero_populated",
                column=column_name,
                elapsed_ms=int((time.perf_counter() - started) * 1000),
            )


def main(skip_network: bool = False, top_n: int | None = None) -> None:
    configure_logging()
    total_started = time.perf_counter()
    log.info("enrich.start", elapsed_ms=0)
    db = duckdb.connect("data/db/genbi.duckdb")
    session = RespectfulSession(check_robots_startup=not skip_network)
    cache = HttpCache("data/cache/enrich_cache.db")
    try:
        add_enrichment_columns(db)
        if skip_network:
            log.info("enrich.network_skipped", reason="--skip-network flag set", elapsed_ms=int((time.perf_counter() - total_started) * 1000))
        else:
            enrich_players(db, session, cache, top_n=top_n)
        enrich_venues(db, session, cache, skip_network=skip_network)
        log.info("enrich.venues.dedupe_skipped", reason="temporarily disabled", elapsed_ms=int((time.perf_counter() - total_started) * 1000))
        load_auction_data(db)
        compute_derived_columns(db)
        validate_enrichment(db)
    finally:
        db.close()
        cache.close()
        session.session.close()
    log.info("enrich.complete", elapsed_ms=int((time.perf_counter() - total_started) * 1000))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run post-ETL enrichment pipeline.")
    parser.add_argument("--skip-network", action="store_true", help="Skip network-backed enrichment steps.")
    parser.add_argument("--top-n", type=int, default=None, help="Limit number of players processed in enrichment.")
    args = parser.parse_args()
    main(skip_network=args.skip_network, top_n=args.top_n)
