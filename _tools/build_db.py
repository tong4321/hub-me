#!/usr/bin/env python3
"""
IPTV Aggregator DB Builder (Split into 3 DBs: live, movies, series)

This script generates 3 separate database files to avoid GitHub quota limits:
- iptv_live.db: Live TV channels
- iptv_movies.db: Movies
- iptv_series.db: TV Series

Highlights:
- Incremental DB (no delete/recreate daily)
- Separate Title tables (global dedupe) + Stream tables (availability per server)
- Duplicate checks:
  - DB-level: UNIQUE(dedupe_key) + UPSERT
  - Python-level: seen sets to avoid repeated inserts in a run
- SQLite tuning PRAGMAs + per-server commit
- Optional skip live channels (default ON)
- Optional only keep TMDB-matched titles (default OFF)
- Truncate plot to keep DB size small

Env vars:
  MAX_WORKERS=20
  SKIP_LIVE=1
  TMDB_PAGES=5
  PLOT_MAXLEN=200
  ONLY_TMDB_MATCHED=0
  REBUILD_DB=0  (set to 1 to delete DB and recreate fresh; useful once after schema changes)
"""

import os
import sqlite3
import requests
import re
from urllib.parse import urlparse, parse_qs
import concurrent.futures
from typing import Optional, Tuple

# ---------------- CONFIG ----------------
XC_FILE_URL = "https://raw.githubusercontent.com/tong4321/hub-me/refs/heads/main/_tools/xc.txt"
TMDB_API_KEY = "304ca56b1b7b57ca7a47d9b59946be94"

DB_FILE_LIVE = "iptv_live.db"
DB_FILE_MOVIES = "iptv_movies.db"
DB_FILE_SERIES = "iptv_series.db"
TIMEOUT = 10

MAX_WORKERS = int(os.getenv("MAX_WORKERS", "20"))
SKIP_LIVE = os.getenv("SKIP_LIVE", "1") == "1"
TMDB_PAGES = int(os.getenv("TMDB_PAGES", "5"))
PLOT_MAXLEN = int(os.getenv("PLOT_MAXLEN", "200"))
ONLY_TMDB_MATCHED = os.getenv("ONLY_TMDB_MATCHED", "0") == "1"
REBUILD_DB = os.getenv("REBUILD_DB", "0") == "1"

HEADERS = {"User-Agent": "VLC/3.0.20 (Windows; x86_64)"}

TMDB_CACHE = {"movies": {}, "series": {}}


# ---------------- UTILS ----------------
def normalize_name(name: str) -> str:
    if not name:
        return ""
    normalized = name.lower()
    normalized = re.sub(r"[^\w\s]", " ", normalized)
    normalized = re.sub(r"\s+", " ", normalized).strip()
    return normalized


def safe_trunc(s: Optional[str], maxlen: int) -> Optional[str]:
    if not s:
        return None
    if maxlen <= 0:
        return None
    return s if len(s) <= maxlen else s[:maxlen]


def safe_year(date_str: Optional[str]) -> Optional[int]:
    """Extract year from strings like YYYY-MM-DD / YYYY."""
    if not date_str:
        return None
    m = re.search(r"\b(19\d{2}|20\d{2}|2100)\b", str(date_str))
    if not m:
        return None
    try:
        return int(m.group(1))
    except Exception:
        return None


def fetch_json(url: str, timeout: int = TIMEOUT):
    r = requests.get(url, headers=HEADERS, timeout=timeout)
    r.raise_for_status()
    return r.json()


def fetch_list(url: str) -> list:
    try:
        data = fetch_json(url)
        return data if isinstance(data, list) else []
    except Exception:
        return []


def make_dedupe_key(tmdb_id: Optional[int], name_norm: str, year: Optional[int]) -> str:
    """
    A single UNIQUE target for dedupe:
      - tmdb present:  tmdb:<id>
      - fallback:      fb:<name_norm>:<year_or_0>
    """
    if tmdb_id is not None:
        return f"tmdb:{int(tmdb_id)}"
    y = int(year) if year is not None else 0
    return f"fb:{name_norm}:{y}"


# ---------------- DB ----------------
def connect_db_live():
    """Connect to live TV database"""
    if REBUILD_DB and os.path.exists(DB_FILE_LIVE):
        os.remove(DB_FILE_LIVE)

    conn = sqlite3.connect(DB_FILE_LIVE)
    cur = conn.cursor()

    cur.execute("PRAGMA journal_mode=WAL;")
    cur.execute("PRAGMA synchronous=NORMAL;")
    cur.execute("PRAGMA temp_store=MEMORY;")
    cur.execute("PRAGMA cache_size=-200000;")
    cur.execute("PRAGMA foreign_keys=ON;")

    # Servers
    cur.execute("""
        CREATE TABLE IF NOT EXISTS servers (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            server_url TEXT NOT NULL,
            username TEXT NOT NULL,
            password TEXT NOT NULL,
            server_name TEXT,
            status TEXT,
            active_cons INTEGER,
            max_connections INTEGER,
            last_checked TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(server_url, username)
        )
    """)

    # Categories (per server)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS categories (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            category_id INTEGER NOT NULL,
            server_id INTEGER NOT NULL,
            category_name TEXT NOT NULL,
            content_type TEXT NOT NULL,
            updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(category_id, server_id, content_type)
        )
    """)

    # Live channels
    cur.execute("""
        CREATE TABLE IF NOT EXISTS live_channels (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            server_id INTEGER NOT NULL,
            stream_id INTEGER NOT NULL,
            name TEXT NOT NULL,
            category_id TEXT,
            stream_icon TEXT,
            epg_channel_id TEXT,
            updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(server_id, stream_id)
        )
    """)

    # Indexes
    cur.execute("CREATE INDEX IF NOT EXISTS ix_live_channels_name ON live_channels(name);")

    conn.commit()
    return conn


def connect_db_movies():
    """Connect to movies database"""
    if REBUILD_DB and os.path.exists(DB_FILE_MOVIES):
        os.remove(DB_FILE_MOVIES)

    conn = sqlite3.connect(DB_FILE_MOVIES)
    cur = conn.cursor()

    cur.execute("PRAGMA journal_mode=WAL;")
    cur.execute("PRAGMA synchronous=NORMAL;")
    cur.execute("PRAGMA temp_store=MEMORY;")
    cur.execute("PRAGMA cache_size=-200000;")
    cur.execute("PRAGMA foreign_keys=ON;")

    # Servers
    cur.execute("""
        CREATE TABLE IF NOT EXISTS servers (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            server_url TEXT NOT NULL,
            username TEXT NOT NULL,
            password TEXT NOT NULL,
            server_name TEXT,
            status TEXT,
            active_cons INTEGER,
            max_connections INTEGER,
            last_checked TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(server_url, username)
        )
    """)

    # Categories (per server)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS categories (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            category_id INTEGER NOT NULL,
            server_id INTEGER NOT NULL,
            category_name TEXT NOT NULL,
            content_type TEXT NOT NULL,
            updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(category_id, server_id, content_type)
        )
    """)

    # Movie Titles (global dedupe by dedupe_key)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS movie_titles (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            dedupe_key TEXT NOT NULL,
            tmdb_id INTEGER,
            name TEXT NOT NULL,
            name_normalized TEXT NOT NULL,
            year INTEGER,
            plot TEXT,
            rating REAL,
            popularity REAL,
            vote_count INTEGER,
            release_date TEXT,
            updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(dedupe_key)
        )
    """)

    # Movie Streams (availability per server)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS movie_streams (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            server_id INTEGER NOT NULL,
            stream_id INTEGER NOT NULL,
            title_id INTEGER NOT NULL,
            stream_icon TEXT,
            container_extension TEXT,
            updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(server_id, stream_id),
            FOREIGN KEY(title_id) REFERENCES movie_titles(id) ON DELETE CASCADE
        )
    """)

    # Indexes
    cur.execute("CREATE INDEX IF NOT EXISTS ix_movie_titles_name_norm ON movie_titles(name_normalized);")
    cur.execute("CREATE INDEX IF NOT EXISTS ix_movie_streams_title_id ON movie_streams(title_id);")

    conn.commit()
    return conn


def connect_db_series():
    """Connect to series database"""
    if REBUILD_DB and os.path.exists(DB_FILE_SERIES):
        os.remove(DB_FILE_SERIES)

    conn = sqlite3.connect(DB_FILE_SERIES)
    cur = conn.cursor()

    cur.execute("PRAGMA journal_mode=WAL;")
    cur.execute("PRAGMA synchronous=NORMAL;")
    cur.execute("PRAGMA temp_store=MEMORY;")
    cur.execute("PRAGMA cache_size=-200000;")
    cur.execute("PRAGMA foreign_keys=ON;")

    # Servers
    cur.execute("""
        CREATE TABLE IF NOT EXISTS servers (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            server_url TEXT NOT NULL,
            username TEXT NOT NULL,
            password TEXT NOT NULL,
            server_name TEXT,
            status TEXT,
            active_cons INTEGER,
            max_connections INTEGER,
            last_checked TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(server_url, username)
        )
    """)

    # Categories (per server)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS categories (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            category_id INTEGER NOT NULL,
            server_id INTEGER NOT NULL,
            category_name TEXT NOT NULL,
            content_type TEXT NOT NULL,
            updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(category_id, server_id, content_type)
        )
    """)

    # Series Titles (global dedupe by dedupe_key)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS series_titles (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            dedupe_key TEXT NOT NULL,
            tmdb_id INTEGER,
            name TEXT NOT NULL,
            name_normalized TEXT NOT NULL,
            year INTEGER,
            plot TEXT,
            rating REAL,
            popularity REAL,
            vote_count INTEGER,
            first_air_date TEXT,
            updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(dedupe_key)
        )
    """)

    # Series Streams (availability per server)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS series_streams (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            server_id INTEGER NOT NULL,
            series_id INTEGER NOT NULL,
            title_id INTEGER NOT NULL,
            cover TEXT,
            updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(server_id, series_id),
            FOREIGN KEY(title_id) REFERENCES series_titles(id) ON DELETE CASCADE
        )
    """)

    # Indexes
    cur.execute("CREATE INDEX IF NOT EXISTS ix_series_titles_name_norm ON series_titles(name_normalized);")
    cur.execute("CREATE INDEX IF NOT EXISTS ix_series_streams_title_id ON series_streams(title_id);")

    conn.commit()
    return conn


# ---------------- INPUT SERVERS ----------------
def parse_servers_from_url():
    print(f"Fetching servers from {XC_FILE_URL}...")
    resp = requests.get(XC_FILE_URL, timeout=TIMEOUT)
    resp.raise_for_status()

    servers = []
    for line in resp.text.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if "username=" in line and "password=" in line:
            try:
                parsed = urlparse(line)
                q = parse_qs(parsed.query)
                user = q.get("username", [""])[0]
                pw = q.get("password", [""])[0]
                if user and pw:
                    server_url = f"{parsed.scheme}://{parsed.netloc}"
                    servers.append({"url": server_url, "user": user, "pass": pw})
            except Exception:
                pass

    unique = {f"{s['url']}_{s['user']}": s for s in servers}.values()
    unique_list = list(unique)
    print(f"Found {len(servers)} potential servers. Unique: {len(unique_list)}")
    return unique_list


# ---------------- TMDB CACHE ----------------
def pre_fetch_tmdb_popular(pages: int = TMDB_PAGES):
    if not TMDB_API_KEY:
        print("TMDB key missing; skipping TMDB cache.")
        return

    print(f"Pre-fetching TMDB popular lists (pages={pages})...")
    for page in range(1, pages + 1):
        try:
            m = fetch_json(f"https://api.themoviedb.org/3/movie/popular?api_key={TMDB_API_KEY}&page={page}")
            for item in m.get("results", []):
                norm = normalize_name(item.get("title", ""))
                if norm:
                    TMDB_CACHE["movies"][norm] = item
        except Exception:
            pass

        try:
            s = fetch_json(f"https://api.themoviedb.org/3/tv/popular?api_key={TMDB_API_KEY}&page={page}")
            for item in s.get("results", []):
                norm = normalize_name(item.get("name", ""))
                if norm:
                    TMDB_CACHE["series"][norm] = item
        except Exception:
            pass

    print(f"TMDB cache: movies={len(TMDB_CACHE['movies'])} series={len(TMDB_CACHE['series'])}")


# ---------------- WORKER (NETWORK) ----------------
def validate_and_fetch_server(server_data):
    url = server_data["url"]
    user = server_data["user"]
    pw = server_data["pass"]
    base = f"{url}/player_api.php?username={user}&password={pw}"

    try:
        auth = fetch_json(base)
        if "user_info" not in auth or auth["user_info"].get("status") != "Active":
            return {"status": "invalid", "server": server_data}

        user_info = auth["user_info"]

        cats_live = [] if SKIP_LIVE else fetch_list(f"{base}&action=get_live_categories")
        cats_vod = fetch_list(f"{base}&action=get_vod_categories")
        cats_series = fetch_list(f"{base}&action=get_series_categories")

        live = [] if SKIP_LIVE else fetch_list(f"{base}&action=get_live_streams")
        vods = fetch_list(f"{base}&action=get_vod_streams")
        series = fetch_list(f"{base}&action=get_series")

        return {
            "status": "success",
            "server": server_data,
            "info": user_info,
            "categories": {"live": cats_live, "movie": cats_vod, "series": cats_series},
            "streams": {"live": live, "movie": vods, "series": series},
        }
    except Exception as e:
        return {"status": "failed", "server": server_data, "error": str(e)}


# ---------------- UPSERT HELPERS ----------------
def upsert_server(cur, s_data, info) -> Tuple[int, str]:
    server_name = urlparse(s_data["url"]).netloc
    cur.execute(
        """
        INSERT INTO servers (server_url, username, password, server_name, status, active_cons, max_connections, last_checked)
        VALUES (?, ?, ?, ?, 'active', ?, ?, datetime('now'))
        ON CONFLICT(server_url, username) DO UPDATE SET
            password=excluded.password,
            server_name=excluded.server_name,
            status=excluded.status,
            active_cons=excluded.active_cons,
            max_connections=excluded.max_connections,
            last_checked=excluded.last_checked
        """,
        (
            s_data["url"],
            s_data["user"],
            s_data["pass"],
            server_name,
            info.get("active_cons", 0),
            info.get("max_connections", 0),
        ),
    )
    cur.execute("SELECT id FROM servers WHERE server_url=? AND username=?", (s_data["url"], s_data["user"]))
    server_id = cur.fetchone()[0]
    return server_id, server_name


def upsert_movie_title(
    cur,
    *,
    tmdb_id: Optional[int],
    name: str,
    name_norm: str,
    year: Optional[int],
    plot: Optional[str],
    rating: Optional[float],
    popularity: Optional[float],
    vote_count: Optional[int],
    release_date: Optional[str],
) -> int:
    dedupe_key = make_dedupe_key(tmdb_id, name_norm, year)
    cur.execute(
        """
        INSERT INTO movie_titles
        (dedupe_key, tmdb_id, name, name_normalized, year, plot, rating, popularity, vote_count, release_date, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
        ON CONFLICT(dedupe_key) DO UPDATE SET
            tmdb_id=excluded.tmdb_id,
            name=excluded.name,
            name_normalized=excluded.name_normalized,
            year=excluded.year,
            plot=excluded.plot,
            rating=excluded.rating,
            popularity=excluded.popularity,
            vote_count=excluded.vote_count,
            release_date=excluded.release_date,
            updated_at=excluded.updated_at
        """,
        (dedupe_key, tmdb_id, name, name_norm, year, plot, rating, popularity, vote_count, release_date),
    )
    cur.execute("SELECT id FROM movie_titles WHERE dedupe_key=?", (dedupe_key,))
    return cur.fetchone()[0]


def upsert_series_title(
    cur,
    *,
    tmdb_id: Optional[int],
    name: str,
    name_norm: str,
    year: Optional[int],
    plot: Optional[str],
    rating: Optional[float],
    popularity: Optional[float],
    vote_count: Optional[int],
    first_air_date: Optional[str],
) -> int:
    dedupe_key = make_dedupe_key(tmdb_id, name_norm, year)
    cur.execute(
        """
        INSERT INTO series_titles
        (dedupe_key, tmdb_id, name, name_normalized, year, plot, rating, popularity, vote_count, first_air_date, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
        ON CONFLICT(dedupe_key) DO UPDATE SET
            tmdb_id=excluded.tmdb_id,
            name=excluded.name,
            name_normalized=excluded.name_normalized,
            year=excluded.year,
            plot=excluded.plot,
            rating=excluded.rating,
            popularity=excluded.popularity,
            vote_count=excluded.vote_count,
            first_air_date=excluded.first_air_date,
            updated_at=excluded.updated_at
        """,
        (dedupe_key, tmdb_id, name, name_norm, year, plot, rating, popularity, vote_count, first_air_date),
    )
    cur.execute("SELECT id FROM series_titles WHERE dedupe_key=?", (dedupe_key,))
    return cur.fetchone()[0]


# ---------------- MAIN ----------------
def main():
    # Connect to all 3 databases
    conn_live = connect_db_live()
    conn_movies = connect_db_movies()
    conn_series = connect_db_series()
    
    cur_live = conn_live.cursor()
    cur_movies = conn_movies.cursor()
    cur_series = conn_series.cursor()

    servers_to_check = parse_servers_from_url()
    pre_fetch_tmdb_popular()

    print(
        f"Starting parallel validation of {len(servers_to_check)} servers "
        f"(workers={MAX_WORKERS}, skip_live={int(SKIP_LIVE)}, only_tmdb={int(ONLY_TMDB_MATCHED)}, plot_maxlen={PLOT_MAXLEN}, rebuild_db={int(REBUILD_DB)})"
    )

    # Python-level dedupe during this run (cuts repeated work)
    seen_movie_titles = set()   # dedupe_key
    seen_series_titles = set()  # dedupe_key

    valid_count = 0
    total_movie_streams = 0
    total_series_streams = 0
    total_live = 0

    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(validate_and_fetch_server, s): s for s in servers_to_check}

        for i, future in enumerate(concurrent.futures.as_completed(futures), 1):
            res = future.result()
            s_data = res["server"]

            if res["status"] != "success":
                print(f"[{i}/{len(servers_to_check)}] ❌ {s_data['url']} ({res['status']})")
                continue

            valid_count += 1
            
            # Upsert server in all 3 databases
            server_id_live, server_name = upsert_server(cur_live, s_data, res["info"])
            server_id_movies, _ = upsert_server(cur_movies, s_data, res["info"])
            server_id_series, _ = upsert_server(cur_series, s_data, res["info"])

            # Categories - Live DB
            if not SKIP_LIVE and res["categories"]["live"]:
                cat_batch_live = [(c.get("category_id"), server_id_live, c.get("category_name"), "live") for c in res["categories"]["live"]]
                if cat_batch_live:
                    cur_live.executemany(
                        """
                        INSERT INTO categories (category_id, server_id, category_name, content_type, updated_at)
                        VALUES (?, ?, ?, ?, datetime('now'))
                        ON CONFLICT(category_id, server_id, content_type) DO UPDATE SET
                            category_name=excluded.category_name,
                            updated_at=excluded.updated_at
                        """,
                        cat_batch_live,
                    )

            # Categories - Movies DB
            if res["categories"]["movie"]:
                cat_batch_movies = [(c.get("category_id"), server_id_movies, c.get("category_name"), "movie") for c in res["categories"]["movie"]]
                if cat_batch_movies:
                    cur_movies.executemany(
                        """
                        INSERT INTO categories (category_id, server_id, category_name, content_type, updated_at)
                        VALUES (?, ?, ?, ?, datetime('now'))
                        ON CONFLICT(category_id, server_id, content_type) DO UPDATE SET
                            category_name=excluded.category_name,
                            updated_at=excluded.updated_at
                        """,
                        cat_batch_movies,
                    )

            # Categories - Series DB
            if res["categories"]["series"]:
                cat_batch_series = [(c.get("category_id"), server_id_series, c.get("category_name"), "series") for c in res["categories"]["series"]]
                if cat_batch_series:
                    cur_series.executemany(
                        """
                        INSERT INTO categories (category_id, server_id, category_name, content_type, updated_at)
                        VALUES (?, ?, ?, ?, datetime('now'))
                        ON CONFLICT(category_id, server_id, content_type) DO UPDATE SET
                            category_name=excluded.category_name,
                            updated_at=excluded.updated_at
                        """,
                        cat_batch_series,
                    )

            # Movies: upsert titles + upsert streams (movies DB)
            movie_stream_batch = []
            for m in res["streams"]["movie"]:
                name = m.get("name") or ""
                if not name:
                    continue
                name_norm = normalize_name(name)
                tmdb = TMDB_CACHE["movies"].get(name_norm)
                if ONLY_TMDB_MATCHED and not tmdb:
                    continue

                tmdb_id = (tmdb or {}).get("id")
                release_date = (tmdb or {}).get("release_date") or m.get("releaseDate")
                year = safe_year(release_date)
                plot = safe_trunc((tmdb or {}).get("overview") or m.get("plot"), PLOT_MAXLEN)

                rating = (tmdb or {}).get("vote_average") or m.get("rating")
                try:
                    rating_f = float(rating) if rating not in (None, "", "N/A") else None
                except Exception:
                    rating_f = None

                popularity = (tmdb or {}).get("popularity")
                try:
                    pop_f = float(popularity) if popularity is not None else None
                except Exception:
                    pop_f = None

                vote_count = (tmdb or {}).get("vote_count")
                try:
                    vc_i = int(vote_count) if vote_count is not None else None
                except Exception:
                    vc_i = None

                dedupe_key = make_dedupe_key(tmdb_id, name_norm, year)

                if dedupe_key in seen_movie_titles:
                    cur_movies.execute("SELECT id FROM movie_titles WHERE dedupe_key=?", (dedupe_key,))
                    row = cur_movies.fetchone()
                    title_id = row[0] if row else upsert_movie_title(
                        cur_movies,
                        tmdb_id=int(tmdb_id) if tmdb_id is not None else None,
                        name=name, name_norm=name_norm, year=year, plot=plot,
                        rating=rating_f, popularity=pop_f, vote_count=vc_i,
                        release_date=release_date
                    )
                else:
                    seen_movie_titles.add(dedupe_key)
                    title_id = upsert_movie_title(
                        cur_movies,
                        tmdb_id=int(tmdb_id) if tmdb_id is not None else None,
                        name=name, name_norm=name_norm, year=year, plot=plot,
                        rating=rating_f, popularity=pop_f, vote_count=vc_i,
                        release_date=release_date
                    )

                stream_id = m.get("stream_id")
                if stream_id is None:
                    continue

                movie_stream_batch.append(
                    (
                        server_id_movies,
                        int(stream_id),
                        title_id,
                        m.get("stream_icon"),
                        m.get("container_extension"),
                    )
                )

            if movie_stream_batch:
                cur_movies.executemany(
                    """
                    INSERT INTO movie_streams
                    (server_id, stream_id, title_id, stream_icon, container_extension, updated_at)
                    VALUES (?, ?, ?, ?, ?, datetime('now'))
                    ON CONFLICT(server_id, stream_id) DO UPDATE SET
                        title_id=excluded.title_id,
                        stream_icon=excluded.stream_icon,
                        container_extension=excluded.container_extension,
                        updated_at=excluded.updated_at
                    """,
                    movie_stream_batch,
                )
                total_movie_streams += len(movie_stream_batch)

            # Series: upsert titles + upsert streams (series DB)
            series_stream_batch = []
            for s in res["streams"]["series"]:
                name = s.get("name") or ""
                if not name:
                    continue
                name_norm = normalize_name(name)
                tmdb = TMDB_CACHE["series"].get(name_norm)
                if ONLY_TMDB_MATCHED and not tmdb:
                    continue

                tmdb_id = (tmdb or {}).get("id")
                first_air = (tmdb or {}).get("first_air_date") or s.get("releaseDate")
                year = safe_year(first_air)
                plot = safe_trunc((tmdb or {}).get("overview") or s.get("plot"), PLOT_MAXLEN)

                rating = (tmdb or {}).get("vote_average") or s.get("rating")
                try:
                    rating_f = float(rating) if rating not in (None, "", "N/A") else None
                except Exception:
                    rating_f = None

                popularity = (tmdb or {}).get("popularity")
                try:
                    pop_f = float(popularity) if popularity is not None else None
                except Exception:
                    pop_f = None

                vote_count = (tmdb or {}).get("vote_count")
                try:
                    vc_i = int(vote_count) if vote_count is not None else None
                except Exception:
                    vc_i = None

                dedupe_key = make_dedupe_key(tmdb_id, name_norm, year)

                if dedupe_key in seen_series_titles:
                    cur_series.execute("SELECT id FROM series_titles WHERE dedupe_key=?", (dedupe_key,))
                    row = cur_series.fetchone()
                    title_id = row[0] if row else upsert_series_title(
                        cur_series,
                        tmdb_id=int(tmdb_id) if tmdb_id is not None else None,
                        name=name, name_norm=name_norm, year=year, plot=plot,
                        rating=rating_f, popularity=pop_f, vote_count=vc_i,
                        first_air_date=first_air
                    )
                else:
                    seen_series_titles.add(dedupe_key)
                    title_id = upsert_series_title(
                        cur_series,
                        tmdb_id=int(tmdb_id) if tmdb_id is not None else None,
                        name=name, name_norm=name_norm, year=year, plot=plot,
                        rating=rating_f, popularity=pop_f, vote_count=vc_i,
                        first_air_date=first_air
                    )

                series_id = s.get("series_id")
                if series_id is None:
                    continue

                series_stream_batch.append(
                    (
                        server_id_series,
                        int(series_id),
                        title_id,
                        s.get("cover"),
                    )
                )

            if series_stream_batch:
                cur_series.executemany(
                    """
                    INSERT INTO series_streams
                    (server_id, series_id, title_id, cover, updated_at)
                    VALUES (?, ?, ?, ?, datetime('now'))
                    ON CONFLICT(server_id, series_id) DO UPDATE SET
                        title_id=excluded.title_id,
                        cover=excluded.cover,
                        updated_at=excluded.updated_at
                    """,
                    series_stream_batch,
                )
                total_series_streams += len(series_stream_batch)

            # Live (optional)
            if not SKIP_LIVE:
                live_batch = []
                for l in res["streams"]["live"]:
                    name = l.get("name") or ""
                    stream_id = l.get("stream_id")
                    if not name or stream_id is None:
                        continue
                    live_batch.append(
                        (
                            server_id_live,
                            int(stream_id),
                            name,
                            l.get("category_id"),
                            l.get("stream_icon"),
                            l.get("epg_channel_id"),
                        )
                    )
                if live_batch:
                    cur_live.executemany(
                        """
                        INSERT INTO live_channels
                        (server_id, stream_id, name, category_id, stream_icon, epg_channel_id, updated_at)
                        VALUES (?, ?, ?, ?, ?, ?, datetime('now'))
                        ON CONFLICT(server_id, stream_id) DO UPDATE SET
                            name=excluded.name,
                            category_id=excluded.category_id,
                            stream_icon=excluded.stream_icon,
                            epg_channel_id=excluded.epg_channel_id,
                            updated_at=excluded.updated_at
                        """,
                        live_batch,
                    )
                    total_live += len(live_batch)

            # Commit all databases
            conn_live.commit()
            conn_movies.commit()
            conn_series.commit()

            print(
                f"[{i}/{len(servers_to_check)}] ✅ {server_name} | "
                f"movie_streams+{len(movie_stream_batch)} series_streams+{len(series_stream_batch)}"
                + (" live(skipped)" if SKIP_LIVE else f" live+{len(res['streams']['live'])}")
            )

    # Optional vacuum for size (can take time on big db; keep it if size is priority)
    for conn, name in [(conn_live, DB_FILE_LIVE), (conn_movies, DB_FILE_MOVIES), (conn_series, DB_FILE_SERIES)]:
        try:
            cur = conn.cursor()
            cur.execute("VACUUM;")
            conn.commit()
            print(f"Vacuumed {name}")
        except Exception as e:
            print(f"Vacuum failed for {name}: {e}")

    conn_live.close()
    conn_movies.close()
    conn_series.close()

    print("=" * 70)
    print("BUILD COMPLETE: 3 Database files created")
    print(f"  - {DB_FILE_LIVE}")
    print(f"  - {DB_FILE_MOVIES}")
    print(f"  - {DB_FILE_SERIES}")
    print(f"Valid Servers: {valid_count}/{len(servers_to_check)}")
    print(f"Movie streams: {total_movie_streams} | Series streams: {total_series_streams} | Live: {total_live}")
    print("=" * 70)


if __name__ == "__main__":
    main()
