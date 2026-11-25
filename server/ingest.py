#!/usr/bin/env python3
"""ArenaSweats collector ingest: tells collectors what to read, and receives the matches they send.

Design rules
  - It is its own process with its own database, so a flood here cannot slow the website.
  - Collectors hold a key and are trusted: the first copy of a match is kept and never asked
    for again. Sizes are still capped and every record is checked for shape, so a broken
    or careless collector cannot fill the disk or store rubbish.
  - No match is fetched twice: collectors report which matches they saw in match lists, and
    are only told to fetch the ones not held yet.
  - Nothing from before the current season is asked for or kept.

Run for development:   python server/ingest.py serve
Manage keys / policy:  python server/ingest.py --help
"""

import argparse
import base64
import calendar
import gzip
import hashlib
import json
import os
import re
import secrets
import shutil
import sqlite3
import sys
import threading
import time
import zlib
from collections import defaultdict, deque

from flask import Flask, g, jsonify, request

clock = time.time                # the time source, replaceable in tests

# ---- limits on what one collector can send
MAX_BODY = 2 * 1024 * 1024       # compressed bytes per request
MAX_RAW = 4 * 1024 * 1024        # bytes per request after decompression
MAX_PARTS = 100000               # JSON objects and lists per request (tiny bodies can expand hugely in memory)
MAX_GAMES = 50                   # matches per upload
MAX_GAME_BYTES = 300 * 1024      # one match record
MAX_REPORTS = 20                 # match lists reported per work request
MAX_REPORT_IDS = 3000            # matches named in one match-list report
REQUESTS_PER_MINUTE = 60         # per key; a collector makes at most ~48 a minute in fast mode (measured on the fake client: up to 21)
GAMES_PER_DAY = 220000           # per key; fast mode's floor (0.4 s between requests, at most a match a request) allows
                                 # 216,000 a day, plus a held-back outbox (measured: ~100k in fast mode, ~30k normally)
SIGNUPS_PER_ADDRESS = 10         # new installs one internet address can sign up a day (many homes in China share one)
SIGNUPS_PER_DAY = 500            # new installs signed up a day in all (close with: policy --signups off)
ARENA_MODES = {"CHERRY"}
DEFAULT_SEASON_START = "2026-10-06"   # UTC. Change with: ingest.py policy --season-start YYYY-MM-DD
DEFAULT_FAST_DELAY = 0.4         # fast mode's shortest gap between requests, s. Change with: policy --fast-delay S
DEFAULT_FAST_FLIGHT = 4          # fast mode's most requests on their way at once. Change with: policy --fast-flight N

# ---- passing matches on to the rankings server (it pulls them over SSH: see `relay` below)
RELAY_BATCH = 5000               # most matches handed over in one pull
KEEP_SHIPPED_DAYS = 3            # a record is kept here this long after the rankings server confirmed it holds it
MAX_BACKLOG = 300000             # matches not passed on yet (about 2 GB); above this, uploads wait
MIN_FREE_BYTES = 5 * 1024 ** 3   # uploads also wait when the disk has less free than this

# ---- handing out work
LEASE_SECONDS = 15 * 60          # a collector has this long to do what it was given
PLAYERS_PER_ROUND = 5            # match lists handed out per work request
GAMES_PER_ROUND = 40             # matches handed out per work request
GAME_TRIES = 6                   # a match nobody could fetch after this many tries is not handed out again
TARGET_NEW = 8                   # read a player's list again when about this many new matches are expected
MIN_INTERVAL = 3600              # ...but not more often than this
MAX_INTERVAL = 7 * 86400         # ...and not less often than this
NEW_PLAYER_BONUS = 6 * 3600      # each further match a not-yet-read player is seen in moves them this much earlier
RETRY_SECONDS = 600              # first retry after a failed read; doubles each time
FIRST_PAGES = 1                  # a list is read one page (20 matches) at first ...
MORE_PAGES = 3                   # ... then three pages at a time, for as long as that turns up matches not held.
                                 # After a stretch with nothing new, the older part is set aside and only read
                                 # when no fresher work is waiting, so nothing is missed in the end.
MAX_INDEX = 1000                 # never further back than this many matches

# ---- event games the rankings server is waiting for (see relay `urgent`): handed out before anything else
URGENT_GAME_SECONDS = 300        # such a match not held yet is handed out again this often (it may not have ended)
URGENT_LIST_SECONDS = 600        # and each of its players' lists is read from the top again this often
URGENT_KEEP_SECONDS = 3600       # the rankings server sends its list every minute; one it stops sending lapses
MAX_URGENT = 1000                # matches in one list
MAX_URGENT_PLAYERS = 18          # players named for one match

PLATFORM_RE = re.compile(r"[A-Z0-9_]{2,12}")
PUUID_RE = re.compile(r"[0-9A-Za-z_-]{20,100}")

SCHEMA = """
-- Sign-ups of the last day, for the daily limits. The address is stored only as a hash.
CREATE TABLE IF NOT EXISTS signups(at INTEGER NOT NULL, address TEXT NOT NULL);
-- When each install last got in touch, and what it said it was doing (collecting, paused, ...).
CREATE TABLE IF NOT EXISTS contributor_seen(
    contributor_id INTEGER PRIMARY KEY, seen_at INTEGER NOT NULL, state TEXT NOT NULL, version TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS contributors(
    id INTEGER PRIMARY KEY, name TEXT NOT NULL UNIQUE, key_hash TEXT NOT NULL UNIQUE,
    created_at INTEGER NOT NULL, disabled INTEGER NOT NULL DEFAULT 0);
-- Every match held. The first copy received is the one kept.
CREATE TABLE IF NOT EXISTS games(
    platform TEXT NOT NULL, game_id INTEGER NOT NULL, created_ms INTEGER NOT NULL,
    received_at INTEGER NOT NULL, contributor_id INTEGER NOT NULL, payload BLOB NOT NULL,
    PRIMARY KEY(platform, game_id));
CREATE INDEX IF NOT EXISTS games_by_contributor ON games(contributor_id, received_at);
-- Matches seen in somebody's match list but not held yet.
CREATE TABLE IF NOT EXISTS wanted(
    platform TEXT NOT NULL, game_id INTEGER NOT NULL,
    leased_until INTEGER NOT NULL DEFAULT 0,   -- a collector is fetching it until then
    tries INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY(platform, game_id));
-- Matches nobody could fetch after GAME_TRIES tries. Kept apart so they are not asked for again.
CREATE TABLE IF NOT EXISTS given_up(
    platform TEXT NOT NULL, game_id INTEGER NOT NULL, PRIMARY KEY(platform, game_id));
-- Every player seen in a held match, and how far their match list has been read.
CREATE TABLE IF NOT EXISTS players(
    platform TEXT NOT NULL, puuid TEXT NOT NULL,
    games INTEGER NOT NULL DEFAULT 0,          -- held matches this player is in
    read_to INTEGER NOT NULL DEFAULT 0,        -- the top of their list has been read down to this time (ms)
    checked_at INTEGER NOT NULL DEFAULT 0,     -- when the top of their list was last read (0 = never)
    due_at INTEGER NOT NULL,                   -- when to read the top again; the most overdue goes first
    fails INTEGER NOT NULL DEFAULT 0,          -- reads that failed in a row
    leased_until INTEGER NOT NULL DEFAULT 0,   -- a collector is reading this list until then ...
    job TEXT,                                  -- ... and its report must carry this token
    job_kind INTEGER NOT NULL DEFAULT 0,       -- that read is 0: from the top, 1: of the older part
    walk_newest INTEGER NOT NULL DEFAULT 0,    -- a read from the top in progress: newest match seen (ms) ...
    walk_new INTEGER NOT NULL DEFAULT 0,       -- ... and matches not held it has found so far
    resume INTEGER,                            -- the older part of the list, from this place on, is unread ...
    resume_floor INTEGER NOT NULL DEFAULT 0,   -- ... back to this time (ms) ...
    older_due INTEGER NOT NULL DEFAULT 0,      -- ... and may be read from this time on (later after failures)
    PRIMARY KEY(platform, puuid));
CREATE INDEX IF NOT EXISTS players_due ON players(platform, due_at);
CREATE INDEX IF NOT EXISTS players_older_part ON players(platform, games) WHERE resume IS NOT NULL;
CREATE INDEX IF NOT EXISTS players_unread ON players(platform) WHERE checked_at = 0;
CREATE INDEX IF NOT EXISTS games_by_time ON games(created_ms);
CREATE TABLE IF NOT EXISTS settings(name TEXT PRIMARY KEY, value TEXT NOT NULL);
CREATE INDEX IF NOT EXISTS games_by_arrival ON games(received_at, platform, contributor_id);
-- Matches not passed on to the rankings server yet, in the order they arrived. A trigger on games fills it
-- (see add_outbox), so every match is queued whatever version of this program received it. seq is never reused.
CREATE TABLE IF NOT EXISTS outbox(seq INTEGER PRIMARY KEY AUTOINCREMENT, platform TEXT NOT NULL, game_id INTEGER NOT NULL);
-- Matches the rankings server confirmed it holds. After KEEP_SHIPPED_DAYS their record here is emptied; the row
-- in games stays, so the match is still known and never asked for again.
CREATE TABLE IF NOT EXISTS shipped(
    platform TEXT NOT NULL, game_id INTEGER NOT NULL, shipped_at INTEGER NOT NULL, PRIMARY KEY(platform, game_id));
CREATE INDEX IF NOT EXISTS shipped_by_time ON shipped(shipped_at);
-- ArenaSweats Events games the rankings server is waiting for, and their players. Only the rankings server's relay
-- key sets them (relay `urgent`, which replaces the whole list); collectors cannot.
CREATE TABLE IF NOT EXISTS urgent_games(
    platform TEXT NOT NULL, game_id INTEGER NOT NULL,
    until INTEGER NOT NULL,                    -- asked for until then, unless the rankings server asks again
    handed_at INTEGER NOT NULL DEFAULT 0,      -- last handed out to a collector
    PRIMARY KEY(platform, game_id));
CREATE TABLE IF NOT EXISTS urgent_players(
    platform TEXT NOT NULL, game_id INTEGER NOT NULL, puuid TEXT NOT NULL,
    handed_at INTEGER NOT NULL DEFAULT 0,      -- their list was last handed out to be read from the top
    PRIMARY KEY(platform, game_id, puuid));
"""


def db_path():
    return os.environ.get("INGEST_DB") or os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "ingest.db")


prepared = set()     # database files this process has already set up


def connect():
    path = os.path.abspath(db_path())
    db = sqlite3.connect(path, timeout=10) if path in prepared else None
    if db is None:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        db = sqlite3.connect(path, timeout=10)
        db.execute("PRAGMA journal_mode=WAL")
        db.executescript(SCHEMA)
        add_outbox(db)
        prepared.add(path)
    db.execute("PRAGMA synchronous=NORMAL")
    return db


def add_outbox(db):
    """Queue every match for the rankings server: the ones held already (once), and from now on each new one.

    Done in one transaction with the trigger, so no match is queued twice or missed, even while an older
    version of this program is still receiving uploads.
    """
    db.execute("BEGIN IMMEDIATE")
    with db:
        if db.execute("SELECT 1 FROM sqlite_master WHERE type = 'trigger' AND name = 'games_to_outbox'").fetchone():
            return
        db.execute("INSERT INTO outbox(platform, game_id) SELECT platform, game_id FROM games ORDER BY rowid")
        db.execute("CREATE TRIGGER games_to_outbox AFTER INSERT ON games BEGIN "
                   "INSERT INTO outbox(platform, game_id) VALUES(NEW.platform, NEW.game_id); END")


def hash_key(key):
    return hashlib.sha256(key.encode()).hexdigest()


def date_ms(text):
    """'2026-10-06' (UTC midnight) as milliseconds."""
    return calendar.timegm(time.strptime(text, "%Y-%m-%d")) * 1000


def get_policy(db):
    """What collectors are told: whether to collect, the slowest pace we ask for, whether fast mode is allowed,
    its shortest gap (collectors whose user turned it on wait a fifth as long, never under fast_delay,
    nor under their own 0.4 s floor) and how many requests it may have on their way at once (never more
    than their own 4), a short message, the season."""
    values = dict(db.execute("SELECT name, value FROM settings"))
    return {"enabled": values.get("enabled", "1") == "1",
            "delay": float(values.get("delay", "2")),
            "fast": values.get("fast", "1") == "1",
            "fast_delay": float(values.get("fast_delay", DEFAULT_FAST_DELAY)),
            "fast_flight": int(values.get("fast_flight", DEFAULT_FAST_FLIGHT)),
            "message": values.get("message", ""),
            "season_start": int(values.get("season_start", date_ms(DEFAULT_SEASON_START)))}


# ---------------------------------------------------------------------------
# Checking the shape of what collectors send
# ---------------------------------------------------------------------------
def is_int(value, low, high):
    return isinstance(value, int) and not isinstance(value, bool) and low <= value <= high


def validate_game(game, now):
    """Return None when the record is acceptable, otherwise the reason it is refused."""
    if not isinstance(game, dict):
        return "not an object"
    if not is_int(game.get("gameId"), 1, 2 ** 53):
        return "bad gameId"
    if not isinstance(game.get("platformId"), str) or not PLATFORM_RE.fullmatch(game["platformId"]):
        return "bad platformId"
    if not isinstance(game.get("gameMode"), str) or game["gameMode"] not in ARENA_MODES:
        return "not an Arena match"
    created = game.get("gameCreation")
    if not is_int(created, 1, 2 ** 53):
        return "bad gameCreation"
    if created > (now + 86400) * 1000:
        return "match is in the future"
    if not is_int(game.get("gameDuration"), 0, 3 * 3600):
        return "bad gameDuration"
    participants, identities = game.get("participants"), game.get("participantIdentities")
    if not isinstance(participants, list) or not isinstance(identities, list):
        return "missing participants"
    if not 2 <= len(participants) <= 32 or len(participants) != len(identities):
        return "bad participant count"
    seats, players, humans = set(), set(), 0
    for identity in identities:
        player = identity.get("player") if isinstance(identity, dict) else None
        if not isinstance(player, dict) or not is_int(identity.get("participantId"), 0, 64):
            return "bad participant identity"
        puuid = player.get("puuid")
        if not isinstance(puuid, str) or not PUUID_RE.fullmatch(puuid):
            return "bad player id"
        seats.add(identity["participantId"])
        if puuid.strip("0-"):           # computer players all share an all-zero id
            players.add(puuid)
            humans += 1
    if len(seats) != len(identities) or len(players) != humans:
        return "repeated participant"
    taken = set()
    for participant in participants:
        if not isinstance(participant, dict) or not is_int(participant.get("participantId"), 0, 64):
            return "bad participant"
        if participant["participantId"] not in seats or participant["participantId"] in taken:
            return "participant without identity"
        taken.add(participant["participantId"])
        if not is_int(participant.get("championId"), 0, 100000) or not isinstance(participant.get("stats"), dict):
            return "bad participant"
        placement = participant["stats"].get("subteamPlacement")
        if placement is not None and not is_int(placement, 0, 32):
            return "bad placement"
    return None


def human_players(game):
    return [i["player"]["puuid"] for i in game["participantIdentities"] if i["player"]["puuid"].strip("0-")]


def clean_report(report, now):
    """One match-list report from a collector, or None when it is not usable.

    A report says: for the read handed out with this token, these Arena matches were in the stretch
    of the list I read, the newest match I saw was at this time, and the list goes on from this place
    (0 = I reached the end or the date I was given), or I failed.
    """
    if not isinstance(report, dict):
        return None
    puuid, ids, job = report.get("puuid"), report.get("ids"), report.get("job")
    if not isinstance(puuid, str) or not PUUID_RE.fullmatch(puuid) or not puuid.strip("0-"):
        return None
    if not isinstance(ids, list) or len(ids) > MAX_REPORT_IDS or not all(is_int(i, 1, 2 ** 53) for i in ids):
        return None
    newest, following = report.get("newest"), report.get("next")
    newest = min(newest, now * 1000) if is_int(newest, 0, 2 ** 53) else 0        # never beyond the present
    return {"puuid": puuid, "ids": sorted(set(ids)), "job": job if isinstance(job, str) else None,
            "newest": newest, "next": following if is_int(following, 0, MAX_INDEX) else 0,
            "failed": report.get("failed") is True}


# ---------------------------------------------------------------------------
# Web application
# ---------------------------------------------------------------------------
app = Flask(__name__)
app.config["MAX_CONTENT_LENGTH"] = MAX_BODY     # larger bodies are refused before being read
recent = defaultdict(deque)                     # contributor id -> times of recent requests
recent_lock = threading.Lock()
seen_written = {}                               # (contributor id, state) -> when this worker last stored it
SEEN_EVERY = 60                                 # store a contributor's "last seen" at most this often per state


def note_seen(db, contributor, now):
    """Remember when an install last got in touch and what it said it was doing, for health checks."""
    state = request.headers.get("X-Collector-State", "")
    state = state if re.fullmatch(r"[a-z_]{1,32}", state) else "unknown"     # "_fast" at the end: fast mode is on
    agent = request.headers.get("User-Agent", "")
    version = agent.split("/", 1)[1] if agent.startswith("arenasweats-collector/") else ""
    version = version if re.fullmatch(r"[0-9A-Za-z.+-]{1,20}", version) else ""
    with recent_lock:
        if now - seen_written.get((contributor, state), 0) < SEEN_EVERY:
            return
        seen_written[(contributor, state)] = now
    with db:
        db.execute("INSERT OR REPLACE INTO contributor_seen VALUES(?, ?, ?, ?)", (contributor, int(now), state, version))


def fail(status, message, **headers):
    response = jsonify({"error": message})
    response.status_code = status
    response.headers.update(headers)
    return response


@app.before_request
def check_key():
    """Every request except the health check needs a valid key. Runs before any body is read."""
    if request.path in ("/healthz", "/check", "/latest", "/download/" + PROGRAM):
        return None
    if request.path == "/v1/register":       # a new install asking for its own key
        g.db = connect()
        return None
    header = request.headers.get("Authorization", "")
    key = header[7:] if header.startswith("Bearer ") else ""
    g.db = connect()
    row = g.db.execute("SELECT id, disabled FROM contributors WHERE key_hash = ?", (hash_key(key),)).fetchone()
    if not key or row is None:
        return fail(401, "unknown key")
    if row[1]:
        return fail(403, "this key has been switched off")
    g.contributor = row[0]
    now = clock()
    with recent_lock:
        times = recent[row[0]]
        while times and times[0] < now - 60:
            times.popleft()
        if len(times) >= REQUESTS_PER_MINUTE:
            return fail(429, "too many requests", **{"Retry-After": "60"})
        times.append(now)
    note_seen(g.db, row[0], now)
    return None


@app.teardown_request
def close_db(_error):
    db = g.pop("db", None)
    if db is not None:
        db.close()


def read_body():
    """The request body as parsed JSON. Returns (data, None) or (None, an error response)."""
    raw = request.get_data(cache=False)
    if request.headers.get("Content-Encoding") == "gzip":
        try:
            unpacker = zlib.decompressobj(16 + zlib.MAX_WBITS)
            raw = unpacker.decompress(raw, MAX_RAW)
            if unpacker.unconsumed_tail:
                return None, fail(413, "body is too large after decompression")
        except zlib.error:
            return None, fail(400, "body is not valid gzip")
    if len(raw) > MAX_RAW or raw.count(b"{") + raw.count(b"[") > MAX_PARTS:
        return None, fail(413, "body is too large")
    try:
        data = json.loads(raw)
    except (ValueError, RecursionError):
        return None, fail(400, "body is not valid JSON")
    if not isinstance(data, dict):
        return None, fail(400, "expected a JSON object")
    return data, None


CHECK_SCRIPT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "tools", "client-check.ps1")
PROGRAM = "ArenaSweatsCollector.exe"


def downloads_dir():
    """Where deploy/fetch_download.sh keeps the newest release (beside this repository's folder)."""
    repo = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    return os.environ.get("COLLECTOR_DOWNLOADS") or os.path.join(os.path.dirname(repo), "arenasweats-collector-downloads")


@app.get("/latest")
def latest():
    """The newest collector, for collectors that offer to update themselves at start."""
    try:
        with open(os.path.join(downloads_dir(), "latest.json"), encoding="utf-8") as handle:
            info = json.load(handle)
    except (OSError, ValueError):
        return fail(404, "no release known")
    return jsonify({"version": info.get("version"), "sha256": info.get("sha256"), "size": info.get("size"),
                    "path": "/download/" + PROGRAM})


@app.get("/download/" + PROGRAM)
def download():
    """The newest collector. In production nginx serves this file itself; this is the fallback."""
    path = os.path.join(downloads_dir(), PROGRAM)
    if not os.path.isfile(path):
        return fail(404, "no release known")
    with open(path, "rb") as handle:
        body = handle.read()
    return app.response_class(body, mimetype="application/octet-stream",
                              headers={"Content-Disposition": 'attachment; filename="%s"' % PROGRAM})


@app.get("/check")
def client_check():
    """A read-only PowerShell check a tester runs with one line: irm https://arenasweats.lol/collector/check | iex"""
    with open(CHECK_SCRIPT, "rb") as handle:
        body = handle.read()
    return app.response_class(body, mimetype="text/plain", headers={"Cache-Control": "no-cache"})


@app.get("/healthz")
def healthz():
    return "ok\n"


def client_address():
    """The sender's internet address. nginx on this machine passes it on in X-Real-IP."""
    if request.remote_addr in ("127.0.0.1", "::1"):
        return request.headers.get("X-Real-IP", "")[:64] or request.remote_addr
    return request.remote_addr or ""


def signups_open(db):
    return dict(db.execute("SELECT name, value FROM settings")).get("signups", "1") == "1"


@app.post("/v1/register")
def register():
    """A new install asks for its own upload key. No key is needed for this; it is limited per
    internet address and per day, and can be closed with: policy --signups off."""
    db, now = g.db, int(clock())
    if not signups_open(db):
        return fail(403, "ArenaSweats is not taking new collectors right now")
    address = hashlib.sha256(("signup:" + client_address()).encode()).hexdigest()[:32]
    with db:
        db.execute("DELETE FROM signups WHERE at < ?", (now - 86400,))
        mine = db.execute("SELECT COUNT(*) FROM signups WHERE address = ?", (address,)).fetchone()[0]
        total = db.execute("SELECT COUNT(*) FROM signups").fetchone()[0]
        if mine >= SIGNUPS_PER_ADDRESS or total >= SIGNUPS_PER_DAY:
            return fail(429, "too many new collectors today", **{"Retry-After": "3600"})
        key, name = "as_" + secrets.token_urlsafe(24), "auto-" + secrets.token_hex(4)
        db.execute("INSERT INTO contributors(name, key_hash, created_at) VALUES(?, ?, ?)", (name, hash_key(key), now))
        db.execute("INSERT INTO signups VALUES(?, ?)", (now, address))
    return jsonify({"key": key, "name": name}), 201


@app.get("/v1/policy")
def policy():
    return jsonify(get_policy(g.db))


def backlog_full(db):
    """Why uploads must wait, or None. Matches pile up here only while the rankings server is not taking
    them; this keeps that pile (and the disk it shares with the website) bounded. Collectors keep what
    they hold and send it later."""
    first, last = db.execute("SELECT MIN(seq), MAX(seq) FROM outbox").fetchone()
    if first is not None and last - first + 1 > MAX_BACKLOG:
        return "ArenaSweats is catching up; matches are kept and sent later"
    if shutil.disk_usage(os.path.dirname(os.path.abspath(db_path()))).free < MIN_FREE_BYTES:
        return "ArenaSweats is low on space; matches are kept and sent later"
    return None


@app.post("/v1/games")
def games():
    """Receive match records. New ones are kept; their players join the list of players to read."""
    body, error = read_body()
    if error:
        return error
    batch = body.get("games")
    if not isinstance(batch, list):
        return fail(400, "expected {\"games\": [...]}")
    if len(batch) > MAX_GAMES:
        return fail(413, "too many matches in one request")

    now = int(clock())
    accepted = duplicates = skipped = 0
    rejected, ready = [], []
    db = g.db
    rules = get_policy(db)
    busy = backlog_full(db)
    if busy:
        return fail(503, busy, **{"Retry-After": "1800"})
    # Checked and packed before the write lock is taken, so other requests wait only for the inserts.
    for game in batch:
        reason = validate_game(game, now)
        if reason is None:
            try:
                payload = json.dumps(game, separators=(",", ":"), sort_keys=True).encode()
            except (ValueError, RecursionError):
                reason = "record cannot be stored"
            else:
                if len(payload) > MAX_GAME_BYTES:
                    reason = "record is too large"
        if reason:
            game_id = game.get("gameId") if isinstance(game, dict) else None
            rejected.append({"gameId": game_id if is_int(game_id, 0, 2 ** 53) else None, "reason": reason})
            continue
        ready.append((game["platformId"], game["gameId"], game["gameCreation"], human_players(game),
                      zlib.compress(payload)))
    db.execute("BEGIN IMMEDIATE")       # one writer at a time, one transaction for the whole batch
    with db:
        today = db.execute("SELECT COUNT(*) FROM games WHERE contributor_id = ? AND received_at > ?",
                           (g.contributor, now - 86400)).fetchone()[0]
        if today + len(batch) > GAMES_PER_DAY:      # refuse the whole batch, so the collector keeps it and waits
            return fail(429, "daily limit reached", **{"Retry-After": "3600"})
        for platform, game_id, created, players, packed in ready:
            key = (platform, game_id)
            db.execute("DELETE FROM wanted WHERE platform = ? AND game_id = ?", key)
            if created < rules["season_start"]:
                skipped += 1            # from before this season: not kept
                continue
            new = db.execute("INSERT OR IGNORE INTO games VALUES(?, ?, ?, ?, ?, ?)",
                             key + (created, now, g.contributor, packed)).rowcount
            if not new:
                duplicates += 1         # already held: the first copy stays
                continue
            accepted += 1
            # Everyone in the match becomes a player whose list is worth reading. Someone not read yet
            # moves earlier in the queue with every further match they turn up in.
            db.executemany("""
                INSERT INTO players(platform, puuid, games, due_at) VALUES(?, ?, 1, ?)
                ON CONFLICT(platform, puuid) DO UPDATE SET games = games + 1,
                    due_at = CASE WHEN checked_at = 0 AND fails = 0 THEN due_at - ? ELSE due_at END""",
                           [(platform, puuid, now, NEW_PLAYER_BONUS) for puuid in players])
    return jsonify({"accepted": accepted, "duplicates": duplicates, "skipped": skipped, "rejected": rejected,
                    "policy": rules})


@app.post("/v1/work")
def work():
    """Take a collector's match-list reports, and give it the next match lists to read and matches to fetch."""
    body, error = read_body()
    if error:
        return error
    platform, reports = body.get("platform"), body.get("reports", [])
    if not isinstance(platform, str) or not PLATFORM_RE.fullmatch(platform):
        return fail(400, "bad platform")
    if not isinstance(reports, list) or len(reports) > MAX_REPORTS:
        return fail(400, "bad reports")

    now = int(clock())
    db = g.db
    rules = get_policy(db)
    season = rules["season_start"]
    # A collector that is stopping, or any collector while collection is switched off, is handed nothing.
    more = body.get("more") is not False and rules["enabled"]
    fetch, handed = [], []

    def hand(puuid, kind, begin, since, pages):
        """Give one stretch of a list to this collector, with a token its report must carry."""
        job = secrets.token_hex(8)
        db.execute("UPDATE players SET leased_until = ?, job = ?, job_kind = ? WHERE platform = ? AND puuid = ?",
                   (now + LEASE_SECONDS, job, kind, platform, puuid))
        handed.append({"puuid": puuid, "job": job, "since": since, "begin": begin, "pages": pages})

    db.execute("BEGIN IMMEDIATE")       # so two collectors are never handed the same work
    with db:
        for report in reports:
            cleaned = clean_report(report, now)
            onward = take_report(db, platform, now, season, cleaned) if cleaned else None
            if onward and more and len(handed) < MAX_REPORTS:
                hand(*onward)           # that list goes on, and the same collector carries on with it
        # A collector names its own account once, so that a server with no players yet has one to start from.
        start = body.get("start")
        if isinstance(start, str) and PUUID_RE.fullmatch(start) and start.strip("0-"):
            db.execute("INSERT OR IGNORE INTO players(platform, puuid, due_at) VALUES(?, ?, ?)", (platform, start, now))

        if more:
            # 0. Event games the rankings server is waiting for (relay `urgent`) and not held yet, however many
            #    tries they took: one may not have ended yet. Each goes out again after URGENT_GAME_SECONDS.
            fetch = [game_id for (game_id,) in db.execute(
                "SELECT u.game_id FROM urgent_games u WHERE u.platform = ? AND u.until > ? AND u.handed_at <= ? "
                "AND NOT EXISTS (SELECT 1 FROM games g WHERE g.platform = u.platform AND g.game_id = u.game_id) "
                "ORDER BY u.handed_at, u.game_id LIMIT ?", (platform, now, now - URGENT_GAME_SECONDS, GAMES_PER_ROUND))]
            db.executemany("UPDATE urgent_games SET handed_at = ? WHERE platform = ? AND game_id = ?",
                           [(now, platform, game_id) for game_id in fetch])
            # (not handed out a second time from the wanted list, and not counted as one of its tries)
            db.executemany("UPDATE wanted SET leased_until = ? WHERE platform = ? AND game_id = ?",
                           [(now + LEASE_SECONDS, platform, game_id) for game_id in fetch])
            # 1. Matches known to exist but not held yet, newest first. One that nobody could fetch after
            #    GAME_TRIES tries is moved aside instead, so it is not asked for (or looked at) again.
            for _ in range(3):
                rows = db.execute("SELECT game_id, tries FROM wanted WHERE platform = ? AND leased_until < ? "
                                  "ORDER BY game_id DESC LIMIT ?", (platform, now, GAMES_PER_ROUND - len(fetch)))
                rows = rows.fetchall()
                spent = [(platform, game_id) for game_id, tries in rows if tries >= GAME_TRIES]
                taken = [game_id for game_id, tries in rows if tries < GAME_TRIES]
                fetch += taken
                # Leased at once, so the next look (after some were moved aside) does not take them again.
                db.executemany("UPDATE wanted SET leased_until = ?, tries = tries + 1 WHERE platform = ? AND game_id = ?",
                               [(now + LEASE_SECONDS, platform, game_id) for game_id in taken])
                db.executemany("INSERT OR IGNORE INTO given_up VALUES(?, ?)", spent)
                db.executemany("DELETE FROM wanted WHERE platform = ? AND game_id = ?", spent)
                if not spent:
                    break
            # The lists of the players in those event games, from the top, ahead of every other list. Each goes
            # out again after URGENT_LIST_SECONDS, whether its last read worked or not, until the game is held.
            urgent = db.execute(
                "SELECT p.puuid, p.read_to FROM urgent_players up "
                "JOIN urgent_games u ON u.platform = up.platform AND u.game_id = up.game_id "
                "JOIN players p ON p.platform = up.platform AND p.puuid = up.puuid "
                "WHERE up.platform = ? AND u.until > ? AND up.handed_at <= ? AND p.leased_until < ? "
                "AND NOT EXISTS (SELECT 1 FROM games g WHERE g.platform = u.platform AND g.game_id = u.game_id) "
                "GROUP BY p.puuid ORDER BY MIN(up.handed_at), p.puuid LIMIT ?",
                (platform, now, now - URGENT_LIST_SECONDS, now, max(PLAYERS_PER_ROUND - len(handed), 0))).fetchall()
            for puuid, read_to in urgent:
                hand(puuid, 0, 0, max(read_to, season), FIRST_PAGES)
            db.executemany("UPDATE urgent_players SET handed_at = ? WHERE platform = ? AND puuid = ?",
                           [(now, platform, puuid) for puuid, _ in urgent])
            # While there is a full load of matches to fetch, no new lists are started: finding more
            # matches would only lengthen the queue.
            room = PLAYERS_PER_ROUND - len(handed) if len(fetch) < GAMES_PER_ROUND else 0
            # 2. The top of the lists that are most overdue.
            for puuid, read_to in db.execute(
                    "SELECT puuid, read_to FROM players WHERE platform = ? AND due_at <= ? AND leased_until < ? "
                    "ORDER BY due_at LIMIT ?", (platform, now, now, max(room, 0))).fetchall():
                hand(puuid, 0, 0, max(read_to, season), FIRST_PAGES)
            # 3. With nothing fresher waiting: the older parts of lists that were set aside,
            #    starting with the players seen in the most matches.
            if room > 0:
                for puuid, resume, floor in db.execute(
                        "SELECT puuid, resume, resume_floor FROM players WHERE platform = ? AND resume IS NOT NULL "
                        "AND older_due <= ? AND leased_until < ? ORDER BY games DESC LIMIT ?",
                        (platform, now, now, max(PLAYERS_PER_ROUND - len(handed), 0))).fetchall():
                    hand(puuid, 1, resume, max(floor, season), MORE_PAGES)
    return jsonify({"games": fetch, "players": handed, "wait": 0 if fetch or handed else 60,
                    "lease": LEASE_SECONDS, "policy": rules})


def take_report(db, platform, now, season, report):
    """A collector read a stretch of one player's match list. Returns the next stretch to hand it, or None.

    The matches not held yet become wanted, whoever reports them. The list's own progress is only
    taken from the report of the read currently handed out (its token must match): a report that
    arrives after its read was handed to someone else is out of date and changes nothing.

    A read from the top goes further back only while that keeps turning up matches not held. After a
    stretch with nothing new, the older part is set aside: it is read later, when no fresher work is
    waiting, so that nothing is missed in the end.
    """
    ids = report["ids"]
    held = set()
    for start in range(0, len(ids), 500):
        chunk = ids[start:start + 500]
        marks = ",".join("?" * len(chunk))
        held.update(row[0] for row in db.execute(
            "SELECT game_id FROM games WHERE platform = ? AND game_id IN (%s) UNION "
            "SELECT game_id FROM given_up WHERE platform = ? AND game_id IN (%s)" % (marks, marks),
            [platform] + chunk + [platform] + chunk))
    missing = [game_id for game_id in ids if game_id not in held]
    db.executemany("INSERT OR IGNORE INTO wanted(platform, game_id) VALUES(?, ?)",
                   [(platform, game_id) for game_id in missing])

    where = (platform, report["puuid"])
    row = db.execute("SELECT job, job_kind, read_to, checked_at, fails, walk_newest, walk_new, resume, resume_floor "
                     "FROM players WHERE platform = ? AND puuid = ?", where).fetchone()
    if row is None or row[0] is None or row[0] != report["job"]:
        return None                     # not the read currently handed out: out of date
    _, kind, read_to, checked_at, fails, walk_newest, walk_new, resume, floor = row
    following = report["next"]
    db.execute("UPDATE players SET job = NULL, leased_until = 0 WHERE platform = ? AND puuid = ?", where)

    if report["failed"]:
        # Try again later, waiting longer each time. A read from the top starts again from the top;
        # an older part keeps its place. Nothing that was not read is marked as read.
        later = now + min(MAX_INTERVAL, RETRY_SECONDS * 2 ** min(fails, 10))
        if kind == 0:
            db.execute("UPDATE players SET fails = fails + 1, due_at = ?, older_due = ?, walk_newest = 0, "
                       "walk_new = 0 WHERE platform = ? AND puuid = ?", (later, later) + where)
        else:
            db.execute("UPDATE players SET fails = fails + 1, older_due = ? WHERE platform = ? AND puuid = ?",
                       (later,) + where)
        return None

    if kind == 1:
        # A stretch of an older part that had been set aside. Note how far it got.
        db.execute("UPDATE players SET resume = ?, fails = 0, older_due = 0 WHERE platform = ? AND puuid = ?",
                   (following or None,) + where)
        if following and missing:
            return report["puuid"], 1, following, max(floor, season), MORE_PAGES
        return None

    # A stretch read from the top of the list (the first one starts at place 0).
    since = max(read_to, season)
    walk_newest, walk_new = max(walk_newest, report["newest"]), walk_new + len(missing)
    if following and missing:
        db.execute("UPDATE players SET walk_newest = ?, walk_new = ?, fails = 0 WHERE platform = ? AND puuid = ?",
                   (walk_newest, walk_new) + where)
        return report["puuid"], 0, following, since, MORE_PAGES
    # It stops here: either everything since the last read has been seen, or this stretch had nothing
    # new and the rest, back to `since`, is set aside (merged with any part set aside before).
    if following:
        resume, floor = (following, since) if resume is None else (min(resume, following), min(floor, since))
    # Come back to the top when about TARGET_NEW new matches are expected, judging by how many this
    # read found in the time since the last one (or since the season began).
    elapsed = max(60, now - max(checked_at, season // 1000))
    interval = min(MAX_INTERVAL, max(MIN_INTERVAL, elapsed * TARGET_NEW / max(walk_new, 0.5)))
    db.execute("UPDATE players SET read_to = ?, checked_at = ?, due_at = ?, fails = 0, older_due = 0, "
               "walk_newest = 0, walk_new = 0, resume = ?, resume_floor = ? WHERE platform = ? AND puuid = ?",
               (max(read_to, walk_newest), now, now + int(interval), resume, floor) + where)
    return None


@app.errorhandler(413)
def too_large(_error):
    return fail(413, "body is too large")


@app.errorhandler(404)
def not_found(_error):
    return fail(404, "not found")


@app.errorhandler(405)
def wrong_method(_error):
    return fail(405, "method not allowed")


# ---------------------------------------------------------------------------
# Admin commands (run on the server by the operator)
# ---------------------------------------------------------------------------
def cmd_add_key(db, args):
    key = "as_" + secrets.token_urlsafe(24)
    with db:
        db.execute("INSERT INTO contributors(name, key_hash, created_at) VALUES(?, ?, ?)",
                   (args.name, hash_key(key), int(time.time())))
    print("Key for %s (shown once, only its hash is stored):\n%s" % (args.name, key))


def cmd_set_key(db, args, disabled):
    with db:
        changed = db.execute("UPDATE contributors SET disabled = ? WHERE name = ?", (disabled, args.name)).rowcount
    print("%s: %s" % (args.name, ("switched off" if disabled else "switched on") if changed else "no such contributor"))


def cmd_keys(db, _args):
    rows = db.execute("""
        SELECT c.name, c.disabled, COUNT(m.game_id), MAX(m.received_at), s.seen_at, s.state, s.version
        FROM contributors c LEFT JOIN games m ON m.contributor_id = c.id
        LEFT JOIN contributor_seen s ON s.contributor_id = c.id GROUP BY c.id ORDER BY c.name""").fetchall()
    when = lambda moment: time.strftime("%Y-%m-%d %H:%M", time.localtime(moment)) if moment else "never"
    for name, disabled, count, last, seen, state, version in rows:
        print("%-24s %-4s %8d matches   last upload: %-16s   last seen: %-16s %s %s" % (
            name, "OFF" if disabled else "on", count, when(last), when(seen), state or "", version or ""))


def cmd_purge(db, args):
    """Remove every match one contributor sent. They become wanted again, so other collectors re-fetch them."""
    with db:
        who = db.execute("SELECT id FROM contributors WHERE name = ?", (args.name,)).fetchone()
        if who is None:
            print("%s: no such contributor" % args.name)
            return
        db.execute("INSERT OR IGNORE INTO wanted(platform, game_id) SELECT platform, game_id FROM games "
                   "WHERE contributor_id = ?", who)
        removed = db.execute("DELETE FROM games WHERE contributor_id = ?", who).rowcount
    print("Removed %d matches that %s sent. They will be fetched again by other collectors." % (removed, args.name))


def cmd_stats(db, _args):
    now = int(clock())
    platforms = [row[0] for row in db.execute(
        "SELECT DISTINCT platform FROM players UNION SELECT DISTINCT platform FROM wanted "
        "UNION SELECT DISTINCT platform FROM given_up ORDER BY 1")]
    for platform in platforms:
        def one(sql, *args):
            return db.execute(sql, (platform,) + args).fetchone()[0] or 0
        print("%s" % platform)
        print("  matches held:      %d (%d received in the last 24 hours)" % (
            one("SELECT COUNT(*) FROM games WHERE platform = ?"),
            one("SELECT COUNT(*) FROM games WHERE platform = ? AND received_at > ?", now - 86400)))
        print("  matches to fetch:  %d (%d given up on)" % (
            one("SELECT COUNT(*) FROM wanted WHERE platform = ?"),
            one("SELECT COUNT(*) FROM given_up WHERE platform = ?")))
        print("  players known:     %d (%d never read, %d due to be read now, %d with an older part still to read)" % (
            one("SELECT COUNT(*) FROM players WHERE platform = ?"),
            one("SELECT COUNT(*) FROM players WHERE platform = ? AND checked_at = 0"),
            one("SELECT COUNT(*) FROM players WHERE platform = ? AND due_at <= ?", now),
            one("SELECT COUNT(*) FROM players WHERE platform = ? AND resume IS NOT NULL")))
    if not platforms:
        print("Nothing received yet.")
    relay = relay_status(db, now)
    print("Relay: %d matches waiting for the rankings server; last pull %s, last confirmed %s%s" % (
        relay["waiting"], *(time.strftime("%Y-%m-%d %H:%M", time.localtime(relay[name])) if relay[name] else "never"
                            for name in ("last_export_at", "last_ack_at")),
        "; UPLOADS PAUSED: " + relay["uploads_paused"] if relay["uploads_paused"] else ""))
    print("Event games asked for first: %d not received yet; list last sent %s" % (
        relay["urgent_not_held"], time.strftime("%Y-%m-%d %H:%M", time.localtime(relay["last_urgent_at"]))
        if relay["last_urgent_at"] else "never"))
    rules = get_policy(db)
    print("Policy: %s, season starts %s UTC" % (
        {name: rules[name] for name in ("enabled", "delay", "fast", "fast_delay", "fast_flight", "message")},
        time.strftime("%Y-%m-%d", time.gmtime(rules["season_start"] / 1000))))


def cmd_policy(db, args):
    season = date_ms(args.season_start) if args.season_start else None
    if args.fast_delay is not None and not 0 <= args.fast_delay <= 60:
        raise ValueError("--fast-delay must be between 0 and 60 seconds")
    if args.fast_flight is not None and not 1 <= args.fast_flight <= 100:
        raise ValueError("--fast-flight must be between 1 and 100")
    with db:
        before = get_policy(db)["season_start"]
        signups, fast = {"on": 1, "off": 0}.get(args.signups), {"on": 1, "off": 0}.get(args.fast)
        for name, value in (("enabled", args.enabled), ("delay", args.delay), ("message", args.message),
                            ("season_start", season), ("signups", signups), ("fast", fast),
                            ("fast_delay", args.fast_delay), ("fast_flight", args.fast_flight)):
            if value is not None:
                db.execute("INSERT OR REPLACE INTO settings VALUES(?, ?)", (name, str(value)))
        if season is not None and season < before:
            # Lists were only read back to the old date. Every list read so far gets the stretch between
            # the two dates set aside, to be read when there is room (from the top, as its place is unknown).
            changed = db.execute("UPDATE players SET resume = 0, resume_floor = ? WHERE checked_at > 0",
                                 (season,)).rowcount
            print("The season now starts earlier: %d match lists will be read again back to the new date." % changed)
    print("Policy: %s, new installs %s" % (get_policy(db), "can sign up" if signups_open(db) else "cannot sign up"))


def cmd_export(db, args):
    """Write the held matches as JSON lines, oldest first. Records already passed on to the rankings server
    and emptied here are not included: the rankings server keeps every match (ClickHouse collector_matches)."""
    count = emptied = 0
    sql, values = "SELECT payload FROM games", []
    if args.platform:
        sql, values = sql + " WHERE platform = ?", [args.platform]
    with gzip.open(args.out, "wt", encoding="ascii") as out:
        for (payload,) in db.execute(sql + " ORDER BY created_ms", values):
            if not payload:
                emptied += 1
                continue
            out.write(zlib.decompress(payload).decode() + "\n")
            count += 1
    print("Wrote %d matches to %s" % (count, args.out) +
          (" (%d more are held only by the rankings server)" % emptied if emptied else ""))


def relay_export(db, limit, out, now):
    """Write the next `limit` matches waiting for the rankings server, oldest first, one JSON object a line.
    Nothing is taken off the queue here: that waits for the rankings server's `ack`."""
    rows = db.execute("SELECT o.seq, o.platform, o.game_id, g.created_ms, g.received_at, g.contributor_id, g.payload "
                      "FROM outbox o JOIN games g ON g.platform = o.platform AND g.game_id = o.game_id "
                      "ORDER BY o.seq LIMIT ?", (limit,))
    count = 0
    for seq, platform, game_id, created, received, contributor, payload in rows:
        if not payload:
            raise RuntimeError("queued match %s %s has no record" % (platform, game_id))
        out.write(json.dumps({"seq": seq, "platform": platform, "game_id": game_id, "created_ms": created,
                              "received_at": received, "contributor": contributor,
                              "payload": base64.b64encode(payload).decode("ascii")}) + "\n")
        count += 1
    with db:
        db.execute("INSERT OR REPLACE INTO settings VALUES('relay_export_at', ?)", (str(now),))
    return count


def relay_ack(db, seqs, now):
    """The rankings server holds these matches (it checked them after storing them). Take them off the queue,
    then empty the records confirmed more than KEEP_SHIPPED_DAYS ago. Returns (taken off, emptied)."""
    taken = 0
    db.execute("BEGIN IMMEDIATE")
    with db:
        for start in range(0, len(seqs), 500):
            chunk = seqs[start:start + 500]
            marks = ",".join("?" * len(chunk))
            keys = db.execute("SELECT platform, game_id FROM outbox WHERE seq IN (%s)" % marks, chunk).fetchall()
            db.executemany("INSERT OR REPLACE INTO shipped VALUES(?, ?, ?)", [key + (now,) for key in keys])
            taken += db.execute("DELETE FROM outbox WHERE seq IN (%s)" % marks, chunk).rowcount
        if seqs:
            # Queued matches whose record was purged since (see `purge`) can never be sent; drop them too.
            db.execute("DELETE FROM outbox WHERE seq <= ? AND NOT EXISTS (SELECT 1 FROM games g "
                       "WHERE g.platform = outbox.platform AND g.game_id = outbox.game_id)", (max(seqs),))
        db.execute("INSERT OR REPLACE INTO settings VALUES('relay_ack_at', ?)", (str(now),))
    emptied = 0
    while True:
        db.execute("BEGIN IMMEDIATE")   # in small steps, so uploads never wait long
        with db:
            keys = db.execute("SELECT platform, game_id FROM shipped WHERE shipped_at < ? LIMIT 2000",
                              (now - KEEP_SHIPPED_DAYS * 86400,)).fetchall()
            db.executemany("UPDATE games SET payload = x'' WHERE platform = ? AND game_id = ?", keys)
            db.executemany("DELETE FROM shipped WHERE platform = ? AND game_id = ?", keys)
        emptied += len(keys)
        if len(keys) < 2000:
            return taken, emptied


def valid_urgent(game):
    return (isinstance(game, dict) and isinstance(game.get("platform"), str) and PLATFORM_RE.fullmatch(game["platform"])
            and is_int(game.get("game_id"), 1, 2 ** 53) and isinstance(game.get("players"), list)
            and len(game["players"]) <= MAX_URGENT_PLAYERS
            and all(isinstance(puuid, str) and PUUID_RE.fullmatch(puuid) and puuid.strip("0-") for puuid in game["players"]))


def relay_urgent(db, games, now):
    """The rankings server's list of ArenaSweats Events games it is waiting for, with their players. It replaces the
    last list. Until a game is held here it is handed out before any other match, and its players' lists before any
    other list (see `work`). Returns (matches, players) on the list."""
    keys = {(game["platform"], game["game_id"]) for game in games}
    players = {(game["platform"], game["game_id"], puuid) for game in games for puuid in game["players"]}
    db.execute("BEGIN IMMEDIATE")
    with db:
        # Matches and players still on the list keep when they were last handed out.
        db.executemany("DELETE FROM urgent_games WHERE platform = ? AND game_id = ?",
                       set(db.execute("SELECT platform, game_id FROM urgent_games")) - keys)
        db.executemany("DELETE FROM urgent_players WHERE platform = ? AND game_id = ? AND puuid = ?",
                       set(db.execute("SELECT platform, game_id, puuid FROM urgent_players")) - players)
        db.executemany("INSERT INTO urgent_games(platform, game_id, until) VALUES(?, ?, ?) "
                       "ON CONFLICT(platform, game_id) DO UPDATE SET until = excluded.until",
                       [key + (now + URGENT_KEEP_SECONDS,) for key in sorted(keys)])
        db.executemany("INSERT OR IGNORE INTO urgent_players(platform, game_id, puuid) VALUES(?, ?, ?)", sorted(players))
        db.executemany("INSERT OR IGNORE INTO players(platform, puuid, due_at) VALUES(?, ?, ?)",
                       sorted({(platform, puuid, now) for platform, _, puuid in players}))
        db.execute("INSERT OR REPLACE INTO settings VALUES('relay_urgent_at', ?)", (str(now),))
    return len(keys), len(players)


def relay_status(db, now):
    """How the relay and the collectors are doing, for the health check."""
    values = dict(db.execute("SELECT name, value FROM settings WHERE name LIKE 'relay_%'"))
    first = db.execute("SELECT g.received_at FROM outbox o JOIN games g ON g.platform = o.platform "
                       "AND g.game_id = o.game_id ORDER BY o.seq LIMIT 1").fetchone()
    platforms = {}
    for platform, held, newest in db.execute("SELECT platform, COUNT(*), MAX(received_at) FROM games GROUP BY platform"):
        hourly = [0] * 24
        for hours_ago, count in db.execute(
                "SELECT (? - received_at) / 3600, COUNT(*) FROM games WHERE received_at > ? AND platform = ? "
                "GROUP BY 1", (now, now - 86400, platform)):
            hourly[min(23, int(hours_ago))] += count
        senders = [db.execute("SELECT COUNT(DISTINCT contributor_id) FROM games WHERE received_at > ? AND platform = ?",
                              (now - seconds, platform)).fetchone()[0] for seconds in (3600, 86400)]
        platforms[platform] = {"held": held, "last_received_at": newest, "hourly_newest_first": hourly,
                               "senders_last_hour": senders[0], "senders_last_day": senders[1]}
    policy = get_policy(db)
    return {"now": now, "season_start": policy["season_start"], "collecting": policy["enabled"], "fast": policy["fast"],
            "signups": signups_open(db), "waiting": db.execute("SELECT COUNT(*) FROM outbox").fetchone()[0],
            "oldest_waiting_received_at": first[0] if first else None,
            "last_export_at": int(values["relay_export_at"]) if "relay_export_at" in values else None,
            "last_ack_at": int(values["relay_ack_at"]) if "relay_ack_at" in values else None,
            "last_urgent_at": int(values["relay_urgent_at"]) if "relay_urgent_at" in values else None,
            "urgent_not_held": db.execute(
                "SELECT COUNT(*) FROM urgent_games u WHERE u.until > ? AND NOT EXISTS "
                "(SELECT 1 FROM games g WHERE g.platform = u.platform AND g.game_id = u.game_id)", (now,)).fetchone()[0],
            "uploads_paused": backlog_full(db), "platforms": platforms,
            "installs_seen": {label: dict(db.execute(
                "SELECT state, COUNT(*) FROM contributor_seen WHERE seen_at > ? GROUP BY state", (now - seconds,)))
                for label, seconds in (("last_hour", 3600), ("last_day", 86400))}}


def cmd_relay(db, args):
    """The rankings server pulls matches with this, over SSH, using a key that may run nothing else
    (see server/README.md). The request is the arguments, or else SSH_ORIGINAL_COMMAND:
      export N   the next N matches waiting (at most RELAY_BATCH), oldest first, one JSON object a line
      ack        standard input is {"seqs": [...]}: those matches are safely held by the rankings server
      status     how the relay and the collectors are doing, as JSON
      urgent     standard input is {"games": [{"platform", "game_id", "players": [PUUIDs]}, ...]}: the ArenaSweats
                 Events games the rankings server is waiting for, replacing the last list; collectors are asked
                 for them, and their players' lists, before anything else
    """
    words = args.request or os.environ.get("SSH_ORIGINAL_COMMAND", "").split()
    now = int(clock())
    if len(words) == 2 and words[0] == "export" and words[1].isdigit() and 1 <= int(words[1]) <= RELAY_BATCH:
        relay_export(db, int(words[1]), sys.stdout, now)
    elif words == ["ack"]:
        seqs = json.loads(sys.stdin.read(10 * 1024 * 1024)).get("seqs")
        if not isinstance(seqs, list) or len(seqs) > RELAY_BATCH or not all(is_int(seq, 1, 2 ** 62) for seq in seqs):
            raise ValueError("ack expects {\"seqs\": [up to %d numbers]}" % RELAY_BATCH)
        taken, emptied = relay_ack(db, sorted(set(seqs)), now)
        print(json.dumps({"taken": taken, "emptied": emptied}))
    elif words == ["status"]:
        print(json.dumps(relay_status(db, now)))
    elif words == ["urgent"]:
        body = json.loads(sys.stdin.read(10 * 1024 * 1024))
        games = body.get("games") if isinstance(body, dict) else None
        if not isinstance(games, list) or len(games) > MAX_URGENT or not all(valid_urgent(game) for game in games):
            raise ValueError("urgent expects {\"games\": [up to %d {\"platform\", \"game_id\", \"players\": "
                             "[up to %d PUUIDs]}]}" % (MAX_URGENT, MAX_URGENT_PLAYERS))
        matches, players = relay_urgent(db, games, now)
        print(json.dumps({"games": matches, "players": players}))
    else:
        raise ValueError("relay expects: export N | ack | status | urgent")


def main(argv=None):
    parser = argparse.ArgumentParser(description="ArenaSweats collector ingest: admin commands.")
    commands = parser.add_subparsers(dest="command", required=True)
    serve = commands.add_parser("serve", help="run a development server (use gunicorn in production)")
    serve.add_argument("--port", type=int, default=8870)
    for name, text in (("add-key", "create a contributor and print their upload key"),
                       ("disable-key", "stop accepting a contributor's uploads"),
                       ("enable-key", "accept a contributor's uploads again"),
                       ("purge", "delete every match a contributor sent")):
        commands.add_parser(name, help=text).add_argument("name")
    commands.add_parser("keys", help="list contributors")
    commands.add_parser("stats", help="show what is held and what is waiting to be read")
    policy_parser = commands.add_parser("policy", help="show or change what collectors are told")
    policy_parser.add_argument("--on", dest="enabled", action="store_const", const=1, help="collectors may collect")
    policy_parser.add_argument("--off", dest="enabled", action="store_const", const=0, help="all collectors pause")
    policy_parser.add_argument("--delay", type=float, help="slowest pace to ask for, in seconds per request")
    policy_parser.add_argument("--fast", choices=("on", "off"),
                               help="whether collectors whose user turned fast mode on may use it (off: normal pace)")
    policy_parser.add_argument("--fast-delay", type=float, metavar="SECONDS",
                               help="fast mode's shortest gap between requests (default %g; collectors never go "
                                    "under 0.4 s whatever this says)" % DEFAULT_FAST_DELAY)
    policy_parser.add_argument("--fast-flight", type=int, metavar="N",
                               help="how many requests fast mode may have on their way at once (default %d; "
                                    "collectors never go above 4 whatever this says)" % DEFAULT_FAST_FLIGHT)
    policy_parser.add_argument("--message", help="short message shown to every collector ('' to clear)")
    policy_parser.add_argument("--signups", choices=("on", "off"),
                               help="whether new installs can get their own key automatically")
    policy_parser.add_argument("--season-start", metavar="YYYY-MM-DD",
                               help="nothing from before this UTC date is asked for or kept")
    relay_parser = commands.add_parser("relay", help="for the rankings server's SSH key: export N | ack | status | urgent")
    relay_parser.add_argument("request", nargs="*")
    export_parser = commands.add_parser("export", help="write the held matches to a .jsonl.gz file")
    export_parser.add_argument("out")
    export_parser.add_argument("--platform", help="only this server's matches, for example OC1")
    args = parser.parse_args(argv)

    if args.command == "serve":
        app.run(host="127.0.0.1", port=args.port)
        return 0
    db = connect()
    handlers = {"add-key": cmd_add_key, "keys": cmd_keys, "purge": cmd_purge, "stats": cmd_stats,
                "policy": cmd_policy, "export": cmd_export, "relay": cmd_relay,
                "disable-key": lambda d, a: cmd_set_key(d, a, 1), "enable-key": lambda d, a: cmd_set_key(d, a, 0)}
    try:
        handlers[args.command](db, args)
    except sqlite3.IntegrityError:
        print("A contributor with that name already exists.")
        return 1
    except ValueError as error:
        print("Not understood: %s" % error)
        return 1
    finally:
        db.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
