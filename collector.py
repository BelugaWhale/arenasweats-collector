#!/usr/bin/env python3
"""ArenaSweats collector: reads Arena match history from your own League client.

WHAT IT DOES
  1. Finds the League client that is running on this computer, through the
     "lockfile" in its folder or, when that cannot be found, the command line
     the client was started with (what Task Manager shows for it).
  2. While you are not playing, slowly asks that client for this season's Arena
     match history: match lists of players, and the full record of Arena matches.
  3. With an upload address and key set, ArenaSweats tells it which players and
     matches to read next (so no two collectors read the same thing) and it
     sends the matches there. Without one, it starts from your own matches,
     moves on to the players you met, and saves everything in the "data" folder
     beside this program.

WHAT IT NEVER DOES
  - It never changes anything. Every request to the League client is a GET to
    one of the five addresses in LCU_ALLOWED below; anything else is refused.
  - It never touches the game itself (its process, memory or files).
  - It never sends your login token, password or account details anywhere.
    What leaves this computer, and only to the upload address, is: match
    records, which Arena matches a player's list contains, and your own player
    id (the same id that is in your match records; a server with no players
    yet starts from it).
  - It needs no administrator rights and installs nothing. (When the League
    client runs as administrator, Windows hides where it is installed; the
    collector then looks in the usual places and, failing that, asks.)

Only the Python standard library is used, so this one file is the whole program.
"""

import argparse
import base64
import calendar
import gzip
import http.client
import json
import logging
import logging.handlers
import ntpath
import os
import random
import re
import socket
import sqlite3
import ssl
import sys
import tempfile
import time
import urllib.error
import urllib.request
import zipfile
import zlib
from collections import Counter

VERSION = "0.2.2"

# ---------------------------------------------------------------------------
# Limits. These keep the collector gentle. The upload server can slow it down
# or switch it off, but nothing can make it go faster than MIN_DELAY.
# ---------------------------------------------------------------------------
MIN_DELAY = 1.0        # hard floor: at most one match-history request per second
DEFAULT_DELAY = 2.0    # normal gap between match-history requests, in seconds
PAGE_SIZE = 20         # matches per match-list request
MAX_PAGES = 50         # never read further back than this many pages of one player's list
FIRST_PAGES = 1        # a list is read one page at first ...
MORE_PAGES = 3         # ... then three at a time, for as long as that turns up matches not seen before
SEASON_START = "2026-10-06"   # UTC. Nothing older is read. The upload server's date replaces this one.
MAX_WORK_GAMES = 50    # most matches to accept from the server in one round
MAX_WORK_PLAYERS = 20  # most match lists to accept from the server in one round
WORK_SECONDS = 5       # least time between two requests for work
RECHECK_HOURS = 6      # wait this long before reading the same player again
RETRY_SECONDS = 120    # ...or this long after a failed read, doubling with every failure in a row
RESUME_GRACE = 20      # seconds the client must stay idle before collection resumes
MAX_BACKOFF = 600      # longest extra wait after repeated failures, in seconds
OUTBOX_LIMIT = 2000    # stop collecting when this many matches are waiting to upload
UPLOAD_BATCH = 20      # matches per upload
POLICY_SECONDS = 600   # how often to ask the server whether collection should continue
TEST_MINUTES = 15      # length of a test run (no upload address set)
MAX_LCU_BYTES = 8 * 1024 * 1024

IDLE_PHASES = {"None", "Lobby"}   # client states in which collection is allowed
ARENA_MODES = {"CHERRY"}          # Riot's internal name for Arena

# A player id may only contain these characters, so it is safe inside an address.
PUUID = r"[0-9A-Za-z_-]{20,100}"
PUUID_RE = re.compile(PUUID)
PLATFORM_RE = re.compile(r"[A-Z0-9_]{2,12}")        # a server name such as OC1 or HN1

# The ONLY addresses this program may ask the League client for. All are GET.
LCU_ALLOWED = [re.compile(p) for p in (
    r"/lol-gameflow/v1/gameflow-phase",       # are you in a game / queue / lobby?
    r"/lol-summoner/v1/current-summoner",     # which account is logged in (to start from)
    r"/riotclient/region-locale",             # which region (written to the log only)
    r"/lol-match-history/v1/products/lol/" + PUUID + r"/matches\?begIndex=\d{1,4}&endIndex=\d{1,4}",
    r"/lol-match-history/v1/games/\d{1,20}",
)]

PENDING, UNAVAILABLE, REFUSED = 0, 2, 3   # states of a match in the local store (uploaded ones are deleted)

# Data from outside (the client, the upload server) is never trusted to have the right shape.
DATA_ERRORS = (TypeError, ValueError, KeyError, AttributeError, IndexError, OverflowError, RecursionError)


def parse_json(raw):
    """Parsed JSON, or None when the text is not usable JSON."""
    try:
        return json.loads(raw)
    except (ValueError, RecursionError):
        return None


def as_list(value):
    return value if isinstance(value, list) else []


def as_dict(value):
    return value if isinstance(value, dict) else {}

log = logging.getLogger("collector")


class ClientGone(Exception):
    """The League client stopped answering (closed or restarting)."""


class Done(Exception):
    """The time limit was reached."""


class WorkExpired(Exception):
    """Work from the upload server was held too long (for example through a game) and may be someone else's now."""


class Fatal(Exception):
    """Something the user has to fix; the message says what."""


class NoRedirect(urllib.request.HTTPRedirectHandler):
    """Never follow redirects, so requests only go to the address we chose."""

    def redirect_request(self, *args, **kwargs):
        return None


# ---------------------------------------------------------------------------
# Finding and talking to the League client
# ---------------------------------------------------------------------------
class Lcu:
    """The League client's local address (LCU). Read-only: GET, allowed paths only."""

    def __init__(self, port, token, scheme="https"):
        self.base = "%s://127.0.0.1:%d" % (scheme, port)
        auth = base64.b64encode(("riot:" + token).encode()).decode()
        self.headers = {"Authorization": "Basic " + auth, "Accept": "application/json"}
        # The client uses its own self-signed certificate on 127.0.0.1, which
        # cannot be verified. This traffic never leaves this computer.
        context = ssl.create_default_context()
        context.check_hostname = False
        context.verify_mode = ssl.CERT_NONE
        # ProxyHandler({}) = never send these local requests through a proxy.
        self.opener = urllib.request.build_opener(
            urllib.request.ProxyHandler({}), urllib.request.HTTPSHandler(context=context), NoRedirect())

    def get(self, path, timeout=20):
        """GET one allowed path. Returns (status, parsed JSON or None); status 0 = timed out."""
        if not any(pattern.fullmatch(path) for pattern in LCU_ALLOWED):
            raise ValueError("refused: not on the allowed list: " + path)
        request = urllib.request.Request(self.base + path, headers=self.headers, method="GET")
        try:
            try:
                with self.opener.open(request, timeout=timeout) as response:
                    status, body = response.status, response.read(MAX_LCU_BYTES)
            except urllib.error.HTTPError as error:
                status, body = error.code, error.read(MAX_LCU_BYTES)
        except (TimeoutError, socket.timeout):
            return 0, None
        except urllib.error.URLError as error:
            if isinstance(error.reason, (TimeoutError, socket.timeout)):
                return 0, None
            raise ClientGone(str(error.reason))
        except (OSError, http.client.HTTPException) as error:
            raise ClientGone(str(error))
        return status, parse_json(body)


def read_lockfile(path):
    """While it runs, the client keeps a file holding 'name:pid:port:token:protocol'."""
    with open(path, "r", encoding="utf-8", errors="replace") as handle:
        parts = handle.read().strip().split(":")
    if len(parts) < 5 or not parts[2].isdigit():
        raise ValueError("unexpected lockfile format (%d fields)" % len(parts))
    return Lcu(int(parts[2]), parts[3], "http" if parts[4] == "http" else "https")


CLIENT_NAMES = ("leagueclient.exe", "leagueclientux.exe")
WINDOW_PROGRAM = "leagueclientux.exe"     # the client's window program, started with its port and password
RELATED_WORDS = ("league", "lol", "riot", "wegame", "tcls", "tencent")   # program names worth noting in the log
LEAGUE_FOLDER_WORDS = ("英雄联盟", "league of legends")
GAME_FOLDER_WORDS = ("game", "游戏", "tencent", "腾讯", "riot", "lol")   # folders worth looking one level deeper in
SKIP_FOLDERS = {"windows", "users", "programdata", "recovery", "system volume information", "perflogs"}
SCAN_SECONDS = 20
noted = {}


def note(topic, text):
    """Write a line to the log, but only when it differs from the last line on the same topic."""
    if noted.get(topic) != text:
        noted[topic] = text
        log.info("%s: %s", topic, text)


_windows = None


def windows_api():
    """The few Windows functions used below, set up once (Windows only)."""
    global _windows
    if _windows is None:
        import ctypes
        from ctypes import wintypes

        class Entry(ctypes.Structure):      # one program in Windows' list of running programs
            _fields_ = [("dwSize", wintypes.DWORD), ("cntUsage", wintypes.DWORD),
                        ("th32ProcessID", wintypes.DWORD), ("th32DefaultHeapID", ctypes.c_size_t),
                        ("th32ModuleID", wintypes.DWORD), ("cntThreads", wintypes.DWORD),
                        ("th32ParentProcessID", wintypes.DWORD), ("pcPriClassBase", wintypes.LONG),
                        ("dwFlags", wintypes.DWORD), ("szExeFile", wintypes.WCHAR * 260)]

        class Text(ctypes.Structure):       # how Windows hands back a command line
            _fields_ = [("Length", wintypes.USHORT), ("MaximumLength", wintypes.USHORT), ("Buffer", ctypes.c_void_p)]

        k32 = ctypes.WinDLL("kernel32", use_last_error=True)
        k32.CreateToolhelp32Snapshot.restype = wintypes.HANDLE
        k32.CreateToolhelp32Snapshot.argtypes = (wintypes.DWORD, wintypes.DWORD)
        k32.Process32FirstW.argtypes = (wintypes.HANDLE, ctypes.POINTER(Entry))
        k32.Process32NextW.argtypes = (wintypes.HANDLE, ctypes.POINTER(Entry))
        k32.OpenProcess.restype = wintypes.HANDLE
        k32.OpenProcess.argtypes = (wintypes.DWORD, wintypes.BOOL, wintypes.DWORD)
        k32.QueryFullProcessImageNameW.argtypes = (
            wintypes.HANDLE, wintypes.DWORD, wintypes.LPWSTR, ctypes.POINTER(wintypes.DWORD))
        k32.CloseHandle.argtypes = (wintypes.HANDLE,)
        k32.GetDriveTypeW.argtypes = (wintypes.LPCWSTR,)
        nt = ctypes.WinDLL("ntdll")
        nt.NtQueryInformationProcess.restype = ctypes.c_long
        nt.NtQueryInformationProcess.argtypes = (
            wintypes.HANDLE, ctypes.c_ulong, ctypes.c_void_p, ctypes.c_ulong, ctypes.POINTER(ctypes.c_ulong))
        _windows = (ctypes, wintypes, k32, nt, Entry, Text)
    return _windows


def running_programs():
    """(process id, program name) of every running program, from Windows' list (Windows only)."""
    if os.name != "nt":
        return []
    ctypes, _, k32, _, Entry, _ = windows_api()
    snapshot = k32.CreateToolhelp32Snapshot(0x2, 0)      # 0x2 = the list of running programs
    if not snapshot or snapshot == ctypes.c_void_p(-1).value:
        return []
    programs = []
    entry = Entry()
    entry.dwSize = ctypes.sizeof(Entry)
    more = k32.Process32FirstW(snapshot, ctypes.byref(entry))
    while more:
        programs.append((entry.th32ProcessID, entry.szExeFile))
        more = k32.Process32NextW(snapshot, ctypes.byref(entry))
    k32.CloseHandle(snapshot)
    return programs


def query_program(pid, command_line=False):
    """Ask Windows for one running program's file path and, if asked, the command line it was
    started with (what Task Manager shows in its "Command line" column). Nothing inside the
    program is read. Returns (path, command line, Windows error code); a value is None when
    Windows will not say, which happens when that program runs as administrator and this one
    does not."""
    ctypes, wintypes, k32, nt, _, Text = windows_api()
    handle = k32.OpenProcess(0x1000, False, pid)        # 0x1000 = "limited information" only
    if not handle:
        return None, None, ctypes.get_last_error()
    try:
        path = line = None
        buffer = ctypes.create_unicode_buffer(32768)
        size = wintypes.DWORD(32768)
        if k32.QueryFullProcessImageNameW(handle, 0, buffer, ctypes.byref(size)):
            path = buffer.value
        if command_line:
            raw = ctypes.create_string_buffer(65536)
            if nt.NtQueryInformationProcess(handle, 60, raw, len(raw), None) == 0:   # 60 = its command line
                text = Text.from_buffer(raw)
                start = ctypes.addressof(raw)
                if text.Buffer and start <= text.Buffer and text.Buffer + text.Length <= start + len(raw):
                    line = ctypes.wstring_at(text.Buffer, text.Length // 2)
        return path, line, 0
    finally:
        k32.CloseHandle(handle)


def running_client_dirs(names=CLIENT_NAMES):
    """Folders of the running League client programs (Windows only). A folder is None when
    Windows would not say where the program is."""
    dirs, related, errors = [], set(), set()
    for pid, name in running_programs():
        if any(word in name.lower() for word in RELATED_WORDS):
            related.add(name)
        if name.lower() in names:
            path, _, error = query_program(pid)
            folder = os.path.dirname(path) if path else None
            if folder is None:
                errors.add(str(error))
            if folder not in dirs:
                dirs.append(folder)
    note("related programs running", ", ".join(sorted(related)) or "none")
    note("League client folders", ", ".join(
        folder or "(Windows would not say: error %s)" % "/".join(sorted(errors)) for folder in dirs)
        or "client not running")
    return dirs


def login_from_command_line(line):
    """(port, password) from the client's command line, or None."""
    port = re.search(r"--app-port=([0-9]{1,5})\b", line or "")
    token = re.search(r"--remoting-auth-token=([A-Za-z0-9_-]{1,200})", line or "")
    if port and token and 0 < int(port.group(1)) < 65536:
        return int(port.group(1)), token.group(1)
    return None


def command_line_logins():
    """(port, password, protocol) of the running client, taken from the command line its window
    program was started with. Used only when no lockfile works. Windows gives this only when it
    lets this program see the client. The password is never written anywhere."""
    logins = []
    for pid, name in running_programs():
        if name.lower() == WINDOW_PROGRAM:
            login = login_from_command_line(query_program(pid, command_line=True)[1])
            if login:
                logins.append(login + ("https",))
    note("client command line", "%d usable" % len(logins))
    return logins


def registry_text(key, name):
    import winreg
    try:
        value = winreg.QueryValueEx(key, name)[0]
    except OSError:
        return ""
    return value if isinstance(value, str) else ""


def folder_in(value):
    """The folder named in an installer's registry value: a folder, or a program or icon path
    with quotes, ",0" or " /S" around it."""
    value = (value or "").strip()
    if value.startswith('"'):
        value = value[1:].split('"', 1)[0]
    else:
        value = re.split(r",-?[0-9]+$| +/", value)[0]
    value = value.strip()
    if re.search(r"\.(exe|ico|dll)$", value, re.I):
        value = ntpath.dirname(value)
    return ntpath.normpath(value) if value else None


def installer_dirs():
    """Folders League's installers recorded in Windows' list of installed programs. They can
    point at a folder inside the install, such as its launcher's."""
    try:
        import winreg
    except ImportError:
        return []
    dirs = []
    for root in (winreg.HKEY_LOCAL_MACHINE, winreg.HKEY_CURRENT_USER):
        for path in (r"SOFTWARE\Microsoft\Windows\CurrentVersion\Uninstall",
                     r"SOFTWARE\WOW6432Node\Microsoft\Windows\CurrentVersion\Uninstall"):
            try:
                key = winreg.OpenKey(root, path)
            except OSError:
                continue
            with key:
                for index in range(winreg.QueryInfoKey(key)[0]):
                    try:
                        with winreg.OpenKey(key, winreg.EnumKey(key, index)) as entry:
                            if any(word in registry_text(entry, "DisplayName").lower() for word in LEAGUE_FOLDER_WORDS):
                                for field in ("InstallLocation", "DisplayIcon", "UninstallString"):
                                    folder = folder_in(registry_text(entry, field))
                                    if folder and folder not in dirs:
                                        dirs.append(folder)
                    except OSError:
                        continue
    return dirs


def installed_dirs():
    """Folders where League is usually installed, for when the running client's path is not available."""
    if sys.platform == "darwin":
        return ["/Applications/League of Legends.app/Contents/LoL"]
    if os.name != "nt":
        return []
    dirs = []
    # Riot's installer records the folder in this file.
    settings = os.path.join(os.environ.get("PROGRAMDATA", r"C:\ProgramData"), "Riot Games", "Metadata",
                            "league_of_legends.live", "league_of_legends.live.product_settings.yaml")
    try:
        with open(settings, "r", encoding="utf-8", errors="replace") as handle:
            found = re.search(r'product_install_full_path:\s*"?([^"\r\n]+)', handle.read())
        if found:
            dirs.append(os.path.normpath(found.group(1).strip().replace("\\\\", "\\")))
    except OSError:
        pass
    # The Chinese (Tencent) installer records the folder in the registry.
    try:
        import winreg
        for root, key in ((winreg.HKEY_CURRENT_USER, r"Software\Tencent\LOL"),
                          (winreg.HKEY_LOCAL_MACHINE, r"SOFTWARE\WOW6432Node\Tencent\LOL")):
            try:
                with winreg.OpenKey(root, key) as opened:
                    dirs.append(str(winreg.QueryValueEx(opened, "InstallPath")[0]))
            except OSError:
                pass
    except ImportError:
        pass
    # Common folder names on every drive.
    for letter in "CDEFGHIJKLMNOPQRSTUVWXYZ":
        if os.path.isdir(letter + ":\\"):
            for name in (r"Riot Games\League of Legends", "WeGameApps\\英雄联盟", "英雄联盟",
                         "腾讯游戏\\英雄联盟", "Program Files\\腾讯游戏\\英雄联盟"):
                dirs.append(letter + ":\\" + name)
    return dirs


def fixed_drives():
    """The computer's own drives (not network or removable ones)."""
    if os.name != "nt":
        return []
    k32 = windows_api()[2]
    return [letter + ":\\" for letter in "CDEFGHIJKLMNOPQRSTUVWXYZ" if k32.GetDriveTypeW(letter + ":\\") == 3]


def looks_like_league(folder):
    name = os.path.basename(folder).lower()
    return (any(word in name for word in LEAGUE_FOLDER_WORDS)
            or os.path.isfile(os.path.join(folder, "LeagueClient", "LeagueClient.exe"))
            or os.path.isfile(os.path.join(folder, "LeagueClient.exe")))


scanned = None


def scan_drives(roots=None, seconds=SCAN_SECONDS):
    """League installs anywhere in the top levels of the computer's drives, found by folder
    name or by the client program inside. Only folder names are looked at. Done once, and only
    when the client is running but could not be found any other way."""
    global scanned
    if roots is None and scanned is not None:
        return scanned
    end = time.monotonic() + seconds
    found, waiting = [], [(root, 0) for root in (fixed_drives() if roots is None else roots)]
    while waiting and time.monotonic() < end:
        folder, depth = waiting.pop(0)
        try:
            with os.scandir(folder) as entries:
                children = sorted(entry.path for entry in entries if entry.is_dir(follow_symlinks=False))
        except OSError:
            continue
        for child in children[:2000]:
            name = os.path.basename(child).lower()
            if name in SKIP_FOLDERS or name[:1] in ("$", "."):
                continue
            if looks_like_league(child):
                found.append(child)
            elif depth < 1 or (depth < 2 and any(word in name for word in GAME_FOLDER_WORDS)):
                waiting.append((child, depth + 1))
    note("League folders found on the drives", ", ".join(found) or "none")
    if roots is None:
        scanned = found
    return found


def lockfile_candidates(folder, parents=0):
    """Where the lockfile can be for a folder that is, or is up to `parents` levels inside, the install."""
    paths = []
    for _ in range(parents + 1):
        paths += [os.path.join(folder, "lockfile"), os.path.join(folder, "LeagueClient", "lockfile")]
        parent = os.path.dirname(folder)
        if parent == folder:
            break
        folder = parent
    return paths


def existing_lockfiles(*groups):
    """The lockfiles that exist, from groups of (folders, parents), without repeats."""
    paths = []
    for folders, parents in groups:
        for folder in folders:
            for path in lockfile_candidates(folder, parents):
                if path not in paths and os.path.isfile(path):
                    paths.append(path)
    return paths


def given_folder(league_dir):
    """A folder typed in by the user: quotes removed, and a file path turned into its folder."""
    league_dir = (league_dir or "").strip().strip('"')
    return os.path.dirname(league_dir) if os.path.isfile(league_dir) else league_dir


def lockfile_paths(league_dir=None):
    """Existing lockfiles, most likely first.

    Returns (paths, client_running, hidden): whether a League client program is running,
    and whether Windows would not say where it is installed."""
    running = running_client_dirs()
    given = [given_folder(league_dir)] if league_dir else []
    groups = [(given, 2), ([folder for folder in running if folder], 0), (installer_dirs(), 2), (installed_dirs(), 0)]
    paths = existing_lockfiles(*groups)
    if running and not paths:
        paths = existing_lockfiles((scan_drives(), 0))
    note("lockfile search", "found: %s" % (paths or "none"))
    return paths, bool(running), None in running


# ---------------------------------------------------------------------------
# Reading the client's match data
# ---------------------------------------------------------------------------
def match_list(data):
    """The list of matches inside a match-list answer."""
    games = data.get("games") if isinstance(data, dict) else None
    if isinstance(games, dict):
        games = games.get("games")
    return [game for game in games if isinstance(game, dict)] if isinstance(games, list) else []


def is_arena(game):
    return isinstance(game.get("gameMode"), str) and game["gameMode"] in ARENA_MODES


def match_id(game):
    """A match's id, or None when it is not a usable number."""
    value = game.get("gameId")
    return value if isinstance(value, int) and not isinstance(value, bool) and 0 < value < 2 ** 53 else None


def date_ms(text):
    """'2026-10-06' (UTC midnight) as milliseconds."""
    return calendar.timegm(time.strptime(text, "%Y-%m-%d")) * 1000


def created_ms(game):
    """When a match started, in milliseconds, or None when the client does not say."""
    value = game.get("gameCreation")
    if isinstance(value, int) and not isinstance(value, bool) and 0 < value < 2 ** 53:
        return value
    return None


def player_ids(game):
    """The player ids in a full match record (computer players are skipped)."""
    ids = []
    for identity in as_list(game.get("participantIdentities")):
        puuid = as_dict(as_dict(identity).get("player")).get("puuid")
        if isinstance(puuid, str) and PUUID_RE.fullmatch(puuid) and puuid.strip("0-"):
            ids.append(puuid)
    return ids


def describe(game):
    """A one-line description of a match record's shape, for the log."""
    participants = as_list(game.get("participants"))
    stats = as_dict(as_dict(participants[0] if participants else None).get("stats"))
    wanted = ("subteamPlacement", "playerSubteamId", "playerAugment1", "win")
    return "platform=%.20s queue=%.10s version=%.30s players=%d identities=%d fields: %s" % (
        game.get("platformId"), game.get("queueId"), game.get("gameVersion"), len(participants),
        len(as_list(game.get("participantIdentities"))),
        ", ".join("%s=%s" % (name, "yes" if name in stats else "NO") for name in wanted))


# ---------------------------------------------------------------------------
# Local store: one small SQLite file per account, in the data folder
# ---------------------------------------------------------------------------
class Store:
    """Which players and matches we know, and the matches waiting to be uploaded."""

    def close(self):
        self.db.close()

    def __init__(self, path):
        self.db = sqlite3.connect(path)
        self.db.execute("PRAGMA journal_mode=WAL")      # fewer disk flushes
        self.db.execute("PRAGMA synchronous=NORMAL")
        self.db.executescript("""
            CREATE TABLE IF NOT EXISTS players(
                puuid TEXT PRIMARY KEY,
                seen INTEGER NOT NULL DEFAULT 0,        -- Arena matches we saw this player in
                checked_at INTEGER NOT NULL DEFAULT 0,  -- when we last read their history
                newest_ms INTEGER NOT NULL DEFAULT 0,   -- their history is read up to this time
                fails INTEGER NOT NULL DEFAULT 0);      -- reads that failed in a row
            CREATE TABLE IF NOT EXISTS games(
                platform TEXT NOT NULL, game_id INTEGER NOT NULL, created_ms INTEGER,
                payload BLOB,                           -- the match record, compressed
                state INTEGER NOT NULL DEFAULT 0,       -- 0 waiting, 2 not available, 3 refused
                PRIMARY KEY(platform, game_id));
            CREATE INDEX IF NOT EXISTS games_by_state ON games(state, created_ms);
        """)
        try:        # the part of a list still to read, kept for test runs (added after the first version)
            self.db.execute("ALTER TABLE players ADD COLUMN resume INTEGER")
        except sqlite3.OperationalError:
            pass

    def one(self, sql, *args):
        row = self.db.execute(sql, args).fetchone()
        return row[0] if row else None

    def add_players(self, puuids, seen=1):
        with self.db:
            self.db.executemany("INSERT INTO players(puuid, seen) VALUES(?, ?) ON CONFLICT(puuid) "
                                "DO UPDATE SET seen = seen + excluded.seen", [(puuid, seen) for puuid in puuids])

    def next_player(self, stale_before):
        """The most active player whose history has not been read recently."""
        return self.one("SELECT puuid FROM players WHERE checked_at < ? "
                        "ORDER BY seen DESC, checked_at ASC LIMIT 1", int(stale_before))

    def set_aside(self, puuid, place):
        """Note that a player's list is unread from `place` on (None: nothing left), keeping the earliest place."""
        with self.db:
            if place is None:
                self.db.execute("UPDATE players SET resume = NULL WHERE puuid = ?", (puuid,))
            else:
                self.db.execute("UPDATE players SET resume = MIN(COALESCE(resume, ?), ?) WHERE puuid = ?",
                                (place, place, puuid))

    def next_older_part(self):
        """The player with a part of their list still unread who turned up in the most matches."""
        row = self.db.execute("SELECT puuid, resume FROM players WHERE resume IS NOT NULL "
                              "ORDER BY seen DESC LIMIT 1").fetchone()
        return row if row else (None, None)

    def newest_ms(self, puuid):
        return self.one("SELECT newest_ms FROM players WHERE puuid = ?", puuid) or 0

    def mark_checked(self, puuid, newest_ms, when, failed=False):
        """Note that a player's history was read. After a failure: come back sooner, but later each time."""
        with self.db:
            fails = 0
            if failed:
                fails = (self.one("SELECT fails FROM players WHERE puuid = ?", puuid) or 0) + 1
                retry = min(RECHECK_HOURS * 3600, RETRY_SECONDS * 2 ** min(fails - 1, 20))
                when = when - RECHECK_HOURS * 3600 + retry
            self.db.execute("UPDATE players SET checked_at = ?, newest_ms = ?, fails = ? WHERE puuid = ?",
                            (int(when), newest_ms, fails, puuid))

    def has_game(self, platform, game_id):
        return self.one("SELECT 1 FROM games WHERE platform = ? AND game_id = ?", platform, game_id) is not None

    def save_game(self, platform, game_id, game, state=PENDING):
        payload = zlib.compress(json.dumps(game, separators=(",", ":")).encode()) if game else None
        with self.db:
            self.db.execute("INSERT OR REPLACE INTO games VALUES(?, ?, ?, ?, ?)",
                            (platform, game_id, created_ms(game) if game else None, payload, state))

    def pending(self, limit):
        rows = self.db.execute("SELECT platform, game_id, payload FROM games WHERE state = ? "
                               "ORDER BY created_ms LIMIT ?", (PENDING, limit)).fetchall()
        return [((platform, game_id), json.loads(zlib.decompress(payload))) for platform, game_id, payload in rows]

    def mark_uploaded(self, keys):
        """The server has these now, so they are removed from this computer."""
        with self.db:
            self.db.executemany("DELETE FROM games WHERE platform = ? AND game_id = ?", keys)

    def mark_refused(self, keys):
        """The server would not take these. The record is kept so nothing is lost."""
        with self.db:
            self.db.executemany("UPDATE games SET state = 3 WHERE platform = ? AND game_id = ?", keys)

    def retry_refused(self):
        with self.db:
            self.db.execute("UPDATE games SET state = 0 WHERE state = 3")

    def count(self, state):
        return self.one("SELECT COUNT(*) FROM games WHERE state = ?", state)

    def count_games(self):
        return self.one("SELECT COUNT(*) FROM games WHERE state != ?", UNAVAILABLE)

    def stored_games(self):
        """Every match record still held locally."""
        for (payload,) in self.db.execute("SELECT payload FROM games WHERE payload IS NOT NULL ORDER BY created_ms"):
            yield json.loads(zlib.decompress(payload))


# ---------------------------------------------------------------------------
# The upload address
# ---------------------------------------------------------------------------
class Server:
    """ArenaSweats' upload address. Match records and the upload key are all it is sent."""

    def __init__(self, url, key):
        self.url = url.rstrip("/")
        local_test = re.match(r"http://(127\.0\.0\.1|localhost)(:\d+)?(/|$)", self.url)
        if not (self.url.startswith("https://") or local_test):
            raise Fatal("The upload address must start with https://")
        if not key:
            raise Fatal("An upload address is set but there is no upload key.")
        self.key = key
        self.opener = urllib.request.build_opener(NoRedirect())

    def call(self, method, path, payload=None):
        """Returns (status, parsed JSON or None, seconds the server asked us to wait). Status 0 = unreachable."""
        headers = {"Authorization": "Bearer " + self.key, "Accept": "application/json",
                   "User-Agent": "arenasweats-collector/" + VERSION}
        body = None
        if payload is not None:
            body = gzip.compress(json.dumps(payload, separators=(",", ":")).encode())
            headers.update({"Content-Type": "application/json", "Content-Encoding": "gzip"})
        request = urllib.request.Request(self.url + path, data=body, headers=headers, method=method)
        try:
            try:
                with self.opener.open(request, timeout=30) as response:
                    status, raw, retry = response.status, response.read(65536), response.headers.get("Retry-After")
            except urllib.error.HTTPError as error:
                status, raw, retry = error.code, error.read(65536), error.headers.get("Retry-After")
        except (OSError, http.client.HTTPException) as error:
            log.info("upload server unreachable: %s", error)
            return 0, None, 0
        wait = int(retry) if retry and re.fullmatch(r"[0-9]{1,5}", retry) else 0
        return status, parse_json(raw), min(3600, wait)


# ---------------------------------------------------------------------------
# The collector
# ---------------------------------------------------------------------------
class Collector:
    def __init__(self, opts, data_dir, server=None, sleep=time.sleep, now=time.monotonic, wall=time.time):
        self.opts, self.data_dir, self.server = opts, data_dir, server
        self._sleep, self.now, self.wall = sleep, now, wall     # replaceable so tests need not wait
        self.test_run = server is None
        self.deadline = now() + opts.minutes * 60 if opts.minutes else None
        self.lcu = self.store = self.me = None
        self.checked_me = False
        self.season_start = opts.since     # ms; nothing older is read
        self.platform = None               # which server this account is on, learned from a match list
        self.reports, self.last_work, self.work_failures = [], float("-inf"), 0    # dealings with the upload server
        self.introduced = False
        self.work_deadline = None          # work from the server must be done before this time (see WorkExpired)
        self.older_rest = {}               # player -> when a failing older part may be tried again (test runs)
        self.next_allowed = 0.0            # earliest time of the next match-history request
        self.failures = 0                  # match-history requests that failed in a row
        self.busy_at = float("-inf")       # last time the client was seen busy
        self.paused = False
        self.enabled, self.policy_delay, self.message, self.policy_at = True, 0.0, "", now()
        self.batch, self.next_upload, self.last_upload, self.upload_failures = UPLOAD_BATCH, 0.0, float("-inf"), 0
        self.stats, self.modes = Counter(), Counter()
        self.wrong_records, self.failed_fetches, self.rests = Counter(), Counter(), Counter()   # per match id
        self.rest_until = {}               # match id -> when a match that keeps failing may be tried again
        self.owed = {}                     # match id -> list entry, for matches still to be fetched after a failure
        self.last_said, self.last_status, self.folder_asks, self.described = None, now(), 0, False

    # -- small helpers -------------------------------------------------------
    @property
    def delay(self):
        """Seconds between match-history requests. The server can raise it, nothing can lower it below MIN_DELAY."""
        return max(MIN_DELAY, self.opts.delay, self.policy_delay)

    def say(self, text, once=False):
        if once and text == self.last_said:
            return
        self.last_said = text
        log.info("%s", text)
        print(time.strftime("%H:%M:%S ") + text, flush=True)

    def sleep(self, seconds):
        end = self.now() + seconds
        while True:
            if self.deadline is not None and self.now() >= self.deadline:
                raise Done()
            left = end - self.now()
            if left <= 0:
                return
            self._sleep(min(left, 1.0))

    # -- main loop -----------------------------------------------------------
    def run(self):
        if self.server:
            self.refresh_policy(first=True)     # a wrong key stops here, before anything is read
        try:
            while True:
                try:
                    if self.lcu is None:
                        self.connect()
                    self.step()
                except ClientGone as error:
                    log.info("client gone: %s", error)
                    self.lcu = None
                    self.say("The League client closed. Waiting for it to open again.")
                    self.sleep(10)
        except Done:
            self.say("Time limit reached.")
        except Fatal:
            raise
        except Exception as error:
            log.exception("unexpected error")       # logged here so a test run's report includes it
            raise Fatal("Unexpected error. Details are in collector.log in the data folder.") from error
        finally:
            self.finish()

    def connect(self):
        """Wait for a running, logged-in League client."""
        rounds = 0
        while True:
            paths, running, hidden = lockfile_paths(self.opts.league_dir)
            results = []
            for path in paths:
                results.append(self.try_client(path, lambda path=path: read_lockfile(path)))
                if results[-1] == "connected":
                    return
            if running and "answered" not in results:
                for port, token, scheme in command_line_logins():
                    results.append(self.try_client("the client's command line",
                                                   lambda port=port, token=token, scheme=scheme: Lcu(port, token, scheme)))
                    if results[-1] == "connected":
                        return
            rounds += 1
            if "answered" in results:
                self.say("Found the League client. Waiting for you to log in.", once=True)
            elif running:
                self.say("The League client is open, but the collector cannot reach it yet.", once=True)
                if rounds >= 2 and self.folder_asks < 3 and sys.stdin and sys.stdin.isatty():
                    self.ask_for_folder(hidden)
                    continue
            else:
                self.say("Waiting for the League client. Open it and log in.", once=True)
            self.sleep(10)

    def try_client(self, where, make):
        """Try one way of reaching the client: "connected" (and in use), "answered" (reachable
        but nobody logged in yet) or "unreachable"."""
        try:
            lcu = make()
            status, me = lcu.get("/lol-summoner/v1/current-summoner")
        except (OSError, ValueError, ClientGone) as error:
            log.info("client via %s not usable: %s", where, error)
            return "unreachable"
        puuid = me.get("puuid") if isinstance(me, dict) else None
        if status == 200 and isinstance(puuid, str) and PUUID_RE.fullmatch(puuid):
            self.use_client(lcu, puuid, where)
            return "connected"
        log.info("client via %s not ready: status %s, player id %s", where, status,
                 "missing" if not puuid else "in an unexpected format")
        return "answered"

    def ask_for_folder(self, hidden):
        """Ask where League is installed, when the client is open but cannot be found."""
        self.folder_asks += 1
        if hidden:
            print("Windows does not let the collector see where League is installed. This happens when\n"
                  "the League client runs as administrator. Either tell the collector the folder below, or\n"
                  "close this window and start the collector with right-click > 'Run as administrator'.")
        print("To find the folder, right-click your League shortcut and choose 'Open file location'.")
        try:
            folder = given_folder(input("Paste the folder where League is installed and press Enter: "))
        except EOFError:
            self.folder_asks = 3
            return
        if folder and existing_lockfiles(([folder], 2)):
            log.info("folder given: %s", folder)
            self.opts.league_dir = folder
            save_config(league_dir=folder)
        elif folder:
            print("No League client was found in or near that folder. Check the client is open and logged in.")

    def use_client(self, lcu, puuid, path):
        self.lcu = lcu
        if puuid != self.me:
            self.me, self.checked_me = puuid, False
            self.close()        # another account: it keeps its own file, and the last one's is left as it is
            self.store = Store(os.path.join(self.data_dir, "collector-%s.db" % puuid[:8]))
            self.store.add_players([puuid], seen=0)
            self.store.retry_refused()       # offer matches the server refused before once more
        status, region = lcu.get("/riotclient/region-locale")
        log.info("connected via %s, account %s..., region-locale: %s %s", path, puuid[:8], status, region)
        self.say("Connected to the League client%s." % (" in " + os.path.dirname(path) if os.path.isabs(path) else ""))
        self.say("Collecting only while you are not playing, one request every %.1f seconds, matches since %s."
                 % (self.delay, time.strftime("%Y-%m-%d", time.gmtime(self.season_start / 1000))))

    def step(self):
        self.upload()
        if self.server and self.store.count(PENDING) + self.store.count(REFUSED) >= OUTBOX_LIMIT:
            self.say("ArenaSweats is not taking uploads from this collector right now, so collection is paused.",
                     once=True)
            self.sleep(60)
        elif self.server:
            self.work_round()
        else:
            self.own_round()
        if self.now() - self.last_status >= 30:
            self.status()

    # -- deciding what to read, on its own (no upload address) -----------------------
    def own_round(self):
        """Read the next player: me first, then whoever turned up in the most matches so far."""
        for game_id, summary in list(self.owed.items()):       # first, matches whose fetch failed earlier
            if self.fetch_game(summary):
                del self.owed[game_id]
        puuid = self.store.next_player(self.wall() - RECHECK_HOURS * 3600) if self.checked_me else self.me
        if puuid is None:
            if not self.own_older_part():
                self.say("Nothing new to read right now. Looking again in a minute.", once=True)
                self.sleep(60)
            return
        try:
            known = self.store.newest_ms(puuid)
            newest, complete, begin = known, True, 0
            while True:
                # Read a stretch of the list and fetch its Arena matches. Go further back only while
                # that turns up matches not seen before.
                arena, top, ok, begin = self.read_list(puuid, max(known, self.season_start), begin,
                                                       MORE_PAGES if begin else FIRST_PAGES)
                was_new = [not self.store.has_game(self.platform_of(game), match_id(game)) for game in arena]
                for game in arena:
                    if not self.fetch_game(game):
                        self.owed[match_id(game)] = game            # tried again at the start of later rounds
                complete = complete and ok
                newest = max(newest, top)
                if begin and not any(was_new):
                    self.store.set_aside(puuid, begin)     # read the rest later, when nothing fresher is waiting
                if not begin or not any(was_new):
                    break
            # If a list request failed, keep the old mark so the list is read again.
            self.store.mark_checked(puuid, newest if complete else known, self.wall(), failed=not complete)
        except DATA_ERRORS:
            log.exception("unexpected data while reading player %s", puuid[:8])
            self.store.mark_checked(puuid, self.store.newest_ms(puuid), self.wall(), failed=True)
        self.checked_me = True

    def own_older_part(self):
        """Read a stretch of a list's part that was set aside. False when there is none to read now."""
        puuid, place = self.store.next_older_part()
        if puuid is None or self.now() < self.older_rest.get(puuid, 0):
            return False
        try:
            arena, _, ok, following = self.read_list(puuid, self.season_start, place, MORE_PAGES)
        except DATA_ERRORS:
            log.exception("unexpected data while reading player %s", puuid[:8])
            arena, ok, following = [], False, place
        for game in arena:
            if not self.fetch_game(game):
                self.owed[match_id(game)] = game
        if ok:
            self.store.set_aside(puuid, None)
            if following:
                self.store.set_aside(puuid, following)
        else:
            self.older_rest[puuid] = self.now() + 600
        return True

    @staticmethod
    def platform_of(game):
        platform = game.get("platformId")
        return platform if isinstance(platform, str) and PLATFORM_RE.fullmatch(platform) else ""

    def fetch_game(self, summary):
        """Fetch one Arena match in full. True when it is dealt with, False when it should be tried again later."""
        game_id, platform = match_id(summary), self.platform_of(summary)
        if self.store.has_game(platform, game_id):
            return True
        if self.now() < self.rest_until.get(game_id, 0):
            return False                # it failed three times lately: leave it alone for a while
        game, problem = self.get_game(game_id, platform)
        if game:
            self.store.save_game(platform, game_id, game)
            self.store.add_players(player_ids(game))
            return True
        if problem == "request failed":         # says nothing about the match itself
            self.failed_fetches[game_id] += 1
            if self.failed_fetches[game_id] % 3 == 0:      # rest 1 hour, then 2, 4 ... up to 32
                self.rests[game_id] += 1
                self.rest_until[game_id] = self.now() + 3600 * 2 ** min(self.rests[game_id] - 1, 5)
            return False
        self.wrong_records[game_id] += 1
        if self.wrong_records[game_id] < 3:
            return False
        self.store.save_game(platform, game_id, None, UNAVAILABLE)   # answered wrongly three times: give up
        return True

    # -- deciding what to read, with ArenaSweats (upload address set) ----------------
    def work_round(self):
        """Tell ArenaSweats what the match lists just read contained, then read and fetch what it hands out."""
        if self.platform is None:
            # One look at the top of my own match list shows which server this account is on.
            self.read_list(self.me, int(self.wall() * 1000), 0, 1)
            if self.platform is None:
                self.say("Could not tell yet which server this account is on. Trying again in a minute.", once=True)
                self.sleep(60)
                return
        self.gate()         # work is only taken on when it can be done straight away (a pause hands in first)
        work = self.ask_for_work()
        if work is None:
            return
        games = [i for i in as_list(work.get("games"))[:MAX_WORK_GAMES] if match_id({"gameId": i})]
        players = [as_dict(item) for item in as_list(work.get("players"))[:MAX_WORK_PLAYERS]]
        lease = work.get("lease")
        lease = lease if isinstance(lease, int) and not isinstance(lease, bool) and 120 <= lease <= 3600 else 900
        self.work_deadline = self.now() + lease - 60
        try:
            for game_id in games:
                game, _ = self.get_game(game_id, self.platform)
                if game:
                    self.store.save_game(self.platform, game_id, game)
                    self.upload()
            for item in players:
                def number(name, low, high, default):
                    value = item.get(name)
                    good = isinstance(value, int) and not isinstance(value, bool) and low <= value <= high
                    return value if good else default
                if isinstance(item.get("puuid"), str) and PUUID_RE.fullmatch(item["puuid"]):
                    report = self.list_report(
                        item["puuid"], max(number("since", 0, 2 ** 53, 0), self.season_start),
                        number("begin", 0, MAX_PAGES * PAGE_SIZE, 0), number("pages", 1, 10, FIRST_PAGES))
                    job = item.get("job")
                    report["job"] = job[:64] if isinstance(job, str) else None
                    self.reports.append(report)
        except WorkExpired:
            # Held too long (usually through a game): the server has handed the rest to someone else.
            log.info("the rest of this round's work waited too long; it is left to other collectors")
        finally:
            self.work_deadline = None
        self.upload(final=True)
        if not games and not players:
            wait = work.get("wait")
            wait = wait if isinstance(wait, (int, float)) and not isinstance(wait, bool) and 10 <= wait <= 3600 else 60
            self.say("ArenaSweats has nothing for this collector to read right now. Asking again shortly.", once=True)
            self.sleep(wait)

    def list_report(self, puuid, since, begin, pages):
        """Read a stretch of a player's match list and put what ArenaSweats needs to know about it in a small record."""
        try:
            arena, newest, complete, following = self.read_list(puuid, since, begin, pages)
        except DATA_ERRORS:
            log.exception("unexpected data while reading player %s", puuid[:8])
            arena, newest, complete, following = [], 0, False, 0
        return {"puuid": puuid, "ids": [match_id(game) for game in arena], "newest": newest, "begin": begin,
                "next": following, "failed": not complete}

    def ask_for_work(self, more=True):
        """Send the waiting match-list reports. Returns what to do next, or None when the server did not answer."""
        if more:                        # (the last hand-in at the end of a run does not wait)
            self.sleep(self.last_work + WORK_SECONDS - self.now())
        self.last_work = self.now()
        request = {"version": VERSION, "platform": self.platform, "reports": self.reports[:MAX_WORK_PLAYERS],
                   "more": more}
        if not self.introduced:         # a server that knows no players yet starts from this account's list
            request["start"] = self.me
        status, work, retry = self.server.call("POST", "/v1/work", request)
        if status in (401, 403):
            raise self.bad_key()
        if status != 200 or not isinstance(work, dict):
            self.work_failures += 1
            wait = max(retry, min(600, 30 * 2 ** min(self.work_failures - 1, 10)))
            log.warning("asking for work failed (status %s); next try in %ds", status, wait)
            if self.work_failures == 3:
                self.say("Cannot reach ArenaSweats right now. Trying again later.")
            del self.reports[:-MAX_WORK_PLAYERS]        # keep only the latest reports while it is away
            if more:
                self.sleep(wait)
            return None
        self.work_failures, self.introduced = 0, True
        del self.reports[:MAX_WORK_PLAYERS]
        self.apply_policy(work.get("policy"))
        return work

    # -- reading match history -------------------------------------------------
    def wait_until_idle(self):
        """Return only when the client is idle (not in queue, champion select, a game or post-game)."""
        while True:
            status, phase = self.lcu.get("/lol-gameflow/v1/gameflow-phase", timeout=10)
            if status == 200 and isinstance(phase, str) and phase in IDLE_PHASES:
                if self.now() - self.busy_at >= RESUME_GRACE:
                    if self.paused:
                        self.paused = False
                        self.say("You are not playing any more. Collection resumed.")
                    return
            else:
                self.busy_at = self.now()
                if not self.paused:
                    self.paused = True
                    if status == 200:
                        self.say("Paused while you play (client state: %.40s)." % (phase,))
                    else:
                        self.say("Paused: the League client is not ready yet (status %s)." % status)
                    self.hand_in()      # so nothing that is ready waits through the game
            self.sleep(5)

    def hand_in(self):
        """Send ArenaSweats the matches and match-list reports that are ready, taking on no new work."""
        if self.server:
            self.upload(final=True)
            if self.reports and self.platform:
                self.ask_for_work(more=False)

    def gate(self):
        """Return only when collecting is allowed: you are not playing and ArenaSweats has not switched it off."""
        while True:
            self.wait_until_idle()
            if self.server and self.now() - self.policy_at >= POLICY_SECONDS:
                self.refresh_policy()
                continue                # that took a moment, so look at the client again
            if self.enabled:
                return
            self.say("ArenaSweats has switched collection off for now. Checking again every 10 minutes.", once=True)
            self.hand_in()
            self.sleep(60)

    def history_get(self, path):
        """One match-history request: paced, and only while collecting is allowed. None on failure."""
        while True:                     # wait out the pace, looking at the client at least every 5 seconds
            self.sleep(min(5.0, self.next_allowed - self.now()))
            self.gate()
            if self.work_deadline is not None and self.now() > self.work_deadline:
                raise WorkExpired()
            if self.now() >= self.next_allowed:
                break
        started, status, data, gone = self.now(), 0, None, None
        try:
            status, data = self.lcu.get(path)
        except ClientGone as error:     # a dropped connection counts as a failure too
            gone = error
        ok = status == 200 and data is not None
        self.failures = 0 if ok else self.failures + 1
        # After failures, wait longer and longer (3s, 6s, 12s ... up to MAX_BACKOFF).
        backoff = 0 if ok else min(MAX_BACKOFF, 3 * 2 ** min(self.failures - 1, 10))
        self.next_allowed = self.now() + max(MIN_DELAY, self.delay * random.uniform(0.8, 1.2)) + backoff
        self.stats["requests"] += 1
        self.stats["failed"] += 0 if ok else 1
        short = re.sub(PUUID, lambda found: found.group(0)[:8], path.split("/lol-match-history/v1/")[-1])
        log.info("GET %s -> %s in %.2fs%s", short, gone or status, self.now() - started,
                 "" if ok else "  body: %.200s  (next wait +%ds)" % (data, backoff))
        if gone:
            raise gone
        return data if ok else None

    def read_list(self, puuid, since, begin=0, pages=MAX_PAGES):
        """Read up to `pages` pages of one player's match list, from place `begin`, no further back than `since` (ms).

        Returns (the Arena matches found, the newest match time seen, whether every request worked,
        the place the list goes on from, or 0 when the end or `since` was reached).
        """
        arena, newest, complete, following = [], 0, True, 0
        for page in range(pages):
            start = begin + page * PAGE_SIZE
            if start >= MAX_PAGES * PAGE_SIZE:
                break
            data = self.history_get("/lol-match-history/v1/products/lol/%s/matches?begIndex=%d&endIndex=%d"
                                    % (puuid, start, start + PAGE_SIZE - 1))
            if data is None:
                complete = False
                break
            games = match_list(data)
            if self.platform is None:       # the list says which server it is from
                for value in [as_dict(data).get("platformId")] + [game.get("platformId") for game in games]:
                    if isinstance(value, str) and PLATFORM_RE.fullmatch(value):
                        self.platform = value
                        log.info("this account is on server %s", value)
                        break
            fresh = [game for game in games if created_ms(game) is None or created_ms(game) > since]
            found = [game for game in fresh if is_arena(game) and match_id(game)]
            log.info("  %d matches listed, %d newer than %s, %d of those Arena", len(games), len(fresh),
                     time.strftime("%Y-%m-%d %H:%M", time.gmtime(since / 1000)), len(found))
            for game in fresh:
                self.modes["%.20s/%.10s" % (game.get("gameMode"), game.get("queueId"))] += 1
                newest = max(newest, created_ms(game) or 0)
            arena += found
            if len(fresh) < len(games) or len(games) < PAGE_SIZE:
                break      # reached matches that are too old or already read, or the end of the list
        else:
            following = begin + pages * PAGE_SIZE       # the list goes on beyond what was asked for
        self.stats["players"] += 1
        return arena, newest, complete, following if following < MAX_PAGES * PAGE_SIZE else 0

    def get_game(self, game_id, platform):
        """Ask the client for one match in full. Returns (the record, None) or (None, why not)."""
        game = self.history_get("/lol-match-history/v1/games/%d" % game_id)
        problem = None
        if game is None:
            return None, "request failed"
        if not isinstance(game, dict):
            problem = "not a match record"
        elif game.get("gameId") != game_id:
            problem = "the record is for match %.30s" % (game.get("gameId"),)
        elif platform and game.get("platformId") and game["platformId"] != platform:
            problem = "the record is from %.20s, expected %s" % (game["platformId"], platform)
        elif not is_arena(game):
            problem = "the record's mode is %.30s" % (game.get("gameMode"),)
        elif not isinstance(game.get("participants"), list):
            problem = "the record has no participants"
        if problem:
            log.info("match %s not stored: %s", game_id, problem)
            return None, problem
        self.stats["matches"] += 1
        if not self.described:
            self.described = True
            log.info("first Arena match this run: %s", describe(game))
        return game, None

    # -- uploading ---------------------------------------------------------------
    def apply_policy(self, policy):
        """What the server can set: pause, go slower, show a short message, the date the season began."""
        if not isinstance(policy, dict):
            return
        self.policy_at = self.now()
        self.enabled = policy.get("enabled") is not False
        season = policy.get("season_start")
        if isinstance(season, int) and not isinstance(season, bool) and 0 < season < 2 ** 53:
            self.season_start = season
        delay = policy.get("delay")
        if isinstance(delay, (int, float)) and not isinstance(delay, bool) and 0 <= delay <= 3600:
            self.policy_delay = min(60.0, float(delay))
        message = policy.get("message")
        if isinstance(message, str) and message != self.message:
            self.message = message
            if message:
                self.say("Message from ArenaSweats: " + "".join(c for c in message[:200] if c.isprintable()))

    def bad_key(self):
        self.server = None
        return Fatal("ArenaSweats did not accept the upload key. Check the key in config.json.")

    def refresh_policy(self, first=False):
        self.policy_at = self.now()
        status, data, _ = self.server.call("GET", "/v1/policy")
        if status == 200:
            self.apply_policy(data)
            if first:
                self.say("Connected to ArenaSweats.")
        elif status in (401, 403):
            raise self.bad_key()
        elif first:
            self.say("Cannot reach ArenaSweats yet. Trying again shortly.")

    def upload(self, final=False):
        """Send waiting matches to ArenaSweats, one batch at a time."""
        while self.server and self.now() >= self.next_upload:
            rows = self.store.pending(self.batch)
            if not rows or (len(rows) < self.batch and not final and self.now() - self.last_upload < 60):
                return
            status, data, retry = self.server.call(
                "POST", "/v1/games", {"version": VERSION, "games": [game for _, game in rows]})
            self.last_upload = self.now()
            keys = [key for key, _ in rows]
            if status == 200 and isinstance(data, dict):
                reasons = {as_dict(item).get("gameId"): as_dict(item).get("reason")
                           for item in as_list(data.get("rejected"))[:len(rows)]}
                refused = [key for key in keys if key[1] in reasons]
                self.store.mark_uploaded([key for key in keys if key[1] not in reasons])
                self.store.mark_refused(refused)
                self.upload_failures = 0
                self.stats["uploaded"] += len(rows) - len(refused)
                log.info("uploaded %d matches, %d refused%s", len(rows) - len(refused), len(refused),
                         "".join("  %s: %.60s" % (key[1], reasons[key[1]]) for key in refused))
                self.apply_policy(data.get("policy"))
            elif status in (401, 403):
                raise self.bad_key()
            elif status == 413 and self.batch > 1:
                self.batch = max(1, self.batch // 2)       # too big for the server: send fewer at a time
            elif status == 413:
                log.warning("match %s is too large for the server", keys[0])
                self.store.mark_refused(keys)
            else:
                self.upload_failures += 1
                wait = max(retry, min(600, 30 * 2 ** min(self.upload_failures - 1, 10)))
                self.next_upload = self.now() + wait
                log.warning("upload failed (status %s); next try in %ds", status, wait)
                if self.upload_failures == 3:
                    self.say("Cannot reach ArenaSweats right now. Matches are kept and will be sent later.")
                return

    # -- reporting ---------------------------------------------------------------
    def status(self):
        self.last_status = self.now()
        line = "%d Arena matches collected, %d match lists read, %d requests (%d failed)" % (
            self.stats["matches"], self.stats["players"], self.stats["requests"], self.stats["failed"])
        if self.server:
            line += ", %d waiting to upload" % self.store.count(PENDING)
            if self.store.count(REFUSED):
                line += ", %d refused by ArenaSweats" % self.store.count(REFUSED)
        self.say(line)

    def close(self):
        """Close the local database. Done on the way out, after anything that reads it."""
        if self.store is not None:
            self.store.close()
            self.store = None

    def finish(self):
        if self.store is None:
            self.say("Stopped before a logged-in League client was found. Nothing was collected.")
        else:
            try:
                self.upload(final=True)
                if self.server and self.platform and self.reports:
                    self.ask_for_work(more=False)       # hand in the last reports without taking on more
            except (Fatal, Done) as error:
                if isinstance(error, Fatal):
                    self.say(str(error))
            self.status()
            log.info("match types seen in match lists: %s", dict(self.modes.most_common()))
        if self.test_run:
            path = write_report(self.data_dir, self.store, (self.me or "none")[:8])
            self.say("Test run finished. Please send this one file back:")
            self.say("  " + path)


def write_report(data_dir, store, tag):
    """A test run's result as one zip: the collected matches and the log."""
    for handler in log.handlers:
        handler.flush()
    path = os.path.join(data_dir, "arenasweats-test-%s-%s.zip" % (tag, time.strftime("%Y%m%d-%H%M%S")))
    with zipfile.ZipFile(path, "w", zipfile.ZIP_DEFLATED) as archive:
        with archive.open("matches.jsonl", "w") as out:
            for game in store.stored_games() if store else ():
                out.write(json.dumps(game, ensure_ascii=False).encode("utf-8", "backslashreplace") + b"\n")
        log_path = os.path.join(data_dir, "collector.log")
        if os.path.isfile(log_path):
            archive.write(log_path, "collector.log")
    return path


# ---------------------------------------------------------------------------
# Start-up
# ---------------------------------------------------------------------------
def program_dir():
    if getattr(sys, "frozen", False):     # running as the packaged .exe
        return os.path.dirname(os.path.abspath(sys.executable))
    return os.path.dirname(os.path.abspath(__file__))


def load_config(folder):
    """config.json beside the program may hold: server, key, league_dir."""
    try:
        with open(os.path.join(folder, "config.json"), "r", encoding="utf-8-sig") as handle:
            config = json.load(handle)
        return config if isinstance(config, dict) else {}
    except (OSError, ValueError):
        return {}


def save_config(**values):
    folder = program_dir()
    config = load_config(folder)
    config.update(values)
    try:
        with open(os.path.join(folder, "config.json"), "w", encoding="utf-8") as handle:
            json.dump(config, handle, indent=2, ensure_ascii=False)
    except OSError as error:
        log.info("could not save config.json: %s", error)


def lock_single_instance():
    """Refuse to start twice: two copies would double the request rate."""
    handle = open(os.path.join(tempfile.gettempdir(), "arenasweats-collector.lock"), "w")
    try:
        if os.name == "nt":
            import msvcrt
            msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
        else:
            import fcntl
            fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        raise Fatal("The collector is already running on this computer.")
    return handle      # stays locked for as long as this object is kept


def export(data_dir, target):
    """Write every match record held locally to a file, one JSON record per line."""
    count = 0
    opener = gzip.open if target.endswith(".gz") else open
    with opener(target, "wt", encoding="utf-8", errors="backslashreplace") as out:
        for name in sorted(os.listdir(data_dir)):
            if name.startswith("collector-") and name.endswith(".db"):
                store = Store(os.path.join(data_dir, name))
                try:
                    for game in store.stored_games():
                        out.write(json.dumps(game, ensure_ascii=False) + "\n")
                        count += 1
                finally:
                    store.close()
    print("Wrote %d matches to %s" % (count, target))


def main(argv=None):
    parser = argparse.ArgumentParser(description="Collect Arena match history from your own League client (read-only).")
    parser.add_argument("--minutes", type=float,
                        help="stop after this many minutes; 0 = run until closed "
                             "(default: %d for a test run, otherwise until closed)" % TEST_MINUTES)
    parser.add_argument("--league-dir", help="folder where League is installed, if it is not found automatically")
    parser.add_argument("--server", help="ArenaSweats upload address (default: from config.json; none = test run)")
    parser.add_argument("--key", help="your upload key (default: from config.json)")
    parser.add_argument("--delay", type=float, default=DEFAULT_DELAY,
                        help="seconds between match-history requests (default %.0f, never below %.0f)"
                             % (DEFAULT_DELAY, MIN_DELAY))
    parser.add_argument("--since", metavar="YYYY-MM-DD", default=SEASON_START,
                        help="read nothing older than this UTC date, the start of the season (default %(default)s; "
                             "with an upload address, ArenaSweats sets it)")
    parser.add_argument("--data-dir", help="where to keep collected data (default: 'data' beside this program)")
    parser.add_argument("--export", metavar="FILE",
                        help="write the match records held locally to FILE (JSON lines; .gz to compress) and exit")
    opts = parser.parse_args(argv)

    for stream in (sys.stdout, sys.stderr):     # never crash on a character the console cannot show
        if stream and hasattr(stream, "reconfigure"):
            stream.reconfigure(errors="replace")

    config = load_config(program_dir())
    data_dir = os.path.abspath(opts.data_dir or os.path.join(program_dir(), "data"))
    os.makedirs(data_dir, exist_ok=True)
    if opts.export:
        export(data_dir, opts.export)
        return 0

    handler = logging.handlers.RotatingFileHandler(
        os.path.join(data_dir, "collector.log"), maxBytes=2_000_000, backupCount=2, encoding="utf-8")
    handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
    log.addHandler(handler)
    log.setLevel(logging.INFO)
    log.info("=== collector %s starting on %s, python %s ===", VERSION, sys.platform, sys.version.split()[0])

    code, lock, made = 0, None, None
    try:
        opts.league_dir = opts.league_dir or config.get("league_dir")
        try:
            opts.since = date_ms(opts.since)
        except ValueError:
            raise Fatal("--since must be a date like 2026-10-06")
        server_url = opts.server or config.get("server")
        server = Server(server_url, opts.key or config.get("key")) if server_url else None
        if opts.minutes is None:
            opts.minutes = 0 if server else TEST_MINUTES
        lock = lock_single_instance()
        print("ArenaSweats collector %s - reads Arena match history from your League client. Read-only." % VERSION)
        if server is None:
            print("No upload address is set, so this is a test run: it collects for up to %g min,\n"
                  "then writes one file for you to send back. Nothing is uploaded." % opts.minutes
                  if opts.minutes else "No upload address is set: matches are only saved on this computer.")
        made = Collector(opts, data_dir, server)
        made.run()
    except KeyboardInterrupt:
        print("Stopped.")
    except Fatal as error:
        log.error("%s", error)
        print(error)
        code = 1
    except Exception:
        log.exception("unexpected error")
        print("Unexpected error. Details are in %s" % os.path.join(data_dir, "collector.log"))
        code = 1
    finally:
        if made:
            made.close()
        if lock:
            lock.close()
        log.removeHandler(handler)
        handler.close()
    if getattr(sys, "frozen", False) and sys.stdin and sys.stdin.isatty():
        try:
            input("Press Enter to close.")
        except (EOFError, KeyboardInterrupt):
            pass
    return code


if __name__ == "__main__":
    sys.exit(main())
