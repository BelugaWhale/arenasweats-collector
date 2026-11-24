#!/usr/bin/env python3
"""ArenaSweats collector: reads Arena match history from your own League client.

WHAT IT DOES
  1. Finds the League client that is running on this computer, through the
     "lockfile" in its folder or, when that cannot be found, the command line
     the client was started with (what Task Manager shows for it).
  2. While you are not playing, slowly asks that client for this season's Arena
     match history: match lists of players, and the full record of Arena matches.
     Fast mode, which you can turn on at start, waits a fifth as long between
     requests, still one at a time. BETA: please only use it with unimportant
     accounts for now.
  3. Sends the matches to ArenaSweats (DEFAULT_SERVER below), which tells it which
     players and matches to read next, so no two collectors read the same thing.
     The first time, it asks ArenaSweats for this install's own upload key and
     keeps it in config.json beside the program. With --test-run it uploads
     nothing: it starts from your own matches, moves on to the players you met,
     and after 15 minutes saves everything in one file in the "data" folder.

WHAT IT NEVER DOES
  - It never changes anything. Every request to the League client is a GET to
    one of the five addresses in LCU_ALLOWED below; anything else is refused.
  - It never touches the game itself (its process, memory or files).
  - It never sends your login token, password or account details anywhere.
    What leaves this computer, and only to the upload address, is: match
    records, which Arena matches a player's list contains, and your own player
    id (the same id that is in your match records; a server with no players
    yet starts from it).
  - It keeps the computer awake while it runs (the screen can still turn off),
    and stops a click in its window from freezing it.
  - It needs no administrator rights and installs nothing. (When the League
    client runs as administrator, Windows hides where it is installed; the
    collector then looks in the usual places and, failing that, asks.)

Only the Python standard library is used, so this one file is the whole program.
"""

import argparse
import base64
import calendar
import errno
import gzip
import hashlib
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
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request
import warnings
import zipfile
import zlib
from collections import Counter, namedtuple

VERSION = "0.3.7"
DEFAULT_SERVER = "https://arenasweats.lol/collector"   # where matches go unless told otherwise
KEY_RE = re.compile(r"as_[A-Za-z0-9_-]{20,100}")

# ---------------------------------------------------------------------------
# Limits. These keep the collector gentle. The upload server can slow it down
# or switch it off, but nothing can make it go faster than MIN_DELAY, or than
# FAST_MIN_DELAY in fast mode, which only the user can turn on.
# ---------------------------------------------------------------------------
MIN_DELAY = 1.0        # hard floor at the normal pace: at most one match-history request per second
DEFAULT_DELAY = 2.0    # normal gap between match-history requests, in seconds
FAST_FACTOR = 5        # fast mode (beta, only when the user turns it on) divides the gap by this, still one at a time,
FAST_MIN_DELAY = 0.4   # never below its own hard floor; ArenaSweats can raise it (policy "fast_delay")
FAST_SPEEDUP = "3"     # matches an hour in fast mode against the normal pace (fake client, real client's answer times)
FAST_REST = 3600       # in fast mode, a 429 or 3 failed requests in a row bring back the normal pace for this long
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
PHASE_SECONDS = 5                 # while it is idle, the client's state is asked at most this often
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
        # cannot be verified. This traffic never leaves this computer. (ArenaSweats
        # is always checked: see https_context.)
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        context.check_hostname = False
        context.verify_mode = ssl.CERT_NONE
        # ProxyHandler({}) = never send these local requests through a proxy.
        self.opener = urllib.request.build_opener(
            urllib.request.ProxyHandler({}), urllib.request.HTTPSHandler(context=context), NoRedirect())
        self.opened, self.sent, self.last = time.monotonic(), 0, None    # for the log: see Collector.doing

    def get(self, path, timeout=20):
        """GET one allowed path. Returns (status, parsed JSON or None); status 0 = timed out."""
        if not any(pattern.fullmatch(path) for pattern in LCU_ALLOWED):
            raise ValueError("refused: not on the allowed list: " + path)
        self.sent += 1
        self.last = (path, time.monotonic(), None)      # what was asked last, when, and its status (None: no answer)
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
        self.last = (path, self.last[1], status)
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


Running = namedtuple("Running", "pid name started memory private handles")
_table_api = None


def process_table():
    """Every running program as {process id: Running}, from the list Windows keeps of them (what
    Task Manager shows): its name, when it started (seconds since 1970), its memory (working set)
    and private memory in bytes, and its handle count. No program is opened for this, so the
    League client is not touched. Empty off Windows, or when Windows will not say."""
    global _table_api
    if os.name != "nt":
        return {}
    ctypes = windows_api()[0]
    if _table_api is None:
        nt = ctypes.WinDLL("ntdll")
        nt.NtQuerySystemInformation.restype = ctypes.c_long
        nt.NtQuerySystemInformation.argtypes = (ctypes.c_ulong, ctypes.c_void_p, ctypes.c_ulong,
                                                ctypes.POINTER(ctypes.c_ulong))

        class Info(ctypes.Structure):       # one program in that list (SYSTEM_PROCESS_INFORMATION)
            _fields_ = [("NextEntryOffset", ctypes.c_ulong), ("NumberOfThreads", ctypes.c_ulong),
                        ("WorkingSetPrivateSize", ctypes.c_longlong), ("HardFaultCount", ctypes.c_ulong),
                        ("NumberOfThreadsHighWatermark", ctypes.c_ulong), ("CycleTime", ctypes.c_ulonglong),
                        ("CreateTime", ctypes.c_longlong), ("UserTime", ctypes.c_longlong),
                        ("KernelTime", ctypes.c_longlong), ("NameLength", ctypes.c_ushort),
                        ("NameMaximumLength", ctypes.c_ushort), ("NameBuffer", ctypes.c_void_p),
                        ("BasePriority", ctypes.c_long), ("UniqueProcessId", ctypes.c_void_p),
                        ("InheritedFromUniqueProcessId", ctypes.c_void_p), ("HandleCount", ctypes.c_ulong),
                        ("SessionId", ctypes.c_ulong), ("UniqueProcessKey", ctypes.c_void_p),
                        ("PeakVirtualSize", ctypes.c_size_t), ("VirtualSize", ctypes.c_size_t),
                        ("PageFaultCount", ctypes.c_ulong), ("PeakWorkingSetSize", ctypes.c_size_t),
                        ("WorkingSetSize", ctypes.c_size_t), ("QuotaPeakPagedPoolUsage", ctypes.c_size_t),
                        ("QuotaPagedPoolUsage", ctypes.c_size_t), ("QuotaPeakNonPagedPoolUsage", ctypes.c_size_t),
                        ("QuotaNonPagedPoolUsage", ctypes.c_size_t), ("PagefileUsage", ctypes.c_size_t),
                        ("PeakPagefileUsage", ctypes.c_size_t), ("PrivatePageCount", ctypes.c_size_t)]
        _table_api = (nt, Info)
    nt, Info = _table_api
    size, status = 1 << 20, None
    for _ in range(6):
        buffer = ctypes.create_string_buffer(size)
        needed = ctypes.c_ulong(0)
        status = nt.NtQuerySystemInformation(5, buffer, size, ctypes.byref(needed)) & 0xFFFFFFFF  # 5 = programs
        if status != 0xC0000004:            # 0xC0000004 = the list did not fit: try a bigger buffer
            break
        size = max(size * 2, needed.value + (1 << 16))
    if status != 0:
        return {}
    table, offset, start = {}, 0, ctypes.addressof(buffer)
    while offset + ctypes.sizeof(Info) <= size:
        info = Info.from_buffer(buffer, offset)
        name = ""
        if info.NameBuffer and start <= info.NameBuffer and info.NameBuffer + info.NameLength <= start + size:
            name = ctypes.wstring_at(info.NameBuffer, info.NameLength // 2)
        pid = info.UniqueProcessId or 0
        started = info.CreateTime / 1e7 - 11644473600 if info.CreateTime > 0 else 0.0   # 1601 -> 1970
        table[pid] = Running(pid, name, started, info.WorkingSetSize, info.PrivatePageCount, info.HandleCount)
        if not info.NextEntryOffset:
            break
        offset += info.NextEntryOffset
    return table


QUERY_RETRY = 600     # seconds before asking again about a program Windows would not tell about
answers = {}          # (process id, start time, with command line) -> (what query_program said, when)


def query_once(pid, command_line=False, table=None):
    """query_program, but each running program is opened once, not every round: its path and
    command line stay the same for as long as it runs. When Windows refused, it is asked again
    only after QUERY_RETRY seconds. `table` (from process_table) tells a program apart from a
    later one that happens to get the same process id."""
    program = (table or {}).get(pid)
    started = program.started if program else None
    for key in ((pid, started, True), (pid, started, command_line)):
        found = answers.get(key)
        if found and (found[0][1 if command_line else 0] is not None or time.monotonic() - found[1] < QUERY_RETRY):
            return found[0]
    answer = query_program(pid, command_line)
    answers[(pid, started, command_line)] = (answer, time.monotonic())
    log.info("asked Windows about program %d (%s)%s: path %s%s", pid, program.name if program else "?",
             " and its command line" if command_line else "", "given" if answer[0] else "refused",
             (", command line " + ("given" if answer[1] else "refused")) if command_line else "")
    return answer


def forget_closed(programs):
    """Drop what was learned about programs that are no longer running."""
    running = {pid for pid, _ in programs}
    for key in [key for key in answers if key[0] not in running]:
        del answers[key]


def running_client_dirs(names=CLIENT_NAMES):
    """Folders of the running League client programs (Windows only). A folder is None when
    Windows would not say where the program is."""
    dirs, related, errors = [], set(), set()
    programs, table = running_programs(), process_table()
    forget_closed(programs)
    for pid, name in programs:
        if any(word in name.lower() for word in RELATED_WORDS):
            related.add(name)
        if name.lower() in names:
            path, _, error = query_once(pid, table=table)
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
    lets this program see the client. Read once for each start of the client (see query_once).
    The password is never written anywhere."""
    logins = []
    programs, table = running_programs(), process_table()
    forget_closed(programs)
    for pid, name in programs:
        if name.lower() == WINDOW_PROGRAM:
            login = login_from_command_line(query_once(pid, command_line=True, table=table)[1])
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
# Watching the client's programs, for the log
# ---------------------------------------------------------------------------
WATCH_SECONDS = 5            # how often the list of running programs is looked at
CLIENT_LOG_SECONDS = 600     # how often the client's programs are described in the log
MAIN_PROGRAMS = ("leagueclient.exe", "leagueclientux.exe")
RENDER_PROGRAM = "leagueclientuxrender.exe"           # the client's window contents; there are several
CRASH_WORDS = ("werfault", "bugsplat", "bssndrpt")   # programs that report a crash (League's own crash
                                                      # handlers run all the time, so they are not counted)


def duration(seconds):
    seconds = max(0, int(seconds))
    if seconds >= 3600:
        return "%dh%02dm" % (seconds // 3600, seconds // 60 % 60)
    return "%dm%02ds" % (seconds // 60, seconds % 60)


def megabytes(size):
    return "%d MB" % round(size / 2 ** 20)


def short_path(path):
    """A client address for the log, with player ids cut to 8 characters."""
    return re.sub(PUUID, lambda found: found.group(0)[:8], path)


class ClientWatch:
    """Writes to the log how the League client's programs are doing, from the list of running
    programs Windows keeps (nothing is opened for it): every 10 minutes their process ids, how long
    they have run and their memory, and, within seconds, when one of them closes or starts, with
    what the collector was doing then. So a report of the client closing comes with data."""

    def __init__(self, now, wall):
        self.now, self.wall = now, wall
        self.known = None            # process id -> Running: the client's programs at the last look
        self.looked_at = self.described_at = float("-inf")

    def tick(self, doing):
        if self.now() - self.looked_at >= WATCH_SECONDS:
            self.look(doing)

    def describe_soon(self):
        self.described_at = float("-inf")

    @staticmethod
    def name(program):
        return program.name[:-4] if program.name.lower().endswith(".exe") else program.name

    def look(self, doing):
        """`doing`: a function that says what the collector is doing."""
        last_look, self.looked_at = self.looked_at, self.now()
        table = process_table()
        if not table:
            return                   # not Windows, or Windows would not say
        client = {pid: p for pid, p in table.items() if p.name.lower() in MAIN_PROGRAMS + (RENDER_PROGRAM,)}
        if self.known is not None:
            def gone(p):
                return p.pid not in client or client[p.pid].started != p.started
            closed = [p for p in self.known.values() if p.name.lower() in MAIN_PROGRAMS and gone(p)]
            opened = [p for p in client.values() if p.name.lower() in MAIN_PROGRAMS
                      and (p.pid not in self.known or self.known[p.pid].started != p.started)]
            if closed:
                renders = [p for p in self.known.values() if p.name.lower() == RENDER_PROGRAM]
                related = sorted({p.name for p in table.values() if any(w in p.name.lower() for w in RELATED_WORDS)})
                crash = sorted({p.name for p in table.values() if any(w in p.name.lower() for w in CRASH_WORDS)})
                log.warning("League client program closed: %s; %s of %d LeagueClientUxRender programs closed too. "
                            "Collector: %s. Programs still running: %s. Crash reporters running: %s",
                            "; ".join("%s (pid %d) after running %s, memory %s, private %s, %d handles at the "
                                      "last look %ds before" % (self.name(p), p.pid, duration(self.wall() - p.started),
                                                                 megabytes(p.memory), megabytes(p.private), p.handles,
                                                                 self.looked_at - last_look) for p in closed),
                            sum(1 for p in renders if gone(p)), len(renders), doing(),
                            ", ".join(related) or "none", ", ".join(crash) or "none")
            for p in opened:
                log.info("League client program started: %s (pid %d)", self.name(p), p.pid)
        self.known = client
        if self.now() - self.described_at >= CLIENT_LOG_SECONDS:
            self.described_at = self.now()
            self.describe(client, doing)

    def describe(self, client, doing):
        parts = ["%s pid %d up %s, memory %s, private %s, %d handles"
                 % (self.name(p), p.pid, duration(self.wall() - p.started), megabytes(p.memory),
                    megabytes(p.private), p.handles)
                 for p in sorted(client.values(), key=lambda p: (p.name.lower(), p.started))
                 if p.name.lower() in MAIN_PROGRAMS]
        renders = [p for p in client.values() if p.name.lower() == RENDER_PROGRAM]
        if renders:
            parts.append("%d LeagueClientUxRender programs, memory %s, private %s" % (
                len(renders), megabytes(sum(p.memory for p in renders)),
                megabytes(sum(p.private for p in renders))))
        log.info("League client: %s. Collector: %s", "; ".join(parts) or "not running", doing())


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
# Reaching ArenaSweats over the internet
# ---------------------------------------------------------------------------
# Python checks ArenaSweats' certificate only against root certificates it already holds: unlike
# Windows' own programs, it cannot fetch a missing root. Windows fetches most roots only when one
# of its own programs first needs them (a fresh Windows holds none for websites at all;
# internet-cafe PCs, reset at every restart, and PCs whose updates are blocked may never keep
# them), while browsers bring their own, so a PC whose browser opens arenasweats.lol can still lack
# the root that ArenaSweats' certificate (from Let's Encrypt) leads to. So every root a Let's
# Encrypt certificate can lead to is carried here, besides Windows' own: ISRG Root X1 and X2, and
# Root YE and YR, the newer roots its certificates lead to now through X1 and X2, and will lead to
# on their own once Let's Encrypt stops sending that link. All four are Let's Encrypt's (ISRG's).
# Google Trust Services' two roots in use, GTS Root R1 and R4, are carried too, so that ArenaSweats
# can move to Google's certificates (see server/README.md, "Certificate") without leaving a
# collector behind. All six are in Mozilla's and the other trusted lists.
EXTRA_ROOT_NAMES = {"96bcec06264976f37460779acf28c5a7cfe8a3c0aae11a8ffcee05c0bddf08c6": "ISRG Root X1",
                    "69729b8e15a86efc177a57afb7171dfc64add28c2fca8cf1507e34453ccb1470": "ISRG Root X2",
                    "e14ffcad5b0025731006caa43a121a22d8e9700f4fb9cf852f02a708aa5d5666": "ISRG Root YE",
                    "e57b7e6f150c419102e8d5c055729ff967b9d1a829bf00cec89ca604ebf4a86f": "ISRG Root YR",
                    "d947432abde7b7fa90fc2e6b59101b1280e0e1c7e4e40fa3c6887fff57a7f4cf": "GTS Root R1",
                    "349dfa4058c5e263123b398ae795573c4e1313c83fe68f93556cd5e8031b3c7d": "GTS Root R4"}
EXTRA_ROOTS = """
-----BEGIN CERTIFICATE-----
MIIFazCCA1OgAwIBAgIRAIIQz7DSQONZRGPgu2OCiwAwDQYJKoZIhvcNAQELBQAw
TzELMAkGA1UEBhMCVVMxKTAnBgNVBAoTIEludGVybmV0IFNlY3VyaXR5IFJlc2Vh
cmNoIEdyb3VwMRUwEwYDVQQDEwxJU1JHIFJvb3QgWDEwHhcNMTUwNjA0MTEwNDM4
WhcNMzUwNjA0MTEwNDM4WjBPMQswCQYDVQQGEwJVUzEpMCcGA1UEChMgSW50ZXJu
ZXQgU2VjdXJpdHkgUmVzZWFyY2ggR3JvdXAxFTATBgNVBAMTDElTUkcgUm9vdCBY
MTCCAiIwDQYJKoZIhvcNAQEBBQADggIPADCCAgoCggIBAK3oJHP0FDfzm54rVygc
h77ct984kIxuPOZXoHj3dcKi/vVqbvYATyjb3miGbESTtrFj/RQSa78f0uoxmyF+
0TM8ukj13Xnfs7j/EvEhmkvBioZxaUpmZmyPfjxwv60pIgbz5MDmgK7iS4+3mX6U
A5/TR5d8mUgjU+g4rk8Kb4Mu0UlXjIB0ttov0DiNewNwIRt18jA8+o+u3dpjq+sW
T8KOEUt+zwvo/7V3LvSye0rgTBIlDHCNAymg4VMk7BPZ7hm/ELNKjD+Jo2FR3qyH
B5T0Y3HsLuJvW5iB4YlcNHlsdu87kGJ55tukmi8mxdAQ4Q7e2RCOFvu396j3x+UC
B5iPNgiV5+I3lg02dZ77DnKxHZu8A/lJBdiB3QW0KtZB6awBdpUKD9jf1b0SHzUv
KBds0pjBqAlkd25HN7rOrFleaJ1/ctaJxQZBKT5ZPt0m9STJEadao0xAH0ahmbWn
OlFuhjuefXKnEgV4We0+UXgVCwOPjdAvBbI+e0ocS3MFEvzG6uBQE3xDk3SzynTn
jh8BCNAw1FtxNrQHusEwMFxIt4I7mKZ9YIqioymCzLq9gwQbooMDQaHWBfEbwrbw
qHyGO0aoSCqI3Haadr8faqU9GY/rOPNk3sgrDQoo//fb4hVC1CLQJ13hef4Y53CI
rU7m2Ys6xt0nUW7/vGT1M0NPAgMBAAGjQjBAMA4GA1UdDwEB/wQEAwIBBjAPBgNV
HRMBAf8EBTADAQH/MB0GA1UdDgQWBBR5tFnme7bl5AFzgAiIyBpY9umbbjANBgkq
hkiG9w0BAQsFAAOCAgEAVR9YqbyyqFDQDLHYGmkgJykIrGF1XIpu+ILlaS/V9lZL
ubhzEFnTIZd+50xx+7LSYK05qAvqFyFWhfFQDlnrzuBZ6brJFe+GnY+EgPbk6ZGQ
3BebYhtF8GaV0nxvwuo77x/Py9auJ/GpsMiu/X1+mvoiBOv/2X/qkSsisRcOj/KK
NFtY2PwByVS5uCbMiogziUwthDyC3+6WVwW6LLv3xLfHTjuCvjHIInNzktHCgKQ5
ORAzI4JMPJ+GslWYHb4phowim57iaztXOoJwTdwJx4nLCgdNbOhdjsnvzqvHu7Ur
TkXWStAmzOVyyghqpZXjFaH3pO3JLF+l+/+sKAIuvtd7u+Nxe5AW0wdeRlN8NwdC
jNPElpzVmbUq4JUagEiuTDkHzsxHpFKVK7q4+63SM1N95R1NbdWhscdCb+ZAJzVc
oyi3B43njTOQ5yOf+1CceWxG1bQVs5ZufpsMljq4Ui0/1lvh+wjChP4kqKOJ2qxq
4RgqsahDYVvTH9w7jXbyLeiNdd8XM2w9U/t7y0Ff/9yi0GE44Za4rF2LN9d11TPA
mRGunUHBcnWEvgJBQl9nJEiU0Zsnvgc/ubhPgXRR4Xq37Z0j4r7g1SgEEzwxA57d
emyPxgcYxn/eR44/KJ4EBs+lVDR3veyJm+kXQ99b21/+jh5Xos1AnX5iItreGCc=
-----END CERTIFICATE-----
-----BEGIN CERTIFICATE-----
MIICGzCCAaGgAwIBAgIQQdKd0XLq7qeAwSxs6S+HUjAKBggqhkjOPQQDAzBPMQsw
CQYDVQQGEwJVUzEpMCcGA1UEChMgSW50ZXJuZXQgU2VjdXJpdHkgUmVzZWFyY2gg
R3JvdXAxFTATBgNVBAMTDElTUkcgUm9vdCBYMjAeFw0yMDA5MDQwMDAwMDBaFw00
MDA5MTcxNjAwMDBaME8xCzAJBgNVBAYTAlVTMSkwJwYDVQQKEyBJbnRlcm5ldCBT
ZWN1cml0eSBSZXNlYXJjaCBHcm91cDEVMBMGA1UEAxMMSVNSRyBSb290IFgyMHYw
EAYHKoZIzj0CAQYFK4EEACIDYgAEzZvVn4CDCuwJSvMWSj5cz3es3mcFDR0HttwW
+1qLFNvicWDEukWVEYmO6gbf9yoWHKS5xcUy4APgHoIYOIvXRdgKam7mAHf7AlF9
ItgKbppbd9/w+kHsOdx1ymgHDB/qo0IwQDAOBgNVHQ8BAf8EBAMCAQYwDwYDVR0T
AQH/BAUwAwEB/zAdBgNVHQ4EFgQUfEKWrt5LSDv6kviejM9ti6lyN5UwCgYIKoZI
zj0EAwMDaAAwZQIwe3lORlCEwkSHRhtFcP9Ymd70/aTSVaYgLXTWNLxBo1BfASdW
tL4ndQavEi51mI38AjEAi/V3bNTIZargCyzuFJ0nN6T5U6VR5CmD1/iQMVtCnwr1
/q4AaOeMSQ+2b1tbFfLn
-----END CERTIFICATE-----
-----BEGIN CERTIFICATE-----
MIIB2TCCAWCgAwIBAgIRAKQCa6LvbHwg1AR+XmWmk4AwCgYIKoZIzj0EAwMwLjEL
MAkGA1UEBhMCVVMxDTALBgNVBAoTBElTUkcxEDAOBgNVBAMTB1Jvb3QgWUUwHhcN
MjUwOTAzMDAwMDAwWhcNNDUwOTAyMjM1OTU5WjAuMQswCQYDVQQGEwJVUzENMAsG
A1UEChMESVNSRzEQMA4GA1UEAxMHUm9vdCBZRTB2MBAGByqGSM49AgEGBSuBBAAi
A2IABDwS/6vhrcVqcbBo+wgdI3fwn9x7DNJJOY/lTOti0vkwuRN87RhEhTH17E7X
yFjWsPYhIPt/wzOqxTd2b+4ZJNy9ID04YywF9U5zasDVyGSNErVNtz8uSGh5izW8
7j77GaNCMEAwDgYDVR0PAQH/BAQDAgEGMA8GA1UdEwEB/wQFMAMBAf8wHQYDVR0O
BBYEFKPIJlqOoUzQNWP8myPIOq5W809WMAoGCCqGSM49BAMDA2cAMGQCMHhMr8N9
LdL1VQKs9BdV81r76eXRB6mtjuNjzk6/lBsPNToWLTDzGYgtQKO1jl63uAIwGV7m
onyF377c+MM1oqVNs17sgu7F9YKZwgLmVbeOMDbKAXHtKMDLbiGllCcs8f47
-----END CERTIFICATE-----
-----BEGIN CERTIFICATE-----
MIIFKTCCAxGgAwIBAgIRAOxGNJNgz0sP+KmC2Tqpyj0wDQYJKoZIhvcNAQELBQAw
LjELMAkGA1UEBhMCVVMxDTALBgNVBAoTBElTUkcxEDAOBgNVBAMTB1Jvb3QgWVIw
HhcNMjUwOTAzMDAwMDAwWhcNNDUwOTAyMjM1OTU5WjAuMQswCQYDVQQGEwJVUzEN
MAsGA1UEChMESVNSRzEQMA4GA1UEAxMHUm9vdCBZUjCCAiIwDQYJKoZIhvcNAQEB
BQADggIPADCCAgoCggIBANvGJnN78CTJdWL3+eGfsLN5TrNBJs+VH9hRXqRbwxu9
sGNiB0BD1fcOxbSUQCJIM1xE13Db+5Cw1w0s0EBYsvuIP/6joF0w8cuImbgR1OGg
YbSQ4OpzI+DG8SGuTlcE873OCS+kh3srlo6vl43M5OJg4Aeo1sfHp6kTJDoIiFBN
JAY+OKfX/FUvYKuhjT+no49lmqmupSBI5PkBQiqrEGtWU5uxU/cQWHGu8jSjFBzn
ZqvbNPLMXMLFxCb3WTfrJBXXjqvWG+v4bjzxjjeAtOlU7qarRDvNOyAuQYLln904
M+faKx8hnLCpJ15ZqaEgcNlY+9MMWcC5yvL2A2j3l9+2buggZX+dOE91zYmIdawT
vSZuVvlbRrAlLxIB6pwMBjneXCjYQ8+3BCCjssbSNpZU3hTcBDdhfAlEDlYr6pEa
tnMdmDT5BqnKC92bd0EhM1fbLHioLccLCuievT8ZkPhZrq7Mii7gNXAcUEAR8+lz
Yal+9zTg7C5DALyVOeG/CqfRAMn1KSHCR0NSA6P8tn/mGRlnCct5rtVCLnVySVpU
6H1qGg3DgTOuskf8eahTMiYbI5ezPJmO5ertalskQ1utp74+eDy92PI4ftHKTbq9
IWhH4YZKh3WnJEIt+oQvlYZbY8tpEroKrFB6PFGzrJIDRyts4HqvuH52RFj2zv/B
AgMBAAGjQjBAMA4GA1UdDwEB/wQEAwIBBjAPBgNVHRMBAf8EBTADAQH/MB0GA1Ud
DgQWBBTe51tg0CJtQCh9Pw0B/qS1UrRRlDANBgkqhkiG9w0BAQsFAAOCAgEAWHnf
713Bdkq7t5yN2dNIgQakUb94X9WuyhMEHHkgx4oDpSUlnG0w4g94MoqaEUE31ZjR
LU7L5LD1g9ujFHTQu8AD215AHMVQFbm6j8hQxdXHAzDajFNQnOlDJrLjzIx176oy
AjvUtejZx2NNmdb5fd0WGVGsCdoAJ3N8ozo7ajE8t6vfxStZb4BQ9WYJGHUDrv2N
i5tJF6CNiPnlzs3BUfECRbE4JSk+jvy8+VoGiFE8qsH/j78x2fjgQhAQFV7P7Zxy
dBTZ1wEkNpZNW2qnaK1SKBLa+xf6E06YRIq5uaI+HWH8SY1y5VbRgzq40EKg3yxP
06fz+uYAUIFJoLNfhwRCc3Q6pQVuMX3yAjHAes4gk4moGcLQ5p7HAh39yeylZc1J
41sx/jKwLIkPE6Rr1Nf4pxdsxf9SA4yOEiAkDgq04DVxn8hgYFdUtBCuiuVC2heA
EiqVEa+8QZjuw8Gj0EbHXcRd1nInvGqRS1o9Is7YBdQN57X1AYveGBNNqjICSb7c
awuw1EawTDrs13VUlJVEsbQ0/O/1aaV73mCdOQ8azqL2KTv1Ewu1xbquE2S+kdQU
To9TUwat3wUA6cwXh1EfpS/3fJ0aGah5hdpRyoCLDlsSn8tkrjMfFFX0viC+GxHc
sI1ANRYvqSFC2X1VRZfDg+wD6E21BccmifG4yWc=
-----END CERTIFICATE-----
-----BEGIN CERTIFICATE-----
MIIFVzCCAz+gAwIBAgINAgPlk28xsBNJiGuiFzANBgkqhkiG9w0BAQwFADBHMQsw
CQYDVQQGEwJVUzEiMCAGA1UEChMZR29vZ2xlIFRydXN0IFNlcnZpY2VzIExMQzEU
MBIGA1UEAxMLR1RTIFJvb3QgUjEwHhcNMTYwNjIyMDAwMDAwWhcNMzYwNjIyMDAw
MDAwWjBHMQswCQYDVQQGEwJVUzEiMCAGA1UEChMZR29vZ2xlIFRydXN0IFNlcnZp
Y2VzIExMQzEUMBIGA1UEAxMLR1RTIFJvb3QgUjEwggIiMA0GCSqGSIb3DQEBAQUA
A4ICDwAwggIKAoICAQC2EQKLHuOhd5s73L+UPreVp0A8of2C+X0yBoJx9vaMf/vo
27xqLpeXo4xL+Sv2sfnOhB2x+cWX3u+58qPpvBKJXqeqUqv4IyfLpLGcY9vXmX7w
Cl7raKb0xlpHDU0QM+NOsROjyBhsS+z8CZDfnWQpJSMHobTSPS5g4M/SCYe7zUjw
TcLCeoiKu7rPWRnWr4+wB7CeMfGCwcDfLqZtbBkOtdh+JhpFAz2weaSUKK0Pfybl
qAj+lug8aJRT7oM6iCsVlgmy4HqMLnXWnOunVmSPlk9orj2XwoSPwLxAwAtcvfaH
szVsrBhQf4TgTM2S0yDpM7xSma8ytSmzJSq0SPly4cpk9+aCEI3oncKKiPo4Zor8
Y/kB+Xj9e1x3+naH+uzfsQ55lVe0vSbv1gHR6xYKu44LtcXFilWr06zqkUspzBmk
MiVOKvFlRNACzqrOSbTqn3yDsEB750Orp2yjj32JgfpMpf/VjsPOS+C12LOORc92
wO1AK/1TD7Cn1TsNsYqiA94xrcx36m97PtbfkSIS5r762DL8EGMUUXLeXdYWk70p
aDPvOmbsB4om3xPXV2V4J95eSRQAogB/mqghtqmxlbCluQ0WEdrHbEg8QOB+DVrN
VjzRlwW5y0vtOUucxD/SVRNuJLDWcfr0wbrM7Rv1/oFB2ACYPTrIrnqYNxgFlQID
AQABo0IwQDAOBgNVHQ8BAf8EBAMCAYYwDwYDVR0TAQH/BAUwAwEB/zAdBgNVHQ4E
FgQU5K8rJnEaK0gnhS9SZizv8IkTcT4wDQYJKoZIhvcNAQEMBQADggIBAJ+qQibb
C5u+/x6Wki4+omVKapi6Ist9wTrYggoGxval3sBOh2Z5ofmmWJyq+bXmYOfg6LEe
QkEzCzc9zolwFcq1JKjPa7XSQCGYzyI0zzvFIoTgxQ6KfF2I5DUkzps+GlQebtuy
h6f88/qBVRRiClmpIgUxPoLW7ttXNLwzldMXG+gnoot7TiYaelpkttGsN/H9oPM4
7HLwEXWdyzRSjeZ2axfG34arJ45JK3VmgRAhpuo+9K4l/3wV3s6MJT/KYnAK9y8J
ZgfIPxz88NtFMN9iiMG1D53Dn0reWVlHxYciNuaCp+0KueIHoI17eko8cdLiA6Ef
MgfdG+RCzgwARWGAtQsgWSl4vflVy2PFPEz0tv/bal8xa5meLMFrUKTX5hgUvYU/
Z6tGn6D/Qqc6f1zLXbBwHSs09dR2CQzreExZBfMzQsNhFRAbd03OIozUhfJFfbdT
6u9AWpQKXCBfTkBdYiJ23//OYb2MI3jSNwLgjt7RETeJ9r/tSQdirpLsQBqvFAnZ
0E6yove+7u7Y/9waLd64NnHi/Hm3lCXRSHNboTXns5lndcEZOitHTtNCjv0xyBZm
2tIMPNuzjsmhDYAPexZ3FL//2wmUspO8IFgV6dtxQ/PeEMMA3KgqlbbC1j+Qa3bb
bP6MvPJwNQzcmRk13NfIRmPVNnGuV/u3gm3c
-----END CERTIFICATE-----
-----BEGIN CERTIFICATE-----
MIICCTCCAY6gAwIBAgINAgPlwGjvYxqccpBQUjAKBggqhkjOPQQDAzBHMQswCQYD
VQQGEwJVUzEiMCAGA1UEChMZR29vZ2xlIFRydXN0IFNlcnZpY2VzIExMQzEUMBIG
A1UEAxMLR1RTIFJvb3QgUjQwHhcNMTYwNjIyMDAwMDAwWhcNMzYwNjIyMDAwMDAw
WjBHMQswCQYDVQQGEwJVUzEiMCAGA1UEChMZR29vZ2xlIFRydXN0IFNlcnZpY2Vz
IExMQzEUMBIGA1UEAxMLR1RTIFJvb3QgUjQwdjAQBgcqhkjOPQIBBgUrgQQAIgNi
AATzdHOnaItgrkO4NcWBMHtLSZ37wWHO5t5GvWvVYRg1rkDdc/eJkTBa6zzuhXyi
QHY7qca4R9gq55KRanPpsXI5nymfopjTX15YhmUPoYRlBtHci8nHc8iMai/lxKvR
HYqjQjBAMA4GA1UdDwEB/wQEAwIBhjAPBgNVHRMBAf8EBTADAQH/MB0GA1UdDgQW
BBSATNbrdP9JNqPV2Py1PsVq8JQdjDAKBggqhkjOPQQDAwNpADBmAjEA6ED/g94D
9J+uHXqnLrmvT/aDHQ4thQEd0dlq7A/Cr8deVl5c1RxYIigL9zC2L7F8AjEA8GE8
p/SgguMh1YQdc4acLa/KNJvxn7kjNuK8YAOdgLOaVsjh4rsUecrNIdSUtUlD
-----END CERTIFICATE-----
"""
_https = None           # the certificate checking set up by https_context()
_way = "proxy"          # the way that reached ArenaSweats last: "proxy" (Windows' setting, when there is one) or "direct"
early_notes = []        # what happened before the log file was opened; written to it once it is


def https_context():
    """Certificate checking for ArenaSweats, used for every request to it: EXTRA_ROOTS plus Windows' own roots.
    The certificate and the name in it are always checked. A Windows store that cannot be read, or holds
    expired or odd certificates, at worst adds nothing; it can never switch the checking off."""
    global _https
    if _https is None:
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)      # requires a valid certificate for the right name
        try:
            context.load_verify_locations(cadata=EXTRA_ROOTS)
        except (ssl.SSLError, ValueError) as error:
            log.warning("could not add the carried root certificates: %s", error)
        try:
            # A bad certificate in Windows' store goes to the log, not the screen.
            with warnings.catch_warnings(record=True) as odd:
                warnings.simplefilter("always")
                context.load_default_certs()                    # Windows' roots (elsewhere, the system's)
            for warning in odd:
                log.info("Windows certificates: %s", warning.message)
        except (OSError, ssl.SSLError, ValueError) as error:
            log.warning("could not read Windows' root certificates; using only the carried ones: %s", error)
        _https = context
    return _https


def system_proxy(url):
    """The proxy that Windows' internet settings (or an HTTPS_PROXY variable) send `url` through, or None.
    An automatic proxy script (PAC) is not followed; network_setup() notes one in the log."""
    parts = urllib.parse.urlsplit(url)
    if parts.hostname in ("127.0.0.1", "localhost"):
        return None
    try:
        proxy = urllib.request.getproxies().get(parts.scheme)
        return proxy if proxy and not urllib.request.proxy_bypass(parts.hostname or "") else None
    except (OSError, ValueError):
        return None


def shown(proxy):
    """A proxy's address without its scheme or any user name and password, for the screen and the log."""
    return re.sub(r"^[A-Za-z0-9+.-]+://", "", proxy or "").rsplit("@", 1)[-1].rstrip("/")


def error_reason(error):
    """The error underneath urllib's wrapper."""
    reason = getattr(error, "reason", None) if isinstance(error, urllib.error.URLError) else None
    return reason if isinstance(reason, BaseException) else error


def error_detail(error):
    """All an error says about a failed request, for the log."""
    reason = error_reason(error)
    text = "%s: %s" % (type(reason).__name__, reason)
    if isinstance(reason, ssl.SSLCertVerificationError):
        text += " (verify code %s: %s)" % (reason.verify_code, reason.verify_message)
    return text


def connection_problem(error):
    """What went wrong with a request that got no answer, in a few words: (English, Simplified Chinese)."""
    reason = error_reason(error)
    if isinstance(reason, ssl.SSLCertVerificationError):
        if reason.verify_code in (9, 10):           # "not valid yet" or "expired": usually the PC's clock
            return "certificate not valid, check this PC's date and time", "证书无效，请检查电脑的日期和时间"
        return "certificate check failed", "证书验证失败"
    if isinstance(reason, (TimeoutError, socket.timeout)):
        return "timed out", "连接超时"
    if isinstance(reason, socket.gaierror):
        return "name lookup (DNS) failed", "域名解析（DNS）失败"
    if isinstance(reason, ssl.SSLError):
        return "secure connection (TLS) failed", "安全连接（TLS）失败"
    if isinstance(reason, ConnectionRefusedError):
        return "connection refused", "连接被拒绝"
    if isinstance(reason, (ConnectionResetError, ConnectionAbortedError, BrokenPipeError)):
        return "connection cut off", "连接被中断"
    if str(reason).startswith("Tunnel connection failed"):
        return "the proxy would not connect", "代理拒绝连接"
    if isinstance(reason, OSError) and reason.errno in (errno.ENETUNREACH, errno.EHOSTUNREACH, 10051, 10065):
        return "no network", "网络不通"
    return "network error", "网络错误"


def why_unreachable(status, tries=()):
    """Why ArenaSweats could not be used, in a few words: (English, Simplified Chinese).
    `tries` is what NoAnswer holds, when there was no answer (status 0)."""
    if status == 429:
        return "ArenaSweats is busy, status 429", "ArenaSweats 繁忙，状态码 429"
    if status >= 500:
        return "ArenaSweats server error, status %d" % status, "ArenaSweats 服务器出错，状态码 %d" % status
    if status:
        return "ArenaSweats answered with status %d" % status, "ArenaSweats 返回状态码 %d" % status
    proxied = any(way != "direct" for way, _, _ in tries)
    english, chinese = [], []
    for way, error, _ in tries:
        en, zh = connection_problem(error)
        if proxied:         # say which way had which problem
            en, zh = "%s: %s" % (way, en), "%s：%s" % ("直连" if way == "direct" else way.replace("proxy", "代理", 1), zh)
        if en not in english:
            english.append(en)
            chinese.append(zh)
    return "; ".join(english) or "no connection", "；".join(chinese) or "无法连接"


class NoAnswer(OSError):
    """No way of reaching ArenaSweats got an answer. `tries` holds (way, error, seconds) for each way tried."""

    def __init__(self, tries):
        super().__init__("; ".join("%s: %s after %.1fs" % (way, error_detail(error), seconds)
                                   for way, error, seconds in tries))
        self.tries = tries


def request_url(url, data=None, headers=None, method=None, timeout=30, limit=65536):
    """Send one request to ArenaSweats. Returns (status, up to `limit` bytes of the body, headers).
    It goes the way Windows' internet settings say: through their proxy when one is set. When that
    gets no answer it tries directly, and whichever way answered goes first from then on, so a
    proxy that is switched off or broken does not cut the collector off. Never follows a redirect.
    Raises NoAnswer when no way got an answer."""
    global _way
    proxy = system_proxy(url)
    ways = ["proxy", "direct"] if proxy else ["direct"]
    if _way == "direct":
        ways.reverse()
    tries = []
    for way in ways:
        opener = urllib.request.build_opener(
            urllib.request.ProxyHandler({urllib.parse.urlsplit(url).scheme: proxy} if way == "proxy" else {}),
            urllib.request.HTTPSHandler(context=https_context()), NoRedirect())
        request = urllib.request.Request(url, data=data, headers=headers or {}, method=method)
        started = time.monotonic()
        try:
            try:
                with opener.open(request, timeout=timeout) as response:
                    answer = response.status, response.read(limit), response.headers
            except urllib.error.HTTPError as error:         # an answer too, just not a happy one
                answer = error.code, error.read(limit), error.headers
        except (OSError, http.client.HTTPException) as error:
            tries.append(("proxy " + shown(proxy) if way == "proxy" else "direct", error, time.monotonic() - started))
            continue
        if tries:
            log.info("ArenaSweats answered %s after: %s", "directly" if way == "direct" else "through the proxy",
                     NoAnswer(tries))
        _way = way if proxy else _way
        return answer
    raise NoAnswer(tries)


def network_setup(url):
    """One line for the log on what reaching `url` depends on: the TLS library, the roots Windows holds, the proxy."""
    parts = [ssl.OPENSSL_VERSION]
    if hasattr(ssl, "enum_certificates"):       # Windows
        try:
            held = {hashlib.sha256(cert).hexdigest() for store in ("ROOT", "CA")
                    for cert, encoding, _ in ssl.enum_certificates(store) if encoding == "x509_asn"}
            parts.append("Windows holds %d certificates%s" % (len(held), "".join(
                ", %s: %s" % (name, "yes" if digest in held else "no") for digest, name in EXTRA_ROOT_NAMES.items())))
        except (OSError, ValueError) as error:
            parts.append("Windows certificates not readable: %s" % error)
    parts.append("proxy: %s" % (shown(system_proxy(url)) or "none"))
    try:
        import winreg
        with winreg.OpenKey(winreg.HKEY_CURRENT_USER, r"Software\Microsoft\Windows\CurrentVersion\Internet Settings") as key:
            script = registry_text(key, "AutoConfigURL")
        if script:
            parts.append("automatic proxy script (not followed): %.200s" % script)
    except (ImportError, OSError):
        pass
    return "; ".join(parts)


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
        self.key = key or None      # None until this install has signed up (Collector.sign_up)
        self.state = "collecting"   # what this collector is doing, sent with every request
        self.problem = []           # after a call that got no answer: what went wrong (NoAnswer.tries)
        log.info("network: %s", network_setup(self.url))
        self.fast = False           # the user turned fast mode on: "_fast" is added to the state sent

    def call(self, method, path, payload=None):
        """Returns (status, parsed JSON or None, seconds the server asked us to wait).
        Status 0 = unreachable; `problem` then says why."""
        headers = {"Accept": "application/json", "User-Agent": "arenasweats-collector/" + VERSION,
                   "X-Collector-State": self.state + ("_fast" if self.fast else "")}
        if self.key:
            headers["Authorization"] = "Bearer " + self.key
        body = None
        if payload is not None:
            body = gzip.compress(json.dumps(payload, separators=(",", ":")).encode())
            headers.update({"Content-Type": "application/json", "Content-Encoding": "gzip"})
        try:
            status, raw, answer_headers = request_url(self.url + path, body, headers, method, timeout=30)
        except NoAnswer as error:
            self.problem = error.tries
            log.warning("%s %s got no answer: %s", method, path, error)
            return 0, None, 0
        self.problem = []
        retry = answer_headers.get("Retry-After")
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
        self.fast = bool(getattr(opts, "fast", False))     # the user turned fast mode on (beta)
        self.fast_allowed = True           # ArenaSweats can switch fast mode off for everyone (policy "fast")
        self.policy_fast_delay = 0.0       # ... or make fast mode keep a longer gap (policy "fast_delay")
        self.fast_rest_until = float("-inf")    # the normal pace until then, after the client pushed back
        if server:
            server.fast = self.fast
        self.batch, self.next_upload, self.last_upload, self.upload_failures = UPLOAD_BATCH, 0.0, float("-inf"), 0
        self.stats, self.modes = Counter(), Counter()
        self.wrong_records, self.failed_fetches, self.rests = Counter(), Counter(), Counter()   # per match id
        self.rest_until = {}               # match id -> when a match that keeps failing may be tried again
        self.owed = {}                     # match id -> list entry, for matches still to be fetched after a failure
        self.last_said, self.last_status, self.folder_asks, self.described = None, now(), 0, False
        self.watch, self.last_lcu = ClientWatch(now, wall), None     # for the log: the client's programs
        self.idle_at = float("-inf")       # last time the client was seen idle

    # -- small helpers -------------------------------------------------------
    @property
    def fast_on(self):
        """Fast mode sets the pace now: the user turned it on, ArenaSweats allows it, and the client
        has not pushed back lately."""
        return self.fast and self.fast_allowed and self.now() >= self.fast_rest_until

    @property
    def floor(self):
        """The shortest gap there may be between two match-history requests: MIN_DELAY, or in fast mode
        FAST_MIN_DELAY, or the longer one ArenaSweats asks fast mode for."""
        return max(FAST_MIN_DELAY, self.policy_fast_delay) if self.fast_on else MIN_DELAY

    @property
    def delay(self):
        """Seconds between match-history requests. The server can raise it and fast mode divides it by
        FAST_FACTOR, but nothing can lower it below the floor."""
        delay = max(MIN_DELAY, self.opts.delay, self.policy_delay)
        return max(self.floor, delay / FAST_FACTOR) if self.fast_on else delay

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
            self.watch.tick(self.doing)
            left = end - self.now()
            if left <= 0:
                return
            self._sleep(min(left, 1.0))

    def doing(self):
        """What the collector is doing, in one line for the log (said when the client closes)."""
        parts = ["paused" if self.paused else "collecting" if self.lcu else "not connected"]
        lcu = self.lcu or self.last_lcu
        if lcu is not None and lcu.last:
            path, when, status = lcu.last
            parts.append("last request to the client %.0fs before: %s -> %s"
                         % (time.monotonic() - when, short_path(path), "no answer" if status is None else status))
            parts.append("%d requests to the client in %s on that connection"
                         % (lcu.sent, duration(time.monotonic() - lcu.opened)))
        if self.last_said:
            parts.append("last message: %s" % self.last_said)
        return "; ".join(parts)

    # -- main loop -----------------------------------------------------------
    def run(self):
        if self.server and not self.server.key:
            try:
                self.sign_up()
            except Done:
                self.say("Time limit reached.")
                return
        if self.server:
            self.refresh_policy(first=True)     # a wrong key stops here, before anything is read
        try:
            while True:
                try:
                    if self.lcu is None:
                        self.connect()
                    self.step()
                except ClientGone as error:
                    log.warning("client gone: %s. Collector: %s", error, self.doing())
                    self.last_lcu, self.lcu = self.lcu, None
                    self.watch.look(self.doing)     # which of the client's programs are still running
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
            self.heartbeat("waiting_for_client" if not running else "client_unreachable")
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
        set_quick_edit(True)            # so the folder can be pasted with a right-click
        try:
            folder = given_folder(input("Paste the folder where League is installed and press Enter: "))
        except EOFError:
            self.folder_asks = 3
            return
        finally:
            set_quick_edit(False)
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
        self.watch.describe_soon()
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
        """Return only when the client is idle (not in queue, champion select, a game or post-game).
        While it stays idle, its state is asked at most every PHASE_SECONDS, not before every request."""
        if not self.paused and self.now() - self.idle_at < PHASE_SECONDS:
            return
        while True:
            status, phase = self.lcu.get("/lol-gameflow/v1/gameflow-phase", timeout=10)
            if status == 200 and isinstance(phase, str) and phase in IDLE_PHASES:
                if self.now() - self.busy_at >= RESUME_GRACE:
                    self.idle_at = self.now()
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
                self.heartbeat("paused")
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
            if self.failures:
                self.heartbeat("client_failing")
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
        if not ok and self.fast_on and (status == 429 or self.failures >= 3):
            self.fast_rest_until = self.now() + FAST_REST       # the client is pushing back: no fast mode for a while
            self.say("The League client is struggling, so fast mode rests for an hour (normal pace). "
                     "客户端响应异常，快速模式暂停一小时（恢复正常速度）。")
        # After failures, wait longer and longer (3s, 6s, 12s ... up to MAX_BACKOFF).
        backoff = 0 if ok else min(MAX_BACKOFF, 3 * 2 ** min(self.failures - 1, 10))
        self.next_allowed = self.now() + max(self.floor, self.delay * random.uniform(0.8, 1.2)) + backoff
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
        """What the server can set: pause, go slower, switch fast mode off or slow it down, show a short
        message, the date the season began."""
        if not isinstance(policy, dict):
            return
        self.policy_at = self.now()
        self.enabled = policy.get("enabled") is not False
        fast_allowed = policy.get("fast") is not False
        if self.fast and fast_allowed != self.fast_allowed:
            self.say("ArenaSweats has switched fast mode back on. 快速模式已恢复。" if fast_allowed else
                     "ArenaSweats has switched fast mode off for now, so collection goes at the normal pace. "
                     "ArenaSweats 已暂时关闭快速模式，现按正常速度采集。")
        self.fast_allowed = fast_allowed
        season = policy.get("season_start")
        if isinstance(season, int) and not isinstance(season, bool) and 0 < season < 2 ** 53:
            self.season_start = season
        delay = policy.get("delay")
        if isinstance(delay, (int, float)) and not isinstance(delay, bool) and 0 <= delay <= 3600:
            self.policy_delay = min(60.0, float(delay))
        fast_delay = policy.get("fast_delay")      # fast mode's shortest gap; never below FAST_MIN_DELAY
        if isinstance(fast_delay, (int, float)) and not isinstance(fast_delay, bool) and 0 <= fast_delay <= 3600:
            self.policy_fast_delay = min(60.0, float(fast_delay))
        message = policy.get("message")
        if isinstance(message, str) and message != self.message:
            self.message = message
            if message:
                self.say("Message from ArenaSweats: " + "".join(c for c in message[:200] if c.isprintable()))

    def bad_key(self):
        self.server = None
        return Fatal("ArenaSweats did not accept the upload key. Check the key in config.json.")

    def sign_up(self):
        """A new install asks ArenaSweats for its own upload key, once, and keeps it in config.json."""
        while True:
            status, data, wait = self.server.call("POST", "/v1/register", {"version": VERSION})
            key = as_dict(data).get("key")
            if status == 201 and isinstance(key, str) and KEY_RE.fullmatch(key):
                self.server.key = key
                save_config(key=key)
                log.info("signed up with ArenaSweats as %.40s", as_dict(data).get("name"))
                self.say("Signed up with ArenaSweats.")
                return
            if status == 403:
                raise Fatal("ArenaSweats is not taking new collectors right now. Please try again another day.\n"
                            "ArenaSweats 目前不接受新的采集器，请改天再试。")
            log.info("sign-up answered %s", status)
            wait = max(60, wait)
            self.cannot_reach(status, -(-wait // 60))
            self.sleep(wait)

    def cannot_reach(self, status, minutes=None):
        """Say briefly, in English and Simplified Chinese, why ArenaSweats could not be reached and
        when the next try is. The details are in the log file, which the last line names."""
        english, chinese = why_unreachable(status, self.server.problem)
        when = (("Trying again in %d min." % minutes, "%d 分钟后重试。" % minutes) if minutes
                else ("Trying again shortly.", "稍后重试。"))
        self.say("Cannot reach ArenaSweats yet: %s. %s\n%s暂时连不上 ArenaSweats：%s。%s\n%sDetails / 详细信息: %s"
                 % (english, when[0], " " * 9, chinese, when[1], " " * 9, os.path.join(self.data_dir, "collector.log")),
                 once=True)

    def heartbeat(self, state):
        """While it waits, still tell ArenaSweats every 10 minutes what it is doing, so a paused
        collector looks different from one that was closed."""
        if self.server and self.server.key and self.now() - self.policy_at >= POLICY_SECONDS:
            self.server.state = state
            try:
                self.refresh_policy()
            finally:
                self.server.state = "collecting"

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
            self.cannot_reach(status)

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


MAX_UPDATE_BYTES = 60 * 1024 * 1024


def version_tuple(text):
    """'0.3.4' as (0, 3, 4), or None when it is not a plain version number."""
    if not isinstance(text, str) or not re.fullmatch(r"[0-9]{1,4}(\.[0-9]{1,4}){1,3}", text):
        return None
    return tuple(int(part) for part in text.split("."))


def latest_release(base_url):
    """What ArenaSweats says the newest collector is ({version, sha256, url}), or None.
    The download must come from the same server this collector talks to."""
    base = base_url.rstrip("/")
    status, body, _ = request_url(base + "/latest", headers={"User-Agent": "arenasweats-collector/" + VERSION},
                                  timeout=10)
    data = as_dict(parse_json(body)) if status == 200 else {}
    path, digest = data.get("path"), data.get("sha256")
    if (version_tuple(data.get("version")) and isinstance(path, str) and re.fullmatch(r"/download/[A-Za-z0-9_.-]{1,80}", path)
            and isinstance(digest, str) and re.fullmatch(r"[0-9a-f]{64}", digest)):
        return {"version": data["version"], "sha256": digest, "url": base + path}
    return None


def install_update(release, exe_path):
    """Download the new program beside this one, check it against its SHA-256 and put it in this
    one's place. The running program is renamed to <name>.old (Windows allows that), not deleted;
    it is removed on the next start. Raises on any problem, leaving this program as it was."""
    temp = os.path.join(os.path.dirname(exe_path), ".ArenaSweatsCollector.update")
    old = exe_path + ".old"
    status, body, _ = request_url(release["url"], headers={"User-Agent": "arenasweats-collector/" + VERSION},
                                  timeout=60, limit=MAX_UPDATE_BYTES + 1)
    if status != 200:
        raise ValueError("the download answered with status %d" % status)
    if len(body) > MAX_UPDATE_BYTES:
        raise ValueError("the download is too large")
    if hashlib.sha256(body).hexdigest() != release["sha256"]:
        raise ValueError("the download does not match its checksum")
    try:
        with open(temp, "wb") as out:
            out.write(body)
        if os.path.exists(old):
            os.remove(old)
        os.replace(exe_path, old)
        try:
            os.replace(temp, exe_path)
        except OSError:
            os.replace(old, exe_path)       # put this program back
            raise
    finally:
        if os.path.exists(temp):
            os.remove(temp)


ANSWER_SECONDS = 10    # a start-up question nobody answers takes its default after this long (admin, 2026-11-24)


def timed_input(prompt, seconds=ANSWER_SECONDS):
    """input() for the start-up questions, so a collector started with nobody at the PC still starts: returns None
    when nothing is typed within `seconds`. In the Windows console it counts the seconds down after the question;
    once a key is pressed the count goes and it waits for Enter, like input()."""
    sys.stdout.write(prompt)
    sys.stdout.flush()
    if os.name == "nt":
        if sys.stdin is sys.__stdin__ and sys.stdin is not None and sys.stdin.isatty():
            return _console_timed_input(seconds)
        return input()                          # not the console (a pipe, or tests): an answer a line, as input()
    import select
    try:
        ready = select.select([sys.stdin], [], [], seconds)[0]
    except (OSError, ValueError):               # not a stream it can wait on: wait for the answer as input() does
        return input()
    if not ready:
        sys.stdout.write("\n")
        return None
    line = sys.stdin.readline()
    if not line:
        raise EOFError
    return line.rstrip("\r\n")


def _console_timed_input(seconds):
    """timed_input() in the Windows console: read key by key, with the seconds left shown after the question."""
    import msvcrt
    typed, deadline, shown = [], time.monotonic() + seconds, ""

    def show(count):                            # replace the shown count (only it: the question stays)
        nonlocal shown
        spare = max(0, len(shown) - len(count))
        sys.stdout.write("\b" * len(shown) + count + " " * spare + "\b" * spare)
        sys.stdout.flush()
        shown = count

    show("%d " % seconds)
    while True:
        if msvcrt.kbhit():
            char = msvcrt.getwch()
            if deadline is not None:            # someone is typing: the count goes, and it waits for Enter
                show("")
                deadline = None
            if char in ("\x00", "\xe0"):        # an arrow or function key; its second half follows
                msvcrt.getwch()
            elif char == "\x03":
                raise KeyboardInterrupt
            elif char in ("\r", "\n"):
                sys.stdout.write("\n")
                sys.stdout.flush()
                return "".join(typed)
            elif char == "\x08":
                if typed:
                    typed.pop()
                    sys.stdout.write("\b \b")
            else:
                typed.append(char)
                sys.stdout.write(char)
            sys.stdout.flush()
        elif deadline is not None:
            left = deadline - time.monotonic()
            if left <= 0:
                show("0")
                sys.stdout.write("\n")
                sys.stdout.flush()
                return None
            count = "%d " % int(left + 0.999)
            if count != shown:
                show(count)
            time.sleep(0.05)
        else:
            time.sleep(0.05)


def offer_update(base_url, ask=timed_input, spawn=subprocess.Popen):
    """When ArenaSweats has a newer collector, ask whether to update; on yes, or no answer within ANSWER_SECONDS,
    install and start it. Returns True when the new version was started (this one should then stop)."""
    try:
        release = latest_release(base_url)
    except (OSError, ValueError, http.client.HTTPException) as error:
        early_notes.append("update check got no answer: %s" % error)
        return False                        # no answer: carry on with this version
    if not release or version_tuple(release["version"]) <= version_tuple(VERSION):
        return False
    print("A new version of the collector is available: %s (you have %s)." % (release["version"], VERSION))
    print("有新版本的采集器：%s（当前 %s）。" % (release["version"], VERSION))
    try:
        answer = ask("Update now? 现在更新？ [Y/n]  No answer = update / 不回答即更新: ")
    except (EOFError, KeyboardInterrupt):
        answer = "n"
    if answer is None:
        print("No answer: updating. 无人回答：自动更新。")
        answer = "y"
    answer = answer.strip().lower()
    if answer not in ("", "y", "yes", "是"):
        print("Keeping version %s." % VERSION)
        return False
    print("Downloading %s ..." % release["version"])
    try:
        install_update(release, sys.executable)
    except (OSError, ValueError, http.client.HTTPException) as error:
        print("The update did not work (%s). Carrying on with version %s." % (error, VERSION))
        return False
    print("Updated to %s. Starting it ..." % release["version"])
    spawn([sys.executable] + sys.argv[1:])
    return True


FAST_ON = "Fast mode is on (beta). 快速模式已开启（测试版）。"


def ask_fast_mode(ask=timed_input):
    """Ask at start whether to turn fast mode on. Only a yes turns it on; Enter, or no answer within ANSWER_SECONDS,
    means no."""
    print("Fast mode collects about %s times faster. BETA: Please only use with unimportant accounts for now."
          % FAST_SPEEDUP)
    print("快速模式的采集速度约为正常速度的 %s 倍。测试版：目前请只在不重要的账号上使用。" % FAST_SPEEDUP)
    try:
        answer = ask("Activate fast mode? 开启快速模式？ [y/N]  No answer = normal mode / 不回答即普通模式: ")
    except (EOFError, KeyboardInterrupt):
        answer = "n"
    if answer is None:
        print("No answer: starting in normal mode. 无人回答：以普通模式开始。")
        return False
    return answer.strip().lower() in ("y", "yes", "是")


def remove_old_program():
    """Delete the previous version left behind by an update, once it is no longer running."""
    try:
        if os.path.exists(sys.executable + ".old"):
            os.remove(sys.executable + ".old")
    except OSError:
        pass


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


def set_quick_edit(on):
    """Clicking in a console window normally freezes the program until Enter is pressed
    (Windows' "QuickEdit" selection). It is off while collecting, so a stray click cannot
    stop collection, and on only while the user is asked to paste something."""
    if os.name != "nt":
        return
    try:
        import ctypes
        k32 = ctypes.WinDLL("kernel32")
        k32.GetStdHandle.restype = ctypes.c_void_p
        handle = ctypes.c_void_p(k32.GetStdHandle(-10))        # the console's input
        mode = ctypes.c_uint32()
        if handle.value and k32.GetConsoleMode(handle, ctypes.byref(mode)):
            mode = (mode.value | 0x0040) if on else (mode.value & ~0x0040)    # 0x40 = QuickEdit
            k32.SetConsoleMode(handle, mode | 0x0080)                         # 0x80 = apply it
    except (OSError, AttributeError, ValueError):
        pass


def keep_awake():
    """Keep the computer from going to sleep while this program runs (the screen can still
    turn off). Windows lifts this by itself when the program closes."""
    if os.name != "nt":
        return
    try:
        import ctypes
        ctypes.WinDLL("kernel32").SetThreadExecutionState(0x80000000 | 0x00000001)   # continuous, system needed
    except (OSError, AttributeError):
        pass


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
    parser.add_argument("--test-run", action="store_true",
                        help="upload nothing: collect for %d minutes, then write one file to send back" % TEST_MINUTES)
    parser.add_argument("--server", help="upload address (default: from config.json, else %s)" % DEFAULT_SERVER)
    parser.add_argument("--key", help="upload key (default: from config.json; a new install gets its own)")
    parser.add_argument("--delay", type=float, default=DEFAULT_DELAY,
                        help="seconds between match-history requests (default %.0f, never below %.0f)"
                             % (DEFAULT_DELAY, MIN_DELAY))
    parser.add_argument("--since", metavar="YYYY-MM-DD", default=SEASON_START,
                        help="read nothing older than this UTC date, the start of the season (default %(default)s; "
                             "with an upload address, ArenaSweats sets it)")
    parser.add_argument("--data-dir", help="where to keep collected data (default: 'data' beside this program)")
    parser.add_argument("--fast", action=argparse.BooleanOptionalAction,
                        help="BETA: collect about %s times faster; please only use with unimportant accounts for now "
                             "(either flag skips the question at start)" % FAST_SPEEDUP)
    parser.add_argument("--no-update", action="store_true", help="don't check for a newer version at start")
    parser.add_argument("--export", metavar="FILE",
                        help="write the match records held locally to FILE (JSON lines; .gz to compress) and exit")
    opts = parser.parse_args(argv)

    for stream in (sys.stdout, sys.stderr):     # never crash on a character the console cannot show
        if stream and hasattr(stream, "reconfigure"):
            stream.reconfigure(errors="replace")

    config = load_config(program_dir())
    if getattr(sys, "frozen", False) and os.name == "nt":       # the packaged Windows program
        remove_old_program()
        if not opts.no_update and not opts.export and sys.stdin and sys.stdin.isatty():
            if offer_update(opts.server or config.get("server") or DEFAULT_SERVER):
                return 0
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
    for text in early_notes:
        log.info("%s", text)

    code, lock, made = 0, None, None
    try:
        opts.league_dir = opts.league_dir or config.get("league_dir")
        try:
            opts.since = date_ms(opts.since)
        except ValueError:
            raise Fatal("--since must be a date like 2026-10-06")
        server_url = None if opts.test_run else (opts.server or config.get("server", DEFAULT_SERVER))
        server = Server(server_url, opts.key or config.get("key")) if server_url else None
        if opts.minutes is None:
            opts.minutes = 0 if server else TEST_MINUTES
        lock = lock_single_instance()
        print("ArenaSweats collector %s - reads Arena match history from your League client. Read-only." % VERSION)
        if server is None:
            print("This is a test run: it collects for up to %g min, then writes one file\n"
                  "for you to send back. Nothing is uploaded." % opts.minutes
                  if opts.minutes else "Nothing is uploaded: matches are only saved on this computer.")
        else:
            print("It sends the Arena matches it reads to ArenaSweats%s. Close this window to stop."
                  % (", for up to %g min" % opts.minutes if opts.minutes else ""))
        if server and opts.fast is None and sys.stdin and sys.stdin.isatty():
            opts.fast = ask_fast_mode()         # after the update offer, so an update never asks twice
        opts.fast = bool(opts.fast)
        log.info("fast mode: %s", "on (experimental)" if opts.fast else "off")
        if opts.fast:
            print(FAST_ON)
        set_quick_edit(False)
        keep_awake()
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
