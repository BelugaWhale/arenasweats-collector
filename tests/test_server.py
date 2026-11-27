"""Tests for server/ingest.py, and for the collector working with it.  Run:  python -m unittest discover tests"""

import argparse
import collections
import contextlib
import copy
import gzip
import hashlib
import http.client
import io
import json
import os
import shutil
import socket
import ssl
import subprocess
import sys
import tempfile
import threading
import time
import unittest
import unittest.mock
import urllib.error
import urllib.request
import warnings

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
sys.path.insert(0, os.path.join(ROOT, "server"))
sys.path.insert(0, os.path.join(ROOT, "tests"))

import collector  # noqa: E402
import ingest  # noqa: E402
from fake_lcu import FakeClient, PretendFlights  # noqa: E402
from test_collector import CollectorCase, history  # noqa: E402
from werkzeug.serving import make_server  # noqa: E402


def utc_date(seconds):
    return time.strftime("%Y-%m-%d", time.gmtime(seconds))


class ServerCase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        os.environ["INGEST_DB"] = os.path.join(self.tmp.name, "ingest.db")
        self.addCleanup(os.environ.pop, "INGEST_DB", None)
        ingest.recent.clear()
        ingest.seen_written.clear()
        self.now = time.time()                       # the server's clock, moved by hand in these tests
        ingest.clock = lambda: self.now
        self.addCleanup(setattr, ingest, "clock", time.time)
        self.db = ingest.connect()
        self.addCleanup(self.db.close)
        self.key = self.add_key("alice")
        self.client = ingest.app.test_client()
        self.world = FakeClient(players=20, arena=12, other=0)
        self.addCleanup(self.world.close)
        self.games = [self.world.games[i] for i in sorted(self.world.arena_ids())]
        self.season = ingest.get_policy(self.db)["season_start"]

    def add_key(self, name):
        return self.admin("add-key", name).split()[-1]

    def relay(self, *request, stdin=None):
        """The rankings server's relay command, as its SSH key runs it."""
        output = io.StringIO()
        with contextlib.redirect_stdout(output), unittest.mock.patch.object(sys, "stdin", io.StringIO(stdin or "")):
            ingest.main(["relay", *request])
        return output.getvalue()

    def admin(self, *args):
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            ingest.main(list(args))
        return output.getvalue()

    def send(self, path, body, key=None, compress=True, raw=None):
        data = raw if raw is not None else json.dumps(body).encode()
        headers = {"Authorization": "Bearer " + (key or self.key), "Content-Type": "application/json"}
        if compress:
            data = gzip.compress(data)
            headers["Content-Encoding"] = "gzip"
        return self.client.post(path, data=data, headers=headers)

    def post(self, games, **kwargs):
        return self.send("/v1/games", {"games": games}, **kwargs)

    def work(self, reports=(), key=None, **extra):
        return self.send("/v1/work", dict({"platform": "OC1", "reports": list(reports)}, **extra), key=key).get_json()

    def report(self, item, ids, complete=True, read_to=None, following=0):
        """A collector's report on a stretch of a list it was handed (`item`), or on any list (a player id)."""
        puuid, job = (item, None) if isinstance(item, str) else (item["puuid"], item["job"])
        return {"puuid": puuid, "job": job, "ids": ids, "failed": not complete, "next": following,
                "newest": int(self.now * 1000) if read_to is None else read_to}

    def handed(self, puuid, key=None, **extra):
        """Ask for work with only `puuid`'s list waiting (the others are held back for this one request), and
        return its hand-out."""
        self.db.execute("INSERT OR IGNORE INTO players(platform, puuid, due_at) VALUES('OC1', ?, 0)", (puuid,))
        self.db.execute("UPDATE players SET due_at = 0, leased_until = 0 WHERE puuid = ?", (puuid,))
        others = self.db.execute("SELECT puuid FROM players WHERE puuid != ? AND leased_until < ?",
                                 (puuid, int(self.now))).fetchall()
        self.db.executemany("UPDATE players SET leased_until = ? WHERE puuid = ?",
                            [(int(self.now) + 5, other) for other, in others])
        self.db.commit()
        item = self.item(self.work(key=key, **extra), puuid)
        self.db.executemany("UPDATE players SET leased_until = 0 WHERE puuid = ?", others)
        self.db.commit()
        return item

    @staticmethod
    def item(answer, puuid):
        found = [item for item in answer["players"] if item["puuid"] == puuid]
        return found[0] if found else None

    @staticmethod
    def shape(item):
        return {name: item[name] for name in ("since", "begin", "pages")} if item else None

    def count(self, table):
        return self.db.execute("SELECT COUNT(*) FROM " + table).fetchone()[0]

    def player(self, puuid):
        return self.db.execute("SELECT games, read_to, checked_at, due_at, fails FROM players WHERE puuid = ?",
                               (puuid,)).fetchone()

    def played(self, game_id, seconds_ago):
        """An Arena match with this id, played that long ago."""
        game = copy.deepcopy(self.games[0])
        game.update(gameId=game_id, gameCreation=int((self.now - seconds_ago) * 1000))
        return game


class Keys(ServerCase):
    def test_the_client_check_is_served_without_a_key(self):
        answer = self.client.get("/check")
        self.assertEqual(answer.status_code, 200)
        self.assertIn(b"ArenaSweats login check", answer.data)
        self.assertFalse(answer.data.startswith(b"\xef\xbb\xbf"))        # no byte-order mark: it is piped into iex

    def test_last_seen_and_state_are_kept(self):
        headers = {"Authorization": "Bearer " + self.key, "X-Collector-State": "paused",
                   "User-Agent": "arenasweats-collector/0.3.2"}
        self.assertEqual(self.client.get("/v1/policy", headers=headers).status_code, 200)
        self.assertEqual(self.db.execute("SELECT state, version FROM contributor_seen").fetchone(), ("paused", "0.3.2"))
        self.assertIn("paused", self.admin("keys"))
        headers["X-Collector-State"] = "<script>"
        self.now += 120
        self.client.get("/v1/policy", headers=headers)
        self.assertEqual(self.db.execute("SELECT state FROM contributor_seen").fetchone()[0], "unknown")
        self.assertEqual(ingest.relay_status(self.db, self.now)["installs_seen"]["last_hour"], {"unknown": 1})

    def test_a_collectors_numbers_about_its_own_pace_are_kept_and_nothing_else(self):
        # These numbers decide how fast collectors may go; junk in them must neither be stored nor break a request.
        headers = {"Authorization": "Bearer " + self.key, "User-Agent": "arenasweats-collector/0.3.8"}
        report = {"seconds": 300, "requests": 700, "failed": 1, "refused": 0,
                  "lists": [[20, 3, 1700, 1900, 60, 1], [100, 30, 9000, 12000, 2950, 2]],
                  "match": [270, 60, 80], "calls": [60, 950, 1400], "paused": 0, "rested": 0,
                  "idle": 4, "expired": 0, "fast": 1, "flight": 3, "most": 4}
        sent = dict(report, name="Somebody#123", calls=[60, 950, "x"], extra={"a": 1})      # only numbers we know stay
        headers["X-Collector-Pace"] = json.dumps(sent)
        self.assertEqual(self.client.get("/v1/policy", headers=headers).status_code, 200)
        kept = dict(report)
        del kept["calls"]
        row = self.db.execute("SELECT version, report FROM contributor_pace").fetchone()
        self.assertEqual((row[0], json.loads(row[1])), ("0.3.8", kept))
        for junk in ("x" * 900, "{", "[1, 2]", '"text"', json.dumps({"requests": "many"}), json.dumps({"requests": -1}),
                     json.dumps({"requests": 1.5}), json.dumps({"requests": True}), "[" * 500,
                     json.dumps({"lists": [[100, 30, 9000, 12000, 2950]]}), json.dumps({"lists": [[1, 2, 3, 4, 5, "6"]]}),
                     json.dumps({"lists": [[1] * 6] * 9}), json.dumps({"lists": {"100": 1}})):
            headers["X-Collector-Pace"] = junk
            self.assertEqual(self.client.get("/v1/policy", headers=headers).status_code, 200, junk[:20])
        self.assertEqual(self.count("contributor_pace"), 1)
        headers["Authorization"] = "Bearer wrong"                          # and never without a key
        headers["X-Collector-Pace"] = json.dumps(report)
        self.assertEqual(self.client.get("/v1/policy", headers=headers).status_code, 401)
        self.assertEqual(self.count("contributor_pace"), 1)
        shown = self.admin("pace")
        self.assertIn("alice", shown)
        self.assertIn("300 s: 700 requests (1 failed, 0 refused), 3 at once (most 4)", shown)
        self.assertIn("matches 270 x 60 ms (slow 80)", shown)
        self.assertIn("lists of 100: 30 x 9000 ms (slow 12000), 0.092 s a match listed, 2 more from the saved copy", shown)
        self.assertIn("No pace numbers received from bob.", self.admin("pace", "bob"))
        self.now += (ingest.KEEP_PACE_DAYS + 1) * 86400                    # old ones go when a new one arrives
        headers["Authorization"] = "Bearer " + self.key
        self.client.get("/v1/policy", headers=headers)
        self.assertEqual(self.count("contributor_pace"), 1)

    def test_why_an_install_is_not_collecting_is_kept(self):
        # Why: this code is the only way to learn why an install sends nothing; lost or mangled, we are blind again.
        headers = {"Authorization": "Bearer " + self.key, "X-Collector-State": "client_unreachable",
                   "X-Collector-Detail": "path_hidden.lock_badfile.cmd_none",
                   "User-Agent": "arenasweats-collector/0.3.8"}
        self.client.get("/v1/policy", headers=headers)
        self.assertEqual(self.db.execute("SELECT state, detail FROM contributor_seen").fetchone(),
                         ("client_unreachable", "path_hidden.lock_badfile.cmd_none"))
        self.assertIn("client_unreachable 0.3.8   why: path_hidden.lock_badfile.cmd_none", self.admin("keys"))
        headers.update({"X-Collector-State": "paused", "X-Collector-Detail": "phase_endofgame"})     # a new reason
        self.client.get("/v1/policy", headers=headers)                                         # is kept at once
        self.assertEqual(self.db.execute("SELECT state, detail FROM contributor_seen").fetchone(),
                         ("paused", "phase_endofgame"))
        for odd in ("C:\\Users\\张三\\英雄联盟", "x" * 65):                                       # never a path
            headers["X-Collector-Detail"] = odd
            self.now += 120
            self.client.get("/v1/policy", headers=headers)
            self.assertEqual(self.db.execute("SELECT detail FROM contributor_seen").fetchone()[0], "unknown")
        del headers["X-Collector-Detail"]                                                      # older collectors
        self.now += 120
        self.client.get("/v1/policy", headers=headers)
        self.assertEqual(self.db.execute("SELECT detail FROM contributor_seen").fetchone()[0], "")

    def say(self, key, state, detail="", version="0.3.8"):
        """A check-in by the install holding `key`, saying what it is doing."""
        headers = {"Authorization": "Bearer " + key, "X-Collector-State": state, "X-Collector-Detail": detail,
                   "User-Agent": "arenasweats-collector/" + version}
        self.assertEqual(self.client.get("/v1/policy", headers=headers).status_code, 200)

    def history(self, name="alice"):
        return self.db.execute("SELECT h.state, h.detail, h.version FROM contributor_history h JOIN contributors c "
                               "ON c.id = h.contributor_id WHERE c.name = ? ORDER BY h.at, h.rowid", (name,)).fetchall()

    def test_each_change_of_what_an_install_says_is_kept_and_stays_small(self):
        # Why: only the latest state was kept, so nobody could tell where new installs gave up; and kept without a limit,
        # a collector flapping between two states could fill the website's disk.
        self.say(self.key, "starting", "admin_no")
        self.say(self.key, "waiting_for_client", "no_client.launcher.admin_no")
        self.now += 5
        self.say(self.key, "waiting_for_client", "no_client.launcher.admin_no")        # the same again: no new row
        self.now += 300
        self.say(self.key, "waiting_for_client", "no_client.launcher.admin_no")
        self.say(self.key, "starting", "admin_no")                                      # back and forth: each kept
        self.say(self.key, "collecting", "found_cmdline")
        self.say(self.key, "collecting", "found_cmdline", version="0.3.9")              # a new version too
        self.assertEqual(self.history(), [("starting", "admin_no", "0.3.8"),
                                          ("waiting_for_client", "no_client.launcher.admin_no", "0.3.8"),
                                          ("starting", "admin_no", "0.3.8"), ("collecting", "found_cmdline", "0.3.8"),
                                          ("collecting", "found_cmdline", "0.3.9")])
        shown = self.admin("keys", "--history", "alice")
        self.assertRegex(shown.splitlines()[1], r"^\d{4}-\d\d-\d\d \d\d:\d\d:\d\d  waiting_for_client +0\.3\.8 +"
                                                r"no_client\.launcher\.admin_no$")
        self.assertIn("nobody: no such contributor", self.admin("keys", "--history", "nobody"))
        self.addCleanup(setattr, ingest, "HISTORY_PER_INSTALL", ingest.HISTORY_PER_INSTALL)
        ingest.HISTORY_PER_INSTALL = 3
        for number in range(6):
            self.now += 1
            self.say(self.key, "paused", "phase_%d" % number)
        self.assertEqual([detail for _, detail, _ in self.history()], ["phase_3", "phase_4", "phase_5"])
        bob = self.add_key("bob")
        self.now += ingest.HISTORY_DAYS * 86400 + 1                                     # old changes go
        self.say(bob, "collecting", "found_running")
        self.assertEqual((self.history(), self.history("bob")), ([], [("collecting", "found_running", "0.3.8")]))
        self.assertEqual(self.db.execute("SELECT state FROM contributor_seen WHERE contributor_id = 1").fetchone()[0],
                         "paused")                                                      # (the latest stays in keys)

    def test_how_far_new_installs_got_is_shown(self):
        # Why: volunteers who give up say nothing; this is where admin sees which step loses them, to fix that step.
        keys = {name: self.add_key(name) for name in ("gave-up", "collects", "declined", "old-one")}
        self.db.execute("UPDATE contributors SET created_at = ? WHERE name = 'old-one'", (int(self.now) - 9 * 86400,))
        self.db.commit()
        self.say(keys["gave-up"], "starting", "admin_no")
        self.say(keys["gave-up"], "waiting_for_client", "no_client.launcher.admin_no")
        self.say(keys["gave-up"], "closed", "window")
        self.say(keys["collects"], "starting", "admin")
        self.say(keys["collects"], "collecting_fast", "found_cmdline")
        self.post(self.games[:2], key=keys["collects"])
        self.now += 2 * 3600
        self.say(keys["collects"], "collecting_fast", "found_cmdline")                  # two hours on, still at it
        self.say(keys["declined"], "starting", "admin_declined")
        self.say(keys["declined"], "client_unreachable", "path_hidden.lock_none.cmd_none.admin_declined")
        self.say(keys["declined"], "collecting", "found_usual")                         # found it, but went quiet
        self.say(keys["old-one"], "collecting", "found_running")
        counts = json.loads(self.relay("status"))["newcomers"]
        self.assertEqual(counts, {"days": 7, "signed_up": 4, "admin": {"admin": 1, "admin_no": 1, "admin_declined": 1},
                                  "found_client": 2, "first_match": 1, "collecting_an_hour": 1,
                                  "gave_up_at": {"collecting found_usual": 1, "never seen": 1,
                                                 "waiting_for_client no_client.launcher.admin_no": 1}})
        shown = self.admin("newcomers")
        self.assertIn("Installs that signed up in the last 7 days: 4", shown)
        self.assertIn("answered the administrator question: yes 1, no or no answer 1, yes but the Windows box was "
                      "declined 1; no answer told 1", shown)
        self.assertIn("still collecting 1 hour after they began: 1", shown)
        self.assertIn("  1  waiting_for_client no_client.launcher.admin_no", shown)
        self.assertRegex(shown, r"collects +signed up .* admin: admin +found_cmdline +first match: \S+ \S+ \(2\) +"
                                r"collecting from .* \(an hour and more\)")
        self.assertRegex(shown, r"gave-up .* last: closed window after waiting_for_client no_client\.launcher\.admin_no")
        self.assertIn("Installs that signed up in the last 10 days: 5", self.admin("newcomers", "--days", "10"))

    def test_a_database_from_before_the_detail_column_still_takes_check_ins(self):
        # Why: every collector request records a check-in first; if that write failed, every collector would stop.
        path = os.path.join(self.tmp.name, "old.db")
        old = ingest.sqlite3.connect(path)
        old.executescript(ingest.SCHEMA.replace(",\n    detail TEXT NOT NULL DEFAULT ''", ""))
        old.execute("INSERT INTO contributor_seen VALUES(99, 100, 'collecting', '0.3.7')")
        old.commit()
        old.close()
        os.environ["INGEST_DB"] = path
        self.key = self.add_key("bob")
        headers = {"Authorization": "Bearer " + self.key, "X-Collector-State": "paused",
                   "X-Collector-Detail": "phase_inprogress"}
        self.assertEqual(self.client.get("/v1/policy", headers=headers).status_code, 200)
        db = ingest.connect()
        self.addCleanup(db.close)
        self.assertEqual(db.execute("SELECT contributor_id, state, detail FROM contributor_seen ORDER BY 1").fetchall(),
                         [(1, "paused", "phase_inprogress"), (99, "collecting", "")])
        self.assertIn("why: phase_inprogress", self.admin("keys"))

    def test_no_key_or_wrong_key_is_refused(self):
        self.assertEqual(self.client.post("/v1/games", data=b"{}").status_code, 401)
        self.assertEqual(self.client.post("/v1/work", data=b"{}").status_code, 401)
        self.assertEqual(self.client.get("/v1/policy").status_code, 401)
        self.assertEqual(self.post(self.games, key="as_wrong").status_code, 401)
        self.assertEqual(self.count("games"), 0)

    def test_key_is_stored_only_as_a_hash(self):
        with open(os.environ["INGEST_DB"], "rb") as handle:
            self.assertNotIn(self.key.encode(), handle.read())

    def test_disabled_key_is_refused_and_can_be_restored(self):
        self.admin("disable-key", "alice")
        self.assertEqual(self.post(self.games).status_code, 403)
        self.admin("enable-key", "alice")
        self.assertEqual(self.post(self.games).status_code, 200)

    def test_health_check_needs_no_key(self):
        self.assertEqual(self.client.get("/healthz").status_code, 200)


class Relay(ServerCase):
    # The relay empties records on this server: a bug here would lose collected matches for good.
    def exported(self, limit=100):
        return [json.loads(line) for line in self.relay("export", str(limit)).splitlines()]

    def test_matches_held_before_the_outbox_existed_are_queued_once(self):
        self.post(self.games[:5])
        self.db.execute("DROP TRIGGER games_to_outbox")
        self.db.execute("DELETE FROM outbox")
        self.db.commit()
        self.post(self.games[5:8])                  # received while an older version ran: no trigger
        ingest.prepared.clear()
        ingest.connect().close()                     # the new version starts
        ingest.connect().close()
        self.post(self.games[8:])
        self.assertEqual(sorted(row["game_id"] for row in self.exported()), sorted(g["gameId"] for g in self.games))

    def test_only_confirmed_matches_leave_the_queue_and_are_emptied_days_later(self):
        self.post(self.games)
        first = self.exported(5)
        self.assertEqual(len(first), 5)
        self.assertEqual(ingest.zlib.decompress(ingest.base64.b64decode(first[0]["payload"])),
                         json.dumps(self.games[0], separators=(",", ":"), sort_keys=True).encode())
        self.assertEqual(len(self.exported()), 12)  # nothing leaves the queue before it is confirmed
        self.assertEqual(json.loads(self.relay("ack", stdin=json.dumps({"seqs": [row["seq"] for row in first]}))),
                         {"taken": 5, "emptied": 0})
        self.assertEqual([row["seq"] for row in self.exported()], [row["seq"] for row in self.exported()][:7])
        self.assertEqual(len(self.exported()), 7)
        self.now += ingest.KEEP_SHIPPED_DAYS * 86400 + 1
        self.assertEqual(json.loads(self.relay("ack", stdin='{"seqs": []}')), {"taken": 0, "emptied": 5})
        empty = {row[0] for row in self.db.execute("SELECT game_id FROM games WHERE payload = x''")}
        self.assertEqual(empty, {row["game_id"] for row in first})
        self.assertEqual(self.count("games"), 12)
        again = self.post(self.games, key=self.add_key("bob")).get_json()   # still known: never kept twice
        self.assertEqual((again["accepted"], again["duplicates"]), (0, 12))
        self.assertIn("Wrote 7 matches", self.admin("export", os.path.join(self.tmp.name, "out.jsonl.gz")))

    def test_uploads_wait_while_the_queue_is_full(self):
        self.post(self.games[:6])
        with unittest.mock.patch.object(ingest, "MAX_BACKLOG", 5):
            refused = self.post(self.games[6:])
        self.assertEqual(refused.status_code, 503)
        self.assertEqual(self.count("games"), 6)
        self.assertEqual(self.post(self.games[6:]).get_json()["accepted"], 6)

    def test_status_counts_what_arrived(self):
        self.post(self.games)
        status = json.loads(self.relay("status"))
        self.assertEqual((status["waiting"], status["platforms"]["OC1"]["held"]), (12, 12))
        self.assertEqual(status["season_start"], self.season)
        self.assertEqual(status["platforms"]["OC1"]["hourly_newest_first"][0], 12)

    def test_unknown_requests_are_refused(self):
        with unittest.mock.patch.dict(os.environ, {"SSH_ORIGINAL_COMMAND": "rm -rf /"}):
            self.assertIn("Not understood", self.relay())


class Uploads(ServerCase):
    def test_accepts_matches_and_keeps_the_first_copy(self):
        first = self.post(self.games).get_json()
        self.assertEqual((first["accepted"], first["duplicates"], first["rejected"]), (12, 0, []))
        changed = copy.deepcopy(self.games[0])
        changed["participants"][0]["stats"]["kills"] = 99
        again = self.post([changed] + self.games[1:], key=self.add_key("bob")).get_json()
        self.assertEqual((again["accepted"], again["duplicates"]), (0, 12))
        self.assertEqual(self.count("games"), 12)
        kept = self.db.execute("SELECT payload FROM games WHERE game_id = ?", (changed["gameId"],)).fetchone()[0]
        self.assertEqual(json.loads(ingest.zlib.decompress(kept)), self.games[0])    # trusted once, never rechecked

    def test_uncompressed_upload_also_works(self):
        self.assertEqual(self.post(self.games, compress=False).get_json()["accepted"], 12)

    def test_matches_from_before_the_season_are_not_kept(self):
        self.admin("policy", "--season-start", utc_date(self.now - 9 * 86400))
        cutoff = ingest.get_policy(self.db)["season_start"]
        recent = {game["gameId"] for game in self.games if game["gameCreation"] >= cutoff}
        self.assertTrue(0 < len(recent) < 12)                             # the made-up world straddles the date
        result = self.post(self.games).get_json()
        self.assertEqual((result["accepted"], result["skipped"], result["rejected"]), (len(recent), 12 - len(recent), []))
        self.assertEqual({row[0] for row in self.db.execute("SELECT game_id FROM games")}, recent)

    def test_everyone_in_an_uploaded_match_becomes_a_player_to_read(self):
        self.post(self.games)
        seen = {}
        for game in self.games:
            for identity in game["participantIdentities"]:
                seen[identity["player"]["puuid"]] = seen.get(identity["player"]["puuid"], 0) + 1
        self.assertEqual(dict(self.db.execute("SELECT puuid, games FROM players")), seen)

    def test_purge_removes_a_contributors_matches_and_asks_for_them_again(self):
        self.post(self.games[:6])
        self.post(self.games[6:], key=self.add_key("bob"))
        self.assertIn("Removed 6 matches", self.admin("purge", "bob"))
        self.assertEqual(self.count("games"), 6)
        self.assertEqual(sorted(self.work()["games"]), sorted(game["gameId"] for game in self.games[6:]))

    def test_export_writes_the_held_matches(self):
        self.post(self.games)
        target = os.path.join(self.tmp.name, "out.jsonl.gz")
        self.assertIn("Wrote 12 matches", self.admin("export", target, "--platform", "OC1"))
        with gzip.open(target, "rt") as handle:
            self.assertEqual(sorted(json.loads(line)["gameId"] for line in handle),
                             [game["gameId"] for game in self.games])
        self.assertIn("Wrote 0 matches", self.admin("export", target, "--platform", "HN1"))

    def test_odd_characters_cannot_break_export(self):
        odd = dict(self.games[0], note="\ud800 half a character")
        raw = json.dumps({"games": [odd] + self.games[1:]}).encode()
        self.assertEqual(self.send("/v1/games", None, raw=raw).get_json()["accepted"], 12)
        self.assertIn("Wrote 12 matches", self.admin("export", os.path.join(self.tmp.name, "out.jsonl.gz")))

    def test_computer_players_do_not_count_as_repeats(self):
        game = copy.deepcopy(self.games[0])
        for identity in game["participantIdentities"][:2]:
            identity["player"]["puuid"] = "00000000-0000-0000-0000-000000000000"
        self.assertEqual(self.post([game]).get_json()["accepted"], 1)
        self.assertEqual(self.count("players"), 14)                       # the two computer players are not listed

    def test_bad_matches_are_refused_one_by_one(self):
        now = int(self.now * 1000)

        def broken(**changes):
            game = copy.deepcopy(self.games[0])
            game.update(changes)
            return game

        no_identity = copy.deepcopy(self.games[0])
        no_identity["participantIdentities"].pop()
        bad_player = copy.deepcopy(self.games[0])
        bad_player["participantIdentities"][0]["player"]["puuid"] = "../../etc/passwd"
        repeated = copy.deepcopy(self.games[0])
        repeated["participantIdentities"][1]["player"]["puuid"] = repeated["participantIdentities"][0]["player"]["puuid"]
        same_seat = copy.deepcopy(self.games[0])
        same_seat["participants"][1]["participantId"] = same_seat["participants"][0]["participantId"]
        bad = [same_seat, broken(gameMode="CLASSIC"), broken(gameId="12"), broken(gameId=True),
               broken(platformId="oc1; drop"), broken(gameCreation=now + 3 * 86400000),
               broken(gameDuration=-5), broken(participants=[]), broken(participants="x"), broken(gameMode=["CHERRY"]),
               no_identity, bad_player, repeated, "a string", None, 17, [], {"gameId": {}}]
        result = self.post(bad + [self.games[1]]).get_json()
        self.assertEqual(result["accepted"], 1)
        self.assertEqual(len(result["rejected"]), len(bad))
        self.assertEqual(self.count("games"), 1)

    def test_junk_never_causes_a_server_error(self):
        junk = [b"", b"{", b"[]", b"null", b'{"games": 5}', b'{"games": {"a": 1}}', b"\xff\xfe", b"[" * 100000,
                b'{"platform": 5}', b'{"platform": "OC1", "reports": 7}', b'{"platform": "OC1", "reports": [1, null, {}]}',
                b'{"platform": "OC1", "reports": [{"puuid": "' + b"a" * 36 + b'", "ids": ["x", 1e400, -1]}]}',
                b'{"platform": "OC1", "reports": [{"puuid": "' + b"a" * 36 + b'", "ids": [1], "newest": "soon", "next": -4}]}',
                json.dumps({"games": [{"participants": [{"participantId": []}], "participantIdentities": [[]]}]}).encode(),
                json.dumps({"games": [dict(self.games[0], participants=[{"participantId": {}, "stats": 1}] * 16)]}).encode()]
        for path in ("/v1/games", "/v1/work"):
            for body in junk:
                for compress in (True, False):
                    self.now += 5                                         # (stay under the request-rate cap)
                    status = self.send(path, None, raw=body, compress=compress).status_code
                    self.assertIn(status, (200, 400), (path, body[:40]))
        self.now += 61
        bad_gzip = self.client.post("/v1/games", data=b"not gzip", headers={
            "Authorization": "Bearer " + self.key, "Content-Encoding": "gzip"})
        self.assertEqual(bad_gzip.status_code, 400)
        self.assertEqual(self.count("games"), 0)


class Work(ServerCase):
    """Handing out what to read: no match fetched twice, the most promising players first, nothing lost."""

    PLAYER = "11111111-2222-3333-4444-555555555555"

    def test_matches_seen_in_a_list_are_handed_out_once(self):
        ids = [game["gameId"] for game in self.games[:3]]
        self.assertEqual(sorted(self.work([self.report(self.PLAYER, ids)])["games"]), ids)
        bob = self.add_key("bob")
        self.assertEqual(self.work(key=bob)["games"], [])                 # alice is fetching them
        self.post(self.games[:1])                                         # she sends one, then disappears
        self.now += ingest.LEASE_SECONDS + 1
        self.assertEqual(sorted(self.work(key=bob)["games"]), ids[1:])    # the other two go to someone else
        self.assertEqual(self.count("wanted"), 2)

    def test_matches_already_held_are_never_asked_for(self):
        self.post(self.games)
        answer = self.work([self.report(self.PLAYER, [game["gameId"] for game in self.games])])
        self.assertEqual(answer["games"], [])
        self.assertEqual(self.count("wanted"), 0)

    def test_a_match_nobody_can_fetch_is_given_up_and_never_asked_for_again(self):
        handed = 0
        answer = self.work([self.report(self.PLAYER, [424242])])
        for _ in range(ingest.GAME_TRIES + 3):
            handed += answer["games"].count(424242)
            self.now += ingest.LEASE_SECONDS + 1
            answer = self.work()
        self.assertEqual(handed, ingest.GAME_TRIES)
        self.assertEqual((self.count("wanted"), self.count("given_up")), (0, 1))   # no longer scanned
        self.work([self.report(self.PLAYER, [424242])], more=False)       # seen in a list again
        self.assertEqual(self.count("wanted"), 0)
        self.assertIn("1 given up on", self.admin("stats"))

    def test_players_seen_in_the_most_matches_are_read_first(self):
        self.post(self.games)
        counts = dict(self.db.execute("SELECT puuid, games FROM players"))
        answer = self.work()
        order = [counts[item["puuid"]] for item in answer["players"]]
        self.assertEqual(len(order), ingest.PLAYERS_PER_ROUND)
        self.assertEqual(order, sorted(counts.values(), reverse=True)[:len(order)])
        self.assertTrue(all(self.shape(item) == {"since": self.season, "begin": 0, "pages": 1}
                            for item in answer["players"]))

    def test_a_player_is_not_handed_to_two_collectors(self):
        self.post(self.games)
        first = {item["puuid"] for item in self.work()["players"]}
        second = {item["puuid"] for item in self.work(key=self.add_key("bob"))["players"]}
        self.assertEqual(len(first), ingest.PLAYERS_PER_ROUND)
        self.assertFalse(first & second)
        self.now += ingest.LEASE_SECONDS + 1                              # nobody reported back: they go out again
        self.assertEqual({item["puuid"] for item in self.work()["players"]}, first)

    def test_of_players_in_equally_many_matches_the_latest_found_is_read_first(self):
        for name in "abc":
            self.db.execute("INSERT INTO players(platform, puuid, games, due_at) VALUES('OC1', ?, 2, 0)", (name * 36,))
        self.db.commit()
        self.assertEqual([item["puuid"][0] for item in self.work()["players"]], ["c", "b", "a"])

    def test_lists_due_again_go_first_and_one_place_is_kept_for_a_list_not_read_back_yet(self):
        self.post(self.games)                                             # 20 players, none read yet
        counts = dict(self.db.execute("SELECT puuid, games FROM players"))
        rest = ingest.PLAYERS_PER_ROUND - 1
        regulars = ["regular-%028d" % n for n in range(rest + 3)]         # lists read before, all due again
        self.db.executemany("INSERT INTO players(platform, puuid, checked_at, due_at) VALUES('OC1', ?, 1, ?)",
                            [(puuid, 100 + n) for n, puuid in enumerate(regulars)])
        self.db.commit()
        first = [item["puuid"] for item in self.work()["players"]]
        self.assertEqual(first[:rest], regulars[:rest])                   # the most overdue lists ...
        self.assertEqual([counts[puuid] for puuid in first[rest:]], [max(counts.values())])   # ... and the never-read
        second = [item["puuid"] for item in self.work()["players"]]                           # player in the most matches
        self.assertEqual(second[:3], regulars[rest:])                     # with fewer lists due again than places,
        self.assertEqual(len(second), ingest.PLAYERS_PER_ROUND)           # players never read take the others
        self.assertEqual(len(self.work(lists=3)["players"]), 3)           # a collector may ask for fewer or more lists
        self.assertEqual(len(self.work(lists=99)["players"]), 9)          # (20 at most; only 9 are left here)
        self.db.execute("UPDATE players SET leased_until = 0")
        self.db.commit()
        full = self.work([self.report("f" * 36, list(range(5000, 5300)))])    # a full load of matches to fetch:
        self.assertEqual(len(full["games"]), ingest.GAMES_PER_ROUND)
        self.assertEqual([item["puuid"] for item in full["players"]], regulars[:ingest.PLAYERS_PER_ROUND])   # only lists
                                                                                                             # due again
    def test_a_list_is_due_again_when_about_ten_more_matches_are_expected(self):
        day = 86400
        wait = lambda puuid: self.player(puuid)[3] - int(self.now)
        self.post([self.played(900 + n, 2 * day - n) for n in range(5)])  # five matches played two days ago
        busy, quiet = "b" * 36, "q" * 36
        jobs = [self.handed(busy), self.handed(quiet)]
        top = int(self.now * 1000) - 5000
        self.work([self.report(jobs[0], list(range(900, 905)), read_to=top, following=20), self.report(jobs[1], [])],
                  more=False)
        # A first read: its 5 matches came in 2 days, so 10 more are expected in 4 days.
        self.assertAlmostEqual(wait(busy), 2 * day * ingest.TARGET_SEEN / 5, delta=60)
        self.assertEqual(wait(quiet), ingest.MAX_INTERVAL)                # no Arena match at all: seldom
        self.assertEqual(self.player(busy)[1], top)
        self.db.execute("DELETE FROM wanted")
        self.db.commit()
        self.now += day                                                   # read a day later: 2 more matches
        again = self.handed(busy)
        self.assertEqual(self.shape(again), {"since": top, "begin": 0, "pages": 1})   # only what is newer is asked for
        self.work([self.report(again, [905, 906])], more=False)
        self.assertAlmostEqual(wait(busy), day * ingest.TARGET_SEEN / 2, delta=60)
        self.now += 5 * day                                               # read too late: all 20 places are new,
        self.post([self.played(1000 + n, day - n) for n in range(20)])    # and the oldest was played a day ago
        self.work([self.report(self.handed(busy), list(range(1000, 1020)), following=20)], more=False)
        self.assertAlmostEqual(wait(busy), day * ingest.TARGET_SEEN / 20, delta=60)
        self.now += 60                                                    # twenty matches within the hour:
        self.post([self.played(2000 + n, 3000 - n) for n in range(20)])
        self.work([self.report(self.handed(busy), list(range(2000, 2020)), following=20)], more=False)
        self.assertEqual(wait(busy), ingest.MIN_INTERVAL)                 # never more often than this
        other = self.send("/v1/work", {"platform": "HN1", "start": busy}).get_json()["players"][0]
        self.send("/v1/work", {"platform": "HN1", "more": False, "reports": [self.report(other, list(range(1, 6)))]})
        due = self.db.execute("SELECT due_at FROM players WHERE platform = 'HN1'").fetchone()[0]
        self.assertEqual(due - int(self.now), ingest.UNKNOWN_SPAN * ingest.TARGET_SEEN // 5)   # no held match to judge by

    def test_a_list_is_read_one_page_and_never_further_back(self):
        # The League client answers a request for a player's older places with the same latest 20 matches. Asking
        # for them was a third of all client requests until this was measured (25 Nov 2026).
        self.post(self.games[:6])
        lacking = [game["gameId"] for game in self.games[6:9]]
        top = int(self.now * 1000) - 1000
        job = self.handed(self.PLAYER)
        self.assertEqual(self.shape(job), {"since": self.season, "begin": 0, "pages": 1})
        answer = self.work([self.report(job, lacking, read_to=top, following=20)])
        self.assertEqual(sorted(answer["games"]), lacking)
        self.assertIsNone(self.item(answer, self.PLAYER))                 # the list goes on, but is not read on
        self.assertEqual(self.player(self.PLAYER)[1:3], (top, int(self.now)))   # it counts as read, up to its newest
        for _ in range(8):                                                # and no request ever names an older place
            answer = self.work()
            self.assertTrue(all((item["begin"], item["pages"]) == (0, 1) for item in answer["players"]))

    def test_an_interrupted_read_loses_nothing(self):
        job = self.handed(self.PLAYER)                                    # ... and that collector is never heard of again
        bob = self.add_key("bob")
        self.assertIsNone(self.item(self.work(key=bob), self.PLAYER))
        self.now += ingest.LEASE_SECONDS + 1
        again = self.handed(self.PLAYER, key=bob)
        self.assertEqual(self.shape(again), self.shape(job))              # handed out again, the same read
        self.assertEqual(self.player(self.PLAYER)[2], 0)                  # not marked as read meanwhile
        self.work([self.report(again, [1, 2, 3])], key=bob, more=False)
        self.assertEqual(self.count("wanted"), 3)

    def test_a_late_report_cannot_undo_newer_progress(self):
        # The review's sequences: a report that arrives after its read was handed to someone else.
        alice = self.handed(self.PLAYER)                                  # alice is handed the list ...
        self.now += ingest.LEASE_SECONDS + 1                              # ... and goes into a game
        bob = self.add_key("bob")
        redo = self.handed(self.PLAYER, key=bob)
        self.work([self.report(redo, [9001], read_to=50)], key=bob, more=False)
        state = self.player(self.PLAYER)
        self.work([self.report(alice, [9002], read_to=5)], more=False)    # alice's report, much later
        self.assertEqual(self.player(self.PLAYER), state)                 # changes nothing about the list ...
        self.assertEqual(self.count("wanted"), 2)                         # ... though its match is still wanted
        failed = self.handed(self.PLAYER)                                 # a failed report from an old read, too
        self.now += ingest.LEASE_SECONDS + 1
        self.handed(self.PLAYER, key=bob)
        before = self.player(self.PLAYER)
        self.work([self.report(failed, [], complete=False)], more=False)
        self.assertEqual(self.player(self.PLAYER), before)

    def test_one_report_twice_in_a_request_counts_once(self):
        job = self.handed(self.PLAYER)
        self.work([self.report(job, [7], complete=False)] * 2, more=False)
        self.assertEqual((self.player(self.PLAYER)[4], self.count("wanted")), (1, 1))     # one failure, one match

    def test_a_failed_read_is_retried_with_growing_waits(self):
        self.work([self.report(self.handed(self.PLAYER), [], read_to=1)], more=False)
        before = self.player(self.PLAYER)[1]
        waits = []
        for _ in range(3):
            job = self.handed(self.PLAYER)
            self.work([self.report(job, [], complete=False, read_to=int(self.now * 1000))], more=False)
            waits.append(self.player(self.PLAYER)[3] - int(self.now))
        self.assertEqual(waits, [ingest.RETRY_SECONDS, ingest.RETRY_SECONDS * 2, ingest.RETRY_SECONDS * 4])
        self.assertEqual(self.player(self.PLAYER)[1], before)             # a failed read does not move the mark

    def test_a_failing_list_backs_off_and_does_not_crowd_out_others(self):
        self.post(self.games)
        everyone = [row[0] for row in self.db.execute("SELECT puuid FROM players ORDER BY games DESC, rowid DESC")]
        failing, others = everyone[:5], everyone[5:]                      # the five that would be read first
        handed = {puuid: 0 for puuid in everyone}
        for _ in range(200):                                              # 200 requests over about 33 hours
            self.now += 600
            answer = self.work()
            reports = []
            for item in answer["players"]:
                handed[item["puuid"]] += 1
                reports.append(self.report(item, [], complete=item["puuid"] not in failing))
            self.work(reports, more=False)
        self.assertTrue(all(handed[puuid] <= 12 for puuid in failing), handed)    # backed off: not 200 times
        self.assertTrue(all(handed[puuid] >= 1 for puuid in others), handed)      # and everyone else had a turn
        self.assertTrue(all(self.player(puuid)[2] > 0 for puuid in others))
        self.assertTrue(all(self.player(puuid)[2] == 0 for puuid in failing))     # never marked as read

    def test_being_seen_in_another_match_does_not_undo_a_failures_wait(self):
        job = self.handed(self.PLAYER)
        self.work([self.report(job, [], complete=False)], more=False)
        due = self.player(self.PLAYER)[3]
        game = copy.deepcopy(self.games[0])
        game["participantIdentities"][0]["player"]["puuid"] = self.PLAYER
        self.post([game])
        self.assertEqual(self.player(self.PLAYER)[3], due)
        self.assertEqual(self.work()["players"] and self.item(self.work(), self.PLAYER), None)

    def test_moving_the_season_earlier_reads_the_lists_again(self):
        self.post(self.games)
        self.work([self.report(self.handed(self.PLAYER), [], read_to=int(self.now * 1000))], more=False)
        self.assertGreater(self.player(self.PLAYER)[3], self.now)         # not due for a long time
        earlier = utc_date(self.now - 60 * 86400)
        self.assertIn("1 match lists will be read again", self.admin("policy", "--season-start", earlier))
        again = self.item(self.work(), self.PLAYER)                       # due at once, ahead of players never read,
        self.assertEqual(self.shape(again), {"since": ingest.date_ms(earlier), "begin": 0, "pages": 1})   # back to the new date
        self.assertEqual(self.db.execute("SELECT COUNT(*) FROM players WHERE back").fetchone()[0], 0)

    def test_what_list_reads_found_is_counted_by_day(self):
        # The site's figure for how complete a day is comes from these counts.
        day = 86400
        self.post([self.played(1001, 5 * day), self.played(1003, 3 * day), self.played(1005, 3600)])
        # 1002 is not held and, by its neighbour, five days old; 1006 is not held either but was only just played.
        self.work([self.report(self.handed(self.PLAYER), [1001, 1002, 1003, 1005, 1006])], more=False)
        self.work([self.report("o" * 36, [1002]), self.report("f" * 36, [], complete=False)], more=False)
        today = utc_date(self.now)
        self.assertEqual(self.db.execute("SELECT pages, places, lists, failed, seen, held, new FROM read_days").fetchall(),
                         [(1, 20, 3, 1, 6, 3, 2)])
        self.assertEqual(self.db.execute("SELECT played, seen, held FROM seen_days ORDER BY played").fetchall(),
                         [(utc_date(self.now - 5 * day), 3, 1), (utc_date(self.now - 3 * day), 1, 1)])
        reads = json.loads(self.relay("status"))["reads"]["OC1"]
        self.assertEqual({name: reads["days"][0][name] for name in ("day", "pages", "lists", "failed", "seen", "held", "new")},
                         {"day": today, "pages": 1, "lists": 3, "failed": 1, "seen": 6, "held": 3, "new": 2})
        self.assertEqual(reads["played"][0], {"day": utc_date(self.now - 5 * day), "seen": 3, "held": 1})
        self.assertIn("lists read %s: 3 (1 failed, 0 answered from a saved copy) for 1 asked with 20 places; 1 reached "
                      "the season's start; Arena matches they showed: 6, 3 held already, 2 not seen before" % today,
                      self.admin("stats"))

    def test_choosing_lists_never_looks_through_every_player(self):
        # Work is handed out while holding the database's only write lock, with 300,000 players on one server:
        # a search or a sort through all of them would stall every upload.
        for sql in (ingest.DUE_AGAIN, ingest.NEVER_READ, ingest.DUE_TOP, ingest.FIRST_READS, ingest.FIRST_READS_OF):
            plan = " | ".join(row[3] for row in self.db.execute("EXPLAIN QUERY PLAN " + sql, ("OC1",) + (0,) * (sql.count("?") - 1)))
            self.assertRegex(plan, r"^SEARCH players USING INDEX players_(again|never|top|back) \(platform=\?")
            self.assertNotIn("B-TREE", plan)                              # nothing is sorted
        self.assertIn("(platform=? AND games=?)", " ".join(row[3] for row in self.db.execute(
            "EXPLAIN QUERY PLAN " + ingest.FIRST_READS_OF, ("OC1",) + (0,) * 6)))     # (one number of held matches)
        for sql in ("SELECT platform, games, pace FROM players INDEXED BY players_paced WHERE pace > 0",
                    "SELECT games, COUNT(*) FROM players INDEXED BY players_back WHERE platform = 'OC1' AND back = 0 "
                    "GROUP BY games"):
            plan = " ".join(row[3] for row in self.db.execute("EXPLAIN QUERY PLAN " + sql))
            self.assertRegex(plan, "COVERING INDEX players_(paced|back)")  # (judging which lists are worth most reads
            self.assertNotIn("B-TREE", plan)                              # only an index, outside the write lock)

    def test_a_database_from_the_version_before_carries_on(self):
        # The live database holds 300,000 players and 200,000 lists waiting: the change-over must lose none of that
        # work, ask for nothing twice and still let the version before run on the same file.
        self.post(self.games)
        counts = dict(self.db.execute("SELECT puuid, games FROM players"))
        reader = max(counts, key=counts.get)
        self.db.executescript("""
            DROP INDEX players_again; DROP INDEX players_never; DROP INDEX players_back;
            DROP INDEX players_top; DROP INDEX players_deeper; DROP INDEX players_paced;
            DROP TABLE read_days; DROP TABLE seen_days; DROP TABLE size_days;
            ALTER TABLE players DROP COLUMN back; ALTER TABLE players DROP COLUMN depth; ALTER TABLE players DROP COLUMN pace;
            ALTER TABLE players DROP COLUMN want; ALTER TABLE players DROP COLUMN reader;
            ALTER TABLE players DROP COLUMN reader_at;
            CREATE INDEX players_due ON players(platform, due_at);
            CREATE INDEX players_older_part ON players(platform, games) WHERE resume IS NOT NULL;
            CREATE INDEX players_unread ON players(platform) WHERE checked_at = 0;""")
        # As the version before left them: players never read, due long ago; one read with an older part set aside
        # and overdue; one read and not due yet; one in the middle of a read.
        self.db.execute("UPDATE players SET due_at = ? - games * 21600", (int(self.now) - 9 * 86400,))
        self.db.execute("INSERT INTO players(platform, puuid, checked_at, due_at, resume, resume_floor) "
                        "VALUES('OC1', 'overdue-000000000000000000', 5, 10, 80, 1)")
        self.db.execute("INSERT INTO players(platform, puuid, checked_at, due_at, resume) "
                        "VALUES('OC1', 'not-due-000000000000000000', 5, ?, 20)", (int(self.now) + 86400,))
        self.db.execute("UPDATE players SET leased_until = ?, job = 'old-token', walk_new = 3 WHERE puuid = ?",
                        (int(self.now) + 600, reader))
        self.db.commit()
        ingest.prepared.clear()
        ingest.connect().close()                                          # the new version starts on that file
        indexes = {row[0] for row in self.db.execute("SELECT name FROM sqlite_master WHERE type = 'index'")}
        self.assertTrue({"players_again", "players_never", "players_back", "players_top", "players_deeper",
                         "players_paced"} <= indexes)
        self.assertFalse({"players_due", "players_older_part", "players_unread"} & indexes)
        answer = self.work([{"puuid": reader, "job": "old-token", "ids": [77], "newest": 5, "next": 20, "failed": False}])
        self.assertEqual(answer["games"], [77])                           # the read under way is taken, and ends there
        self.assertGreater(self.player(reader)[2], 0)
        handed = [item["puuid"] for item in answer["players"]]
        self.assertEqual(handed[0], "overdue-000000000000000000")         # the overdue list first, from its top ...
        self.assertTrue(all((item["begin"], item["pages"]) == (0, 1) for item in answer["players"]))
        self.assertNotIn("not-due-000000000000000000", handed)            # ... then never-read players, most matches first
        self.assertEqual([counts[puuid] for puuid in handed[1:]],
                         sorted((n for puuid, n in counts.items() if puuid != reader), reverse=True)[:len(handed) - 1])
        self.db.execute("UPDATE players SET leased_until = 0")
        self.db.commit()
        deep = self.work(key=self.add_key("bob"), block=200)["players"]   # a collector that reads in one request gets
        again = self.item({"players": deep}, reader)                      # the list just read as a page of 20 (its player
        self.assertEqual((again["count"], again["since"]), (200, self.season))   # is in the most held matches), further
        self.assertEqual([counts[item["puuid"]] for item in deep],        # back, with the others never read in one, the
                         sorted(counts.values(), reverse=True)[:len(deep)])   # ones in the most held matches first

    def test_a_collector_that_is_stopping_is_handed_nothing(self):
        self.post(self.games)
        answer = self.work([self.report(self.PLAYER, [777])], more=False)
        self.assertEqual((answer["games"], answer["players"]), ([], []))
        self.assertEqual(self.count("wanted"), 1)                         # but its report was taken

    def test_nothing_is_handed_out_while_collection_is_switched_off(self):
        self.post(self.games)
        self.admin("policy", "--off")
        answer = self.work([self.report(self.PLAYER, [777])])
        self.assertEqual((answer["games"], answer["players"], answer["policy"]["enabled"]), ([], [], False))
        self.assertEqual(self.count("wanted"), 1)

    def test_a_match_given_up_on_does_not_send_the_others_twice(self):
        self.work([self.report(self.PLAYER, [100, 200])], more=False)
        self.db.execute("UPDATE wanted SET tries = ? WHERE game_id = 200", (ingest.GAME_TRIES,))
        self.db.commit()
        self.assertEqual(self.work()["games"], [100])
        self.assertEqual(self.db.execute("SELECT tries FROM wanted WHERE game_id = 100").fetchone()[0], 1)

    def test_a_full_load_of_matches_means_no_more_lists(self):
        self.post(self.games)
        answer = self.work([self.report(self.PLAYER, list(range(5000, 5300)))])
        self.assertEqual(len(answer["games"]), ingest.GAMES_PER_ROUND)
        self.assertEqual(answer["players"], [])

    def test_a_new_server_starts_from_the_collectors_own_account(self):
        answer = self.work(start=self.PLAYER)
        self.assertEqual(self.shape(self.item(answer, self.PLAYER)), {"since": self.season, "begin": 0, "pages": 1})
        self.work([self.report(self.item(answer, self.PLAYER), [])], more=False)
        before = self.player(self.PLAYER)
        self.work(start=self.PLAYER, more=False)                          # naming it again changes nothing
        self.assertEqual(self.player(self.PLAYER), before)

    def test_work_is_kept_apart_by_server(self):
        self.post(self.games)
        answer = self.send("/v1/work", {"platform": "HN1", "reports": []}).get_json()
        self.assertEqual((answer["games"], answer["players"], answer["wait"]), ([], [], 60))
        self.assertEqual(self.send("/v1/work", {"platform": "oc1"}).status_code, 400)

    def test_reading_cannot_be_marked_beyond_the_present(self):
        self.work([self.report(self.handed(self.PLAYER), [], read_to=2 ** 52)], more=False)
        self.assertLessEqual(self.player(self.PLAYER)[1], int(self.now * 1000))


class ListsInOneRequest(ServerCase):
    """Collectors that read a list in one request (0.3.8 on). A League client gives as many of a player's latest
    matches as the first request for that player asked for, keeps a copy, and answers every later request from the
    copy. So the endpoint says how many to ask for, and never takes a failed, short or saved-copy answer for a full read."""

    PLAYER = "22222222-3333-4444-5555-666666666666"
    DAY = 86400

    def setUp(self):
        super().setUp()
        self.days = (self.now - self.season / 1000) / self.DAY           # since the season began
        self.addCleanup(setattr, ingest, "DEFAULT_RATE", ingest.DEFAULT_RATE)

    @staticmethod
    def pages(places, most=200):
        """`places` as whole pages of 20, at most `most`."""
        return min(most, 20 * -int(-places // 20))

    def read(self, item, ids=(), listed=None, oldest_days=1.0, took=3.0, complete=True, following=0, key=None, block=200):
        """Report a read of the list handed out as `item`: `listed` matches going back `oldest_days`."""
        listed = item["count"] if listed is None else listed
        report = {"puuid": item["puuid"], "job": item["job"], "ids": list(ids), "failed": not complete, "next": following,
                  "begin": 0, "newest": int(self.now * 1000) - 5000, "asked": item["count"], "listed": listed,
                  "oldest": int((self.now - oldest_days * self.DAY) * 1000) if listed else 0, "took": took}
        return self.work([report], more=False, block=block, key=key)

    def state(self, puuid=None):
        return self.db.execute("SELECT checked_at, back, depth, pace, fails FROM players WHERE puuid = ?",
                               (puuid or self.PLAYER,)).fetchone()

    def first(self, puuid=None, games=10, block=200):
        """The hand-out of a first read to a collector that reads a list in one request, for a player in `games`
        held matches."""
        self.db.execute("INSERT INTO players(platform, puuid, games, due_at) VALUES('OC1', ?, ?, 0)",
                        (puuid or self.PLAYER, games))
        self.db.commit()
        return self.handed(puuid or self.PLAYER, block=block)

    def test_a_collector_is_told_how_many_matches_to_ask_for(self):
        self.post(self.games)                                             # 20 players never read
        again = lambda *reports, **said: (self.db.execute("UPDATE players SET leased_until = 0"), self.db.commit(),
                                          self.work(*reports, **said))[2]
        self.assertTrue(all("count" not in item and item["pages"] == 1 for item in again()["players"]))   # up to 0.3.7
        new = again(block=200)["players"]                                 # a first read: all one request gives
        self.assertEqual({item["count"] for item in new}, {200})
        self.assertTrue(all((item["begin"], item["pages"], item["since"]) == (0, 1, self.season) for item in new))
        self.assertEqual({item["count"] for item in again(block=1000)["players"]}, {200})   # no client lists more
        many = again(block=200, lists=20)["players"]                      # an answer holds 20 lists at most, and of places
        self.assertLess(len(many), 20)                                    # no more than a collector gets through in time
        self.assertLessEqual(sum(item["count"] for item in many), ingest.PLACES_PER_ROUND)
        self.assertEqual({item["count"] for item in again(block=60)["players"]}, {60})      # never more than it can ask for
        self.assertEqual(len(again(block=200, lists=3)["players"]), 3)                      # as many lists as it has room for
        full = again([self.report("f" * 36, list(range(7000, 7005)))], block=200, lists=0)
        self.assertEqual((len(full["games"]), full["players"]), (5, []))                    # (which may be none)
        self.assertIn("up to 100 matches", self.admin("policy", "--block", "100"))          # turned down, and at once
        self.assertEqual({item["count"] for item in again(block=200)["players"]}, {100})
        self.admin("policy", "--block", "20")                                               # ... or off: a page for all
        self.assertEqual({item["count"] for item in again(block=200)["players"]}, {20})
        self.assertIn("Not understood", self.admin("policy", "--block", "5"))
        self.assertIn("Not understood", self.admin("policy", "--block", "300"))             # more than any client lists
        self.db.execute("UPDATE settings SET value = '1000' WHERE name = 'block'")          # (as set before 27 Nov)
        self.db.commit()
        self.assertEqual({item["count"] for item in again(block=1000)["players"]}, {200})

    def test_only_a_read_that_got_there_counts_as_back_to_the_seasons_start(self):
        cases = {"reached the season's start": (dict(listed=200, oldest_days=60), 1),
                 "fewer than asked (the list's end? all the client gives?)": (dict(listed=137, oldest_days=9), 2),
                 "all that may be asked for, and it goes on": (dict(listed=200, oldest_days=9), 2),
                 "the client gave fewer than asked, whole pages": (dict(listed=100, oldest_days=9), 2),
                 "the client gave one page": (dict(listed=20, oldest_days=2), 2),
                 "the client listed nothing": (dict(listed=0), 0),
                 "failed": (dict(listed=0, complete=False), 0)}
        for number, (name, (answer, back)) in enumerate(cases.items()):
            puuid = "p%035d" % number
            item = self.first(puuid)
            self.assertEqual((item["count"], item["since"]), (200, self.season))
            self.read(item, ids=[5], **answer)
            self.assertEqual(self.state(puuid)[1:3], (back, answer["listed"]), name)
            self.assertEqual(self.state(puuid)[0] > 0, back > 0, name)    # (a failed read is no read at all)
        failed = self.handed("p%035d" % 6, block=200)
        self.assertEqual(failed["count"], 200)                            # the failed one is asked for in full again
        self.assertIsNone(self.item(self.work(block=200), "p%035d" % 1))  # none of the others: read, and not due
        # Less allowed, then more again: the ones that listed less than is allowed now are read in one request again,
        # by another install (its client has a copy of what it listed).
        self.admin("policy", "--block", "100")
        self.assertIn("3 match lists will be read again", self.admin("policy", "--block", "200"))
        self.assertEqual([puuid for puuid, in self.db.execute("SELECT puuid FROM players WHERE back = 0 AND checked_at > 0 "
                                                              "ORDER BY puuid")], ["p%035d" % number for number in (1, 3, 4)])
        self.db.execute("UPDATE players SET leased_until = 0")
        self.db.commit()
        self.assertFalse({item["puuid"] for item in self.work(block=200)["players"]} & {"p%035d" % n for n in range(1, 5)})
        again = self.work(key=self.add_key("bob"), block=200)["players"]
        self.assertEqual(sorted((item["puuid"][-1], item["count"]) for item in again if item["puuid"] != "p%035d" % 6),
                         [("1", 200), ("3", 200), ("4", 200)])

    def test_a_read_in_one_request_is_as_far_back_as_a_list_can_be_seen(self):
        # A Tencent client lists 200 at most whatever is asked for, and its first request fixes its copy (measured 27
        # Nov 2026): handing such a list out again to be read further back cost volunteers a read for nothing new.
        item = self.first(games=12)
        self.assertEqual(item["count"], 200)
        self.read(item, ids=list(range(5, 30)), listed=200, oldest_days=10)   # it goes back 10 days only
        self.assertEqual(self.state()[1:4], (2, 200, 10 * self.DAY // 200))
        bob = self.add_key("bob")
        self.assertIsNone(self.item(self.work(key=bob, block=200), self.PLAYER))   # nobody reads it back further,
        self.db.execute("UPDATE players SET due_at = 0")
        self.db.commit()
        top = self.handed(self.PLAYER, key=bob, block=200)                # only for what is new at its top
        self.assertEqual((top["count"], top["since"]), (20, self.db.execute(
            "SELECT read_to FROM players WHERE puuid = ?", (self.PLAYER,)).fetchone()[0]))
        self.now += 30 * self.DAY                                         # and when even 200 come back all newer than
        late = self.handed(self.PLAYER, key=bob, block=200)               # the last read, the ones in between are out of
        self.assertEqual(late["count"], 200)                              # every request's reach: it stays as it is
        self.read(late, listed=200, oldest_days=2, key=bob)
        self.assertEqual(self.state()[1:3], (2, 200))

    def test_a_database_from_the_version_of_26_nov_carries_on(self):
        # The live database (480,000 players) is changed over when the service starts: the lists that version left to
        # be read further back can go no further, unless their read listed fewer than a client gives.
        checked = int(self.now) - 3600
        alice = self.db.execute("SELECT id FROM contributors WHERE name = 'alice'").fetchone()[0]
        self.db.executemany("INSERT INTO players(platform, puuid, back, depth, pace, games, checked_at, due_at, want, "
                            "reader, reader_at) VALUES('OC1', ?, 3, ?, 4320, 80, ?, ?, 40, ?, ?)",
                            [(puuid, depth, checked, checked + 86400, alice, checked)
                             for puuid, depth in (("a" * 36, 200), ("b" * 36, 120))])
        self.db.execute("INSERT INTO players(platform, puuid, back, depth, pace, games, checked_at, due_at) "
                        "VALUES('OC1', ?, 2, 200, 4320, 90, ?, ?)", ("c" * 36, checked, checked + 86400))
        self.db.commit()
        ingest.prepared.clear()
        ingest.connect().close()
        self.assertEqual(self.db.execute("SELECT puuid, back FROM players ORDER BY puuid").fetchall(),
                         [("a" * 36, 2), ("b" * 36, 0), ("c" * 36, 2)])
        self.assertIsNone(self.item(self.work(block=200), "b" * 36))      # not by the install that read it,
        again = self.item(self.work(key=self.add_key("bob"), block=200), "b" * 36)
        self.assertEqual((again["count"], again["since"]), (200, self.season))   # by another, in full

    def test_a_database_from_the_version_of_25_nov_carries_on(self):
        # The live database (370,000 players) is changed over when the service starts: the lists that version left cut
        # short below what collectors may ask for must be read again in one request, and nothing else may change.
        self.db.executescript("""
            DROP INDEX players_top; DROP INDEX players_deeper; DROP INDEX players_paced;
            ALTER TABLE players DROP COLUMN want; ALTER TABLE players DROP COLUMN reader;
            ALTER TABLE players DROP COLUMN reader_at; ALTER TABLE size_days DROP COLUMN took_ms;""")
        # As it left them: one read back to the season's start, one at the most a collector may ask for (200), one asked
        # for 60 that went on 4 days back, and one never read.
        checked = int(self.now) - 3600
        self.db.executemany("INSERT INTO players(platform, puuid, back, depth, pace, games, checked_at, due_at) "
                            "VALUES('OC1', ?, ?, ?, ?, ?, ?, ?)",
                            [("a" * 36, 1, 200, 8640, 50, checked, checked), ("b" * 36, 2, 200, 4320, 80, checked, checked),
                             ("c" * 36, 2, 60, 5760, 6, checked, checked + 86400), ("d" * 36, 0, 0, 0, 3, 0, 0)])
        self.db.commit()
        ingest.prepared.clear()
        ingest.connect().close()
        self.assertEqual(self.db.execute("SELECT puuid, back FROM players ORDER BY puuid").fetchall(),
                         [("a" * 36, 1), ("b" * 36, 2), ("c" * 36, 0), ("d" * 36, 0)])     # c: read in one request again
        self.assertIn("took_ms", {row[1] for row in self.db.execute("PRAGMA table_info(size_days)")})
        handed = {item["puuid"][0]: item for item in self.work(block=200)["players"]}
        self.assertEqual(sorted(handed), ["a", "b", "c", "d"])             # due again at their top, and read back
        self.assertEqual((handed["c"]["count"], handed["c"]["since"]), (200, self.season))
        self.assertEqual(handed["a"]["since"], self.db.execute("SELECT read_to FROM players WHERE puuid = ?",
                                                               ("a" * 36,)).fetchone()[0] or self.season)

    def test_progress_back_to_the_season_start_is_shown_per_server(self):
        # Admin decides from these lines whether the season's matches can be had in time and what to change: wrong
        # numbers here would send volunteers' time the wrong way for days.
        ingest.DEFAULT_RATE = 4.0
        players = [(1, 200, 0, 50), (1, 200, 0, 40), (2, 200, 8640, 30), (0, 0, 0, 25), (0, 0, 0, 3)]
        self.db.executemany("INSERT INTO players(platform, puuid, back, depth, pace, games, checked_at, due_at) "
                            "VALUES('OC1', ?, ?, ?, ?, ?, ?, ?)", [("p%035d" % n, back, depth, pace, games, 1 if back else 0,
                                                                     int(self.now) + 86400)
                                                                    for n, (back, depth, pace, games) in enumerate(players)])
        today = utc_date(self.now)
        self.db.execute("INSERT INTO size_days(day, platform, asked, fresh, listed, took_ms) VALUES(?, 'OC1', 200, 5, 1000, "
                        "80000)", (today,))
        self.db.execute("INSERT INTO read_days(platform, day, lists) VALUES('OC1', ?, 5)", (today,))
        self.db.execute("INSERT INTO seen_days(day, platform, played, seen, held) VALUES(?, 'OC1', '2026-10-10', 100, 53)",
                        (today,))
        self.db.commit()
        state = ingest.progress(self.db, "OC1", int(self.now))
        self.assertEqual({name: state[name] for name in ("reached", "as_far", "not_back", "not_back_regulars")},
                         {"reached": 2, "as_far": 1, "not_back": 2, "not_back_regulars": 1})
        self.assertEqual((state["places_left"], state["places_left_regulars"]), (400, 200))   # one read of 200 each
        self.assertEqual(state["seconds_a_place"], 0.08)
        self.assertAlmostEqual(state["hours_left"], 400 * 0.08 / 3600, delta=0.1)
        self.assertEqual(state["season_held"], 0.53)
        # Every day 53% held (the one day judged), 4 matches a day (no list's pace judged): 200 reach back 50 days, and
        # a player's Arena matches a day are their held ones over the days held, at most 80% of their 4.
        worth = lambda games: min(games / (0.53 * self.days), 0.8 * 4) * 0.47 * min(50, self.days)
        self.assertAlmostEqual(state["worth_next"], (worth(25) + worth(3)) / 2, delta=0.1)
        self.assertAlmostEqual(state["worth_left"], worth(25) + worth(3), delta=1)
        self.assertEqual(json.loads(self.relay("status"))["reads"]["OC1"]["progress"], state)
        stats = self.admin("stats")
        self.assertIn("lists read in one request: 2 back to the season's start, 1 as far back as one request goes (a "
                      "client lists 200 at most); 2 not read in one request yet (1 of them of players in 20 or more held "
                      "matches)", stats)
        self.assertIn("to read those once each: about 0 client hours at 0.080 s a listed match;", stats)
        self.assertIn("what they should show (an estimate): about %.1f Arena matches not held yet a list for the next "
                      "1000 handed out" % state["worth_next"], stats)
        self.assertIn("the season held: about 53%, the days played averaged, judged by the list reads of the last 7 "
                      "days", stats)

    def test_the_lists_worth_the_most_matches_not_held_are_read_first(self):
        # Client time is the volunteers' own. A read shows a list's latest 200 matches at most, so the players in the
        # most held matches (who play the most) were the worst first reads on 27 Nov: 200 matches back to days held
        # already. The lists whose 200 reach back into days not held yet must go first.
        self.addCleanup(setattr, ingest, "FIT_MIN", ingest.FIT_MIN)
        ingest.FIT_MIN = 3
        today = utc_date(self.now)
        days = int(self.days)
        self.db.executemany("INSERT INTO seen_days(day, platform, played, seen, held) VALUES(?, 'OC1', ?, 100, ?)",
                            [(today, utc_date(self.now - back * self.DAY), 100 if back <= 20 else 10)
                             for back in range(2, days)])         # the last 20 days held, the ones before 10%
        # Lists read before (not due): of players in 256-511 held matches, 20 matches a day; in 32-63, 4 a day.
        self.db.executemany("INSERT INTO players(platform, puuid, games, pace, checked_at, back, depth, due_at) "
                            "VALUES('OC1', ?, ?, ?, 1, 2, 200, ?)",
                            [("%s%035d" % (name, n), games, 86400 // rate, int(self.now) + 86400)
                             for name, games, rate in (("x", 300, 20), ("y", 40, 4)) for n in range(3)])
        for puuid, games in (("h" * 36, 300), ("m" * 36, 40), ("l" * 36, 2), ("z" * 36, 0)):
            self.db.execute("INSERT INTO players(platform, puuid, games, due_at) VALUES('OC1', ?, ?, 0)", (puuid, games))
        self.db.commit()
        # The heavy player's 200 reach back 10 days, all held: nothing to find. The one in 40 reaches back 50 days, 30
        # of them 90% not held; the one in 2 as far (the nearest band judged), with fewer Arena matches.
        order = ingest.worths(self.db, "OC1", int(self.now))["order"]
        self.assertEqual([games for _, games, _ in order], [40, 2, 300, 0])
        held = 20 + self.days % 1                                         # (today, as far as it has gone, is held too)
        self.assertAlmostEqual(order[0][0], min(40 / (held + 0.1 * (self.days - held)), 3.2) * 0.9 * (min(50, self.days) - held),
                               delta=0.1)
        self.assertEqual(order[2][0], 0)
        self.assertEqual([item["puuid"][0] for item in self.work(block=200)["players"]], ["m", "l", "h", "z"])
        # A player in a number of held matches not judged yet (that is judged every 10 minutes) is still read, after.
        self.db.execute("INSERT INTO players(platform, puuid, games, due_at) VALUES('OC1', ?, 41, 0)", ("n" * 36,))
        self.db.commit()
        self.assertEqual([item["puuid"][0] for item in self.work(key=self.add_key("bob"), block=200)["players"]], ["n"])

    def test_how_fast_lists_fill_is_judged_by_band(self):
        # How far back a player's 200 matches reach decides which lists are read first (see the test above).
        self.addCleanup(setattr, ingest, "FIT_MIN", ingest.FIT_MIN)
        ingest.FIT_MIN = 3
        rows = [("OC1", "h%035d" % n, 100, 86400 // rate) for n, rate in enumerate((2, 4, 6, 8, 10))]   # 64-127 held
        rows += [("OC1", "l%035d" % n, 5, 86400 // rate) for n, rate in enumerate((1, 2, 3))]            # 4-7 held
        rows += [("KR", "k%035d" % n, 5, 86400 // rate) for n, rate in enumerate((20, 20, 20))]
        self.db.executemany("INSERT INTO players(platform, puuid, games, pace, checked_at, back, depth, due_at) "
                            "VALUES(?, ?, ?, ?, 1, 1, 200, ?)", [row + (int(self.now) + 86400,) for row in rows])
        self.db.commit()
        rates = ingest.list_rates(self.db, "OC1", int(self.now))
        self.assertEqual((rates[7], rates[3]), (6, 2))                    # its band's middle,
        self.assertEqual((rates[1], rates[5], rates[6], rates[10]), (2, 2, 6, 6))    # else the nearest band's (the lighter)
        self.assertEqual(ingest.list_rates(self.db, "NJ100", int(self.now))[7], 6)    # a server with none: all servers'
        ingest.FIT_MIN = 30
        ingest.fits.clear()
        ingest.DEFAULT_RATE = 3.0
        self.assertEqual(ingest.list_rates(self.db, "OC1", int(self.now))[7], 3.0)    # too few anywhere: DEFAULT_RATE

    def test_a_list_seen_back_to_the_start_is_read_again_only_for_what_is_new(self):
        self.read(self.first(), ids=[900, 901], listed=200, oldest_days=100)   # 200 matches in 100 days
        checked, back, depth, pace, _ = self.state()
        self.assertEqual((back, depth, pace), (1, 200, 100 * self.DAY // 200))
        top = self.db.execute("SELECT read_to FROM players WHERE puuid = ?", (self.PLAYER,)).fetchone()[0]
        self.now += 10 * self.DAY                                         # ten days on, about 20 more:
        again = self.handed(self.PLAYER, block=200)
        self.assertEqual((again["since"], again["count"]), (top, 40))     # asked for with places to spare
        self.read(again, ids=[902], listed=40, oldest_days=20)
        self.assertEqual(self.state()[1], 1)
        self.now += 10 * self.DAY
        late = self.handed(self.PLAYER, block=200)
        self.read(late, listed=late["count"], oldest_days=1, following=late["count"])   # every place newer than last time:
        self.assertEqual(self.state()[1], 0)                              # matches were missed, so back it goes,
        self.assertIsNone(self.handed(self.PLAYER, block=200))            # by another install (this client has a copy)
        back_again = self.handed(self.PLAYER, key=self.add_key("bob"), block=200)
        self.assertEqual(back_again["since"], self.season)                # everything since the season began,
        self.assertEqual(back_again["count"], 200)                        # which at one match in 36 minutes is over 200

    def test_a_saved_copy_is_not_a_read(self):
        self.read(self.first(), ids=[900], listed=200, oldest_days=100)
        before = self.state()
        self.now += 3 * self.DAY
        self.read(self.handed(self.PLAYER, block=200), ids=[900, 901], listed=200, took=0.05)   # the client's copy
        self.assertEqual(self.state()[:4], before[:4])                    # nothing about the list has changed,
        self.assertEqual(self.count("wanted"), 2)                         # though what it shows is still wanted
        self.assertIsNone(self.item(self.work(block=200), self.PLAYER))   # and it is tried again later, like a failure
        self.now += ingest.RETRY_SECONDS + 1
        self.read(self.item(self.work(block=200), self.PLAYER), ids=[902], listed=40, oldest_days=5, took=4.2)
        self.assertEqual(self.state()[0], int(self.now))
        self.read(self.first("q" * 36), listed=3, oldest_days=400, took=0.2)   # a list of three matches, quickly answered,
        self.assertEqual(self.state("q" * 36)[1], 1)                      # is no copy: it goes back before the season
        own = self.first("o" * 36, games=0)                               # A list nobody has read, from a client's copy
        self.assertEqual(own["count"], 200)                               # (a collector looks at its own when it starts):
        self.read(own, ids=[903, 904], listed=20, oldest_days=3, took=0.02)
        self.assertEqual(self.state("o" * 36)[:3], (int(self.now), 2, 20))    # that is a read of what it shows
        self.assertEqual(self.db.execute("SELECT SUM(saved), SUM(lists) FROM read_days").fetchone(), (2, 5))

    def test_players_read_as_a_page_of_20_are_read_back_first_the_ones_in_the_most_matches(self):
        self.post(self.games)                                             # 20 players in 5 to 12 held matches
        counts = dict(self.db.execute("SELECT puuid, games FROM players"))
        seen = sorted(counts, key=counts.get)[-6:]                        # the six in the most were read by a 0.3.7
        self.db.executemany("UPDATE players SET checked_at = ?, read_to = ?, due_at = ? WHERE puuid = ?",
                            [(int(self.now) - 600, int(self.now * 1000) - 900000, int(self.now) + 86400, p) for p in seen])
        self.db.commit()
        old = [item["puuid"] for item in self.work()["players"]]
        self.assertFalse(set(old) & set(seen))                            # a 0.3.7 is not handed them: not due again
        self.db.execute("UPDATE players SET leased_until = 0")
        self.db.commit()
        new = self.work(block=200)["players"]                             # one that reads in one request is,
        self.assertEqual([counts[item["puuid"]] for item in new], sorted(counts.values(), reverse=True)[:len(new)])
        self.assertTrue(set(seen) <= {item["puuid"] for item in new})     # before anybody in fewer matches,
        self.assertTrue(all(item["since"] == self.season for item in new))   # and for everything since the season began
        self.assertEqual({item["count"] for item in new}, {200})

    def test_reads_are_counted_by_the_number_of_matches_asked_for(self):
        # These counts are how we learn, on the first day, whether a server's clients give more than 20.
        answers = [dict(ids=[5], listed=200, oldest_days=30, took=14.0), dict(listed=200, oldest_days=70, took=17.5),
                   dict(listed=20, oldest_days=2, took=1.6), dict(listed=200, took=0.04), dict(listed=0, complete=False, took=30.0)]
        for number, answer in enumerate(answers):
            self.read(self.first("p%035d" % number), **answer)
        self.admin("policy", "--block", "60")
        self.read(self.first("s" * 36), listed=60, oldest_days=6, took=4.0)
        today = utc_date(self.now)
        rows = self.db.execute("SELECT asked, handed, failed, saved, fresh, short, listed, reached, days_back, took_2, "
                               "took_20 FROM size_days WHERE day = ? ORDER BY asked", (today,)).fetchall()
        self.assertEqual(rows, [(60, 1, 0, 0, 1, 0, 60, 0, 6, 0, 0), (200, 5, 1, 1, 3, 1, 420, 1, 102, 1, 2)])
        sizes = json.loads(self.relay("status"))["reads"]["OC1"]["sizes"]
        self.assertEqual(sizes[1], {"asked": 200, "handed": 5, "failed": 1, "saved": 1, "fresh": 3, "new": 1, "short": 1,
                                    "listed": 420, "reached": 1, "days_back": 102, "seconds": 20})
        self.assertIn("lists of 200 places (3 days): 5 handed out, 1 failed, 1 from the client's saved copy, 3 fetched "
                      "afresh; those listed 140 matches each (1 fewer than asked), back 34 days (1 to the season's "
                      "start), with 0.3 new Arena matches each; the middle answer took up to 20 s", self.admin("stats"))
        self.assertEqual(self.db.execute("SELECT SUM(took_ms) FROM size_days WHERE asked = 200").fetchone()[0], 33100)
        self.admin("policy", "--block", "100")                            # lists are asked for by the page, so stats
        self.read(self.first("t" * 36), listed=100, oldest_days=9, took=8.0)    # shows sizes together
        self.assertIn("lists of 60-100 places (3 days): 2 handed out, 0 failed, 0 from the client's saved copy, 2 fetched "
                      "afresh; those listed 80 matches each", self.admin("stats"))
        self.assertEqual([size["asked"] for size in json.loads(self.relay("status"))["reads"]["OC1"]["sizes"]],
                         [60, 100, 200])

    def test_a_collector_of_one_page_cannot_mark_a_list_read_with_nothing_new(self):
        job = self.handed(self.PLAYER)
        self.work([self.report(job, [7], read_to=int(self.now * 1000))], more=False)
        checked = self.player(self.PLAYER)[2]
        self.now += 3 * self.DAY
        again = self.handed(self.PLAYER)
        self.work([self.report(again, [], read_to=0)], more=False)        # nothing newer: fresh, or its client's copy?
        self.assertEqual(self.player(self.PLAYER)[2], checked)            # not counted as a read,
        self.assertEqual(self.player(self.PLAYER)[3], int(self.now) + 3 * self.DAY)   # and tried again as long after
        again = self.handed(self.PLAYER)
        self.work([self.report(again, [], read_to=int(self.now * 1000) - 5)], more=False)   # a newer match: afresh
        self.assertEqual(self.player(self.PLAYER)[2], int(self.now))


sys.path.insert(0, os.path.join(ROOT, "server", "tools"))
import simulate  # noqa: E402


class MadeUpWorld(unittest.TestCase):
    """The rules for handing out work, played through the real endpoint on a small made-up world: players whose
    clients give as many matches of a list as the first request asks for, and answer from a saved copy after that
    (server/tools/simulate.py)."""

    def test_a_season_is_caught_up_on_and_kept_up_with_without_waste(self):
        # These rules decide what every volunteer's collector does all day. A slip (a match asked for twice, a list
        # read over and over, a short read taken for the whole list) would cost days of requests before anyone saw it.
        ingest.recent.clear()
        world = simulate.World(40, 40, seed=5)
        start = next(p for p in range(world.players) if sum(1 for when in world.ptimes[p] if 28 < when < 30) > 5)
        results = {}
        for name, block in (("pages", None), ("blocks", 200)):            # collectors up to 0.3.7, then ones of 0.3.8
            installs = [simulate.Install("%s-%d" % (name, number), simulate.NORMAL, 0.006, block) for number in range(2)]
            rules, counts = simulate.Endpoint(world, installs, start=start), collections.defaultdict(collections.Counter)
            self.addCleanup(rules.close)
            simulate.collect(rules, installs, 30.0, 40.0, counts)         # ten days, from the season's 30th day on
            fetches = sum(day["fetches"] for day in counts.values())
            self.assertEqual(fetches, len(rules.held_at))                 # no match was fetched twice
            share = lambda first, last: (sum(rules.held[m] for m, when in enumerate(world.mtime) if first <= when < last)
                                         / sum(1 for when in world.mtime if first <= when < last))
            results[name] = share(0, 15), share(32, 38), sum(day["requests"] for day in counts.values()) / fetches
            if block:
                asked, saved, fresh, listed, short = rules.db.execute(
                    "SELECT SUM(handed), SUM(saved), SUM(fresh), SUM(listed), SUM(short) FROM size_days").fetchone()
                self.assertEqual(asked, saved + fresh)                    # every list handed out came back,
                self.assertEqual(short, 0)                                # this client gives what is asked for,
                self.assertEqual(saved, sum(day["copies"] for day in counts.values()))    # its copies were told apart,
                self.assertEqual(listed, sum(day["places"] for day in counts.values()))   # and the counts are right
        early, meanwhile, requests = results["blocks"]
        self.assertGreater(early, 0.9)                                    # the season's first half is held,
        self.assertGreater(meanwhile, 0.97)                               # and so is what was played meanwhile,
        self.assertLess(requests, 4.0)                                    # for under 4 client requests a match
        self.assertGreater(results["pages"][1], 0.97)                     # (pages of 20 keep up as well,
        self.assertGreater(results["pages"][2], requests)                 # with more requests a match)


class Urgent(ServerCase):
    """ArenaSweats Events games the rankings server is waiting for (relay `urgent`): they and their players' lists go
    out before anything else until the match is held. Only the rankings server's relay key can name them."""

    PLAYERS = ["11111111-2222-3333-4444-55555555555%d" % n for n in range(3)]

    def setUp(self):
        super().setUp()
        # A full load of other matches to fetch, so no ordinary list goes out: only the urgent ones.
        self.work([self.report("f" * 36, list(range(5000, 5300)))], more=False)

    def urgent(self, *games, platform="OC1"):
        """Send the rankings server's list: (game_id, players) pairs."""
        body = {"games": [{"platform": platform, "game_id": game_id, "players": list(players)}
                          for game_id, players in games]}
        return self.relay("urgent", stdin=json.dumps(body))

    def test_event_games_and_their_players_lists_go_out_first(self):
        self.assertEqual(json.loads(self.urgent((424242, self.PLAYERS[:2]), (424243, []))), {"games": 2, "players": 2})
        answer = self.work()
        self.assertEqual(answer["games"][:2], [424242, 424243])
        self.assertEqual(len(answer["games"]), ingest.GAMES_PER_ROUND)
        self.assertEqual(len(set(answer["games"])), len(answer["games"]))
        self.assertEqual(sorted(item["puuid"] for item in answer["players"]), self.PLAYERS[:2])
        self.assertTrue(all(self.shape(item) == {"since": self.season, "begin": 0, "pages": 1}
                            for item in answer["players"]))

    def test_an_event_game_is_asked_for_again_until_it_arrives(self):
        game_id = self.games[0]["gameId"]
        self.urgent((game_id, self.PLAYERS[:1]))
        handed = 0
        for _ in range(ingest.GAME_TRIES + 3):                            # never given up on, unlike other matches
            answer = self.work()
            handed += answer["games"].count(game_id)
            self.assertNotIn(game_id, self.work(key=self.add_key("bob%d" % handed))["games"])  # not twice at once
            self.now += ingest.URGENT_GAME_SECONDS
            self.urgent((game_id, self.PLAYERS[:1]))
        self.assertEqual(handed, ingest.GAME_TRIES + 3)
        self.post(self.games[:1])
        self.now += ingest.URGENT_LIST_SECONDS
        self.db.execute("UPDATE players SET due_at = ?", (int(self.now) + 86400,))   # no list is due otherwise
        self.db.commit()
        answer = self.work()
        self.assertNotIn(game_id, answer["games"])                        # held: no longer urgent ...
        self.assertIsNone(self.item(answer, self.PLAYERS[0]))             # ... nor its players' lists
        self.assertEqual(json.loads(self.relay("status"))["urgent_not_held"], 0)

    def test_players_lists_are_read_again_every_few_minutes_even_when_their_reads_fail(self):
        self.urgent((424242, self.PLAYERS[:2]))
        first = self.work()
        self.work([self.report(self.item(first, self.PLAYERS[0]), [], complete=False),
                   self.report(self.item(first, self.PLAYERS[1]), [])], more=False)
        self.assertEqual(self.work()["players"], [])                      # read a moment ago
        self.now += ingest.URGENT_LIST_SECONDS
        again = self.work()
        self.assertEqual(sorted(item["puuid"] for item in again["players"]), self.PLAYERS[:2])
        self.work([self.report(self.item(again, self.PLAYERS[1]), [424242])], more=False)
        self.assertIn(424242, [row[0] for row in self.db.execute("SELECT game_id FROM wanted")])

    def test_each_list_replaces_the_last_and_one_not_sent_again_lapses(self):
        self.urgent((424242, self.PLAYERS[:1]))
        self.urgent((424243, self.PLAYERS[1:2]))
        answer = self.work()
        self.assertEqual((answer["games"][0], [item["puuid"] for item in answer["players"]]), (424243, self.PLAYERS[1:2]))
        self.assertNotIn(424242, answer["games"])
        self.urgent((424243, self.PLAYERS[1:2]))                          # sent again: not handed out again yet
        self.assertNotIn(424243, self.work()["games"])
        self.now += ingest.URGENT_KEEP_SECONDS
        self.assertNotIn(424243, self.work()["games"])                    # not sent for an hour
        self.urgent()
        self.assertEqual(self.count("urgent_games") + self.count("urgent_players"), 0)

    def test_a_wanted_or_given_up_event_game_goes_out_once_without_counting_tries(self):
        self.work([self.report("p" * 36, [424242])], more=False)
        self.db.execute("INSERT INTO given_up VALUES('OC1', 424243)")
        self.db.commit()
        tries = self.db.execute("SELECT tries FROM wanted WHERE game_id = 424242").fetchone()[0]
        self.urgent((424242, []), (424243, []))
        answer = self.work()
        self.assertEqual((answer["games"].count(424242), answer["games"].count(424243)), (1, 1))
        self.assertEqual(self.db.execute("SELECT tries FROM wanted WHERE game_id = 424242").fetchone()[0], tries)
        self.assertNotIn(424242, self.work(key=self.add_key("bob"))["games"])   # nor from the wanted list meanwhile

    def test_event_games_are_kept_apart_by_server(self):
        self.urgent((424242, self.PLAYERS[:1]), platform="HN1")
        self.assertNotIn(424242, self.work()["games"])
        answer = self.send("/v1/work", {"platform": "HN1", "reports": []}).get_json()
        self.assertEqual((answer["games"], [item["puuid"] for item in answer["players"]]), ([424242], self.PLAYERS[:1]))

    def test_collectors_cannot_name_event_games(self):
        games = [{"platform": "OC1", "game_id": 424242, "players": self.PLAYERS}]
        self.work(urgent=games, games=games)
        self.assertEqual(self.send("/v1/urgent", {"games": games}).status_code, 404)
        self.assertEqual(self.count("urgent_games"), 0)

    def test_bad_lists_are_refused_and_change_nothing(self):
        self.urgent((424242, self.PLAYERS[:1]))
        good = {"platform": "OC1", "game_id": 1, "players": []}
        for body in ("not json", "[]", {"games": None}, {"games": [dict(good, game_id="1")]},
                     {"games": [dict(good, platform="oc1")]}, {"games": [dict(good, game_id=0)]},
                     {"games": [dict(good, players=["x"])]}, {"games": [dict(good, players=["0" * 36])]},
                     {"games": [dict(good, players=self.PLAYERS * 7)]}, {"games": [good] * (ingest.MAX_URGENT + 1)}):
            self.assertIn("Not understood", self.relay("urgent", stdin=body if isinstance(body, str) else json.dumps(body)))
        self.assertEqual([row[0] for row in self.db.execute("SELECT game_id FROM urgent_games")], [424242])

    def test_status_shows_the_list(self):
        self.assertIsNone(json.loads(self.relay("status"))["last_urgent_at"])
        self.urgent((424242, []))
        status = json.loads(self.relay("status"))
        self.assertEqual((status["last_urgent_at"], status["urgent_not_held"]), (int(self.now), 1))
        self.assertIn("Event games asked for first: 1 not received yet", self.admin("stats"))


class Me(ServerCase):
    """GET /v1/me: what one install has done, for the collector's own window."""

    def setUp(self):
        super().setUp()
        ingest.server_numbers.clear()

    def me(self, key=None):
        answer = self.client.get("/v1/me", headers={"Authorization": "Bearer " + (key or self.key)})
        self.assertEqual(answer.status_code, 200)
        return answer.get_json()

    def on(self, platform, *ids):
        """Matches with these ids on another server, played an hour before now."""
        return [dict(self.played(game_id, 3600), platformId=platform) for game_id in ids]

    def test_an_install_sees_its_own_matches_and_its_servers_progress(self):
        # Why: this is what volunteers see of their own help; a wrong count or another install's data in it would be
        # worse than no window at all.
        bob = self.add_key("bob")
        self.now -= 40 * 86400                                            # matches sent 40 days ago: in the total only
        self.post(self.on("HN1", 1, 2))
        self.now += 38 * 86400
        self.post(self.on("HN1", 3, 4, 5))                        # 3 the day before yesterday
        self.now += 2 * 86400
        self.post(self.on("HN1", 6))                        # 1 today
        self.post(self.on("HN1", 7, 8), key=bob)
        self.post(self.on("HN10", 9), key=bob)
        self.send("/v1/work", {"platform": "HN1", "more": False, "reports": []})   # (its server: where it asks for work)
        self.db.execute("INSERT INTO seen_days(day, platform, played, seen, held) VALUES(?, 'HN1', '2026-10-10', 200, 120)",
                        (utc_date(self.now),))
        self.db.commit()
        mine = self.me()
        self.assertEqual(len(mine["days"]), ingest.ME_DAYS)
        self.assertEqual(mine["days"][-1], {"day": utc_date(self.now), "matches": 1})
        self.assertEqual(mine["days"][-3], {"day": utc_date(self.now - 2 * 86400), "matches": 3})
        self.assertEqual(mine["days"][0]["day"], utc_date(self.now - (ingest.ME_DAYS - 1) * 86400))
        self.assertEqual(sum(day["matches"] for day in mine["days"]), 4)
        self.assertEqual({name: mine[name] for name in ("total", "server", "server_held", "server_held_share_estimate",
                                                        "server_helpers", "season_start")},
                         {"total": 6, "server": "HN1", "server_held": 8, "server_held_share_estimate": 0.6,
                          "server_helpers": 2, "season_start": ingest.DEFAULT_SEASON_START})
        self.assertEqual(mine["servers"][-2:], [{"server": "HN10", "helpers": 1}, {"server": "HN1", "helpers": 2}])
        self.assertEqual(mine["servers"][0], {"server": "NJ100", "helpers": 0})   # the fewest helpers first
        self.assertEqual({entry["server"] for entry in mine["servers"]}, set(ingest.CHINA_SERVERS))
        self.assertNotIn("bob", json.dumps(mine))                         # nobody else's identity
        self.assertEqual(self.me(bob)["total"], 3)
        self.assertIsNone(self.me(self.add_key("carol"))["server"])      # (not asked for work yet: no server)
        self.assertEqual(self.client.get("/v1/me").status_code, 401)
        self.admin("purge", "alice")
        self.assertEqual(self.me()["total"], 0)

    def test_matches_from_before_the_season_are_not_counted_for_the_server(self):
        # Why: when a new season begins, the old one's matches stay held; counting them would tell volunteers their
        # server is further along than it is.
        later = self.season + 30 * 86400 * 1000
        rows = [("OC1", 100 + n, later - (5 - n) * 3600000, int(self.now), 1, b"") for n in range(5)]
        rows += [("OC1", 200 + n, later + n * 3600000, int(self.now), 1, b"") for n in range(3)]
        self.db.executemany("INSERT INTO games VALUES(?, ?, ?, ?, ?, ?)", rows)
        self.db.commit()
        self.assertEqual([ingest.held_since(self.db, "OC1", season) for season in (self.season, later, later + 86400000)],
                         [8, 3, 0])
        self.assertEqual(ingest.held_since(self.db, "HN1", self.season), 0)

    def test_counts_held_before_this_version_are_carried_over(self):
        # Why: the live database holds 360,000 matches from before these counts existed; volunteers' totals must not
        # start from nothing.
        self.post(self.games[:3])
        self.db.executescript("DROP TRIGGER games_to_contributor_days; DROP TABLE contributor_days;")
        self.post(self.games[3:5])                                        # (sent while this version was not running)
        ingest.prepared.clear()
        ingest.connect().close()
        self.assertEqual(self.me()["total"], 5)
        self.post(self.games[5:6])
        self.assertEqual(self.me()["total"], 6)


class Limits(ServerCase):
    def test_oversized_body_is_refused(self):
        big = os.urandom(ingest.MAX_BODY + 1000)
        self.assertEqual(self.send("/v1/games", None, raw=big, compress=False).status_code, 413)

    def test_decompression_bomb_is_refused(self):
        bomb = b'{"games": [' + b" " * (ingest.MAX_RAW + 1000) + b"]}"
        self.assertLess(len(gzip.compress(bomb)), ingest.MAX_BODY)        # small on the wire
        self.assertEqual(self.send("/v1/games", None, raw=bomb).status_code, 413)

    def test_small_body_that_expands_in_memory_is_refused(self):
        bomb = b'{"games": [' + b"{}," * 200000 + b"{}]}"
        self.assertLess(len(gzip.compress(bomb)), 5000)
        self.assertEqual(self.send("/v1/games", None, raw=bomb).status_code, 413)

    def test_too_many_matches_in_one_request(self):
        self.assertEqual(self.post(self.games * 5).status_code, 413)

    def test_oversized_match_is_refused(self):
        fat = dict(self.games[0], padding="x" * (ingest.MAX_GAME_BYTES + 1))
        self.assertEqual(self.post([fat]).get_json()["rejected"][0]["reason"], "record is too large")

    def test_oversized_reports_are_ignored(self):
        self.work([self.report(self.handed("a" * 36), list(range(1, ingest.MAX_REPORT_IDS + 2)))], more=False)
        self.assertEqual((self.count("wanted"), self.player("a" * 36)[2]), (0, 0))
        too_many = [self.report("a" * 36, [])] * (ingest.MAX_REPORTS + 1)
        self.assertEqual(self.send("/v1/work", {"platform": "OC1", "reports": too_many}).status_code, 400)

    def test_request_rate_is_capped_per_key(self):
        statuses = [self.post([]).status_code for _ in range(ingest.REQUESTS_PER_MINUTE + 3)]
        self.assertEqual(statuses.count(200), ingest.REQUESTS_PER_MINUTE)
        self.assertEqual(statuses[-1], 429)
        self.assertEqual(self.post([], key=self.add_key("bob")).status_code, 200)   # others are unaffected
        self.now += 61
        self.assertEqual(self.post([]).status_code, 200)

    def test_daily_match_cap_per_key(self):
        original, ingest.GAMES_PER_DAY = ingest.GAMES_PER_DAY, 8
        self.addCleanup(setattr, ingest, "GAMES_PER_DAY", original)
        self.assertEqual(self.post(self.games[:6]).get_json()["accepted"], 6)
        over = self.post(self.games[6:])                                  # would pass the cap: nothing is taken
        self.assertEqual((over.status_code, over.headers["Retry-After"]), (429, "3600"))
        self.assertEqual(self.count("games"), 6)

    def test_the_limits_never_hold_back_an_honest_fast_collector(self):
        # Fast mode reads at most a match a request, never two requests within FAST_MIN_DELAY; a collector that
        # could not upload for a while holds at most OUTBOX_LIMIT more.
        self.assertGreater(ingest.GAMES_PER_DAY, 86400 / collector.FAST_MIN_DELAY + collector.OUTBOX_LIMIT)
        self.assertEqual(ingest.DEFAULT_FAST_DELAY, collector.FAST_MIN_DELAY)
        self.assertEqual(ingest.DEFAULT_FAST_FLIGHT, collector.FAST_MAX_FLIGHT)
        # At most one work request every FAST_WORK_SECONDS, a full upload every UPLOAD_BATCH matches, and one more
        # upload with each work request.
        most = 60 / collector.FAST_WORK_SECONDS * 2 + 60 / collector.FAST_MIN_DELAY / collector.UPLOAD_BATCH
        self.assertLess(most, ingest.REQUESTS_PER_MINUTE)
        self.assertLessEqual(collector.FAST_WORK_SECONDS, collector.WORK_SECONDS)

    def test_database_path_may_be_a_bare_file_name(self):
        here = os.getcwd()
        os.chdir(self.tmp.name)
        self.addCleanup(os.chdir, here)
        os.environ["INGEST_DB"] = "bare.db"
        ingest.connect().close()
        self.assertTrue(os.path.isfile(os.path.join(self.tmp.name, "bare.db")))


class Policy(ServerCase):
    def test_policy_is_served_and_changeable(self):
        headers = {"Authorization": "Bearer " + self.key}
        self.assertEqual(self.client.get("/v1/policy", headers=headers).get_json(),
                         {"enabled": True, "delay": 2.0, "fast": True, "fast_delay": 0.2, "fast_flight": 4,
                          "message": "", "season_start": ingest.date_ms("2026-10-06")})
        self.admin("policy", "--off", "--delay", "5", "--message", "back soon", "--season-start", "2027-01-15",
                   "--fast", "off", "--fast-delay", "1.5", "--fast-flight", "2")
        self.assertEqual(self.client.get("/v1/policy", headers=headers).get_json(),
                         {"enabled": False, "delay": 5.0, "fast": False, "fast_delay": 1.5, "fast_flight": 2,
                          "message": "back soon", "season_start": ingest.date_ms("2027-01-15")})
        for wrong in ("-1", "nan", "inf", "61"):
            self.assertIn("Not understood", self.admin("policy", "--fast-delay", wrong))
        for wrong in ("0", "-1", "101"):
            self.assertIn("Not understood", self.admin("policy", "--fast-flight", wrong))
        self.assertEqual(ingest.get_policy(self.db)["fast_delay"], 1.5)
        self.assertEqual(ingest.get_policy(self.db)["fast_flight"], 2)
        self.assertIn("'fast_delay': 1.5, 'fast_flight': 2", self.admin("stats"))
        self.assertEqual(self.post(self.games).get_json()["policy"]["enabled"], False)
        self.assertEqual(self.work()["policy"]["fast"], False)
        self.assertFalse(ingest.relay_status(self.db, self.now)["fast"])
        self.assertIn("Not understood", self.admin("policy", "--season-start", "next week"))
        self.assertIn("season starts 2027-01-15", self.admin("stats"))
        self.assertIn("'fast': False", self.admin("stats"))
        self.admin("policy", "--fast", "on")
        self.assertEqual(self.client.get("/v1/policy", headers=headers).get_json()["fast"], True)


class Signups(ServerCase):
    def register(self, address="203.0.113.5"):
        return self.client.post("/v1/register", headers={"X-Real-IP": address})

    def test_a_new_install_gets_its_own_working_key(self):
        answer = self.register()
        self.assertEqual(answer.status_code, 201)
        key, name = answer.get_json()["key"], answer.get_json()["name"]
        self.assertEqual(self.client.get("/v1/policy", headers={"Authorization": "Bearer " + key}).status_code, 200)
        self.assertEqual(self.post(self.games[:1], key=key).get_json()["accepted"], 1)
        listed = self.admin("keys")
        self.assertIn(name, listed)
        self.assertNotIn(key, listed)
        with open(os.environ["INGEST_DB"], "rb") as handle:
            self.assertNotIn(key, handle.read().decode("latin-1"))     # only its hash is kept

    def test_sign_ups_are_limited_per_address_and_per_day(self):
        for _ in range(ingest.SIGNUPS_PER_ADDRESS):
            self.assertEqual(self.register().status_code, 201)
        self.assertEqual(self.register().status_code, 429)
        self.assertEqual(self.register("198.51.100.7").status_code, 201)       # someone else
        self.addCleanup(setattr, ingest, "SIGNUPS_PER_DAY", ingest.SIGNUPS_PER_DAY)
        ingest.SIGNUPS_PER_DAY = ingest.SIGNUPS_PER_ADDRESS + 1
        self.assertEqual(self.register("192.0.2.9").status_code, 429)          # the day's limit is reached
        self.now += 86400 + 1
        self.assertEqual(self.register().status_code, 201)                      # a day later

    def test_sign_ups_can_be_closed_without_affecting_existing_keys(self):
        self.admin("policy", "--signups", "off")
        self.assertEqual(self.register().status_code, 403)
        self.assertEqual(self.post(self.games[:1]).status_code, 200)
        self.admin("policy", "--signups", "on")
        self.assertEqual(self.register().status_code, 201)


class WithServer(CollectorCase):
    """The real collector, the fake League client and the real ingest application together."""

    def setUp(self):
        super().setUp()
        os.environ["INGEST_DB"] = os.path.join(self.tmp.name, "ingest.db")
        self.addCleanup(os.environ.pop, "INGEST_DB", None)
        ingest.recent.clear()
        ingest.seen_written.clear()
        ingest.clock = lambda: self.real_start + self.clock.now()         # the server lives in the same pretend time
        self.addCleanup(setattr, ingest, "clock", time.time)
        self.db = ingest.connect()
        self.addCleanup(self.db.close)
        self.http = make_server("127.0.0.1", 0, ingest.app, threaded=True)
        threading.Thread(target=self.http.serve_forever, daemon=True).start()
        self.addCleanup(self.http.shutdown)
        self.url = "http://127.0.0.1:%d" % self.http.server_port
        self.key = self.admin("add-key", "alice").split()[-1]
        # In these tests every list is read as one block of `blocks` matches, whoever its player is: the endpoint
        # is told to ask for no more than that, and for as much for any player (not 20 for every held match).
        # None: an endpoint that names no number, as before 0.3.8.
        self.blocks = 100
        self.admin("policy", "--block", "100")
        for name, value in (("DEFAULT_RATE", 10 ** 6), ("FIT_MIN", 10 ** 9)):
            self.addCleanup(setattr, ingest, name, getattr(ingest, name))
            setattr(ingest, name, value)
        handed = ingest.app.view_functions["work"]

        def work():
            response = handed()
            if response.status_code == 200 and self.blocks is None:
                answer = response.get_json()
                for item in answer["players"]:
                    item.pop("count", None)
                response = ingest.jsonify(answer)
            return response
        ingest.app.view_functions["work"] = work
        self.addCleanup(ingest.app.view_functions.__setitem__, "work", handed)

    def admin(self, *args):
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            ingest.main(list(args))
        return output.getvalue()

    def server(self, key=None):
        return collector.Server(self.url, key or self.key)

    def server_ids(self):
        return {row[0] for row in self.db.execute("SELECT game_id FROM games")}

    def fetched(self, *fakes):
        """Every full-match request made to the given fake clients."""
        return [path for fake in fakes or (self.fake,) for _, path, _ in fake.requests if "/v1/games/" in path]


class EndToEnd(WithServer):
    def test_a_paused_collector_still_checks_in(self):
        states = []
        original = ingest.note_seen
        def record(db, contributor, now):
            states.append(ingest.request.headers.get("X-Collector-State"))
            return original(db, contributor, now)
        ingest.note_seen = record
        self.addCleanup(setattr, ingest, "note_seen", original)
        self.fake.phase = "InProgress"                     # in a game for the whole run
        self.run_collector(minutes=25, server=self.server())
        self.assertGreaterEqual(states.count("paused"), 2)  # every 10 minutes while paused

    def told(self):
        """Every (state, detail, pretend time) the collector sent with its requests, from the server's side."""
        told = []
        original = ingest.note_seen

        def record(db, contributor, now):
            told.append((ingest.request.headers.get("X-Collector-State"),
                         ingest.request.headers.get("X-Collector-Detail"), self.clock.now()))
            return original(db, contributor, now)
        ingest.note_seen = record
        self.addCleanup(setattr, ingest, "note_seen", original)
        self.addCleanup(setattr, collector, "save_config", collector.save_config)
        collector.save_config = lambda **values: None
        return told

    def test_how_the_client_was_found_reaches_arenasweats(self):
        # Why: without this nobody can tell which steps of the search real installs need, and where it fails.
        self.addCleanup(setattr, collector, "installed_dirs", collector.installed_dirs)
        collector.installed_dirs = lambda: [self.league_dir]
        self.run_collector(minutes=2, server=self.server(), league_dir=None)
        self.assertEqual(self.db.execute("SELECT state, detail FROM contributor_seen").fetchone(),
                         ("collecting", "found_usual"))
        self.assertIn("collecting 0.3.", self.admin("keys"))
        self.assertIn("why: found_usual", self.admin("keys"))

    def test_an_install_that_finds_no_client_says_why_at_once(self):
        # Why: installs that never found a client looked like "collecting" and went quiet, so nobody could tell
        # why they sent nothing; this is what shows it now, even for one that is closed within a minute.
        told = self.told()
        os.remove(os.path.join(self.league_dir, "lockfile"))
        start = self.clock.now()
        self.run_collector(minutes=1, server=collector.Server(self.url, None))
        self.assertEqual(told[0][:2], ("starting", ""))                                # signed up, nothing found yet
        self.assertNotIn("collecting", [state for state, _, _ in told])
        self.assertIn(("waiting_for_client", "no_client"), [(state, detail) for state, detail, _ in told])
        self.assertLess(told[1][2] - start, 30)
        self.assertIn("请打开英雄联盟并登录", self.output.getvalue())

    def test_a_client_that_is_open_but_out_of_reach_says_which_step_failed(self):
        # Why: the Tencent client's lockfile is empty, so a refused command line leaves it unreachable; the code
        # must say so, or the fix (run as administrator) cannot be told apart from other causes.
        told = self.told()
        open(os.path.join(self.league_dir, "lockfile"), "w").close()                  # the Tencent client's: empty
        for name, value in (("running_client_dirs", lambda: [self.league_dir]), ("command_line_logins", lambda: [])):
            self.addCleanup(setattr, collector, name, getattr(collector, name))
            setattr(collector, name, value)
        self.run_collector(minutes=1, server=self.server())
        self.assertIn(("client_unreachable", "path_seen.lock_empty.cmd_none"),
                      [(state, detail) for state, detail, _ in told])
        self.assertIn("以管理员身份运行", self.output.getvalue())

    def test_an_unexpected_error_is_told_before_the_collector_stops(self):
        # Why: a crash used to end with nothing sent, looking exactly like a window someone closed.
        told = self.told()

        def explode(self):
            raise RuntimeError("boom for the test")
        self.addCleanup(setattr, collector.Collector, "work_round", collector.Collector.work_round)
        collector.Collector.work_round = explode
        made = self.collector(minutes=5, server=self.server())
        with self.assertRaises(collector.Fatal) as raised, self.assertLogs(collector.log, "ERROR"), \
                contextlib.redirect_stdout(io.StringIO()):
            made.run()
        self.assertEqual(told[-1][:2], ("error", "error.runtimeerror.step"))
        self.assertIn("发生意外错误", str(raised.exception))
        sent = len(told)
        self.assertFalse(made.closing(2))               # the window closed afterwards: the error stays what we see
        self.assertEqual(len(told), sent)
        self.collector(server=self.server()).closing(2)            # a window closed while it ran, as such
        self.assertEqual(told[-1][:2], ("closed", "window"))

    def test_a_new_install_signs_up_and_uploads(self):
        saved = {}
        self.addCleanup(setattr, collector, "save_config", collector.save_config)
        collector.save_config = lambda **values: saved.update(values)
        self.run_collector(minutes=30, server=collector.Server(self.url, None))
        self.assertIn("Signed up with ArenaSweats.", self.output.getvalue())
        self.assertTrue(collector.KEY_RE.fullmatch(saved["key"]))
        row = self.db.execute("SELECT id, name FROM contributors WHERE key_hash = ?",
                              (ingest.hash_key(saved["key"]),)).fetchone()
        self.assertTrue(row[1].startswith("auto-"))
        self.assertTrue(self.server_ids())
        self.assertEqual({r[0] for r in self.db.execute("SELECT DISTINCT contributor_id FROM games")}, {row[0]})

    def test_nothing_is_read_until_signed_up(self):
        self.run_collector(minutes=3, server=collector.Server("http://127.0.0.1:9", None))   # nothing listens there
        self.assertIn("Cannot reach ArenaSweats yet", self.output.getvalue())
        self.assertEqual([path for _, path, _ in self.fake.requests if "match-history" in path], [])

    def test_everything_reaches_the_server_and_nothing_is_fetched_twice(self):
        made = self.run_collector(server=self.server())
        self.assertEqual(self.server_ids(), self.fake.arena_ids())
        fetched = self.fetched()
        self.assertEqual(len(fetched), len(set(fetched)))                 # each match asked for exactly once
        self.assertEqual(len(fetched), len(self.fake.arena_ids()))
        self.assertEqual(self.db.execute("SELECT COUNT(*) FROM wanted").fetchone()[0], 0)
        self.assertEqual(made.store.count(collector.PENDING), 0)
        self.assertEqual(list(made.store.stored_games()), [])             # nothing left on this computer
        self.assertEqual(made.store.one("SELECT COUNT(*) FROM games"), 0)
        self.assertEqual([n for n in os.listdir(self.data_dir) if n.endswith(".zip")], [])   # not a test run
        stored = self.db.execute("SELECT payload FROM games LIMIT 1").fetchone()[0]
        self.assertEqual(len(json.loads(ingest.zlib.decompress(stored))["participants"]), 16)

    def test_an_event_game_is_fetched_before_anything_else(self):
        game_id = min(game for game in self.fake.arena_ids()
                      if self.fake.me not in collector.player_ids(self.fake.games[game]))
        players = [puuid for puuid in collector.player_ids(self.fake.games[game_id]) if puuid.strip("0-")][:3]
        body = {"games": [{"platform": "OC1", "game_id": game_id, "players": players}]}
        with contextlib.redirect_stdout(io.StringIO()), unittest.mock.patch.object(sys, "stdin", io.StringIO(json.dumps(body))):
            ingest.main(["relay", "urgent"])
        self.run_collector(minutes=5, server=self.server())
        self.assertTrue(self.fetched()[0].endswith("/games/%d" % game_id))
        self.assertIn(game_id, self.server_ids())

    def test_work_is_not_taken_on_while_playing_and_stale_work_is_dropped(self):
        """The review's case: work taken on just before a game must not be done after someone else redid it."""
        ids = sorted(self.fake.arena_ids())
        client = ingest.app.test_client()
        seed = gzip.compress(json.dumps({"platform": "OC1", "more": False, "reports": [
            {"puuid": self.fake.players[3], "ids": ids, "next": 0, "newest": 1}]}).encode())
        start = self.clock.now()
        calls, original = [], collector.Server.call

        def logged(server, method, path, payload=None):
            calls.append((self.clock.now() - start, server.key, path, (payload or {}).get("more"), self.fake.phase))
            return original(server, method, path, payload)
        collector.Server.call = logged
        self.addCleanup(setattr, collector.Server, "call", original)

        self.clock.at(start + 30, lambda: setattr(self.fake, "phase", "InProgress"))     # a 25-minute game
        self.clock.at(start + 40, lambda: client.post("/v1/work", data=seed, headers={
            "Authorization": "Bearer " + self.key, "Content-Encoding": "gzip"}))       # work appears meanwhile
        self.clock.at(start + 30 + 25 * 60, lambda: setattr(self.fake, "phase", "None"))
        other = FakeClient(clock=self.clock)
        self.addCleanup(other.close)
        other.me = other.players[1]
        bob_dir = os.path.join(self.tmp.name, "data2")
        os.makedirs(bob_dir)
        bob_league = other.write_lockfile(os.path.join(self.tmp.name, "League2"))
        bob_key = self.admin("add-key", "bob").split()[-1]

        def bob_runs():                                                   # 16 minutes in, bob collects for 4 minutes
            opts = argparse.Namespace(league_dir=bob_league, delay=2.0, minutes=4,
                                      since=int((self.real_start - 30 * 86400) * 1000))
            bob = collector.Collector(opts, bob_dir, self.server(bob_key), sleep=self.clock.sleep,
                                      now=self.clock.now, wall=lambda: self.real_start + self.clock.now())
            with contextlib.redirect_stdout(io.StringIO()):
                bob.run()
            bob.close()
        self.clock.at(start + 16 * 60, bob_runs)
        self.run_collector(minutes=40, server=self.server())

        taken_while_playing = [c for c in calls if c[1] == self.key and c[2] == "/v1/work" and c[3] is not False
                               and c[4] != "None"]
        self.assertEqual(taken_while_playing, [])                         # no work taken on during the game
        both = set(self.fetched(self.fake)) & set(self.fetched(other))
        self.assertEqual(both, set())                                     # and nothing fetched by both
        self.assertEqual(self.server_ids(), set(ids))

    def test_reports_are_handed_in_when_a_game_starts(self):
        made = self.collector(minutes=3, server=self.server())
        start = self.clock.now()
        self.clock.at(start + 75, lambda: setattr(self.fake, "phase", "ChampSelect"))
        with contextlib.redirect_stdout(io.StringIO()):
            made.run()
        self.assertEqual(made.reports, [])                                # nothing left waiting on this computer
        handed = self.db.execute("SELECT COUNT(*) FROM players WHERE checked_at > 0").fetchone()[0]
        self.assertGreater(handed, 0)

    def test_two_collectors_share_the_work_without_overlap(self):
        other = FakeClient(clock=self.clock)                              # the same world, seen from another account
        self.addCleanup(other.close)
        other.me = other.players[1]
        folders = {self.fake: (self.league_dir, self.data_dir),
                   other: (other.write_lockfile(os.path.join(self.tmp.name, "League2")),
                           os.path.join(self.tmp.name, "data2"))}
        os.makedirs(folders[other][1])
        bob = self.admin("add-key", "bob").split()[-1]
        for fake, minutes in ((self.fake, 0.5), (other, 2), (self.fake, 2), (other, 2), (other, 60), (self.fake, 60)):
            self.league_dir, self.data_dir = folders[fake]
            self.run_collector(minutes=minutes, server=self.server(self.key if fake is self.fake else bob))
        self.assertEqual(self.server_ids(), self.fake.arena_ids())
        fetched = self.fetched(self.fake, other)
        self.assertEqual(len(fetched), len(set(fetched)))                 # not one match was fetched by both
        self.assertTrue(self.fetched(self.fake) and self.fetched(other))  # and both did part of the job

    def test_a_second_pass_fetches_nothing_again(self):
        self.run_collector(server=self.server())
        before = len(self.fake.requests)
        new_game = self.fake.add_game("CHERRY", 1700, self.fake.players[:16], age_seconds=-8 * 3600)
        self.clock.sleep(8 * 86400)                                       # a week later every list is due again
        self.run_collector(minutes=120, server=self.server())
        again = self.fake.requests[before:]
        self.assertEqual([path for _, path, _ in again if "/v1/games/" in path],
                         ["/lol-match-history/v1/games/%d" % new_game])   # only the match that is new
        self.assertTrue(all("begIndex=0&" in path for _, path, _ in again if "/matches?" in path))
        self.assertIn(new_game, self.server_ids())

    def test_nothing_from_before_the_season_is_fetched(self):
        started = self.real_start + self.clock.now() - 9 * 86400
        self.admin("policy", "--season-start", utc_date(started))
        cutoff = ingest.get_policy(self.db)["season_start"]
        in_season = {i for i in self.fake.arena_ids() if self.fake.games[i]["gameCreation"] >= cutoff}
        self.assertTrue(0 < len(in_season) < len(self.fake.arena_ids()))
        self.run_collector(server=self.server())
        self.assertEqual(self.server_ids(), in_season)
        self.assertEqual({int(path.rsplit("/", 1)[1]) for path in self.fetched()}, in_season)

    def test_without_the_server_almost_nothing_is_read(self):
        closed = collector.Server("http://127.0.0.1:9", self.key)        # nothing listens here
        self.run_collector(server=closed)
        self.assertEqual(len(history(self.fake.requests)), 1)             # one look at my own match list
        self.assertEqual(self.fetched(), [])
        self.run_collector(server=self.server())                          # the server is back: all is collected
        self.assertEqual(self.server_ids(), self.fake.arena_ids())

    def test_wrong_key_stops_the_collector_before_it_reads_anything(self):
        with self.assertRaises(collector.Fatal):
            self.run_collector(server=self.server("as_wrong"))
        self.assertEqual(history(self.fake.requests), [])

    def test_server_can_switch_collection_off_and_on(self):
        self.admin("policy", "--off")
        start = self.clock.now()
        self.clock.at(start + 1500, lambda: self.admin("policy", "--on"))
        made = self.run_collector(minutes=90, server=self.server())
        times = [when - start for _, _, when in history(self.fake.requests)]
        self.assertTrue(times)
        self.assertGreater(min(times), 1500)
        self.assertEqual(self.server_ids(), self.fake.arena_ids())
        self.assertIn("switched collection off", self.output.getvalue())
        self.assertTrue(made.enabled)

    def test_switching_off_reaches_a_collector_that_is_busy_failing(self):
        self.fake.fail = lambda path: 500 if "/v1/games/" in path else None
        start = self.clock.now()
        self.clock.at(start + 60, lambda: self.admin("policy", "--off"))
        self.run_collector(minutes=8 * 60, server=self.server())
        times = [when - start for _, _, when in history(self.fake.requests)]
        self.assertTrue(times)
        self.assertLessEqual(max(times), 60 + collector.POLICY_SECONDS + 30)

    def test_server_can_slow_collectors_down(self):
        self.admin("policy", "--delay", "6", "--message", "going gently")
        self.run_collector(minutes=20, server=self.server())
        times = [when for _, _, when in history(self.fake.requests)]
        self.assertGreaterEqual(min(b - a for a, b in zip(times, times[1:])), 6 * 0.8 - 0.001)
        self.assertIn("Message from ArenaSweats: going gently", self.output.getvalue())

    def test_server_cannot_speed_collectors_up(self):
        self.admin("policy", "--delay", "0")
        self.run_collector(minutes=20, server=self.server())
        times = [when for _, _, when in history(self.fake.requests)]
        self.assertGreater(len(times), 50)
        self.assertGreaterEqual(min(b - a for a, b in zip(times, times[1:])), 2.0 * 0.8 - 0.001)

    def test_refused_matches_are_kept_not_deleted(self):
        unlucky = sorted(self.fake.arena_ids())[:3]
        original = ingest.validate_game
        ingest.validate_game = lambda game, now: "refused for the test" if game.get("gameId") in unlucky else original(game, now)
        self.addCleanup(setattr, ingest, "validate_game", original)
        made = self.run_collector(server=self.server())
        self.assertEqual(made.store.count(collector.REFUSED), 3)
        self.assertEqual({game["gameId"] for game in made.store.stored_games()}, set(unlucky))
        self.assertIn("3 refused by ArenaSweats", self.output.getvalue())
        ingest.validate_game = original                                   # the server is fixed ...
        made = self.run_collector(minutes=5, server=self.server())
        self.assertEqual(self.server_ids(), self.fake.arena_ids())        # ... and the next run sends them
        self.assertEqual(made.store.count(collector.REFUSED), 0)

    def test_daily_cap_keeps_matches_waiting(self):
        original, ingest.GAMES_PER_DAY = ingest.GAMES_PER_DAY, 25
        self.addCleanup(setattr, ingest, "GAMES_PER_DAY", original)
        made = self.run_collector(server=self.server())
        waiting = made.store.count(collector.PENDING)
        self.assertGreater(waiting, 0)
        self.assertEqual(len(self.server_ids()) + waiting, len(set(self.fetched())))
        self.assertEqual(len(list(made.store.stored_games())), waiting)   # every waiting record is still here

    def test_upload_address_must_be_https(self):
        for url in ("http://example.com", "ftp://example.com", "http://127.0.0.1.evil.com"):
            with self.assertRaises(collector.Fatal):
                collector.Server(url, "k")
        collector.Server("https://arenasweats.lol/collector", "k")
        self.assertIsNone(collector.Server("https://arenasweats.lol/collector", "").key)    # it signs up instead


class Reaching(WithServer):
    """When ArenaSweats cannot be reached, the collector gets round what it can and says why."""

    def setUp(self):
        super().setUp()
        self.addCleanup(setattr, collector, "_way", collector._way)
        self.addCleanup(setattr, collector, "save_config", collector.save_config)
        collector.save_config = lambda **values: None

    def dead_proxy(self):
        """Windows' settings name a proxy that is not running (nothing listens on port 9)."""
        self.addCleanup(setattr, collector, "system_proxy", collector.system_proxy)
        collector.system_proxy = lambda url: "http://user:secret@127.0.0.1:9"
        collector._way = "proxy"
        patch = unittest.mock.patch.object(urllib.request, "proxy_bypass", lambda host: False)   # even for 127.0.0.1
        patch.start()
        self.addCleanup(patch.stop)

    def windows_store(self, load):
        """Stand in for Windows' certificate store: `load` runs where the collector reads it."""
        patch = unittest.mock.patch.object(ssl.SSLContext, "load_default_certs", load)
        patch.start()
        self.addCleanup(patch.stop)
        self.addCleanup(setattr, collector, "_https", None)
        collector._https = None

    def assert_checks_certificates(self, context):
        self.assertEqual(context.verify_mode, ssl.CERT_REQUIRED)
        self.assertTrue(context.check_hostname)

    def test_every_root_our_certificates_can_lead_to_is_carried_and_trusted_when_windows_has_none(self):
        found = {hashlib.sha256(ssl.PEM_cert_to_DER_cert("-----BEGIN" + block)).hexdigest()
                 for block in collector.EXTRA_ROOTS.split("-----BEGIN")[1:]}
        self.assertEqual(found, set(collector.EXTRA_ROOT_NAMES))
        self.windows_store(lambda context, purpose=None: None)         # a store with nothing in it
        context = collector.https_context()
        trusted = {dict(item[0] for item in cert["subject"])["commonName"]: cert for cert in context.get_ca_certs()}
        self.assertEqual(set(trusted), {"ISRG Root X1", "ISRG Root X2", "Root YE", "Root YR",    # Let's Encrypt's
                                        "GTS Root R1", "GTS Root R4"})                          # Google's
        for name, cert in trusted.items():                                # none runs out for years yet
            self.assertGreater(ssl.cert_time_to_seconds(cert["notAfter"]), time.time() + 8 * 365 * 86400, name)
        self.assert_checks_certificates(context)

    def test_a_windows_store_that_cannot_be_read_leaves_the_carried_roots_and_the_checking(self):
        def broken(context, purpose=None):
            raise OSError(1450, "Insufficient system resources exist to complete the requested service")
        self.windows_store(broken)
        with self.assertLogs("collector", "WARNING") as logged:
            context = collector.https_context()
        self.assertEqual(len(context.get_ca_certs()), len(collector.EXTRA_ROOT_NAMES))
        self.assert_checks_certificates(context)
        self.assertIn("using only the carried ones", "\n".join(logged.output))

    def test_odd_certificates_in_windows_store_go_to_the_log_not_the_screen(self):
        def odd(context, purpose=None):
            warnings.warn("Bad certificate in Windows certificate store: nested asn1 error")
        self.windows_store(odd)
        with contextlib.redirect_stderr(io.StringIO()) as screen, self.assertLogs("collector", "INFO") as logged:
            context = collector.https_context()
        self.assertEqual(screen.getvalue(), "")
        self.assertIn("Bad certificate in Windows certificate store", "\n".join(logged.output))
        self.assertEqual(len(context.get_ca_certs()), len(collector.EXTRA_ROOT_NAMES))

    def test_every_request_to_arenasweats_is_checked_with_the_carried_roots(self):
        used, opened = [], http.client.HTTPSConnection.__init__

        def watched(connection, *args, context=None, **kwargs):
            used.append(context)
            opened(connection, *args, context=context, **kwargs)

        for patch in (unittest.mock.patch.object(http.client.HTTPSConnection, "__init__", watched),
                      unittest.mock.patch.object(ssl, "create_default_context", side_effect=AssertionError("default")),
                      unittest.mock.patch.object(ssl, "_create_default_https_context",
                                                 side_effect=AssertionError("default"))):
            patch.start()
            self.addCleanup(patch.stop)
        nobody = socket.create_server(("127.0.0.1", 0))         # an address where nothing answers
        url = "https://127.0.0.1:%d" % nobody.getsockname()[1]
        nobody.close()
        self.assertEqual(collector.Server(url, None).call("POST", "/v1/register", {"version": "0"})[0], 0)
        with self.assertRaises(collector.NoAnswer):
            collector.latest_release(url)                       # the update check
        with self.assertRaises(collector.NoAnswer):              # the download of an update
            collector.install_update({"url": url + "/download/ArenaSweatsCollector.exe", "sha256": "0" * 64},
                                     os.path.join(self.tmp.name, "ArenaSweatsCollector.exe"))
        self.assertEqual(len(used), 3)
        self.assertTrue(all(context is collector.https_context() for context in used))
        self.assert_checks_certificates(collector.https_context())
        with open(collector.__file__, encoding="utf-8") as handle:
            source = handle.read()
        for unchecked in ("urlopen(", "create_default_context(", "_create_unverified_context"):
            self.assertNotIn(unchecked, source)

    def make_certificates(self):
        """A root of our own and a certificate for 127.0.0.1 from it, made with the openssl program.
        Returns the paths of (root, certificate, key)."""
        path = lambda name: os.path.join(self.tmp.name, name)
        with open(path("leaf.ext"), "w") as handle:
            handle.write("basicConstraints=CA:FALSE\nsubjectAltName=IP:127.0.0.1\n")
        for step in (["req", "-x509", "-newkey", "rsa:2048", "-nodes", "-keyout", path("root.key"), "-out", path("root.pem"),
                      "-days", "2", "-subj", "/CN=Test Root", "-addext", "basicConstraints=critical,CA:TRUE",
                      "-addext", "keyUsage=critical,keyCertSign,cRLSign"],
                     ["req", "-newkey", "rsa:2048", "-nodes", "-keyout", path("leaf.key"), "-out", path("leaf.csr"),
                      "-subj", "/CN=127.0.0.1"],
                     ["x509", "-req", "-in", path("leaf.csr"), "-CA", path("root.pem"), "-CAkey", path("root.key"),
                      "-CAcreateserial", "-out", path("leaf.pem"), "-days", "2", "-extfile", path("leaf.ext")]):
            if subprocess.run(["openssl"] + step, capture_output=True).returncode:
                self.skipTest("openssl could not make the certificates here")
        return path("root.pem"), path("leaf.pem"), path("leaf.key")

    @unittest.skipUnless(shutil.which("openssl"), "needs the openssl program to make certificates")
    def test_a_certificate_leading_to_a_carried_root_works_for_sign_up_uploads_and_updates_without_windows(self):
        root, cert, key = self.make_certificates()
        with open(root, encoding="ascii") as handle:
            self.addCleanup(setattr, collector, "EXTRA_ROOTS", collector.EXTRA_ROOTS)
            collector.EXTRA_ROOTS = handle.read()
        self.windows_store(lambda context, purpose=None: None)         # Windows holds none of it
        tls = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        tls.load_cert_chain(cert, key)
        https = make_server("127.0.0.1", 0, ingest.app, threaded=True, ssl_context=tls)
        threading.Thread(target=https.serve_forever, daemon=True).start()
        self.addCleanup(https.shutdown)
        url = "https://127.0.0.1:%d" % https.server_port
        self.run_collector(minutes=30, server=collector.Server(url, None))
        self.assertIn("Signed up with ArenaSweats.", self.output.getvalue())
        self.assertTrue(self.server_ids())
        downloads = os.path.join(self.tmp.name, "downloads")
        os.makedirs(downloads)
        self.addCleanup(os.environ.pop, "COLLECTOR_DOWNLOADS", None)
        os.environ["COLLECTOR_DOWNLOADS"] = downloads
        with open(os.path.join(downloads, ingest.PROGRAM), "wb") as handle:
            handle.write(b"new program")
        with open(os.path.join(downloads, "latest.json"), "w") as handle:
            json.dump({"version": "99.0.0", "sha256": hashlib.sha256(b"new program").hexdigest()}, handle)
        release = collector.latest_release(url)
        self.assertEqual(release["version"], "99.0.0")
        program = os.path.join(self.tmp.name, "program", "ArenaSweatsCollector.exe")
        os.makedirs(os.path.dirname(program))
        with open(program, "wb") as handle:
            handle.write(b"old program")
        collector.install_update(release, program)
        with open(program, "rb") as handle:
            self.assertEqual(handle.read(), b"new program")

    @unittest.skipUnless(shutil.which("openssl"), "needs the openssl program to make a certificate")
    def test_an_untrusted_certificate_is_named_as_the_reason(self):
        cert, key = os.path.join(self.tmp.name, "cert.pem"), os.path.join(self.tmp.name, "key.pem")
        made = subprocess.run(["openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-keyout", key,
                               "-out", cert, "-days", "2", "-subj", "/CN=127.0.0.1"], capture_output=True)
        if made.returncode:
            self.skipTest("openssl could not make a certificate here")
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        context.load_cert_chain(cert, key)
        listener = context.wrap_socket(socket.create_server(("127.0.0.1", 0)), server_side=True)
        self.addCleanup(listener.close)

        def serve():
            while listener.fileno() != -1:          # until it is closed at the end of the test
                try:
                    listener.accept()[0].close()
                except OSError:
                    pass                # the collector refused the certificate, as it should

        threading.Thread(target=serve, daemon=True).start()
        server = collector.Server("https://127.0.0.1:%d" % listener.getsockname()[1], None)
        with self.assertLogs("collector", "WARNING") as logged:
            self.run_collector(minutes=3, server=server)
        output = self.output.getvalue()
        self.assertIn("Cannot reach ArenaSweats yet: certificate check failed. Trying again in 1 min.", output)
        self.assertIn("暂时连不上 ArenaSweats：证书验证失败。1 分钟后重试。", output)
        self.assertIn(os.path.join(self.data_dir, "collector.log"), output)
        self.assertEqual(output.count("Cannot reach"), 1)            # said once, not every minute
        self.assertIn("SSLCertVerificationError", "\n".join(logged.output))

    def test_a_proxy_that_is_not_running_is_gone_round(self):
        self.dead_proxy()
        with self.assertLogs("collector", "INFO") as logged:
            self.run_collector(minutes=30, server=collector.Server(self.url, None))
        self.assertIn("Signed up with ArenaSweats.", self.output.getvalue())
        self.assertTrue(self.server_ids())
        self.assertEqual(collector._way, "direct")                  # tried directly first from then on
        log_text = "\n".join(logged.output)
        self.assertIn("answered directly after: proxy 127.0.0.1:9: ConnectionRefusedError", log_text)
        self.assertNotIn("secret", log_text)

    def test_when_no_way_answers_each_ways_problem_is_named(self):
        self.dead_proxy()
        self.run_collector(minutes=3, server=collector.Server("http://127.0.0.1:9", None))
        output = self.output.getvalue()
        self.assertIn("Cannot reach ArenaSweats yet: proxy 127.0.0.1:9: connection refused; "
                      "direct: connection refused.", output)
        self.assertIn("代理 127.0.0.1:9：连接被拒绝；直连：连接被拒绝。", output)
        self.assertNotIn("secret", output)

    def test_a_busy_server_is_named_and_its_wait_is_kept(self):
        self.addCleanup(setattr, ingest, "SIGNUPS_PER_ADDRESS", ingest.SIGNUPS_PER_ADDRESS)
        ingest.SIGNUPS_PER_ADDRESS = 0
        asked = []
        original = collector.Server.call
        def call(server, method, path, payload=None):
            asked.append(path)
            return original(server, method, path, payload)
        self.addCleanup(setattr, collector.Server, "call", original)
        collector.Server.call = call
        self.run_collector(minutes=90, server=collector.Server(self.url, None))
        output = self.output.getvalue()
        self.assertIn("Cannot reach ArenaSweats yet: ArenaSweats is busy, status 429. Trying again in 60 min.", output)
        self.assertIn("ArenaSweats 繁忙，状态码 429。60 分钟后重试。", output)
        self.assertEqual(asked, ["/v1/register"] * 2)              # at the start and an hour later, as asked

    def test_each_kind_of_problem_has_its_own_words(self):
        expired = ssl.SSLCertVerificationError(1, "certificate verify failed")
        expired.verify_code, expired.verify_message = 10, "certificate has expired"
        cases = [
            (urllib.error.URLError(expired), "check this PC's date and time", "请检查电脑的日期和时间"),
            (urllib.error.URLError(TimeoutError("timed out")), "timed out", "超时"),
            (TimeoutError("The read operation timed out"), "timed out", "超时"),
            (urllib.error.URLError(socket.gaierror(11001, "getaddrinfo failed")), "DNS", "DNS"),
            (urllib.error.URLError(ConnectionResetError(10054, "reset by peer")), "cut off", "中断"),
            (urllib.error.URLError(ssl.SSLEOFError(8, "EOF occurred in violation of protocol")), "TLS", "TLS"),
            (urllib.error.URLError(OSError("Tunnel connection failed: 502 Bad Gateway")), "proxy", "代理"),
            (urllib.error.URLError(OSError(10051, "network is unreachable")), "no network", "网络不通"),
        ]
        for error, english, chinese in cases:
            words = collector.why_unreachable(0, [("direct", error, 1.0)])
            self.assertIn(english, words[0], error)
            self.assertIn(chinese, words[1], error)
        self.assertEqual(collector.why_unreachable(502)[0], "ArenaSweats server error, status 502")
        self.assertEqual(collector.why_unreachable(404)[1], "ArenaSweats 返回状态码 404")

    def test_an_update_check_that_gets_no_answer_is_noted_for_the_log(self):
        self.assertFalse(collector.offer_update("http://127.0.0.1:9"))
        self.assertIn("update check got no answer: direct: ConnectionRefusedError", collector.early_notes[-1])


class FastWithServer(WithServer):
    """Fast mode with the real ingest application: ArenaSweats still has the last word."""

    def setUp(self):
        super().setUp()
        self.fake.close()
        # Enough to keep it reading for a while: a list is read once, one page, so that takes many players.
        self.fake = FakeClient(players=1500, arena=1500, other=0, clock=self.clock)
        self.fake.write_lockfile(self.league_dir)

    def gaps(self, start=0.0):
        """(when, gap to the next one) for every match-history request, `when` counted from `start`."""
        times = [when - start for _, _, when in history(self.fake.requests)]
        return [(a, b - a) for a, b in zip(times, times[1:])]

    @staticmethod
    def median(values):
        return sorted(values)[len(values) // 2]

    def test_fast_mode_is_reported_so_we_can_see_who_uses_it(self):
        self.run_collector(minutes=5, server=self.server(), fast=True)
        self.assertTrue(self.server_ids())
        self.assertLess(self.median([gap for _, gap in self.gaps()]), 0.5)
        self.assertEqual(self.db.execute("SELECT state FROM contributor_seen").fetchone()[0], "collecting_fast")
        self.assertEqual(ingest.relay_status(self.db, ingest.clock())["installs_seen"]["last_hour"],
                         {"collecting_fast": 1})
        self.assertIn("collecting_fast", self.admin("keys"))

    def test_the_server_can_switch_fast_mode_off_for_everyone(self):
        start = self.clock.now()
        self.clock.at(start + 300, lambda: self.admin("policy", "--fast", "off"))
        self.clock.at(start + 900, lambda: self.admin("policy", "--fast", "on"))
        self.run_collector(minutes=20, server=self.server(), fast=True)
        gaps = self.gaps(start)
        before = [gap for at, gap in gaps if at < 300]
        off = [gap for at, gap in gaps if 300 + 90 < at < 900]           # its next answer from ArenaSweats says so
        again = [gap for at, gap in gaps if at > 900 + 90]
        self.assertTrue(before and off and again)
        self.assertLess(self.median(before), 0.5)
        self.assertGreaterEqual(min(off), 2.0 * 0.8 - 0.001)             # the normal pace
        self.assertLess(self.median(again), 0.5)
        self.assertIn("ArenaSweats has switched fast mode off for now", self.output.getvalue())
        self.assertIn("ArenaSweats has switched fast mode back on", self.output.getvalue())

    def test_the_server_can_cap_fast_modes_speed(self):
        start = self.clock.now()
        self.clock.at(start + 300, lambda: self.admin("policy", "--fast-delay", "1.5"))
        self.clock.at(start + 900, lambda: self.admin("policy", "--fast-delay", "0"))    # cannot go under the floor
        self.run_collector(minutes=20, server=self.server(), fast=True)
        gaps = self.gaps(start)
        before = [gap for at, gap in gaps if at < 300]
        capped = [gap for at, gap in gaps if 300 + 90 < at < 900]         # its next answer from ArenaSweats says so
        again = [gap for at, gap in gaps if at > 900 + 90]
        self.assertTrue(before and capped and again)
        self.assertLess(self.median(before), 0.5)
        self.assertGreaterEqual(min(capped), 1.5 - 0.001)
        self.assertLess(self.median(capped), 1.7)
        self.assertGreaterEqual(min(again), collector.FAST_MIN_DELAY - 0.001)
        self.assertLess(self.median(again), 0.5)

    def test_our_own_server_never_holds_back_a_fast_collector(self):
        answers, original = [], collector.Server.call

        def call(server, method, path, payload=None):
            answer = original(server, method, path, payload)
            answers.append((self.clock.now(), path, answer[0]))
            return answer
        self.addCleanup(setattr, collector.Server, "call", original)
        collector.Server.call = call
        self.run_collector(minutes=60, server=self.server(), fast=True)
        self.assertGreater(len(self.server_ids()), 500)
        self.assertEqual({status for _, _, status in answers}, {200})
        times = [when for when, _, _ in answers]
        busiest = max(sum(1 for other in times if when <= other < when + 60) for when in times)
        self.assertLess(busiest, ingest.REQUESTS_PER_MINUTE * 2 / 3)          # 23 when this was written

    def test_the_server_can_still_slow_it_down_or_stop_it(self):
        self.admin("policy", "--delay", "6")
        self.run_collector(minutes=10, server=self.server(), fast=True)
        self.assertGreaterEqual(min(gap for _, gap in self.gaps()), 6.0 / collector.FAST_FACTOR * 0.8 - 0.001)
        self.admin("policy", "--off")
        before = len(self.fake.requests)
        self.run_collector(minutes=30, server=self.server(), fast=True, hours_later=7)
        self.assertEqual(history(self.fake.requests[before:]), [])


class SlowAnswers(WithServer):
    """The fake client's answers take pretend time here (PretendFlights), so requests can be on their way together."""

    def world(self, **players_and_matches):
        self.fake.close()
        self.fake = FakeClient(clock=self.clock, **players_and_matches)
        self.fake.write_lockfile(self.league_dir)

    def collector(self, *args, **kwargs):
        made = super().collector(*args, **kwargs)
        made.flights = self.flights = PretendFlights(self.clock, self.fake)
        return made

    def starts(self, since=0.0):
        """When each match-history request was sent, in seconds from `since`, with its address."""
        return [(when - since, path) for _, path, when in history(self.fake.requests) if when >= since]

    def gaps(self, since=0.0):
        times = [when for when, _ in self.starts(since)]
        return [b - a for a, b in zip(times, times[1:])]


class SeveralAtOnce(SlowAnswers):
    """Fast mode sends the next request before the last one is answered. A list takes 1.5 s here, about what
    Chinese clients were measured to take for 20 matches; a match 0.5 s, longer than theirs, so that every answer
    keeps the next request waiting whatever the mix of lists and matches."""

    def setUp(self):
        super().setUp()
        # Every list is short here, and there is enough to read for 40 minutes at fast mode's full pace.
        self.world(players=3000, arena=3000, other=0)
        self.blocks = 100
        self.fake.delay = lambda path: 0.5 if "/games/" in path else 1.5

    def test_the_normal_pace_still_sends_one_request_at_a_time(self):
        # Every volunteer was promised one gentle request at a time; a slip here would break that for all of them.
        made = self.run_collector(minutes=20, server=self.server())
        self.assertEqual((self.flights.most, self.flights.together, made.flight_limit), (1, 0, 1))
        flown = self.flights.flown
        self.assertGreater(len(flown), 150)
        for (_, answered, _), (sent, _, _) in zip(flown, flown[1:]):     # the gap still runs from the answer
            self.assertGreaterEqual(sent - answered, 2.0 * 0.8 - 0.001)
        self.assertTrue(self.server_ids())

    def test_fast_mode_overlaps_requests_within_its_ceiling_and_its_pace(self):
        # Fast mode's two hard limits protect the volunteer's account: never more at once, never closer together.
        start = self.clock.now()
        early = []
        self.clock.at(start + 5, lambda: early.append(self.flights.most))
        made = self.run_collector(minutes=15, server=self.server(), fast=True)
        self.assertEqual(early, [1])                                     # it starts with one at a time
        self.assertEqual(self.flights.most, collector.FAST_MAX_FLIGHT)   # and works up to the ceiling, not beyond
        self.assertEqual(made.flight, collector.FAST_MAX_FLIGHT)
        for kind in ("list", "match"):      # how long each kind of request takes alone was measured first
            self.assertGreaterEqual(len(made.alone[kind]), collector.FLIGHT_ALONE)
        self.assertGreaterEqual(min(self.gaps()), collector.FAST_MIN_DELAY - 0.001)
        late = self.gaps(start + 600)
        self.assertLess(sorted(late)[len(late) // 2], 0.5)               # a request every 0.4 s, whatever answers take
        self.assertEqual(made.stats["failed"], 0)
        fetched = self.fetched()
        self.assertEqual(len(fetched), len(set(fetched)))                # still nothing fetched twice

    def test_fewer_at_once_when_the_client_answers_slower_for_it(self):
        # Sending several at once to a client that cannot take it only loads Tencent's servers: it must back down by itself.
        free = [0.0]

        def one_after_the_other(path):          # the client works on one request at a time: the others wait
            begins = max(self.clock.now(), free[0])
            free[0] = begins + 1.5
            return free[0] - self.clock.now()
        self.fake.delay = one_after_the_other
        with self.assertLogs(collector.log, "INFO") as logs:
            made = self.run_collector(minutes=60, server=self.server(), fast=True)
        self.assertLessEqual(self.flights.most, 2)
        self.assertEqual(made.flight, 1)
        self.assertTrue(any("with others on their way" in line and "down to 1 requests at once" in line
                            for line in logs.output))
        self.assertLess(self.flights.together, len(self.starts()) * 0.1)   # it tried more now and then, and gave up

    def test_a_failure_means_fewer_at_once_and_a_429_the_normal_pace(self):
        # A client that fails or says "too many requests" must be left alone at once, or the account is at risk.
        start = self.clock.now()
        failing = {}
        self.fake.fail = lambda path: failing.pop("status", None) if "match-history" in path else None
        for when, status in ((250, 500), (400, 500), (410, 500), (800, 429)):
            self.clock.at(start + when, lambda status=status: failing.update(status=status))
        self.clock.at(start + 830, lambda: setattr(self.flights, "most", 0))
        with self.assertLogs(collector.log, "INFO") as logs:
            made = self.run_collector(minutes=30, server=self.server(), fast=True)
        down = [line for line in logs.output if "requests failed within 100: down to" in line]
        self.assertEqual(len(down), 1)          # not for the one failure on its own, but for two close together
        self.assertIn("no more for 10m", down[0])
        self.assertEqual(made.stats["failed"], 4)
        self.assertEqual(self.output.getvalue().count("fast mode rests for an hour"), 1)
        self.assertEqual((self.flights.most, made.flight, made.flight_limit), (1, 1, 1))   # one at a time since the 429
        resting = [flight for flight in self.flights.flown if flight[0] >= start + 830]
        self.assertGreater(len(resting), 50)
        for (_, answered, _), (sent, _, _) in zip(resting, resting[1:]):     # at the normal pace
            self.assertGreaterEqual(sent - answered, 2.0 * 0.8 - 0.001)

    def test_arenasweats_can_allow_fewer_at_once_but_never_more(self):
        # ArenaSweats' setting is the brake for every collector at once; it must work, and must not work as a throttle up.
        self.admin("policy", "--fast-flight", "2")
        made = self.run_collector(minutes=10, server=self.server(), fast=True)
        self.assertEqual((self.flights.most, made.flight_limit), (2, 2))
        self.admin("policy", "--fast-flight", "50")
        made = self.run_collector(minutes=15, server=self.server(), fast=True, hours_later=1)
        self.assertEqual((self.flights.most, made.flight_limit), (collector.FAST_MAX_FLIGHT, collector.FAST_MAX_FLIGHT))
        with contextlib.redirect_stdout(io.StringIO()):
            made.apply_policy({"fast": False})
            self.assertEqual(made.flight_limit, 1)
            made.apply_policy({"fast": True})                            # switched back on: one at a time again at first
        self.assertEqual((made.fast_on, made.flight_limit), (True, 1))

    def test_a_game_starting_stops_requests_and_drops_the_ones_on_their_way(self):
        # "Never while you play" is the collector's first promise: nothing may be sent or taken in once a game is queued.
        start = self.clock.now()
        self.fake.delay = lambda path: 1.5                               # so that some are always on their way
        self.clock.at(start + 400, lambda: setattr(self.fake, "phase", "ChampSelect"))
        self.clock.at(start + 1000, lambda: setattr(self.fake, "phase", "None"))
        self.run_collector(minutes=25, server=self.server(), fast=True)
        starts = self.starts(start)
        self.assertFalse([when for when, _ in starts if 400 + 5 <= when < 1000 + collector.RESUME_GRACE - 5])
        self.assertTrue([when for when, _ in starts if when > 1000])
        paths = [path for _, path in starts]
        # The requests that were on their way when the game was noticed: dropped unanswered, and asked again afterwards.
        dropped = [path for when, path in starts if 400 - 1.5 <= when < 400 + 5 and paths.count(path) == 2]
        self.assertTrue(1 <= len(dropped) <= collector.FAST_MAX_FLIGHT, dropped)
        for path in dropped:
            self.assertGreater([when for when, other in starts if other == path][1], 1000)
            if "/games/" in path:       # and what they brought was not kept: the match arrived only after the game
                arrived = self.db.execute("SELECT received_at FROM games WHERE game_id = ?", (int(path.split("/")[-1]),))
                self.assertGreater(arrived.fetchone()[0], self.real_start + start + 1000)
        self.assertIn("Paused while you play", self.output.getvalue())

    def test_it_tells_arenasweats_a_few_numbers_about_its_own_pace(self):
        # What leaves the volunteer's PC is a promise too: these reports may hold numbers and nothing else.
        seen, original = [], ingest.note_seen

        def record(db, contributor, now):
            seen.append(ingest.request.headers.get("X-Collector-Pace"))
            return original(db, contributor, now)
        ingest.note_seen = record
        self.addCleanup(setattr, ingest, "note_seen", original)
        self.run_collector(minutes=11, server=self.server(), fast=True)
        reports = [json.loads(text) for text in seen if text]
        self.assertEqual(len(reports), 2)                                # one every 5 minutes, each sent once
        self.assertEqual([json.loads(row[0]) for row in self.db.execute(
            "SELECT report FROM contributor_pace ORDER BY at")], reports)    # and ArenaSweats kept them as sent
        for report in reports:
            self.assertEqual(sorted(report), sorted(["seconds", "requests", "failed", "refused", "lists", "match",
                                                     "calls", "paused", "rested", "idle", "expired", "fast", "flight",
                                                     "most"]))
            for name, value in report.items():
                numbers = [value] if name not in ("lists", "match", "calls") else value
                for number in [n for row in numbers for n in row] if name == "lists" else numbers:
                    self.assertIs(type(number), int, name)
            self.assertTrue(295 <= report["seconds"] <= 310)
            self.assertGreater(report["requests"], 300)
            # lists asked for as blocks of 100: [100, how many, the middle time and a slow one in ms, the matches
            # they listed (about 16 each in this world), how many more came from the client's saved copy]
            (block,) = [row for row in report["lists"] if row[0] == 100]
            self.assertEqual(block[2:4], [1500, 1500])
            self.assertTrue(10 * block[1] < block[4] < 25 * block[1], block)
            self.assertEqual(report["match"][1:], [500, 500])
            self.assertTrue(1 <= report["most"] <= report["flight"] <= collector.FAST_MAX_FLIGHT, report)
        self.assertEqual(reports[0]["lists"][-1][5], 1)                   # my own list: saved by the first look at it
        self.assertGreater(reports[1]["flight"], 1)

    def test_real_requests_overlap_in_fast_mode_only(self):
        # The other tests here use pretend time; this one proves the real threads keep the same two promises.
        ingest.clock = time.time
        self.addCleanup(setattr, collector, "FLIGHT_CLEAN", collector.FLIGHT_CLEAN)
        self.addCleanup(setattr, collector, "FLIGHT_ALONE", collector.FLIGHT_ALONE)
        collector.FLIGHT_CLEAN, collector.FLIGHT_ALONE = 3, 2             # so that a few seconds are enough
        fake = FakeClient(players=60, arena=20)                          # no pretend clock: its answers really take time
        self.addCleanup(fake.close)
        fake.delay = lambda path: 0.02 if "/games/" in path else 0.5
        league_dir = fake.write_lockfile(os.path.join(self.tmp.name, "real", "League"))
        known = [fake.games[game_id] for game_id in sorted(fake.arena_ids())[:8]]       # so there are lists to hand out,
        self.assertEqual(ingest.app.test_client().post("/v1/games", json={"games": known}, headers={
            "Authorization": "Bearer " + self.key}).get_json()["accepted"], 8)          # which show a few matches not held
        for fast, seconds in ((True, 8), (False, 4)):                   # (fast first: it finds lists waiting, not matches)
            time.sleep(0.6)                 # (a request of the run before may still be on its way)
            fake.most_at_once = 0
            before = len(fake.requests)
            data_dir = os.path.join(self.tmp.name, "real", "data-%s" % fast)
            os.makedirs(data_dir)
            opts = argparse.Namespace(league_dir=league_dir, delay=2.0, minutes=seconds / 60, fast=fast,
                                      since=int((time.time() - 30 * 86400) * 1000))
            made = collector.Collector(opts, data_dir, self.server())
            self.addCleanup(made.close)
            with contextlib.redirect_stdout(io.StringIO()):
                made.run()
            times = [when for _, _, when in history(fake.requests[before:])]
            self.assertGreater(len(times), 5 if fast else 1)
            if fast:
                self.assertTrue(2 <= fake.most_at_once <= collector.FAST_MAX_FLIGHT, fake.most_at_once)
                self.assertGreaterEqual(min(b - a for a, b in zip(times, times[1:])), collector.FAST_MIN_DELAY - 0.05)
            else:
                self.assertEqual(fake.most_at_once, 1)
                self.assertGreaterEqual(min(b - a for a, b in zip(times, times[1:])), 2.0 * 0.8 - 0.05)


class Blocks(SlowAnswers):
    """A player's match list is read in ONE request. The real client gives as many matches as its first request
    for a player asks for and afterwards only its saved copy of those (measured on one, 25 Nov 2026); the fake
    client does the same. A fresh list takes it 0.25 s for every match listed, as it took that client."""

    def setUp(self):
        super().setUp()
        self.world(players=300, arena=600, other=12)                     # lists of 150 matches or so
        self.blocks = 100
        self.reports, original = [], collector.Server.call      # the match-list reports sent to ArenaSweats

        def call(server, method, path, payload=None):
            self.reports += payload["reports"] if path == "/v1/work" else []
            return original(server, method, path, payload)
        collector.Server.call = call
        self.addCleanup(setattr, collector.Server, "call", original)

    def world(self, **players_and_matches):
        super().world(**players_and_matches)
        self.fake.delay = lambda path: 0.05
        self.fake.entry_seconds = 0.25

    def listed(self, puuid, count):
        """The matches the client lists for a player when asked for their latest `count`, newest first."""
        return [self.fake.games[i] for i in sorted(self.fake.history[puuid],
                                                    key=lambda i: -self.fake.games[i]["gameCreation"])[:count]]

    def test_a_list_is_one_request_and_its_report_says_what_the_client_gave(self):
        # ArenaSweats plans every later read from these reports: a wrong number here would have it believe lists
        # were read through that were not, and matches would silently never be collected.
        self.run_collector(minutes=20, server=self.server())
        lists = [path for _, path in self.starts() if "/matches?" in path]
        me = self.fake.me
        self.assertEqual(lists[0], "/lol-match-history/v1/products/lol/%s/matches?begIndex=0&endIndex=19" % me)
        others = [path for path in lists if me not in path]
        self.assertGreater(len(others), 5)
        self.assertEqual(len(others), len(set(others)))                  # each player's list is asked for once,
        self.assertTrue(all(path.endswith("?begIndex=0&endIndex=99") for path in others))    # as one block
        read = {report["puuid"]: report for report in self.reports if report["asked"]}
        for puuid, report in read.items():
            listed = self.listed(puuid, 20 if puuid == me else 100)       # (mine: the copy saved by the first look)
            self.assertEqual((report["asked"], report["listed"], report["failed"]), (100, len(listed), False))
            self.assertEqual(report["oldest"], listed[-1]["gameCreation"])
            self.assertEqual(report["newest"], listed[0]["gameCreation"])
            self.assertEqual(sorted(report["ids"]), sorted(g["gameId"] for g in listed if g["gameMode"] == "CHERRY"))
            self.assertEqual(report["took"], 0 if puuid == me else 25.05)  # 100 matches at 0.25 s each
            self.assertEqual(report["next"], 0 if puuid == me else 100)    # the list may go on: only a larger block shows
        self.assertIn(len(others) + 1 - len(read), (0, 1))               # (one may have been on its way at the end)

    def test_asked_for_a_following_page_it_asks_the_client_nothing(self):
        # An endpoint that still pages would get the client's saved first page back as if it were the next one,
        # and would take matches for read that never were: the collector must say instead that the list ends.
        made = self.collector(server=self.server())
        before = len(self.fake.requests)
        made.take_work({"games": [], "lease": 900, "players": [
            {"puuid": self.fake.players[3], "job": "abc", "since": 0, "begin": 20, "pages": 3}]})
        self.assertEqual((list(made.todo), len(self.fake.requests)), ([], before))
        self.assertEqual(made.reports, [{"puuid": self.fake.players[3], "job": "abc", "begin": 20, "asked": 0, "listed": 0,
                                         "ids": [], "newest": 0, "oldest": 0, "took": 0.0, "next": 0, "failed": False}])
        made.take_work({"games": [], "lease": 900, "players": [          # with a number, the block is read from the place
            {"puuid": self.fake.players[4], "job": "def", "since": 0, "begin": 20, "pages": 3, "count": 50},
            {"puuid": self.fake.players[5], "job": "ghi", "since": 0, "begin": 0, "pages": 1, "count": 5000},
            {"puuid": self.fake.players[6], "job": "jkl", "since": 0, "begin": 9500, "pages": 1, "count": 200}]})
        self.assertEqual([read.path.split("?")[1] for read in made.todo],         # (from 0.3.11; up to 0.3.10 from the top)
                         ["begIndex=20&endIndex=69", "begIndex=0&endIndex=%d" % (collector.MAX_BLOCK - 1),
                          "begIndex=0&endIndex=199"])                     # (a place the client cannot be asked for: the top)

    def test_an_endpoint_that_names_no_number_gets_twenty_and_no_following_page(self):
        # Collectors and the endpoint are updated at different times: with today's endpoint the new collector must
        # not ask the client for pages it cannot give, nor leave ArenaSweats waiting for them.
        self.blocks = None
        self.run_collector(minutes=20, server=self.server())
        lists = [path for _, path in self.starts() if "/matches?" in path]
        self.assertGreater(len(lists), 20)
        self.assertTrue(all(path.endswith("?begIndex=0&endIndex=19") for path in lists))
        self.assertEqual(len(lists), len(set(lists)) + 1)                 # only my own list twice: a look, then the read
        self.assertEqual({(report["asked"], report["listed"], report["next"], report["begin"]) for report in self.reports},
                         {(20, 20, 0, 0)})
        self.assertTrue(self.server_ids())

    def test_a_long_list_is_not_taken_for_a_struggling_client(self):
        # A list of 100 takes a real client 25 s: if that looked like trouble, every collector would crawl or rest.
        for count, seconds in ((20, 20), (100, 48), (200, 83), (10000, 360)):
            self.assertEqual(collector.ListRead("p" * 36, 0, min(count, 1000)).timeout if count < 10000 else
                             collector.LCU_SECONDS_MAX, seconds)       # the time a list is given grows with its size
        with self.assertLogs(collector.log, "INFO") as logs:
            made = self.run_collector(minutes=45, server=self.server(), fast=True)
        self.assertEqual((made.stats["failed"], made.failures), (0, 0))
        self.assertFalse([line for line in logs.output if "down to" in line or "rests for an hour" in line])
        self.assertGreaterEqual(made.flight, 2)                          # it was allowed more at once all the same
        self.assertLessEqual(self.flights.most, collector.FAST_MAX_FLIGHT)
        self.assertGreaterEqual(min(self.gaps()), collector.FAST_MIN_DELAY - 0.001)
        lists = [(sent, answered) for sent, answered, path in self.flights.flown if "/matches?" in path]
        matches = [sent for sent, _, path in self.flights.flown if "/games/" in path]
        at_once = max(sum(1 for sent, answered in lists if sent <= when < answered) for when, _ in lists)
        self.assertLessEqual(at_once, made.flight - 1)                   # one place is kept for the matches,
        flown = self.flights.flown
        first = min(sent for place, (sent, _, _) in enumerate(flown)     # when two were first on their way together
                    if any(other < sent < answered for other, answered, _ in flown[:place]))
        late = [when for when in matches if when > first]
        during = [when for when in late if any(sent < when < answered for sent, answered in lists)]
        self.assertGreater(len(late), 50)
        self.assertGreater(len(during), len(late) * 0.8)                 # which go on while lists are being answered

    def test_which_request_goes_next(self):
        # The mix decides both promises of fast mode at once: never more than four on their way, and lists (the
        # requests that weigh on the client) never all of them.
        made = self.collector(server=self.server(), fast=True)
        lists = [collector.ListRead(puuid, 0, 100) for puuid in self.fake.players[:6]]
        matches = [collector.MatchFetch(game_id) for game_id in sorted(self.fake.arena_ids())[:6]]

        def order(flight):
            made.flight, made.todo, made.flying = flight, collector.deque(matches[:3] + lists + matches[3:]), {}
            sent = []
            while made.choose() is not None:
                job = made.choose()
                made.todo.remove(job)
                made.flying[len(sent)] = job
                sent.append("list" if job in lists else "match")
            return sent
        self.assertEqual(order(4), ["list", "list", "list", "match"])    # one place is kept for the matches
        self.assertEqual(order(3), ["list", "list", "match"])
        self.assertEqual(order(2), ["list", "match"])
        self.assertEqual(order(1), ["match"])                            # one at a time: matches before lists
        made.fast = False                                                # the normal pace: always one at a time
        self.assertEqual((made.flight_limit, order(4)), (1, ["match"]))

    def test_at_the_normal_pace_a_long_list_is_waited_for(self):
        # One request at a time is the promise: the next one must wait for a list's answer however long that takes.
        made = self.run_collector(minutes=30, server=self.server())
        self.assertEqual((made.stats["failed"], self.flights.most), (0, 1))
        flown = self.flights.flown
        self.assertGreater(sum(1 for sent, answered, _ in flown if answered - sent >= 20), 5)
        for (_, answered, _), (sent, _, _) in zip(flown, flown[1:]):
            self.assertGreaterEqual(sent - answered, 2.0 * 0.8 - 0.001)


    def test_a_list_dropped_for_a_game_comes_from_the_saved_copy_afterwards(self):
        # Nothing may be sent while a game is on; and what the game interrupted must not cost the client a second
        # long read afterwards, nor be reported as if it had taken no time for the wrong reason.
        asked, game = [], {}

        def queue_up(player, page):             # a game is queued just after the eighth list was asked for
            asked.append(player)
            if len(asked) == 8:
                game.update(began=self.clock.now(), player=player)
                self.fake.phase = "ChampSelect"
                self.clock.at(self.clock.now() + 600, lambda: setattr(self.fake, "phase", "None"))
            return page
        self.fake.list_hook = queue_up
        self.run_collector(minutes=40, server=self.server(), fast=True)
        starts = self.starts(game["began"])
        self.assertFalse([when for when, _ in starts if 5 <= when < 600 + collector.RESUME_GRACE - 5])
        again = [when for when, path in starts if game["player"] in path]
        self.assertEqual(len(again), 2)                                  # on its way when the game was noticed: dropped,
        self.assertGreater(again[1], 600)                                # and asked again afterwards,
        (report,) = [r for r in self.reports if r["puuid"] == game["player"] and r["asked"]]
        self.assertEqual((report["listed"], report["failed"]), (100, False))    # when it came in full
        self.assertLess(report["took"], collector.SAVED_SECONDS)         # and at once: the client had saved its answer
        self.assertEqual(asked.count(game["player"]), 2)


class Updates(WithServer):
    """A collector offers to update itself from the server it talks to."""

    def setUp(self):
        super().setUp()
        self.downloads = os.path.join(self.tmp.name, "downloads")
        os.makedirs(self.downloads)
        os.environ["COLLECTOR_DOWNLOADS"] = self.downloads
        self.addCleanup(os.environ.pop, "COLLECTOR_DOWNLOADS", None)
        self.program = os.path.join(self.tmp.name, "program", "ArenaSweatsCollector.exe")
        os.makedirs(os.path.dirname(self.program))
        with open(self.program, "wb") as handle:
            handle.write(b"old program")

    def publish(self, version, body, digest=None):
        with open(os.path.join(self.downloads, ingest.PROGRAM), "wb") as handle:
            handle.write(body)
        with open(os.path.join(self.downloads, "latest.json"), "w") as handle:
            json.dump({"version": version, "sha256": digest or hashlib.sha256(body).hexdigest(), "size": len(body)}, handle)

    def test_versions_compare_as_numbers(self):
        self.assertGreater(collector.version_tuple("0.3.10"), collector.version_tuple("0.3.9"))
        for bad in (None, "", "v0.3.4", "0.3.4-beta", "1"):
            self.assertIsNone(collector.version_tuple(bad), bad)

    def test_nothing_is_offered_without_a_release_or_a_newer_one(self):
        asked = []
        self.assertFalse(collector.offer_update(self.url, ask=lambda prompt: asked.append(prompt) or "y"))
        self.publish(collector.VERSION, b"same version")
        self.assertFalse(collector.offer_update(self.url, ask=lambda prompt: asked.append(prompt) or "y"))
        self.assertEqual(asked, [])

    def test_yes_installs_the_new_version_and_starts_it(self):
        self.publish("99.0.0", b"new program")
        started = []
        self.addCleanup(setattr, sys, "executable", sys.executable)
        sys.executable = self.program
        with contextlib.redirect_stdout(io.StringIO()):
            self.assertTrue(collector.offer_update(self.url, ask=lambda prompt: "y", spawn=started.append))
        with open(self.program, "rb") as handle:
            self.assertEqual(handle.read(), b"new program")
        with open(self.program + ".old", "rb") as handle:
            self.assertEqual(handle.read(), b"old program")
        self.assertEqual(started[0][0], self.program)
        self.assertEqual(sorted(os.listdir(os.path.dirname(self.program))),
                         ["ArenaSweatsCollector.exe", "ArenaSweatsCollector.exe.old"])

    def test_no_keeps_this_version(self):
        self.publish("99.0.0", b"new program")
        self.addCleanup(setattr, sys, "executable", sys.executable)
        sys.executable = self.program
        with contextlib.redirect_stdout(io.StringIO()):
            self.assertFalse(collector.offer_update(self.url, ask=lambda prompt: "n", spawn=self.fail))
        with open(self.program, "rb") as handle:
            self.assertEqual(handle.read(), b"old program")

    def test_the_question_is_update_now_y_or_n_with_yes_on_enter(self):
        self.publish("99.0.0", b"new program")
        self.addCleanup(setattr, sys, "executable", sys.executable)
        sys.executable = self.program
        for answer, updates in (("", True), ("Y", True), ("yes", True), ("是", True), ("n", False), ("no", False),
                                ("否", False), (None, True)):       # None: nobody answered in time
            with open(self.program, "wb") as handle:
                handle.write(b"old program")
            asked, started = [], []
            with contextlib.redirect_stdout(io.StringIO()) as output:
                result = collector.offer_update(self.url, ask=lambda prompt: asked.append(prompt) or answer,
                                                spawn=started.append)
            self.assertEqual(result, updates, answer)
            self.assertEqual(asked, ["Update now? 现在更新？ [Y/n]  No answer = update / 不回答即更新: "])
            self.assertEqual("No answer: updating. 无人回答：自动更新。" in output.getvalue(), answer is None)
            self.assertIn("A new version of the collector is available: 99.0.0 (you have %s).\n"
                          "有新版本的采集器：99.0.0（当前 %s）。" % (collector.VERSION, collector.VERSION), output.getvalue())
            with open(self.program, "rb") as handle:
                self.assertEqual(handle.read(), b"new program" if updates else b"old program")
            self.assertEqual(started, [[self.program] + sys.argv[1:]] if updates else [])
            self.assertEqual("Keeping version %s." % collector.VERSION in output.getvalue(), not updates)

    def start(self, typed, *flags):
        """Start main() as the packaged Windows program does, with `typed` as what the player types (one answer
        a line). Returns (exit code, what the window showed, the programs it started, its own arguments)."""
        class Console(io.StringIO):
            def isatty(self):
                return True

        class WindowsOs:                    # this computer's os, but named as Windows' until the update offer
            name = "nt"

            def __getattr__(self, name):
                return getattr(os, name)
        started, offer = [], collector.offer_update

        def offer_update(url):
            collector.os = os               # from here on, what really runs here
            return offer(url, spawn=started.append)
        argv = [self.program, "--server", self.url, "--key", self.key, "--league-dir", self.league_dir,
                "--data-dir", self.data_dir, "--minutes", "0.04"] + list(flags)
        with contextlib.ExitStack() as stack:
            for name, value in (("executable", self.program), ("argv", argv), ("frozen", True),
                                ("stdin", Console(typed))):
                stack.enter_context(unittest.mock.patch.object(sys, name, value, create=True))
            stack.enter_context(unittest.mock.patch.object(collector, "offer_update", offer_update))
            # already administrator: the question about it (test_collector.AdminRights) is not what is tested here
            stack.enter_context(unittest.mock.patch.object(collector, "is_elevated", lambda: True))
            stack.enter_context(unittest.mock.patch.object(collector, "os", WindowsOs()))
            output = stack.enter_context(contextlib.redirect_stdout(io.StringIO()))
            code = collector.main()
        return code, output.getvalue(), started, argv

    def assertInOrder(self, text, *parts):
        places = [text.find(part) for part in parts]
        self.assertNotIn(-1, places, parts)
        self.assertEqual(places, sorted(places), parts)

    def test_no_to_the_update_then_the_fast_mode_question_then_collection(self):
        self.publish("99.0.0", b"new program")
        code, shown, started, _ = self.start("n\ny\n")
        self.assertEqual((code, started), (0, []))
        self.assertInOrder(shown, "Update now? 现在更新？ [Y/n] ", "Keeping version", "Activate fast mode? 开启快速模式？ [y/N] ",
                           collector.FAST_ON, "Connected to the League client", "Press Enter to close.")
        self.assertEqual(shown.count("Activate fast mode?"), 1)
        self.assertTrue(history(self.fake.requests))                       # and it collects
        self.assertEqual(self.db.execute("SELECT state FROM contributor_seen").fetchone()[0], "collecting_fast")

    def test_enter_to_the_fast_mode_question_means_no(self):
        code, shown, _, _ = self.start("\n")
        self.assertInOrder(shown, "Activate fast mode? 开启快速模式？ [y/N] ", "Connected to the League client")
        self.assertNotIn(collector.FAST_ON, shown)
        self.assertEqual(self.db.execute("SELECT state FROM contributor_seen").fetchone()[0], "collecting")

    def test_yes_to_the_update_starts_the_new_version_with_the_same_choice_and_asks_nothing_more(self):
        self.publish("99.0.0", b"new program")
        code, shown, started, argv = self.start("\n", "--fast")
        self.assertEqual(code, 0)
        self.assertEqual(started, [argv])                                  # the same arguments, --fast included
        self.assertEqual(started[0][-1], "--fast")
        self.assertIn("Updated to 99.0.0. Starting it ...", shown)
        self.assertNotIn("Activate fast mode?", shown)
        self.assertEqual(history(self.fake.requests), [])                  # the old version stops there

    def test_the_flags_skip_the_question(self):
        self.publish(collector.VERSION, b"this version")                   # nothing newer
        for flag, on in (("--fast", True), ("--no-fast", False)):
            code, shown, _, _ = self.start("", flag)
            self.assertEqual(code, 0)
            self.assertNotIn("Update now?", shown)
            self.assertNotIn("Activate fast mode?", shown)
            self.assertEqual(collector.FAST_ON in shown, on)

    def test_a_download_that_fails_its_checksum_changes_nothing(self):
        self.publish("99.0.0", b"tampered program", digest="0" * 64)
        self.addCleanup(setattr, sys, "executable", sys.executable)
        sys.executable = self.program
        with contextlib.redirect_stdout(io.StringIO()) as output:
            self.assertFalse(collector.offer_update(self.url, ask=lambda prompt: "y", spawn=self.fail))
        self.assertIn("did not work", output.getvalue())
        with open(self.program, "rb") as handle:
            self.assertEqual(handle.read(), b"old program")
        self.assertEqual(os.listdir(os.path.dirname(self.program)), ["ArenaSweatsCollector.exe"])

    def test_latest_and_download_need_no_key(self):
        self.assertEqual(self.http_get("/latest")[0], 404)
        self.publish("99.0.0", b"new program")
        status, body = self.http_get("/latest")
        self.assertEqual(status, 200)
        self.assertEqual(json.loads(body)["path"], "/download/ArenaSweatsCollector.exe")
        self.assertEqual(self.http_get("/download/ArenaSweatsCollector.exe"), (200, b"new program"))

    def http_get(self, path):
        try:
            with urllib.request.urlopen(self.url + path, timeout=10) as response:
                return response.status, response.read()
        except urllib.error.HTTPError as error:
            return error.code, error.read()


class HostileServer(CollectorCase):
    """An upload server (or a proxy in front of it) that answers with rubbish must not hurt the collector."""

    ANSWERS = [
        (503, {"Retry-After": "\xb2"}, b"busy"),                            # a 'digit' that is not a number
        (503, {"Retry-After": "9" * 5000}, b""),
        (200, {}, b'{"enabled": true, "delay": ' + b"9" * 400 + b"}"),       # a delay too big for a float
        (200, {}, b"[" * 60000),                                              # nesting deep enough to exhaust Python
        (200, {}, b'{"enabled": "no", "delay": -5, "message": 7, "season_start": "never"}'),
        (200, {}, b'{"delay": NaN, "message": "\\ud800\\u0007 hi", "season_start": 1e400}'),
        (200, {}, b'{"delay": true, "policy": [1], "games": 5, "players": "all", "wait": -1}'),
        (200, {}, b'{"accepted": "x", "rejected": 5, "policy": {"delay": "fast", "enabled": null, "season_start": -4}}'),
        (200, {}, b'{"rejected": [5, null, {"gameId": [1]}, {"gameId": {}}], "policy": {"delay": 0.001, '
                  b'"fast_delay": 0.001}}'),
        (200, {}, b'{"enabled": true, "fast_delay": -1}'),
        (200, {}, b'{"enabled": true, "fast_delay": NaN}'),
        (200, {}, b'{"games": [1, "x", 1e400, -5, 1e3, true, null, {"a": 1}, 99999999999999999999999], "wait": 0}'),
        (200, {}, b'{"players": [{"puuid": "../../x", "since": 5}, {"puuid": 7}, 7, null, [],'
                  b' {"puuid": "' + b"z" * 36 + b'", "since": "a"}, {"puuid": "' + b"y" * 36 + b'", "since": 1e400}]}'),
        (200, {}, b'{"games": [' + b"7," * 5000 + b'7], "players": [' + b'{"puuid": "' + b"w" * 36 + b'"},' * 500
                  + b"null]}"),
        (200, {}, b"<html>captive portal</html>"),
        (302, {"Location": "http://example.com/"}, b""),
        (200, {}, b"null"),
    ]

    def setUp(self):
        super().setUp()
        from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
        case = self
        case.served = []

        class Handler(BaseHTTPRequestHandler):
            def answer(self):
                self.rfile.read(int(self.headers.get("Content-Length") or 0))
                status, headers, body = case.ANSWERS[len(case.served) % len(case.ANSWERS)]
                case.served.append(self.path)
                self.send_response(status)
                for name, value in headers.items():
                    self.send_header(name, value)
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            do_GET = do_POST = answer

            def log_message(self, *args):
                pass

        self.http = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        threading.Thread(target=self.http.serve_forever, daemon=True).start()
        self.addCleanup(self.http.shutdown)

    def test_rubbish_answers_cannot_crash_it_or_speed_it_up(self):
        server = collector.Server("http://127.0.0.1:%d" % self.http.server_port, "as_key")
        self.run_collector(minutes=12 * 60, server=server)
        self.assertGreater(len(self.served), len(self.ANSWERS) * 3)       # every kind of answer was met
        times = [when for _, _, when in history(self.fake.requests)]
        self.assertGreater(len(times), 20)
        self.assertGreaterEqual(min(b - a for a, b in zip(times, times[1:])), 1.0 - 0.001)
        for method, path, _ in self.fake.requests:                        # and it only ever did what it may do
            self.assertEqual(method, "GET")
            self.assertTrue(any(p.fullmatch(path) for p in collector.LCU_ALLOWED), path)
        work_calls = [path for path in self.served if path == "/v1/work"]
        self.assertLessEqual(len(work_calls), 12 * 3600 / collector.WORK_SECONDS + 1)

    def test_rubbish_answers_cannot_push_fast_mode_below_the_floor(self):
        server = collector.Server("http://127.0.0.1:%d" % self.http.server_port, "as_key")
        self.run_collector(minutes=3 * 60, server=server, fast=True)
        times = [when for _, _, when in history(self.fake.requests)]
        self.assertGreater(len(times), 20)
        self.assertGreaterEqual(min(b - a for a, b in zip(times, times[1:])), collector.FAST_MIN_DELAY - 0.001)


if __name__ == "__main__":
    unittest.main()
