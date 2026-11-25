"""Tests for server/ingest.py, and for the collector working with it.  Run:  python -m unittest discover tests"""

import argparse
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

    def handed(self, puuid, key=None):
        """Make `puuid` the most overdue list, ask for work, and return its hand-out."""
        self.db.execute("INSERT OR IGNORE INTO players(platform, puuid, due_at) VALUES('OC1', ?, 0)", (puuid,))
        self.db.execute("UPDATE players SET due_at = 0, leased_until = 0 WHERE puuid = ?", (puuid,))
        self.db.commit()
        return self.item(self.work(key=key), puuid)

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
        return self.db.execute("SELECT games, read_to, checked_at, due_at, fails, resume, resume_floor, older_due "
                               "FROM players WHERE puuid = ?",
                               (puuid,)).fetchone()


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
        report = {"seconds": 300, "requests": 700, "failed": 1, "refused": 0, "top": [150, 1600, 1900],
                  "page": [280, 700, 900], "match": [270, 60, 80], "calls": [60, 950, 1400], "paused": 0, "rested": 0,
                  "idle": 4, "expired": 0, "fast": 1, "flight": 3, "most": 4}
        sent = dict(report, name="Somebody#123", top=[150, 1600, "x"], extra={"a": 1})      # only numbers we know stay
        headers["X-Collector-Pace"] = json.dumps(sent)
        self.assertEqual(self.client.get("/v1/policy", headers=headers).status_code, 200)
        kept = dict(report)
        del kept["top"]
        row = self.db.execute("SELECT version, report FROM contributor_pace").fetchone()
        self.assertEqual((row[0], json.loads(row[1])), ("0.3.8", kept))
        for junk in ("x" * 700, "{", "[1, 2]", '"text"', json.dumps({"requests": "many"}), json.dumps({"requests": -1}),
                     json.dumps({"requests": 1.5}), json.dumps({"requests": True}), "[" * 500):
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
        self.assertIn("No pace numbers received from bob.", self.admin("pace", "bob"))
        self.now += (ingest.KEEP_PACE_DAYS + 1) * 86400                    # old ones go when a new one arrives
        headers["Authorization"] = "Bearer " + self.key
        self.client.get("/v1/policy", headers=headers)
        self.assertEqual(self.count("contributor_pace"), 1)

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

    def test_a_list_is_read_again_sooner_when_it_had_more_that_was_new(self):
        busy, quiet = "b" * 36, "q" * 36
        read_to = int(self.now * 1000) - 5000
        jobs = [self.handed(busy), self.handed(quiet)]
        self.work([self.report(jobs[0], list(range(1000, 1400)), read_to=read_to), self.report(jobs[1], [])],
                  more=False)
        wait_busy, wait_quiet = self.player(busy)[3] - self.now, self.player(quiet)[3] - self.now
        self.assertLess(wait_busy, wait_quiet)
        self.assertGreaterEqual(wait_busy, ingest.MIN_INTERVAL - 1)
        self.assertLessEqual(wait_quiet, ingest.MAX_INTERVAL)
        self.assertEqual(self.player(busy)[1], read_to)
        self.db.execute("DELETE FROM wanted")                             # (pretend those matches were fetched)
        self.db.commit()
        self.now += wait_busy + 5                                         # the busy player comes due first ...
        answer = self.work()
        self.assertEqual(self.shape(self.item(answer, busy)), {"since": read_to, "begin": 0, "pages": 1})
        self.assertIsNone(self.item(answer, quiet))                       # ... from where it was left

    def test_a_list_is_read_further_back_while_that_finds_matches_not_held(self):
        self.post(self.games[:6])
        held, lacking = [game["gameId"] for game in self.games[:6]], [game["gameId"] for game in self.games[6:9]]
        top = int(self.now * 1000) - 1000
        job = self.handed(self.PLAYER)
        onward = self.item(self.work([self.report(job, lacking, read_to=top, following=20)]), self.PLAYER)
        self.assertEqual(self.shape(onward), {"since": self.season, "begin": 20, "pages": 3})
        self.assertEqual(self.player(self.PLAYER)[1], 0)                  # not marked as read while still going
        self.work([self.report(onward, held, read_to=top - 9000, following=80)], more=False)
        self.assertEqual(self.player(self.PLAYER)[1], top)                # nothing new there: read up to the newest
        self.assertEqual(self.player(self.PLAYER)[5:7], (80, self.season))   # the older part is set aside, not dropped

    def test_older_parts_are_read_only_when_nothing_fresher_is_waiting(self):
        self.post(self.games)                                             # 20 players, none read yet
        top = int(self.now * 1000) - 1000
        self.work([self.report(self.handed(self.PLAYER), [], read_to=top, following=20)], more=False)
        for _ in range(10):                                               # the unread players come first ...
            answer = self.work()
            older = self.item(answer, self.PLAYER)
            if older:
                break
            self.assertEqual(len(answer["players"]), ingest.PLAYERS_PER_ROUND)
        waiting = self.db.execute("SELECT COUNT(*) FROM players WHERE due_at <= ? AND leased_until < ?",
                                  (self.now, self.now)).fetchone()[0]
        self.assertEqual(waiting, 0)                                      # ... until none is left waiting
        self.assertEqual(self.shape(older), {"since": self.season, "begin": 20, "pages": 3})
        self.work([self.report(older, [], following=80, read_to=5)], more=False)
        self.assertEqual(self.player(self.PLAYER)[5], 80)                 # it got further ...
        self.assertEqual(self.player(self.PLAYER)[1], top)                # ... without disturbing the top of the list
        older = self.item(self.work(), self.PLAYER)
        self.assertEqual(older["begin"], 80)
        self.work([self.report(older, [], following=0, read_to=5)], more=False)
        self.assertIsNone(self.player(self.PLAYER)[5])                    # reached the season's start: done

    def test_an_interrupted_read_loses_nothing(self):
        top = int(self.now * 1000) - 1000
        job = self.handed(self.PLAYER)
        self.work([self.report(job, [1, 2, 3], read_to=top, following=20)])    # ... and is never heard of again
        bob = self.add_key("bob")
        self.assertIsNone(self.item(self.work(key=bob), self.PLAYER))
        self.now += ingest.LEASE_SECONDS + 1
        again = self.item(self.work(key=bob), self.PLAYER)
        self.assertEqual(self.shape(again), {"since": self.season, "begin": 0, "pages": 1})   # read again from the top
        self.work([self.report(again, [1, 2, 3], read_to=top, following=20)], key=bob, more=False)
        self.assertEqual(self.count("wanted"), 3)

    def test_a_late_report_cannot_undo_newer_progress(self):
        # The review's sequences: a report that arrives after its read was handed to someone else.
        older_floor = self.season
        job = self.handed(self.PLAYER)
        self.work([self.report(job, [], read_to=10, following=20)], more=False)     # older part set aside at 20
        self.assertEqual(self.player(self.PLAYER)[5], 20)
        self.db.execute("UPDATE players SET due_at = 0")
        self.db.commit()
        alice = self.item(self.work(), self.PLAYER)                       # alice reads the top again ...
        onward = self.item(self.work([self.report(alice, [9001], read_to=50, following=20)]), self.PLAYER)
        self.now += ingest.LEASE_SECONDS + 1                              # ... and goes into a game
        bob = self.add_key("bob")
        redo = self.handed(self.PLAYER, key=bob)
        self.work([self.report(redo, [9001], read_to=50, following=0)], key=bob, more=False)
        state = self.player(self.PLAYER)
        self.work([self.report(onward, [], following=0, read_to=5)], more=False)   # alice's report, much later
        self.assertEqual(self.player(self.PLAYER), state)                 # changes nothing
        self.assertEqual(self.player(self.PLAYER)[5:7], (20, older_floor))   # the older part is still set aside
        failed = self.handed(self.PLAYER)                                 # a failed report from an old read, too
        self.now += ingest.LEASE_SECONDS + 1
        self.handed(self.PLAYER, key=bob)
        before = self.player(self.PLAYER)
        self.work([self.report(failed, [], complete=False)], more=False)
        self.assertEqual(self.player(self.PLAYER), before)

    def test_one_report_twice_in_a_request_counts_once(self):
        job = self.handed(self.PLAYER)
        answer = self.work([self.report(job, [7], read_to=50, following=20)] * 2)
        self.assertEqual(len([item for item in answer["players"] if item["puuid"] == self.PLAYER]), 1)

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

    def test_a_failing_older_part_backs_off_and_does_not_crowd_out_others(self):
        self.post(self.games)
        everyone = [row[0] for row in self.db.execute("SELECT puuid FROM players ORDER BY games DESC")]
        for puuid in everyone:                                            # every list read, older parts set aside
            self.work([self.report(self.handed(puuid), [], read_to=10, following=20)], more=False)
        failing, others = everyone[:5], everyone[5:]
        handed = {puuid: 0 for puuid in everyone}
        for _ in range(200):                                              # 200 requests over about 33 hours
            self.now += 600
            answer = self.work()
            reports = []
            for item in answer["players"]:
                handed[item["puuid"]] += 1
                bad = item["puuid"] in failing
                reports.append(self.report(item, [], complete=not bad, following=0 if not bad else 40))
            self.work(reports, more=False)
        self.assertTrue(all(handed[puuid] <= 12 for puuid in failing), handed)    # backed off: not 200 times
        self.assertTrue(all(handed[puuid] >= 1 for puuid in others), handed)      # and everyone else had a turn
        self.assertTrue(all(self.player(puuid)[5] is None for puuid in others))

    def test_a_new_players_bonus_does_not_undo_a_failures_wait(self):
        job = self.handed(self.PLAYER)
        self.work([self.report(job, [], complete=False)], more=False)
        due = self.player(self.PLAYER)[3]
        game = copy.deepcopy(self.games[0])
        game["participantIdentities"][0]["player"]["puuid"] = self.PLAYER
        self.post([game])
        self.assertEqual(self.player(self.PLAYER)[3], due)

    def test_moving_the_season_earlier_reads_the_gap(self):
        self.post(self.games)
        self.work([self.report(self.handed(self.PLAYER), [], read_to=int(self.now * 1000))], more=False)
        earlier = utc_date(self.now - 60 * 86400)
        self.assertIn("1 match lists will be read again", self.admin("policy", "--season-start", earlier))
        self.assertEqual(self.player(self.PLAYER)[5:7], (0, ingest.date_ms(earlier)))
        for _ in range(5):                                                # once the unread players are done ...
            older = self.item(self.work(), self.PLAYER)
            if older:
                break
        self.assertEqual(self.shape(older), {"since": ingest.date_ms(earlier), "begin": 0, "pages": 3})

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
                         {"enabled": True, "delay": 2.0, "fast": True, "fast_delay": 0.4, "fast_flight": 4,
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
        for fake, minutes in ((self.fake, 0.5), (other, 2), (self.fake, 2), (other, 2), (self.fake, 60), (other, 60)):
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
        self.fake = FakeClient(players=150, arena=800, clock=self.clock)    # enough to keep it reading for a while
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


class SeveralAtOnce(WithServer):
    """Fast mode sends the next request before the last one is answered. Here the fake client's answers take
    pretend time (a list 1.5 s, a match 0.05 s, about what Chinese clients were measured to take), so requests
    can be on their way together."""

    def setUp(self):
        super().setUp()
        self.fake.close()
        self.fake = FakeClient(players=400, arena=1600, clock=self.clock)   # enough to keep it reading for a while
        self.fake.write_lockfile(self.league_dir)
        self.fake.delay = lambda path: 0.05 if "/games/" in path else 1.5

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

    def test_the_normal_pace_still_sends_one_request_at_a_time(self):
        # Every volunteer was promised one gentle request at a time; a slip here would break that for all of them.
        made = self.run_collector(minutes=20, server=self.server())
        self.assertEqual((self.flights.most, self.flights.together, made.flight_limit), (1, 0, 1))
        starts = self.starts()[1:]              # (but for the first look at my own list, which takes no pretend time)
        self.assertGreater(len(starts), 150)
        for (when, path), (after, _) in zip(starts, starts[1:]):       # the gap still runs from the answer
            self.assertGreaterEqual(after - when, self.fake.delay(path) + 2.0 * 0.8 - 0.001)
        self.assertTrue(self.server_ids())

    def test_fast_mode_overlaps_requests_within_its_ceiling_and_its_pace(self):
        # Fast mode's two hard limits protect the volunteer's account: never more at once, never closer together.
        start = self.clock.now()
        early = []
        self.clock.at(start + 20, lambda: early.append(self.flights.most))
        made = self.run_collector(minutes=15, server=self.server(), fast=True)
        self.assertEqual(early, [1])                                     # it starts with one at a time
        self.assertEqual(self.flights.most, collector.FAST_MAX_FLIGHT)   # and works up to the ceiling, not beyond
        self.assertEqual(made.flight, collector.FAST_MAX_FLIGHT)
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
            free[0] = begins + (0.05 if "/games/" in path else 1.5)
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
        self.fake.fail = lambda path: failing.pop("status", None) if "/matches?" in path else None
        self.clock.at(start + 400, lambda: failing.update(status=500))
        self.clock.at(start + 800, lambda: failing.update(status=429))
        self.clock.at(start + 830, lambda: setattr(self.flights, "most", 0))
        with self.assertLogs(collector.log, "INFO") as logs:
            made = self.run_collector(minutes=30, server=self.server(), fast=True)
        down = [line for line in logs.output if "a request failed: down to" in line]
        self.assertEqual(len(down), 1)
        self.assertIn("no more for 10m", down[0])
        self.assertEqual(self.output.getvalue().count("fast mode rests for an hour"), 1)
        self.assertEqual((self.flights.most, made.flight, made.flight_limit), (1, 1, 1))   # one at a time since the 429
        resting = self.starts(start + 830)
        for (when, path), (after, _) in zip(resting, resting[1:]):       # at the normal pace
            self.assertGreaterEqual(after - when, self.fake.delay(path) + 2.0 * 0.8 - 0.001)

    def test_arenasweats_can_allow_fewer_at_once_but_never_more(self):
        # ArenaSweats' setting is the brake for every collector at once; it must work, and must not work as a throttle up.
        self.admin("policy", "--fast-flight", "2")
        made = self.run_collector(minutes=10, server=self.server(), fast=True)
        self.assertEqual((self.flights.most, made.flight_limit), (2, 2))
        self.admin("policy", "--fast-flight", "50")
        made = self.run_collector(minutes=15, server=self.server(), fast=True, hours_later=1)
        self.assertEqual((self.flights.most, made.flight_limit), (collector.FAST_MAX_FLIGHT, collector.FAST_MAX_FLIGHT))
        made.apply_policy({"fast": False})
        self.assertEqual(made.flight_limit, 1)
        made.apply_policy({"fast": True})                                # switched back on: one at a time again at first
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
            self.assertEqual(sorted(report), sorted(["seconds", "requests", "failed", "refused", "top", "page", "match",
                                                     "calls", "paused", "rested", "idle", "expired", "fast", "flight",
                                                     "most"]))
            for name, value in report.items():
                for number in value if name in ("top", "page", "match", "calls") else [value]:
                    self.assertIs(type(number), int, name)
            self.assertTrue(295 <= report["seconds"] <= 310)
            self.assertGreater(report["requests"], 400)
            self.assertEqual(report["top"][1:], [1500, 1500])            # how long a list's first page took, in ms
            self.assertEqual(report["match"][1:], [50, 50])
        self.assertTrue(1 < reports[1]["flight"] <= collector.FAST_MAX_FLIGHT)
        self.assertEqual(reports[1]["most"], reports[1]["flight"])

    def test_real_requests_overlap_in_fast_mode_only(self):
        # The other tests here use pretend time; this one proves the real threads keep the same two promises.
        ingest.clock = time.time
        self.addCleanup(setattr, collector, "FLIGHT_CLEAN", collector.FLIGHT_CLEAN)
        collector.FLIGHT_CLEAN = 3
        fake = FakeClient(players=60, arena=200)                         # no pretend clock: its answers really take time
        self.addCleanup(fake.close)
        fake.delay = lambda path: 0.02 if "/games/" in path else 0.5
        league_dir = fake.write_lockfile(os.path.join(self.tmp.name, "real", "League"))
        known = [fake.games[game_id] for game_id in sorted(fake.arena_ids())[:20]]      # so there are lists to hand out
        self.assertEqual(ingest.app.test_client().post("/v1/games", json={"games": known}, headers={
            "Authorization": "Bearer " + self.key}).get_json()["accepted"], 20)
        for fast, seconds in ((True, 7), (False, 4)):                   # (fast first: it finds lists waiting, not matches)
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
