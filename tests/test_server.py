"""Tests for server/ingest.py, and for the collector working with it.  Run:  python -m unittest discover tests"""

import argparse
import contextlib
import copy
import gzip
import io
import json
import os
import sys
import tempfile
import threading
import time
import unittest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
sys.path.insert(0, os.path.join(ROOT, "server"))
sys.path.insert(0, os.path.join(ROOT, "tests"))

import collector  # noqa: E402
import ingest  # noqa: E402
from fake_lcu import FakeClient  # noqa: E402
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
                         {"enabled": True, "delay": 2.0, "message": "", "season_start": ingest.date_ms("2026-10-06")})
        self.admin("policy", "--off", "--delay", "5", "--message", "back soon", "--season-start", "2027-01-15")
        self.assertEqual(self.client.get("/v1/policy", headers=headers).get_json(),
                         {"enabled": False, "delay": 5.0, "message": "back soon",
                          "season_start": ingest.date_ms("2027-01-15")})
        self.assertEqual(self.post(self.games).get_json()["policy"]["enabled"], False)
        self.assertIn("Not understood", self.admin("policy", "--season-start", "next week"))
        self.assertIn("season starts 2027-01-15", self.admin("stats"))


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
        (200, {}, b'{"rejected": [5, null, {"gameId": [1]}, {"gameId": {}}], "policy": {"delay": 0.001}}'),
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


if __name__ == "__main__":
    unittest.main()
