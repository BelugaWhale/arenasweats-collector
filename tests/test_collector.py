"""Tests for collector.py against the fake League client.  Run:  python -m unittest discover tests"""

import argparse
import builtins
import contextlib
import io
import json
import os
import sys
import tempfile
import time
import unittest
import unittest.mock
import zipfile

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
sys.path.insert(0, os.path.join(ROOT, "tests"))

import collector  # noqa: E402
from fake_lcu import Clock, FakeClient  # noqa: E402


def history(requests):
    """Only the match-history requests (the ones that reach Riot's servers)."""
    return [r for r in requests if r[1].startswith("/lol-match-history/")]


class CollectorCase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.clock = Clock()
        self.real_start = time.time() - self.clock.now()     # pretend time and calendar time move together
        self.fake = FakeClient(clock=self.clock)
        self.addCleanup(self.fake.close)
        self.league_dir = self.fake.write_lockfile(os.path.join(self.tmp.name, "League"))
        self.data_dir = os.path.join(self.tmp.name, "data")
        os.makedirs(self.data_dir)
        # This computer's own programs and League install are not part of the tests: a League client
        # running here must never be found and read from.
        for name, value in (("process_table", lambda: {}), ("running_programs", lambda: []),
                            ("installer_dirs", lambda: []), ("installed_dirs", lambda: [])):
            original = getattr(collector, name)
            setattr(collector, name, value)
            self.addCleanup(setattr, collector, name, original)

    def collector(self, minutes=60, server=None, hours_later=0, **options):
        opts = argparse.Namespace(league_dir=self.league_dir, delay=2.0, minutes=minutes,
                                  since=int((self.real_start - 30 * 86400) * 1000))     # "the season" began 30 days ago
        vars(opts).update(options)
        if hours_later:
            self.fake.forget()      # hours later the client (restarted since) no longer has the match lists it saved
        made = collector.Collector(opts, self.data_dir, server, sleep=self.clock.sleep, now=self.clock.now,
                                   wall=lambda: self.real_start + self.clock.now() + hours_later * 3600)
        self.addCleanup(made.close)        # Windows cannot delete the temporary folder while it is open
        return made

    def run_collector(self, *args, **kwargs):
        made = self.collector(*args, **kwargs)
        self.output = io.StringIO()
        with contextlib.redirect_stdout(self.output):
            made.run()
        return made

    def stored_ids(self, made):
        return {row[0] for row in made.store.db.execute("SELECT game_id FROM games WHERE state != 2")}


class ReadOnly(CollectorCase):
    def test_refuses_anything_not_on_the_allowed_list(self):
        lcu = collector.read_lockfile(os.path.join(self.league_dir, "lockfile"))
        for path in ("/lol-login/v1/session", "/lol-lobby/v2/lobby", "/lol-match-history/v1/games/1/../../x",
                     "/lol-match-history/v1/products/lol/../matches?begIndex=0&endIndex=19",
                     "/lol-gameflow/v1/gameflow-phase?x=1", "/process-control/v1/process/quit"):
            with self.assertRaises(ValueError):
                lcu.get(path)
        self.assertEqual(self.fake.requests, [])

    def test_every_request_is_an_allowed_get(self):
        self.run_collector()
        self.assertGreater(len(self.fake.requests), 50)
        for method, path, _ in self.fake.requests:
            self.assertEqual(method, "GET")
            self.assertTrue(any(p.fullmatch(path) for p in collector.LCU_ALLOWED), path)

    def test_token_is_not_written_to_the_data_folder(self):
        self.run_collector()
        for name in os.listdir(self.data_dir):
            with open(os.path.join(self.data_dir, name), "rb") as handle:
                self.assertNotIn(self.fake.token.encode(), handle.read(), name)


class Crawl(CollectorCase):
    def test_collects_every_arena_match_and_nothing_else(self):
        made = self.run_collector()
        self.assertEqual(self.stored_ids(made), self.fake.arena_ids())
        for game in made.store.stored_games():
            self.assertEqual(game["gameMode"], "CHERRY")
            self.assertEqual(len(game["participants"]), 16)      # the full record, not one player's row

    def test_each_match_is_fetched_once(self):
        self.run_collector()
        details = [path for _, path, _ in self.fake.requests if "/v1/games/" in path]
        self.assertEqual(len(details), len(set(details)))

    def test_second_run_fetches_no_match_again(self):
        self.run_collector()
        before = len(self.fake.requests)
        self.run_collector(minutes=10)
        again = history(self.fake.requests[before:])
        self.assertEqual([path for _, path, _ in again if "/v1/games/" in path], [])
        self.assertLessEqual(len(again), 2)      # only my own match list; everyone else was read recently

    def test_later_run_reads_only_what_is_new(self):
        self.run_collector()
        new_game = self.fake.add_game("CHERRY", 1700, self.fake.players[:16], age_seconds=-3600)
        before = len(self.fake.requests)
        made = self.run_collector(hours_later=7)
        again = history(self.fake.requests[before:])
        self.assertEqual([path for _, path, _ in again if "/v1/games/" in path],
                         ["/lol-match-history/v1/games/%d" % new_game])
        self.assertTrue(all("begIndex=0&" in path for _, path, _ in again if "/matches?" in path))
        self.assertIn(new_game, self.stored_ids(made))

    def test_the_rest_of_a_list_is_read_later_not_dropped(self):
        """The review's case: a list whose newest page holds nothing new still gets its older part read."""
        fake = self.fake
        fake.history.clear()
        fake.games.clear()
        me, other = fake.players[0], fake.players[1]
        strangers = ["%08d-0000-4000-8000-%012d" % (n, n) for n in range(100, 400)]
        older = [fake.add_game("CHERRY", 1700, [other] + strangers[i * 15:(i + 1) * 15], age_seconds=86400 * 5 + i * 3600)
                 for i in range(20)]
        recent = [fake.add_game("CHERRY", 1700, [me, other] + fake.players[2:16], age_seconds=3600 + i * 600)
                  for i in range(20)]
        made = self.run_collector(minutes=6 * 60)
        self.assertTrue(set(recent) | set(older) <= self.stored_ids(made))

    def test_nothing_from_before_the_season_is_fetched(self):
        old = self.fake.add_game("CHERRY", 1700, self.fake.players[:16], age_seconds=40 * 86400)
        made = self.run_collector()
        self.assertNotIn(old, self.stored_ids(made))
        self.assertNotIn("/lol-match-history/v1/games/%d" % old, [path for _, path, _ in self.fake.requests])

    def test_a_long_history_is_read_in_one_request(self):
        heavy = self.fake.players[0]
        ids = [self.fake.add_game("CHERRY", 1700, [heavy] + self.fake.players[1:16], age_seconds=3600 * n)
               for n in range(1, 60)]                                     # 59 more matches: over 120 in the list
        made = self.run_collector(minutes=180)
        self.assertTrue(set(ids) <= self.stored_ids(made))
        self.assertGreater(len(self.fake.history[heavy]), collector.OWN_BLOCK)
        # The client gives as many matches as its first request for a player asks for, and only ever those: so the
        # list is asked for once, as one block, never page by page.
        self.assertEqual([path for _, path, _ in self.fake.requests if heavy in path],
                         ["/lol-match-history/v1/products/lol/%s/matches?begIndex=0&endIndex=%d"
                          % (heavy, collector.OWN_BLOCK - 1)])

    def test_wrong_record_is_not_stored(self):
        target, other = min(self.fake.arena_ids()), max(self.fake.arena_ids())
        self.fake.wrong_record[target] = self.fake.games[other]     # the client answers with a different match
        made = self.run_collector(minutes=600)
        self.assertEqual(self.stored_ids(made), self.fake.arena_ids() - {target})
        self.assertEqual(made.store.one("SELECT state FROM games WHERE game_id = ?", target), collector.UNAVAILABLE)


class Gentle(CollectorCase):
    def gaps(self, requests):
        times = [when for _, _, when in history(requests)]
        return [b - a for a, b in zip(times, times[1:])]

    def test_keeps_the_pace(self):
        self.run_collector()
        gaps = self.gaps(self.fake.requests)
        self.assertGreater(len(gaps), 50)
        self.assertGreaterEqual(min(gaps), 2.0 * 0.8 - 0.001)

    def test_the_client_state_is_not_asked_before_every_request(self):
        self.run_collector()
        phase = [r for r in self.fake.requests if r[1] == "/lol-gameflow/v1/gameflow-phase"]
        self.assertGreater(len(history(self.fake.requests)), 50)
        self.assertLessEqual(len(phase), len(history(self.fake.requests)) / 2)      # was one per request

    def test_pace_has_a_hard_floor(self):
        self.run_collector(delay=0.0)
        self.assertGreaterEqual(min(self.gaps(self.fake.requests)), 1.0 - 0.001)   # never above one a second

    def test_no_match_history_requests_while_playing(self):
        start = self.clock.now()
        self.fake.phase = "InProgress"                                    # already in a game at start
        self.clock.at(start + 600, lambda: setattr(self.fake, "phase", "EndOfGame"))
        self.clock.at(start + 660, lambda: setattr(self.fake, "phase", "None"))
        self.clock.at(start + 760, lambda: setattr(self.fake, "phase", "ChampSelect"))
        self.clock.at(start + 1000, lambda: setattr(self.fake, "phase", "Lobby"))
        self.run_collector(minutes=30)
        times = [when - start for _, _, when in history(self.fake.requests)]
        self.assertTrue(times)
        busy = [(0, 660 + collector.RESUME_GRACE - 5), (760 + 5, 1000 + collector.RESUME_GRACE - 5)]
        for when in times:
            for begin, end in busy:
                self.assertFalse(begin <= when < end, "request at %.0fs while the player was busy" % when)
        self.assertTrue(any(660 < when < 760 for when in times))
        self.assertTrue(any(when > 1000 for when in times))
        self.assertIn("Paused while you play", self.output.getvalue())

    def test_backs_off_when_requests_fail(self):
        self.run_collector()                                              # learn the players first
        self.fake.fail = lambda path: 500 if "match-history" in path else None
        before = len(self.fake.requests)
        made = self.run_collector(minutes=120, hours_later=7)
        gaps = self.gaps(self.fake.requests[before:])
        self.assertLessEqual(len(gaps), 25)                              # two hours, a couple of dozen tries
        self.assertGreaterEqual(max(gaps), collector.MAX_BACKOFF)
        self.assertEqual(gaps[:6], sorted(gaps[:6]))                     # waits grow
        self.assertEqual(made.stats["failed"], made.stats["requests"])

    def test_recovers_after_failures(self):
        failing = {"on": True}
        self.fake.fail = lambda path: 503 if failing["on"] and "match-history" in path else None
        self.clock.at(self.clock.now() + 300, lambda: failing.update(on=False))
        made = self.run_collector(minutes=90)
        self.assertEqual(self.stored_ids(made), self.fake.arena_ids())

    def test_survives_a_client_restart(self):
        def restart():
            self.fake.close()
            self.fake2 = FakeClient(clock=self.clock)
            self.fake2.write_lockfile(self.league_dir)
        self.clock.at(self.clock.now() + 60, restart)
        made = self.run_collector()
        self.addCleanup(lambda: self.fake2.close())
        self.assertEqual(self.stored_ids(made), self.fake2.arena_ids())
        self.assertIn("The League client closed", self.output.getvalue())

    def test_waits_for_the_client_to_open(self):
        os.remove(os.path.join(self.league_dir, "lockfile"))
        self.clock.at(self.clock.now() + 120, lambda: self.fake.write_lockfile(self.league_dir))
        made = self.run_collector()
        self.assertIn("Waiting for the League client", self.output.getvalue())
        self.assertEqual(self.stored_ids(made), self.fake.arena_ids())


class FastMode(CollectorCase):
    """Fast mode (beta, only when the user turns it on): a fifth of the gap, never below its own floor, with every
    other limit kept. These are test runs, which read one request at a time in fast mode too; fast mode with
    ArenaSweats, where several requests can be on their way at once, is in test_server.py (SeveralAtOnce)."""

    def setUp(self):
        super().setUp()
        self.fake.close()
        self.fake = FakeClient(players=100, arena=400, clock=self.clock)    # enough to keep it reading for a while
        self.fake.write_lockfile(self.league_dir)

    def gaps(self, requests):
        times = [when for _, _, when in history(requests)]
        return [b - a for a, b in zip(times, times[1:])]

    def test_is_off_unless_turned_on(self):
        made = self.run_collector(minutes=10)
        self.assertFalse(made.fast_on)
        self.assertGreaterEqual(min(self.gaps(self.fake.requests)), 2.0 * 0.8 - 0.001)

    def test_a_fifth_of_the_gap_but_never_below_its_own_floor(self):
        made = self.run_collector(minutes=10, fast=True)
        gaps = self.gaps(self.fake.requests)
        self.assertGreater(len(gaps), 400)
        self.assertEqual(collector.DEFAULT_DELAY / collector.FAST_FACTOR, collector.FAST_MIN_DELAY)
        self.assertGreaterEqual(min(gaps), collector.FAST_MIN_DELAY - 0.001)     # never above 2.5 a second
        self.assertLess(sorted(gaps)[len(gaps) // 2], 0.5)
        self.assertEqual(made.delay, collector.FAST_MIN_DELAY)
        self.assertIn("one request every 0.4 seconds", self.output.getvalue())
        self.fake.requests.clear()
        self.data_dir = os.path.join(self.tmp.name, "data-again")            # start again from nothing
        os.makedirs(self.data_dir)
        self.run_collector(minutes=10, fast=True, delay=0.0)                  # asking for less changes nothing
        self.assertGreaterEqual(min(self.gaps(self.fake.requests)), collector.FAST_MIN_DELAY - 0.001)

    def test_reads_more_in_the_same_time(self):
        self.fake.close()
        self.fake = FakeClient(players=400, arena=2000, clock=self.clock)    # more than fast mode reads in 5 min
        self.addCleanup(self.fake.close)
        self.fake.write_lockfile(self.league_dir)
        normal = self.run_collector(minutes=5)
        requests = len(history(self.fake.requests))
        self.fake.requests.clear()
        self.data_dir = os.path.join(self.tmp.name, "data-fast")             # start again from nothing
        os.makedirs(self.data_dir)
        fast = self.run_collector(minutes=5, fast=True)
        self.assertGreater(len(history(self.fake.requests)), requests * 4)    # answers take no time here
        self.assertGreater(len(self.stored_ids(fast)), len(self.stored_ids(normal)) * 3)

    def test_normal_mode_is_unchanged(self):
        made = self.run_collector(minutes=10)
        gaps = self.gaps(self.fake.requests)
        self.assertEqual((made.floor, made.delay), (collector.MIN_DELAY, 2.0))
        self.assertGreaterEqual(min(gaps), 2.0 * 0.8 - 0.001)
        self.assertTrue(1.9 < sorted(gaps)[len(gaps) // 2] < 2.1)
        made.apply_policy({"fast_delay": 0.0})                                # fast mode's settings change nothing
        self.assertEqual((made.floor, made.delay), (collector.MIN_DELAY, 2.0))
        self.assertIn("one request every 2.0 seconds", self.output.getvalue())

    def test_a_slower_pace_the_user_chose_is_divided_not_ignored(self):
        self.run_collector(minutes=10, fast=True, delay=6.0)
        self.assertGreaterEqual(min(self.gaps(self.fake.requests)), 6.0 / collector.FAST_FACTOR * 0.8 - 0.001)

    def test_no_match_history_requests_while_playing(self):
        start = self.clock.now()
        self.fake.phase = "InProgress"                                    # already in a game at start
        self.clock.at(start + 600, lambda: setattr(self.fake, "phase", "None"))
        self.clock.at(start + 700, lambda: setattr(self.fake, "phase", "ChampSelect"))
        self.clock.at(start + 1000, lambda: setattr(self.fake, "phase", "Lobby"))
        self.run_collector(minutes=30, fast=True)
        times = [when - start for _, _, when in history(self.fake.requests)]
        for when in times:
            for begin, end in ((0, 600 + collector.RESUME_GRACE - 5), (700 + 5, 1000 + collector.RESUME_GRACE - 5)):
                self.assertFalse(begin <= when < end, "request at %.0fs while the player was busy" % when)
        self.assertTrue(any(600 < when < 700 for when in times))
        self.assertTrue(any(when > 1000 for when in times))
        self.assertIn("Paused while you play", self.output.getvalue())

    def test_backs_off_when_requests_fail(self):
        self.run_collector(minutes=10, fast=True)                         # learn the players first
        self.fake.fail = lambda path: 500 if "match-history" in path else None
        before = len(self.fake.requests)
        made = self.run_collector(minutes=120, hours_later=7, fast=True)
        gaps = self.gaps(self.fake.requests[before:])
        self.assertLessEqual(len(gaps), 25)                              # two hours, a couple of dozen tries
        self.assertGreaterEqual(max(gaps), collector.MAX_BACKOFF)
        self.assertEqual(gaps[:6], sorted(gaps[:6]))                     # waits grow
        self.assertGreaterEqual(min(gaps), 2.0 * 0.8 - 0.001)            # and fast mode rests after 3 failures
        self.assertFalse(made.fast_on)

    def test_rests_at_the_normal_pace_after_the_client_pushes_back(self):
        self.addCleanup(setattr, collector, "FAST_REST", collector.FAST_REST)
        collector.FAST_REST = 600
        start = self.clock.now()
        self.fake.fail = lambda path: 429 if "/v1/games/" in path and self.clock.now() < start + 60 else None
        self.run_collector(minutes=30, fast=True)
        times = [when - start for _, _, when in history(self.fake.requests)]
        resting = [b - a for a, b in zip(times, times[1:]) if 60 < a < 590]
        later = [b - a for a, b in zip(times, times[1:]) if a > 700]
        self.assertTrue(resting and later)
        self.assertGreaterEqual(min(resting), 2.0 * 0.8 - 0.001)          # the normal pace while resting
        self.assertLess(sorted(later)[len(later) // 2], 0.5)              # then fast again
        self.assertEqual(self.output.getvalue().count("fast mode rests for an hour"), 1)

    def test_the_question_defaults_to_no(self):
        for answer, expected in (("", False), ("n", False), ("no", False), ("whatever", False),
                                 ("y", True), (" Yes ", True), ("是", True)):
            asked = []
            with contextlib.redirect_stdout(io.StringIO()) as output:
                self.assertEqual(collector.ask_fast_mode(lambda prompt: asked.append(prompt) or answer), expected)
            self.assertEqual(asked, ["Activate fast mode? 开启快速模式？ [y/N]  No answer = normal mode / 不回答即普通模式: "])
        self.assertEqual(collector.FAST_SPEEDUP, "3")
        self.assertEqual(output.getvalue(),
                         "Fast mode collects about 3 times faster. BETA: Please only use with unimportant accounts "
                         "for now.\n快速模式的采集速度约为正常速度的 3 倍。测试版：目前请只在不重要的账号上使用。\n")
        for words in ("not sure", "throwaway", "小号", "不确定"):
            self.assertNotIn(words, output.getvalue())
            self.assertNotIn(words, collector.__doc__)

        def closed(prompt):
            raise EOFError
        with contextlib.redirect_stdout(io.StringIO()):
            self.assertFalse(collector.ask_fast_mode(closed))
        with contextlib.redirect_stdout(io.StringIO()) as output:     # nobody at the PC: normal mode
            self.assertFalse(collector.ask_fast_mode(lambda prompt: None))
        self.assertIn("No answer: starting in normal mode. 无人回答：以普通模式开始。", output.getvalue())

    @unittest.skipIf(os.name == "nt", "the Windows console is read key by key; this checks the pipe version")
    def test_an_unanswered_question_gives_up_after_its_time(self):
        reader, writer = os.pipe()
        self.addCleanup(os.close, writer)
        with os.fdopen(reader, "r") as stream, unittest.mock.patch.object(sys, "stdin", stream), \
                contextlib.redirect_stdout(io.StringIO()) as output:
            started = time.monotonic()
            self.assertIsNone(collector.timed_input("Question? ", seconds=0.2))
            self.assertLess(time.monotonic() - started, 5)
            os.write(writer, "是\n".encode("utf-8"))
            self.assertEqual(collector.timed_input("Question? ", seconds=5), "是")
        self.assertEqual(output.getvalue(), "Question? \nQuestion? ")

    def test_help_and_readmes_give_the_same_warning(self):
        with contextlib.redirect_stdout(io.StringIO()) as output, self.assertRaises(SystemExit):
            collector.main(["--help"])
        self.assertIn("BETA: collect about 3 times faster; please only use with unimportant accounts for now",
                      " ".join(output.getvalue().split()))
        texts = {}
        for path in ("README.md", os.path.join("server", "README.md")):
            with open(os.path.join(ROOT, path), encoding="utf-8") as handle:
                texts[path] = " ".join(handle.read().split())
            self.assertNotIn("throwaway", texts[path], path)
            self.assertNotIn("not sure", texts[path], path)
        self.assertIn("Fast mode collects about 3 times faster: about two requests a second, sent without waiting for "
                      "the last one's answer, so up to four can be on their way at once. "
                      "**BETA: Please only use with unimportant accounts for now.**", texts["README.md"])
        self.assertEqual(collector.FAST_MAX_FLIGHT, 4)

    def test_the_flag_turns_it_on_and_says_so_every_start(self):
        for flags, on in ((["--fast"], True), (["--no-fast"], False), ([], False)):
            with contextlib.redirect_stdout(io.StringIO()) as output:
                collector.main(["--test-run", "--minutes", "0.02", "--league-dir", self.league_dir,
                                "--data-dir", self.data_dir] + flags)
            self.assertEqual(collector.FAST_ON in output.getvalue(), on, flags)
            with open(os.path.join(self.data_dir, "collector.log"), encoding="utf-8") as handle:
                last = [line for line in handle if "fast mode: " in line][-1]
            self.assertIn("fast mode: on (experimental)" if on else "fast mode: off", last)


class Failures(CollectorCase):
    """What the independent review found: failures must slow the collector down and never lose matches."""

    def test_dropped_connections_are_backed_off_too(self):
        self.run_collector()                                              # learn the players first
        self.fake.drop = lambda path: "match-history" in path            # the client hangs up on every request
        before = len(self.fake.requests)
        self.run_collector(minutes=120, hours_later=7, delay=45)
        times = [when for _, _, when in history(self.fake.requests[before:])]
        gaps = [b - a for a, b in zip(times, times[1:])]
        self.assertLessEqual(len(times), 25)
        self.assertGreaterEqual(min(gaps), 45 * 0.8)                      # the chosen pace still applies
        self.assertGreaterEqual(max(gaps), collector.MAX_BACKOFF)

    def test_a_long_outage_loses_no_matches(self):
        down = {"on": True}
        self.fake.fail = lambda path: 503 if down["on"] and "/v1/games/" in path else None
        self.clock.at(self.clock.now() + 3 * 3600, lambda: down.update(on=False))
        made = self.run_collector(minutes=30 * 60)
        self.assertEqual(self.stored_ids(made), self.fake.arena_ids())
        self.assertEqual(made.store.count(collector.UNAVAILABLE), 0)

    def test_a_match_that_keeps_failing_is_left_to_rest(self):
        target = min(self.fake.arena_ids())
        self.fake.fail = lambda path: 500 if path.endswith("/games/%d" % target) else None
        made = self.run_collector(minutes=24 * 60)
        tries = [when for _, path, when in self.fake.requests if path.endswith("/games/%d" % target)]
        self.assertLessEqual(len(tries), 15)                              # 3 tries, then rests of 1h, 2h, 4h ...
        self.assertEqual(self.stored_ids(made), self.fake.arena_ids() - {target})
        self.assertEqual(made.store.count(collector.UNAVAILABLE), 0)      # never written off for good

    def test_a_player_whose_list_keeps_failing_is_asked_less_and_less(self):
        self.run_collector()
        victim = self.fake.players[5]
        self.fake.fail = lambda path: 500 if victim in path else None
        before = len(self.fake.requests)
        self.run_collector(minutes=24 * 60, hours_later=7)
        times = [when for _, path, when in self.fake.requests[before:] if victim in path]
        gaps = [b - a for a, b in zip(times, times[1:])]
        self.assertLessEqual(len(times), 14)                              # was 168 a day before the fix
        self.assertEqual(gaps[:5], sorted(gaps[:5]))
        self.assertGreaterEqual(max(gaps), 3600)

    def test_odd_data_from_the_client_does_not_stop_it(self):
        ids = sorted(self.fake.arena_ids())
        odd = [dict(self.fake.games[ids[0]], participantIdentities=5),
               dict(self.fake.games[ids[1]], participants=[{"participantId": 1, "stats": None}]),
               dict(self.fake.games[ids[2]], participantIdentities=[None, 7, {"player": 3}, {"player": {"puuid": 9}}]),
               "text", None, 12, []]
        for game_id, record in zip(ids, odd):
            self.fake.wrong_record[game_id] = record

        def spoil(player, page):
            return page + [{"gameId": 2 ** 70, "gameMode": "CHERRY", "gameCreation": float("inf")},
                           {"gameId": ids[10], "gameMode": "CHERRY", "platformId": "\ud800", "gameCreation": 2 ** 80},
                           {"gameId": "x", "gameMode": ["CHERRY"]}, {"gameMode": "CHERRY"}, "junk", None, 5,
                           {"gameId": True, "gameMode": "CHERRY", "queueId": {"a": 1}}]
        self.fake.list_hook = spoil
        made = self.run_collector(minutes=180)
        self.assertGreaterEqual(len(self.stored_ids(made)), len(ids) - 7)
        for name in ("out.jsonl", "out.jsonl.gz"):                        # odd characters must not break the export
            with contextlib.redirect_stdout(io.StringIO()):
                collector.export(self.data_dir, os.path.join(self.tmp.name, name))

    def test_unexpected_shapes_everywhere_do_not_crash(self):
        self.fake.list_hook = lambda player, page: {"not": "a list"}
        self.run_collector(minutes=30)
        self.fake.list_hook = lambda player, page: [[1, 2], {"gameId": 5}] * 30
        self.run_collector(minutes=30, hours_later=7)


class TestRun(CollectorCase):
    def test_writes_one_file_to_send_back(self):
        made = self.run_collector(minutes=15)
        reports = [name for name in os.listdir(self.data_dir) if name.endswith(".zip")]
        self.assertEqual(len(reports), 1)
        with zipfile.ZipFile(os.path.join(self.data_dir, reports[0])) as archive:
            lines = archive.read("matches.jsonl").decode().splitlines()
        self.assertEqual({json.loads(line)["gameId"] for line in lines}, self.stored_ids(made))
        self.assertGreater(len(lines), 10)
        self.assertIn("Please send this one file back", self.output.getvalue())

    def test_still_writes_the_file_when_no_client_was_found(self):
        os.remove(os.path.join(self.league_dir, "lockfile"))
        self.run_collector(minutes=15)
        reports = [name for name in os.listdir(self.data_dir) if name.endswith(".zip")]
        self.assertEqual(len(reports), 1)
        self.assertIn("Nothing was collected", self.output.getvalue())
        self.assertEqual(history(self.fake.requests), [])

    def test_a_crash_is_in_the_file_that_gets_sent_back(self):
        def explode(self):
            raise RuntimeError("boom for the test")
        original, collector.Collector.own_round = collector.Collector.own_round, explode
        self.addCleanup(setattr, collector.Collector, "own_round", original)
        with contextlib.redirect_stdout(io.StringIO()) as output:
            code = collector.main(["--test-run", "--minutes", "1", "--league-dir", self.league_dir, "--data-dir", self.data_dir])
        self.assertEqual(code, 1)
        self.assertIn("Unexpected error", output.getvalue())
        reports = [name for name in os.listdir(self.data_dir) if name.endswith(".zip")]
        with zipfile.ZipFile(os.path.join(self.data_dir, reports[0])) as archive:
            self.assertIn("boom for the test", archive.read("collector.log").decode())

    def test_program_entry_point(self):
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            code = collector.main(["--test-run", "--minutes", "0.05", "--league-dir", self.league_dir, "--data-dir", self.data_dir])
        self.assertEqual(code, 0)
        self.assertIn("test run", output.getvalue())
        with open(os.path.join(self.data_dir, "collector.log"), encoding="utf-8") as handle:
            text = handle.read()
        self.assertIn("first Arena match this run: platform=OC1", text)
        self.assertIn("subteamPlacement=yes", text)
        self.assertNotIn(self.fake.token, text)
        target = os.path.join(self.tmp.name, "out.jsonl")
        with contextlib.redirect_stdout(io.StringIO()):
            collector.main(["--data-dir", self.data_dir, "--export", target])
        with open(target, encoding="utf-8") as handle:
            self.assertGreaterEqual(len(handle.read().splitlines()), 1)


class Lockfile(unittest.TestCase):
    def test_reads_port_token_and_protocol(self):
        with tempfile.TemporaryDirectory() as folder:
            path = os.path.join(folder, "lockfile")
            with open(path, "w") as handle:
                handle.write("LeagueClient:100:54321:secret:https")
            lcu = collector.read_lockfile(path)
            self.assertEqual(lcu.base, "https://127.0.0.1:54321")
            for bad in ("", "LeagueClient:100", "a:b:notaport:d:https"):
                with open(path, "w") as handle:
                    handle.write(bad)
                with self.assertRaises(ValueError):
                    collector.read_lockfile(path)

    def setUp(self):
        for name in ("running_client_dirs", "installer_dirs", "installed_dirs"):     # this computer's own League, if any
            original = getattr(collector, name)
            setattr(collector, name, lambda: [])
            self.addCleanup(setattr, collector, name, original)

    def test_accepts_the_folder_its_subfolder_or_the_file(self):
        with tempfile.TemporaryDirectory() as folder:
            inner = os.path.join(folder, "LeagueClient")
            os.makedirs(inner)
            path = os.path.join(inner, "lockfile")
            open(path, "w").close()
            for given in (folder, inner, path, '"%s"' % folder):
                self.assertEqual(collector.lockfile_paths(given), ([path], False, False))

    def test_a_folder_inside_the_install_is_enough(self):
        with tempfile.TemporaryDirectory() as folder:
            install = os.path.join(folder, "WeGameApps", "英雄联盟")
            os.makedirs(os.path.join(install, "LeagueClient"))
            os.makedirs(os.path.join(install, "TCLS"))
            path = os.path.join(install, "LeagueClient", "lockfile")
            open(path, "w").close()
            launcher = os.path.join(install, "TCLS", "Client.exe")
            open(launcher, "w").close()
            for given in (install, os.path.join(install, "TCLS"), launcher, os.path.join(install, "Game")):
                self.assertEqual(collector.lockfile_paths(given)[0], [path], given)

    def test_installer_entries_are_read_as_folders(self):
        cases = {'"D:\\WeGameApps\\英雄联盟\\TCLS\\Client.exe",0': "D:\\WeGameApps\\英雄联盟\\TCLS",
                 "D:\\Games\\英雄联盟\\uninst.exe /S": "D:\\Games\\英雄联盟",
                 '"D:\\Program Files\\英雄联盟\\uninst.exe" /S': "D:\\Program Files\\英雄联盟",
                 "D:\\英雄联盟\\": "D:\\英雄联盟",
                 "C:\\Riot Games\\League of Legends\\icon.ico,-101": "C:\\Riot Games\\League of Legends",
                 "": None, "  ": None}
        for value, folder in cases.items():
            self.assertEqual(collector.folder_in(value), folder, value)

    def test_drive_search_finds_installs_by_name_or_by_the_client(self):
        with tempfile.TemporaryDirectory() as root:
            def make(*parts, program=False):
                folder = os.path.join(root, *parts)
                os.makedirs(os.path.join(folder, "LeagueClient") if program else folder, exist_ok=True)
                if program:
                    open(os.path.join(folder, "LeagueClient", "LeagueClient.exe"), "w").close()
                return folder
            wanted = {make("Games", "英雄联盟"),
                      make("Program Files (x86)", "WeGameApps", "英雄联盟"),
                      make("Stuff", "MyLoL", program=True),
                      make("Riot Games", "League of Legends")}
            make("Users", "me", "英雄联盟")                  # people's own folders are not looked in
            make("Other", "Deep", "英雄联盟")                # too deep, and not in a games folder
            self.assertEqual(set(collector.scan_drives(roots=[root])), wanted)

    def test_reads_the_port_and_password_from_the_command_line(self):
        line = ('"D:\\WeGameApps\\英雄联盟\\LeagueClient\\LeagueClientUx.exe" "--riotclient-auth-token=xyz" '
                '"--riotclient-app-port=50000" "--app-port=50123" "--remoting-auth-token=Zx_9-q" "--region=TENCENT"')
        self.assertEqual(collector.login_from_command_line(line), (50123, "Zx_9-q"))
        for bad in (None, "", "--app-port=50123", "--remoting-auth-token=abc", "--app-port=99999 --remoting-auth-token=a"):
            self.assertIsNone(collector.login_from_command_line(bad), bad)

    def test_console_and_sleep_settings_never_fail(self):
        for on in (False, True):
            collector.set_quick_edit(on)      # no console here: must quietly do nothing
        collector.keep_awake()

    @unittest.skipUnless(os.name == "nt", "asks Windows about a running program")
    def test_windows_gives_a_programs_path_and_command_line(self):
        import subprocess
        child = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(60)",
                                  "--app-port=50123", "--remoting-auth-token=abc_DEF-1"])
        self.addCleanup(child.wait)
        self.addCleanup(child.kill)
        self.assertIn(child.pid, [pid for pid, _ in collector.running_programs()])
        path, line, error = collector.query_program(child.pid, command_line=True)
        self.assertEqual(os.path.normcase(path), os.path.normcase(sys.executable))
        self.assertEqual(collector.login_from_command_line(line), (50123, "abc_DEF-1"))
        self.assertEqual(error, 0)
        self.assertIn("C:\\", collector.fixed_drives())


class HiddenClient(CollectorCase):
    """The client runs as administrator, so Windows will not say where it is."""

    def setUp(self):
        super().setUp()
        os.remove(os.path.join(self.league_dir, "lockfile"))
        self.logins = []
        for name, value in (("running_client_dirs", lambda: [None]), ("installer_dirs", lambda: []),
                            ("installed_dirs", lambda: []), ("scan_drives", lambda: []),
                            ("command_line_logins", lambda: self.logins)):
            original = getattr(collector, name)
            setattr(collector, name, value)
            self.addCleanup(setattr, collector, name, original)

    def test_connects_through_the_command_line_when_no_lockfile_is_found(self):
        self.logins = [(self.fake.port, self.fake.token, "http")]
        made = self.run_collector(minutes=10, league_dir=None)
        self.assertIn("Connected to the League client.", self.output.getvalue())
        self.assertTrue(self.stored_ids(made))

    def test_says_the_client_is_open_but_out_of_reach(self):
        made = self.run_collector(minutes=2, league_dir=None)
        self.assertIn("The League client is open, but the collector cannot reach it yet.", self.output.getvalue())
        self.assertNotIn("Waiting for the League client", self.output.getvalue())
        self.assertEqual(self.stored_ids(made) if made.store else set(), set())

    def test_the_folder_typed_in_is_used_and_remembered(self):
        install = os.path.join(self.tmp.name, "WeGameApps", "英雄联盟")
        self.fake.write_lockfile(os.path.join(install, "LeagueClient"))
        saved = {}
        self.addCleanup(setattr, collector, "save_config", collector.save_config)
        collector.save_config = lambda **values: saved.update(values)
        self.addCleanup(setattr, builtins, "input", builtins.input)
        builtins.input = lambda prompt: '"%s"' % os.path.join(install, "TCLS")      # the shortcut's folder
        made = self.collector(league_dir=None)
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            made.ask_for_folder(hidden=True)
        self.assertIn("Run as administrator", output.getvalue())
        self.assertEqual(saved, {"league_dir": os.path.join(install, "TCLS")})
        self.assertEqual(made.opts.league_dir, os.path.join(install, "TCLS"))
        self.assertEqual(collector.lockfile_paths(made.opts.league_dir)[0],
                         [os.path.join(install, "LeagueClient", "lockfile")])


class PlainLcu(collector.Lcu):
    """The fake client speaks plain HTTP, where the real one speaks HTTPS."""

    def __init__(self, port, token, scheme="https"):
        super().__init__(port, token, "http")


class ClientPrograms(CollectorCase):
    """The Tencent client: its lockfile has one field, so the port and password come from
    LeagueClientUx's command line. Windows' list of running programs is pretend here."""

    def setUp(self):
        super().setUp()
        with open(os.path.join(self.league_dir, "lockfile"), "w") as handle:
            handle.write("12345")
        self.client = self.fake
        self.programs = {}
        self.start_client(4141, 4242, up=3600)
        for pid, name in ((77, "SGuard64.exe"), (78, "tcls_core.exe"), (79, "wegame.exe")):
            self.programs[pid] = collector.Running(pid, name, self.wall() - 7200, 50 << 20, 40 << 20, 300)
        self.opened, self.refuse = [], False

        def query_program(pid, command_line=False):
            self.opened.append((pid, command_line))
            if self.refuse:
                return None, None, 5
            line = "LeagueClientUx.exe --app-port=%d --remoting-auth-token=%s" % (self.client.port, self.client.token)
            return os.path.join(self.league_dir, self.programs[pid].name), line if command_line else None, 0
        for name, value in (("running_programs", lambda: [(pid, p.name) for pid, p in self.programs.items()]),
                            ("process_table", lambda: dict(self.programs)), ("query_program", query_program),
                            ("installer_dirs", lambda: []), ("installed_dirs", lambda: []),
                            ("scan_drives", lambda: []), ("Lcu", PlainLcu)):
            original = getattr(collector, name)
            setattr(collector, name, value)
            self.addCleanup(setattr, collector, name, original)
        collector.answers.clear()
        self.addCleanup(collector.answers.clear)

    def wall(self):
        return self.real_start + self.clock.now()

    def start_client(self, main_pid, window_pid, up=0):
        for pid, name, memory in ((main_pid, "LeagueClient.exe", 300), (window_pid, "LeagueClientUx.exe", 200),
                                  (window_pid + 1, "LeagueClientUxRender.exe", 100),
                                  (window_pid + 2, "LeagueClientUxRender.exe", 150)):
            self.programs[pid] = collector.Running(pid, name, self.wall() - up, memory << 20, (memory + 50) << 20, 900)

    def close_client(self):
        for pid in [pid for pid, p in self.programs.items() if p.name.lower().startswith("leagueclient")]:
            del self.programs[pid]
        self.client.close()

    def restart_client(self):
        self.close_client()
        self.client = FakeClient(clock=self.clock)
        self.addCleanup(self.client.close)
        self.start_client(5151, 5252)

    def run_logged(self, **kwargs):
        with self.assertLogs(collector.log, "INFO") as logs:
            made = self.run_collector(**kwargs)
        return made, logs.output

    def test_the_client_is_opened_once_while_it_runs(self):
        dropped = []

        def drop(path):         # the connection drops once, 2 minutes in
            if self.clock.now() > 1000 + 120 and not dropped:
                dropped.append(path)
                return True
            return False
        self.fake.drop = drop
        made = self.run_collector()
        self.assertIn("The League client closed", self.output.getvalue())
        self.assertEqual(len([r for r in self.fake.requests if r[1] == "/lol-summoner/v1/current-summoner"]), 2)
        self.assertEqual(self.opened, [(4141, False), (4242, False), (4242, True)])     # was three more each round
        self.assertEqual(self.stored_ids(made), self.fake.arena_ids())

    def test_a_restarted_client_is_read_again(self):
        self.clock.at(self.clock.now() + 120, self.restart_client)
        self.run_collector()
        self.assertEqual(self.opened, [(4141, False), (4242, False), (4242, True),
                                       (5151, False), (5252, False), (5252, True)])
        self.assertTrue(history(self.client.requests))           # collecting from the new client

    def test_a_refusal_is_asked_again_only_now_and_then(self):
        self.refuse = True
        self.addCleanup(setattr, sys, "stdin", sys.stdin)
        sys.stdin = io.StringIO()          # not a console: the collector must not stop to ask for the folder
        self.run_collector(minutes=60, league_dir=None)
        self.assertIn("cannot reach it yet", self.output.getvalue())
        self.assertLessEqual(len(self.opened), 3 * (3600 / collector.QUERY_RETRY + 1))   # was 3 every 10 seconds

    def test_a_new_program_with_the_same_process_id_is_read_again(self):
        first = {4242: collector.Running(4242, "LeagueClientUx.exe", 100.0, 0, 0, 0)}
        later = {4242: collector.Running(4242, "LeagueClientUx.exe", 200.0, 0, 0, 0)}
        for table in (first, first, later, later):
            collector.query_once(4242, command_line=True, table=table)
        self.assertEqual(self.opened, [(4242, True), (4242, True)])

    def test_the_client_is_described_every_ten_minutes(self):
        _, output = self.run_logged(minutes=35)
        lines = [line for line in output if "League client: LeagueClient pid 4141 up 1h" in line]
        self.assertGreaterEqual(len(lines), 4)
        self.assertIn("memory 300 MB, private 350 MB, 900 handles; LeagueClientUx pid 4242 up 1h", lines[-1])
        self.assertIn("2 LeagueClientUxRender programs, memory 250 MB", lines[-1])
        self.assertIn("requests to the client", lines[-1])

    def test_the_moment_the_client_closes_is_logged_with_what_the_collector_was_doing(self):
        self.clock.at(self.clock.now() + 120, self.close_client)
        _, output = self.run_logged(minutes=30)
        self.assertIn("Waiting for the League client", self.output.getvalue())
        closed = [line for line in output if "League client program closed" in line]
        self.assertEqual(len(closed), 1)
        for text in ("LeagueClient (pid 4141) after running 1h02m, memory 300 MB", "LeagueClientUx (pid 4242)",
                     "2 of 2 LeagueClientUxRender programs closed too", "last request to the client",
                     "Programs still running: tcls_core.exe, wegame.exe", "Crash reporters running: none"):
            self.assertIn(text, closed[0])
        gone = [line for line in output if "client gone:" in line]
        self.assertTrue(gone and "last request to the client" in gone[0], gone)
        self.assertNotIn(self.fake.token, "\n".join(output))

    @unittest.skipUnless(os.name == "nt", "reads Windows' list of running programs")
    def test_windows_list_of_programs(self):
        import subprocess
        started = time.time()
        child = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(60)"])
        self.addCleanup(child.wait)
        self.addCleanup(child.kill)
        time.sleep(1)
        table = self.original_process_table()
        me, kid = table[os.getpid()], table[child.pid]
        self.assertEqual(os.path.normcase(kid.name), os.path.normcase(os.path.basename(sys.executable)))
        self.assertLess(abs(kid.started - started), 5)
        self.assertGreater(me.memory, 1 << 20)
        self.assertGreater(me.private, 1 << 20)
        self.assertGreater(me.handles, 0)

    original_process_table = staticmethod(collector.process_table)


if __name__ == "__main__":
    unittest.main()
