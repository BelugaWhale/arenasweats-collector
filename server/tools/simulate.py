#!/usr/bin/env python3
"""Try out ways of handing out work on a made-up world of Arena players.

    python server/tools/simulate.py              # the comparison tables (about a minute)
    python server/tools/simulate.py --endpoint   # also play the rules through the real ingest.py, as a check

The world is shaped like what was measured on HN1 (Ionia) for 12-19 Nov 2026, when nearly every match of
those days was held: how many Arena matches players play, how many of the 18 in a match play a lot, how
often the same people queue together. What could not be measured (how much players play other modes, which
pushes Arena matches out of their latest 20) was set so that today's rules, replayed on the requests HN1's
collectors really made on 13-24 Nov, give what really happened. Section 2 of the output shows how close
that comes. Everything is scaled down (HN1 has about 20 times the matches); numbers are printed at HN1's size.

One fact decides most of it: the League client gives a player's latest 20 matches (all modes) and no more.
Asked for older places it answers with the same 20 (see README.md, "Whose list to read next").
"""

import argparse
import bisect
import copy
import heapq
import math
import os
import random
import sys
import tempfile
from collections import Counter, defaultdict

DAY = 86400.0
LOBBY = 18                      # players in an Arena match
SLOTS = 48                      # half-hour starting times in a day
GAMES_PER_ROUND, PLAYERS_PER_ROUND, NEW_PLAYERS_PER_ROUND = 40, 10, 5      # as in ingest.py

# ---- measured on HN1 (collector_matches on Crane and the endpoint's players table, 25 Nov 2026)
# Players by Arena matches in 8 days: (up to, % of players, % of seats)
REAL_SPREAD = [(1, 24.4, 2.4), (2, 13.3, 2.6), (3, 9.3, 2.8), (5, 11.7, 5.2), (8, 10.0, 6.8), (12, 7.8, 8.0),
               (20, 8.9, 14.1), (30, 6.1, 15.0), (50, 5.4, 20.6), (80, 2.5, 15.0), (120, 0.6, 5.5), (400, 0.1, 2.1)]
# 13-24 Nov, a day each: client requests (thousands), lists finished, matches received, % of lists with a new match
REAL_DAYS = [(20.6, 2696, 10779, 87), (18.3, 2625, 9065, 84), (39.7, 5925, 19444, 80), (33.7, 5437, 16002, 75),
             (47.0, 8844, 21131, 64), (35.7, 6909, 16409, 59), (40.7, 7700, 19311, 59), (49.0, 10536, 23117, 48),
             (43.7, 10619, 20049, 41), (46.3, 12309, 21045, 35), (44.2, 13650, 19093, 28), (84.3, 31419, 33892, 20)]
REAL_MATCHES_A_DAY = 11000      # HN1's Arena matches a day: the value at which the replay fits (see section 2)
FIRST_DAY, NOW = 38.25, 50.1    # days since the season began (6 Oct): collecting began 13 Nov 06:00; 25 Nov 02:00

# ---- the made-up players: (Arena matches per 8 days while playing, share of all seats)
CLASSES = [(1, 0.024), (2, 0.024), (3, 0.024), (5, 0.045), (8, 0.060), (12, 0.075), (19, 0.135), (30, 0.150),
           (48, 0.215), (75, 0.160), (110, 0.063), (170, 0.025)]
PARTY_SIZES = [(1, 0.50), (2, 0.33), (3, 0.09), (4, 0.08)]     # people who queue together, by party
ATTEND = 0.8                    # chance a party member joins a sitting
PAUSE, RESUME, RETIRE = 0.04, 0.1, 0.012     # a day: stop Arena for a while, come back, leave for good
CASUAL_RETIRE = 4               # players under 12 matches per 8 days leave this many times as often
QUIT = 0.5                      # of those who leave Arena, the share who stop playing altogether
PURE, CASUAL_PURE = 0.4, 0.1    # share who play nothing but Arena (regulars, casual players)
OTHER_RATIO, CASUAL_OTHER = 0.7, 4.0         # the others: other-mode games per Arena match (regulars), per day (casual)


def poisson(rng, mean):
    limit, count, product = math.exp(-mean), 0, 1.0
    while True:
        product *= rng.random()
        if product <= limit:
            return count
        count += 1


class World:
    """Players, the Arena matches they play over `days`, and what each one's match list shows at any moment."""

    def __init__(self, days, matches_a_day, seed=1):
        rng = self.rng = random.Random(seed)
        self.days = days
        self.other, self.quit_at = [], []       # per player: other-mode games a day; when they stopped playing
        seats = matches_a_day * LOBBY
        sizes, weights = zip(*PARTY_SIZES)
        mean_size = sum(size * weight * (1 if size == 1 else ATTEND) for size, weight in PARTY_SIZES)
        playing = RESUME / (RESUME + PAUSE)
        parties = []                            # [matches per 8 days, members, pausing]
        for per8, share in CLASSES:
            for _ in range(round(seats * share * 8 / per8 / mean_size / playing)):
                members = [self.new_player(per8) for _ in range(rng.choices(sizes, weights)[0])]
                parties.append([per8, members, rng.random() > playing])
        played = []                             # (when, [players])
        for day in range(days):
            waiting = [[] for _ in range(SLOTS)]
            for party in parties:
                per8 = party[0]
                if party[2]:
                    party[2] = rng.random() >= RESUME
                    continue
                roll = rng.random()
                if roll < RETIRE * (CASUAL_RETIRE if per8 < 12 else 1):
                    for old in party[1]:
                        if rng.random() < QUIT:
                            self.quit_at[old] = day
                    party[1] = [self.new_player(per8) for _ in range(rng.choices(sizes, weights)[0])]
                elif roll < RETIRE + PAUSE:
                    party[2] = True
                    continue
                sitting = max(1.0, 1.25 * math.sqrt(per8))          # matches in a day they play (measured shape)
                if rng.random() < min(0.95, per8 / 8 / sitting):
                    here = [p for p in party[1] if rng.random() < ATTEND] or [rng.choice(party[1])]
                    start = rng.randrange(SLOTS)
                    for i in range(1 + poisson(rng, sitting - 1)):
                        waiting[(start + i) % SLOTS].append(here)
            for slot, parties_now in enumerate(waiting):            # fill lobbies of 18 from who is queueing
                rng.shuffle(parties_now)
                lobby, later = [], []
                for queue in (parties_now, later):
                    for party_here in list(queue):
                        if len(lobby) + len(party_here) > LOBBY:
                            if queue is parties_now:
                                later.append(party_here)
                            continue
                        lobby += party_here
                        if len(lobby) == LOBBY:
                            played.append((day + (slot + rng.random()) / SLOTS, lobby))
                            lobby = []
        played.sort(key=lambda item: item[0])
        self.mtime = [when for when, _ in played]
        self.mplayers = [lobby for _, lobby in played]
        self.ptimes = [[] for _ in self.other]  # per player: when their Arena matches were played ...
        self.pmatches = [[] for _ in self.other]            # ... and which
        for match, (when, lobby) in enumerate(played):
            for p in lobby:
                self.ptimes[p].append(when)
                self.pmatches[p].append(match)
        self.players = len(self.other)

    def new_player(self, per8):
        rng = self.rng
        if per8 < 12:
            other = 0.0 if rng.random() < CASUAL_PURE else rng.lognormvariate(math.log(CASUAL_OTHER), 0.8)
        elif rng.random() < PURE:
            other = 0.0
        else:
            other = per8 / 8 * rng.lognormvariate(math.log(OTHER_RATIO), 1.0)
        self.other.append(other)
        self.quit_at.append(1e9)
        return len(self.other) - 1

    def others(self, p, t):
        """How many other-mode games player p has played by time t."""
        return int(self.other[p] * min(t, self.quit_at[p]))

    def read(self, p, t, since):
        """The top of p's match list at time t: (Arena matches among the latest 20 that are newer than `since`,
        whether all 20 places were newer than `since`)."""
        times, matches = self.ptimes[p], self.pmatches[p]
        last = bisect.bisect_right(times, t)
        others_now = self.others(p, t)
        found, i = [], last - 1
        while i >= 0 and times[i] > since and (last - 1 - i) + others_now - self.others(p, times[i]) < 20:
            found.append(matches[i])
            i -= 1
        newer = (last - bisect.bisect_right(times, since)) + others_now - self.others(p, since)
        return found, newer >= 20

    def listed(self, match, t):
        """Whether any of the match's players still has it among their latest 20 at time t."""
        return any(match in self.read(p, t, self.mtime[match] - 1e-9)[0] for p in self.mplayers[match])


# ---------------------------------------------------------------------------
# The server's rules (stand-ins for ingest.py, fast enough to play months)
# ---------------------------------------------------------------------------
class Rules:
    """What every rule shares: the matches held, the wanted list, the players known."""

    def __init__(self, world, start):
        self.w = world
        self.held = bytearray(len(world.mtime))
        self.asked = bytearray(len(world.mtime))             # on the wanted list, or held
        self.wanted = []                                     # newest first
        self.held_at = {}
        size = world.players
        self.games, self.known = [0] * size, bytearray(size)
        self.checked, self.due, self.since = [0.0] * size, [0.0] * size, [0.0] * size
        self.reading = {}                                    # player -> when the list was handed out
        self.log = []                                        # (when, new matches) for every list read
        self.meet(start, 0.0)

    def upload(self, match, now):
        self.held[match] = 1
        self.held_at[match] = now
        for p in self.w.mplayers[match]:
            self.games[p] += 1
            if not self.checked[p]:
                self.meet(p, now)

    def take(self, found):
        """Put a list's matches on the wanted list. Returns (how many were not held, how many no list had shown)."""
        missing = [match for match in found if not self.held[match]]
        new = [match for match in missing if not self.asked[match]]
        for match in new:
            self.asked[match] = 1
            heapq.heappush(self.wanted, -match)
        return len(missing), len(new)

    def fetches(self):
        return [-heapq.heappop(self.wanted) for _ in range(min(GAMES_PER_ROUND, len(self.wanted)))]

    def hand(self, p, now, pages=1):
        self.reading[p] = now
        return p, pages, self.since[p], None


class Before(Rules):
    """The rules until 25 Nov 2026: the most overdue list first, a never-read player 6 hours earlier per further
    match, one page and then three more for as long as that finds matches not held, the top again 7 days later.
    (The three more pages show the same 20 matches again.)"""

    def __init__(self, world, start, more_pages=3):
        self.queue, self.more_pages = [], more_pages
        super().__init__(world, start)

    def meet(self, p, now):
        if self.known[p]:
            self.due[p] -= 0.25
        else:
            self.known[p], self.due[p] = 1, now
        heapq.heappush(self.queue, (self.due[p], p))

    def work(self, reports, now):
        items = []
        for p, job, found, full, first_page in reports:
            missing, new = self.take(found)
            if first_page:
                self.log.append((now, new))
            if full and missing and self.more_pages:
                items.append((p, self.more_pages, self.since[p], None))
                continue
            self.checked[p], self.since[p], self.due[p] = now, self.reading.pop(p), now + 7
            heapq.heappush(self.queue, (self.due[p], p))
        fetch = self.fetches()
        room = 5 if len(fetch) < GAMES_PER_ROUND else 0
        while room and self.queue and self.queue[0][0] <= now:
            due, p = heapq.heappop(self.queue)
            if due == self.due[p] and p not in self.reading:
                items.append(self.hand(p, now))
                room -= 1
        return fetch, items


class Proposed(Rules):
    """ingest.py's rules: lists read before go first when they are due again, with places kept for players never
    read; those go by the held matches they are in, the latest found first. One page a read. A list is due again
    when about `target` more Arena matches are expected in it."""

    def __init__(self, world, start, target=10, most=30.0, kept=NEW_PLAYERS_PER_ROUND, order="matches", count="seen"):
        self.again, self.never, self.rank, self.found = [], [], {}, 0
        self.target, self.most, self.kept, self.order, self.count = target, most, kept, order, count
        super().__init__(world, start)

    def meet(self, p, now):
        self.known[p] = 1
        self.found += 1
        key = {"matches": (-self.games[p], -self.found), "latest": (-self.found,), "oldest": (self.found,)}[self.order]
        if self.order != "oldest" or p not in self.rank:
            self.rank[p] = key
            heapq.heappush(self.never, key + (p,))

    def carry_on(self, before):
        """Take over from the rules before, as the live server does: every player keeps their place and date."""
        for name in ("held", "asked", "wanted", "held_at", "games", "known", "checked", "due", "since", "log"):
            setattr(self, name, getattr(before, name))
        for p in range(self.w.players):
            if self.checked[p]:
                heapq.heappush(self.again, (self.due[p], p))
            elif self.known[p]:
                self.meet(p, 0.0)

    def work(self, reports, now):
        for p, job, found, full, _ in reports:
            missing, new = self.take(found)
            self.log.append((now, new))
            if self.checked[p] and not full:
                span = now - self.checked[p]                 # they came since the last read
            elif found:
                span = now - min(self.w.mtime[match] for match in found)     # ... or since the oldest was played
            else:
                span = now
            counted = {"seen": len(found), "new": new}[self.count]
            self.checked[p], self.since[p] = now, self.reading.pop(p)
            self.due[p] = now + min(self.most, max(1 / 24, span * self.target / max(counted, 0.5)))
            heapq.heappush(self.again, (self.due[p], p))
        fetch = self.fetches()
        items = []
        room = PLAYERS_PER_ROUND if len(fetch) < GAMES_PER_ROUND else 0

        def due_again(places):
            while len(items) < places and self.again and self.again[0][0] <= now:
                due, p = heapq.heappop(self.again)
                if due == self.due[p] and p not in self.reading:
                    items.append(self.hand(p, now))

        due_again(room - self.kept)
        while len(items) < room and self.never:
            key = heapq.heappop(self.never)
            p = key[-1]
            if self.rank.get(p) == key[:-1] and not self.checked[p] and p not in self.reading:
                items.append(self.hand(p, now))
        due_again(room)
        return fetch, items


class Endpoint:
    """The real ingest.py behind the same two calls, so its rules can be played on the same world."""
    FIRST_ID = 5_000_000_000

    def __init__(self, world, start):
        sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        import ingest
        self.w, self.ingest, self.now = world, ingest, 0.0
        self.folder = tempfile.TemporaryDirectory()
        self.before = (os.environ.get("INGEST_DB"), ingest.clock, ingest.REQUESTS_PER_MINUTE)
        os.environ["INGEST_DB"] = os.path.join(self.folder.name, "ingest.db")
        ingest.clock = lambda: self.season + self.now * DAY
        ingest.REQUESTS_PER_MINUTE = 10 ** 9                # one key stands for every collector here
        self.db = db = ingest.connect()                     # kept open, so SQLite does not tidy up after every request
        self.season = ingest.get_policy(db)["season_start"] / 1000
        db.execute("INSERT INTO contributors(name, key_hash, created_at) VALUES('made-up', ?, 0)", (ingest.hash_key("k"),))
        db.commit()
        self.client = ingest.app.test_client()
        self.start, self.held, self.held_at, self.log = start, bytearray(len(world.mtime)), {}, []

    def close(self):
        """Put ingest.py back as it was and remove the database."""
        self.db.close()
        path, self.ingest.clock, self.ingest.REQUESTS_PER_MINUTE = self.before
        if path is None:
            del os.environ["INGEST_DB"]
        else:
            os.environ["INGEST_DB"] = path
        self.folder.cleanup()

    def call(self, path, body):
        answer = self.client.post(path, json=body, headers={"Authorization": "Bearer k"})
        if answer.status_code != 200:
            raise RuntimeError("%s answered %s: %s" % (path, answer.status_code, answer.get_data(as_text=True)))
        return answer.get_json()

    def puuid(self, p):
        return "made-up-player-%09d" % p

    def upload(self, match, now):
        self.now = now
        players = self.w.mplayers[match]
        game = {"gameId": self.FIRST_ID + match, "platformId": "HN1", "gameMode": "CHERRY", "gameDuration": 1200,
                "gameCreation": int((self.season + self.w.mtime[match] * DAY) * 1000),
                "participants": [{"participantId": seat, "championId": 1, "stats": {}} for seat in range(len(players))],
                "participantIdentities": [{"participantId": seat, "player": {"puuid": self.puuid(p)}}
                                          for seat, p in enumerate(players)]}
        if self.call("/v1/games", {"games": [game]})["accepted"] != 1:
            raise RuntimeError("match %d was handed out twice" % match)
        self.held[match], self.held_at[match] = 1, now

    def work(self, reports, now):
        self.now = now
        body = {"platform": "HN1", "start": self.puuid(self.start), "reports": [
            {"puuid": self.puuid(p), "job": job, "ids": [self.FIRST_ID + match for match in found],
             "newest": int((self.season + now * DAY) * 1000), "next": 20 if full else 0, "failed": False}
            for p, job, found, full, _ in reports]}
        answer = self.call("/v1/work", body)
        self.log += [(now, 0)] * len(reports)
        if any(item["begin"] or item["pages"] != 1 for item in answer["players"]):
            raise RuntimeError("asked for more than the first page")
        return ([game_id - self.FIRST_ID for game_id in answer["games"]],
                [(int(item["puuid"][-9:]), 1, (item["since"] / 1000 - self.season) / DAY, item["job"])
                 for item in answer["players"]])


def collect(rules, now, until, requests_a_day, counts, reports=()):
    """One collector, standing for all of them, asks for work until `until`. Returns (now, reports not handed in)."""
    reports = list(reports)
    while now < until:
        fetch, items = rules.work(reports, now)
        reports = []
        step, day = 1.0 / requests_a_day(now), int(now)
        for match in fetch:
            now += step
            rules.upload(match, now)
        for p, pages, since, job in items:
            found, full = rules.w.read(p, now, since)
            now += step * pages
            reports.append((p, job, found, full, pages == 1))
            counts[day]["pages"] += pages
        counts[day]["fetches"] += len(fetch)
        if not fetch and not items:
            now += 60 / DAY                                  # nothing to do: ask again in a minute
    return now, reports


# ---------------------------------------------------------------------------
# The tables
# ---------------------------------------------------------------------------
def show_world(world):
    print("1. The made-up world against HN1, 8 days (sim / measured)")
    seats, lobbies = Counter(), [lobby for when, lobby in zip(world.mtime, world.mplayers) if 37 <= when < 45]
    for lobby in lobbies:
        seats.update(lobby)
    print("   players per match: %.2f / 1.78" % (len(seats) / len(lobbies)))
    low = 0
    for up_to, real_players, real_seats in REAL_SPREAD:
        group = [n for n in seats.values() if low < n <= up_to]
        print("   %3d-%-3d matches: players %4.1f%% / %4.1f%%   seats %4.1f%% / %4.1f%%" % (
            low + 1, up_to, 100 * len(group) / len(seats), real_players, 100 * sum(group) / sum(seats.values()), real_seats))
        low = up_to
    busy = sorted(sum(1 for p in lobby if seats[p] >= 30) for lobby in lobbies)
    print("   matches with no player of 30+ matches: %.1f%% / 0.3%%; such players in the middle match: %d / 8" % (
        100 * busy.count(0) / len(busy), busy[len(busy) // 2]))


def replay(world, real_matches_a_day, show=True):
    """Today's rules on the requests HN1's collectors really made on 13-24 Nov. Returns the state on 25 Nov."""
    scale = len(world.mtime) / world.days / real_matches_a_day
    start = next(p for p in range(world.players) if sum(1 for t in world.ptimes[p] if FIRST_DAY - 2 < t < FIRST_DAY) > 10)
    rules, counts = Before(world, start), defaultdict(Counter)

    def requests_a_day(now):
        thousands = REAL_DAYS[min(int(now) - 38, len(REAL_DAYS) - 1)][0]
        return thousands * 1000 * scale * (4 / 3 if int(now) == 38 else 1)         # (the first day began at 06:00)
    now, reports = collect(rules, FIRST_DAY, NOW, requests_a_day, counts)
    if show:
        print("\n2. The rules until 25 Nov replayed on HN1's real requests, taking %d Arena matches a day (sim / measured)"
              % real_matches_a_day)
        print("   day     lists finished    matches received   new per list   lists with a new match")
        days = defaultdict(lambda: [0, 0, 0])
        for when, new in rules.log:
            days[int(when)][0] += 1
            days[int(when)][1] += new
            days[int(when)][2] += 1 if new else 0
        for n, (_, real_lists, real_matches, real_share) in enumerate(REAL_DAYS):
            lists, matches, with_new = days[38 + n]
            print("   %2d Nov  %6d / %6d   %6d / %6d    %.2f / %.2f     %3.0f%% / %d%%" % (
                13 + n, lists / scale, real_lists, matches / scale, real_matches, matches / max(lists, 1),
                real_matches / real_lists, 100 * with_new / max(lists, 1), real_share))
        never = [p for p in range(world.players) if rules.known[p] and not rules.checked[p]]
        print("   on 25 Nov: matches held %d / 231964; players known %d / 302265, never read %d / 180762, of those seen "
              "in one match %.0f%% / 47%%" % (len(rules.held_at) / scale, sum(rules.known) / scale, len(never) / scale,
                                             100 * sum(1 for p in never if rules.games[p] <= 1) / len(never)))
        age = sorted(now - world.mtime[m] for m, when in rules.held_at.items() if int(when) == 49)
        print("   age of the matches received on 24 Nov, middle one: %.0f days / 22 days" % age[len(age) // 2])
        past = [m for m, when in enumerate(world.mtime) if when < NOW]
        print("   so on 25 Nov: %.0f%% of the season's matches held, %.0f%% held or still among some player's latest 20" % (
            100 * sum(rules.held[m] for m in past) / len(past),
            100 * sum(1 for m in past if rules.held[m] or world.listed(m, NOW)) / len(past)))
    return rules, scale, now, reports


def measure(world, rules, counts, scale, end, requests_a_day):
    """One line of a comparison table."""
    days = range(int(NOW), int(end))
    late = [d for d in days if d >= end - 10]
    requests = lambda which: sum(counts[d]["fetches"] + counts[d]["pages"] for d in which)
    played, got = sorted(world.mtime), sorted(rules.held_at.values())
    share = lambda t: bisect.bisect_right(got, t) / bisect.bisect_right(played, t)
    reach = lambda level: next(("%4.1f" % (step / 4) for step in range(int((end - NOW) * 4) + 1)
                                if share(NOW + step / 4) >= level), "   -")
    new = [m for m, when in enumerate(world.mtime) if NOW + 3 <= when < end - 3]
    wait = sorted(24 * (rules.held_at[m] - world.mtime[m]) for m in new if rules.held[m]) or [0.0]
    lost = sum(1 for m in new if not rules.held[m] and not world.listed(m, end))
    return "%5.2f  %5.2f   %3.0f%%   %s %s %s   %4.0f%% %4.0f%% %4.0f%%   %5.1f%%  %5.1f %5.1f   %4.2f%%" % (
        requests(days) / max(sum(counts[d]["fetches"] for d in days), 1),
        requests(late) / max(sum(counts[d]["fetches"] for d in late), 1), 100 * requests(late) / (requests_a_day * len(late)),
        reach(0.5), reach(0.8), reach(0.95), 100 * share(NOW + 7), 100 * share(NOW + 14), 100 * share(end),
        100 * len(wait) / len(new), wait[len(wait) // 2], wait[int(len(wait) * 0.9)], 100 * lost / len(new))


HEAD = ("%-42s requests a match   days until held:    season held after   new matches: held, hours until  lost\n"
        "%-42s  all  at end  used   50%%  80%%  95%%     7 d  14 d  %2d d             (middle, 9 in 10)")
CHOICES = [("as until 25 Nov", None), ("as until 25 Nov, but one page a list", 0), ("as now in ingest.py", {}),
           ("  but never-read players before the rest", dict(kept=PLAYERS_PER_ROUND)),
           ("  but of never-read, latest found first", dict(order="latest")),
           ("  but of never-read, oldest found first", dict(order="oldest")),
           ("  but due again after 5 more matches", dict(target=5)),
           ("  but due again after 15 more matches", dict(target=15)),
           ("  but every list at least every 7 days", dict(most=7.0)),
           ("  but due by matches no list had shown", dict(count="new"))]


def compare(world, title, state, requests_a_day, days, choices=CHOICES):
    before, scale, now, reports = state
    end = NOW + days
    print("\n" + title)
    print(HEAD % ("", "", days))
    for label, change in choices:
        counts = defaultdict(Counter)
        if isinstance(change, dict):
            rules = Proposed(world, 0, **change)
            rules.carry_on(copy.deepcopy(before))
            collect(rules, now, end, lambda t: requests_a_day * scale, counts)
        else:
            rules = copy.deepcopy(before)
            rules.more_pages = 3 if change is None else change
            collect(rules, now, end, lambda t: requests_a_day * scale, counts, reports)
        print("%-42s %s" % (label, measure(world, rules, counts, scale, end, requests_a_day * scale)))


def check_endpoint(world, title, state, requests_a_day, days):
    """ingest.py itself beside its stand-in. Both start on 25 Nov with the same matches held and no list read yet
    (the real code cannot be handed the stand-in's half-read state)."""
    before, scale, now, _ = state
    print("\n" + title)
    print(HEAD % ("", "", days))
    for label, make in (("the stand-in used above", Proposed), ("ingest.py itself", Endpoint)):
        rules, counts = make(world, 0), defaultdict(Counter)
        for match in sorted(before.held_at, key=before.held_at.get):
            rules.upload(match, before.held_at[match])
        collect(rules, now, NOW + days, lambda t: requests_a_day * scale, counts)
        print("%-42s %s" % (label, measure(world, rules, counts, scale, NOW + days, requests_a_day * scale)))
        if make is Endpoint:
            rules.close()


def fresh(world, real_matches_a_day, requests_a_day):
    """A server like HN10 on 25 Nov: one collector since the afternoon before, under the rules until then."""
    scale = len(world.mtime) / world.days / real_matches_a_day
    start = next(p for p in range(world.players) if sum(1 for t in world.ptimes[p] if 47.5 < t < 49.67) > 10)
    rules = Before(world, start)
    now, reports = collect(rules, 49.67, NOW, lambda t: requests_a_day * scale, defaultdict(Counter))
    return rules, scale, now, reports


def main():
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("--matches-a-day", type=int, default=550, help="size of the made-up world (HN1 has about 11,000)")
    parser.add_argument("--days", type=int, default=30, help="days played on from 25 Nov")
    parser.add_argument("--seed", type=int, default=1)
    parser.add_argument("--real", type=int, default=REAL_MATCHES_A_DAY, help="HN1's Arena matches a day to assume")
    parser.add_argument("--endpoint", action="store_true", help="also play ingest.py itself (slower)")
    args = parser.parse_args()
    world = World(int(NOW) + 2 + args.days, args.matches_a_day, args.seed)
    show_world(world)
    state = replay(world, args.real)
    compare(world, "3. From 25 Nov on HN1: 4 collectors, 84,000 client requests a day", state, 84000, args.days)
    compare(world, "4. The same with 3 times the collectors", state, 252000, args.days, CHOICES[:3])
    small = fresh(world, args.real * 0.54, 36000)
    compare(world, "5. A new server like HN10 (about %d matches a day, 3%% held): 1 fast collector, 36,000 requests a day"
            % (args.real * 0.54), small, 36000, args.days, CHOICES[:3] + CHOICES[8:9])
    compare(world, "6. The same with 3 fast collectors", small, 108000, args.days, CHOICES[:3])
    if args.endpoint:
        check_endpoint(world, "7. ingest.py itself beside its stand-in (HN1, 84,000 requests a day, no list read yet)",
                       state, 84000, args.days)


if __name__ == "__main__":
    main()
