#!/usr/bin/env python3
"""Play the rules for handing out work on a made-up world of Arena players.

    python server/tools/simulate.py            # the tables (about half an hour)
    python server/tools/simulate.py --quick    # fewer cases and 14 days instead of 30 (a few minutes)

The world is shaped like what was measured on HN1 (Ionia) for 12-19 Nov 2026, when nearly every match of
those days was held: how many Arena matches players play, how many of the 18 in a match play a lot, how
often the same people queue together. What could not be measured (how much players play other modes, which
pushes Arena matches down their lists) was set so that the rules until 25 Nov, replayed on the requests
HN1's collectors really made on 13-24 Nov, give what really happened. Section 2 of the output shows how
close that comes. Everything is scaled down (HN1 has about 20 times the matches); numbers are printed at
HN1's size.

From 25 Nov on it is ingest.py itself that is played, with made-up collectors. Each has its own League client,
which behaves as measured on a real one that day: the first request for a player's list decides how many
matches the client will give for that player; it keeps a copy and answers later requests from the copy; a
list fetched afresh takes about as long as it has places. How long a copy lives was not known, so the tables
show several lives. What a request costs a client on the Chinese servers (T_PLACE, T_MATCH) is inferred from
how fast collectors went there, not measured.
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
PAGE = 20                       # places in one page of a match list
GAMES_PER_ROUND = 40            # as in ingest.py
T_PLACE, T_COPY, T_MATCH = 0.085, 0.05, 1.5    # seconds a client takes: per place of a list fetched afresh, for a
                                # list answered from its copy, for one match (1.7 s for a page of 20 was inferred)
NORMAL, FAST = 2.0, 0.4         # seconds a collector waits between two requests; in fast mode it also has
FLIGHT = 3                      # this many lists on their way at once (0.3.8: about 35 listed matches a second)
IDLE = 600                      # with nothing to do, a made-up collector asks again this often (a real one: 60 s)

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

    def newer(self, p, t, when):
        """How many matches (all modes) player p's list holds at time t that were played after `when`."""
        times = self.ptimes[p]
        return bisect.bisect_right(times, t) - bisect.bisect_right(times, when) + self.others(p, t) - self.others(p, when)

    def read(self, p, t, since, places=PAGE):
        """The top `places` of p's match list at time t: (the Arena matches among them that are newer than `since`,
        when the oldest of those places was played, or -1 if that was before the season; whether any of them is
        newer than `since`)."""
        oldest, floor = -1.0, since
        if self.newer(p, t, 0.0) >= places:
            low, oldest = 0.0, t                    # the oldest place was played between these two
            for _ in range(14):
                middle = (low + oldest) / 2
                low, oldest = (middle, oldest) if self.newer(p, t, middle) >= places else (low, middle)
            floor = max(since, low)
        times, matches = self.ptimes[p], self.pmatches[p]
        found = matches[bisect.bisect_right(times, floor):bisect.bisect_right(times, t)]
        return found, oldest, self.newer(p, t, since) > 0

    def listed(self, match, t, places=PAGE):
        """Whether any of the match's players still has it among their latest `places` at time t."""
        return any(match in self.read(p, t, self.mtime[match] - 1e-9, places)[0] for p in self.mplayers[match])


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




def collect_before(rules, now, until, requests_a_day, counts, reports=()):
    """One collector, standing for all of them, works under the rules until 25 Nov. Every request (a page of 20, a
    match) takes the same time. Returns (now, the reports not handed in yet)."""
    reports = list(reports)
    while now < until:
        fetch, items = rules.work(reports, now)
        reports = []
        step, day = 1.0 / requests_a_day(now), int(now)
        for match in fetch:
            now += step
            rules.upload(match, now)
        for p, pages, since, job in items:
            found, oldest, _ = rules.w.read(p, now, since)
            now += step * pages
            reports.append((p, job, found, oldest > since, pages == 1))
            counts[day]["requests"] += pages
        counts[day]["requests"] += len(fetch)
        counts[day]["fetches"] += len(fetch)
        if not fetch and not items:
            now += 60 / DAY                                  # nothing to do: ask again in a minute
    return now, reports


# ---------------------------------------------------------------------------
# Made-up collectors and their League clients, working for ingest.py itself
# ---------------------------------------------------------------------------
class Install:
    """One collector and its League client. `block`: the most matches of a list it asks for in one request (None: it
    is a collector up to 0.3.7 and always asks for a page of 20). `share`: the part of its time it has for this
    made-up world, which is as much smaller than a real server as the world is. Its client keeps a copy of every
    list it fetched for `life` days, and loses them all when it restarts, every `restarts` days."""

    def __init__(self, name, wait=NORMAL, share=1.0, block=None, life=1 / 24, restarts=None, joined=FIRST_DAY):
        self.name, self.wait, self.share, self.block, self.joined = name, wait, share, block, joined
        self.flight = FLIGHT if wait == FAST else 1
        self.life, self.restarts, self.first_restart = life, restarts, random.Random(name).random() * (restarts or 0)
        self.copies = {}                                     # player -> (when it was fetched, how many places)

    def copy(self, p, now):
        """The copy of p's list this client still holds, or None."""
        made = self.copies.get(p)
        if made is None or now - made[0] >= self.life:
            return None
        if self.restarts and (now - self.first_restart) // self.restarts > (made[0] - self.first_restart) // self.restarts:
            return None
        return made


class Endpoint:
    """ingest.py itself, on a database of its own, with the made-up world's clock."""
    FIRST_ID = 5_000_000_000

    def __init__(self, world, installs, changes=None, start=None):
        sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        import ingest
        self.w, self.ingest, self.now, self.start = world, ingest, 0.0, start
        self.folder = tempfile.TemporaryDirectory()
        changes = dict(changes or {}, clock=lambda: self.season + self.now * DAY,
                       REQUESTS_PER_MINUTE=10 ** 9, GAMES_PER_DAY=10 ** 9)     # (these limits are not what is tried here)
        self.before = (os.environ.get("INGEST_DB"), {name: getattr(ingest, name) for name in changes})
        for name, value in changes.items():
            setattr(ingest, name, value)
        os.environ["INGEST_DB"] = os.path.join(self.folder.name, "ingest.db")
        self.db = db = ingest.connect()                     # kept open, so SQLite does not tidy up after every request
        self.season = ingest.get_policy(db)["season_start"] / 1000
        for install in installs:
            db.execute("INSERT INTO contributors(name, key_hash, created_at) VALUES(?, ?, ?)",
                       (install.name, ingest.hash_key(install.name), int(self.season + install.joined * DAY)))
        db.commit()
        self.client = ingest.app.test_client()
        self.held, self.held_at = bytearray(len(world.mtime)), {}

    def close(self):
        """Put ingest.py back as it was and remove the database."""
        self.db.close()
        path, values = self.before
        for name, value in values.items():
            setattr(self.ingest, name, value)
        if path is None:
            del os.environ["INGEST_DB"]
        else:
            os.environ["INGEST_DB"] = path
        self.folder.cleanup()

    def call(self, install, path, body, now):
        self.now = now
        answer = self.client.post(path, json=body, headers={"Authorization": "Bearer " + install.name})
        if answer.status_code != 200:
            raise RuntimeError("%s answered %s: %s" % (path, answer.status_code, answer.get_data(as_text=True)))
        return answer.get_json()

    def puuid(self, p):
        return "made-up-player-%09d" % p

    def stamp(self, when):
        return int((self.season + when * DAY) * 1000)

    def upload(self, install, matches, now):
        games = [{"gameId": self.FIRST_ID + match, "platformId": "HN1", "gameMode": "CHERRY", "gameDuration": 1200,
                  "gameCreation": self.stamp(self.w.mtime[match]),
                  "participants": [{"participantId": seat, "championId": 1, "stats": {}} for seat in range(LOBBY)],
                  "participantIdentities": [{"participantId": seat, "player": {"puuid": self.puuid(p)}}
                                            for seat, p in enumerate(self.w.mplayers[match])]} for match in matches]
        if self.call(install, "/v1/games", {"games": games}, now)["accepted"] != len(matches):
            raise RuntimeError("a match was handed out twice")
        for match in matches:
            self.held[match], self.held_at[match] = 1, now

    def work(self, install, reports, now, more=True):
        """Hand in reports and ask for work. Returns (matches to fetch, lists to read: (player, since, places, token))."""
        body = {"platform": "HN1", "more": more, "reports": reports}
        if install.block:
            body["block"] = install.block
        if self.start is not None:                           # (a server with no players yet starts from one account)
            body["start"] = self.puuid(self.start)
        answer = self.call(install, "/v1/work", body, now)
        if any(item["begin"] or item["pages"] != 1 or ("count" in item) != bool(install.block) for item in answer["players"]):
            raise RuntimeError("a hand-out this collector cannot follow: %s" % answer["players"])
        return ([game_id - self.FIRST_ID for game_id in answer["games"]],
                [(int(item["puuid"][-9:]), (item["since"] / 1000 - self.season) / DAY, item.get("count", PAGE), item["job"])
                 for item in answer["players"]])

    def take_over(self, before, readers):
        """Start from what the rules until 25 Nov left: the same matches held, the same lists read (each by one of
        `readers`, whose client fetched a page of 20 then; who it was, the endpoint did not keep)."""
        matches = sorted(before.held_at, key=before.held_at.get)
        for first in range(0, len(matches), 50):
            self.upload(readers[0], matches[first:first + 50], before.held_at[matches[first + 49 if first + 49 < len(matches) else -1]])
        rng = random.Random(1)
        for p in range(self.w.players):
            if before.checked[p]:
                self.db.execute("UPDATE players SET checked_at = ?, read_to = ?, due_at = ? WHERE puuid = ?", (
                    self.stamp(before.checked[p]) // 1000, self.stamp(before.since[p]), self.stamp(before.due[p]) // 1000,
                    self.puuid(p)))
                rng.choice(readers).copies[p] = (before.since[p], PAGE)
        self.db.commit()


def collect(endpoint, installs, now, until, counts):
    """The collectors take turns asking the endpoint for work and doing it, each at its own pace, until `until`."""
    world, turns = endpoint.w, [(now + number * 1e-5, number) for number in range(len(installs))]
    while turns[0][0] < until:
        now, number = heapq.heappop(turns)
        install, day, seconds, reports = installs[number], counts[int(now)], 0.0, []
        fetch, items = endpoint.work(install, [], now)
        seconds += len(fetch) * (install.wait + T_MATCH)
        if fetch:
            endpoint.upload(install, fetch, now + seconds / DAY)
        for p, since, places, job in items:
            at = now + seconds / DAY
            made = install.copy(p, at)
            when, held_places = made or (at, places)
            found, oldest, fresh = world.read(p, when, since, held_places)
            took = T_COPY if made else T_PLACE * places
            install.copies[p] = (when, held_places)
            seconds += (install.wait + took) / install.flight
            report = {"puuid": endpoint.puuid(p), "job": job, "ids": [endpoint.FIRST_ID + match for match in found],
                      "newest": endpoint.stamp(when) if fresh else 0, "begin": 0, "failed": False,
                      "next": places if held_places >= places and oldest > since else 0}
            if install.block:
                report.update(asked=places, listed=held_places, oldest=endpoint.stamp(oldest), took=took)
            reports.append(report)
            day["places"] += 0 if made else places
            day["copies"] += 1 if made else 0
        if reports:                                          # handed in as soon as the round is done
            endpoint.work(install, reports, now + seconds / DAY, more=False)
        day["requests"] += len(fetch) + len(items)
        day["fetches"] += len(fetch)
        day["seconds"] += seconds
        # The rest of its time goes to the other worlds this one stands for; with nothing to do, it asks again soon.
        heapq.heappush(turns, (now + (seconds / install.share if seconds else IDLE) / DAY, number))


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
    """The rules until 25 Nov on the requests HN1's collectors really made on 13-24 Nov. Returns the state on 25 Nov."""
    scale = len(world.mtime) / world.days / real_matches_a_day
    start = next(p for p in range(world.players) if sum(1 for t in world.ptimes[p] if FIRST_DAY - 2 < t < FIRST_DAY) > 10)
    rules, counts = Before(world, start), defaultdict(Counter)

    def requests_a_day(now):
        thousands = REAL_DAYS[min(int(now) - 38, len(REAL_DAYS) - 1)][0]
        return thousands * 1000 * scale * (4 / 3 if int(now) == 38 else 1)         # (the first day began at 06:00)
    now, reports = collect_before(rules, FIRST_DAY, NOW, requests_a_day, counts)
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
        print("   so on 25 Nov: %.0f%% of the season's matches held. Held or still among some player's latest 20: %.0f%%; "
              "latest 100: %.0f%%; latest 200: %.0f%%; latest 1,000: %.0f%%" % ((100 * sum(rules.held[m] for m in past) / len(past),) + tuple(
                  100 * sum(1 for m in past if rules.held[m] or world.listed(m, NOW, places)) / len(past)
                  for places in (20, 100, 200, 1000))))
    return rules, scale, now, reports


def fresh(world, real_matches_a_day, requests_a_day):
    """A server like HN10 on 25 Nov: one collector since the afternoon before, under the rules until then."""
    scale = len(world.mtime) / world.days / real_matches_a_day
    start = next(p for p in range(world.players) if sum(1 for t in world.ptimes[p] if 47.5 < t < 49.67) > 10)
    rules = Before(world, start)
    now, reports = collect_before(rules, 49.67, NOW, lambda t: requests_a_day * scale, defaultdict(Counter))
    return rules, scale, now, reports


def measure(world, rules, counts, end, seconds_a_day, places=PAGE):
    """One line of a table. `places`: how far down a list the collectors could see (for what counts as lost)."""
    days = range(int(NOW), int(end))
    late = [d for d in days if d >= end - 10]
    total = lambda name, which: sum(counts[d][name] for d in which)
    played, got = sorted(world.mtime), sorted(rules.held_at.values())
    share = lambda t: bisect.bisect_right(got, t) / bisect.bisect_right(played, t)
    reach = lambda level: next(("%4.1f" % (step / 4) for step in range(int((end - NOW) * 4) + 1)
                                if share(NOW + step / 4) >= level), "   -")
    new = [m for m, when in enumerate(world.mtime) if NOW + 3 <= when < end - 3]
    wait = sorted(24 * (rules.held_at[m] - world.mtime[m]) for m in new if rules.held[m]) or [0.0]
    lost = sum(1 for m in new if not rules.held[m] and not world.listed(m, end, places))
    return "%5.1f %5.1f  %5.2f   %3.0f%%   %s %s %s   %4.0f%% %4.0f%% %4.0f%%   %5.1f%%  %5.1f %5.1f   %4.2f%%" % (
        total("seconds", days) / max(total("fetches", days), 1), total("seconds", late) / max(total("fetches", late), 1),
        total("requests", days) / max(total("fetches", days), 1), 100 * total("seconds", late) / (seconds_a_day * len(late)),
        reach(0.5), reach(0.8), reach(0.95), 100 * share(NOW + 7), 100 * share(NOW + 14), 100 * share(end),
        100 * sum(1 for m in new if rules.held[m]) / max(len(new), 1), wait[len(wait) // 2], wait[int(len(wait) * 0.9)],
        100 * lost / max(len(new), 1))


HEAD = ("%-46s client seconds   requests  time   days until held:   season held after   new matches: held, hours until  lost\n"
        "%-46s a match: all,    a match   used   50%%  80%%  95%%      7 d  14 d  %2d d             (middle, 9 in 10)\n"
        "%-46s          at end            at end")
HOUR = 1 / 24


def table(world, title, state, cases, days, team, seconds_a_day):
    """Play each case from 25 Nov and print its line. A case: (label, None for the rules until 25 Nov, or what the
    collectors are like: (block, life of a client's copies in days, days between client restarts, changes to ingest.py))."""
    before, scale, now, reports = state
    end = NOW + days
    print("\n" + title)
    print(HEAD % ("", "", days, ""))
    for label, case in cases:
        counts = defaultdict(Counter)
        if case is None:
            rules = copy.deepcopy(before)
            collect_before(rules, now, end, lambda t: seconds_a_day * scale / (NORMAL + T_PLACE * PAGE), counts, reports)
            for day in counts.values():
                day["seconds"] = day["requests"] * (NORMAL + T_PLACE * PAGE)
            print("%-46s %s" % (label, measure(world, rules, counts, end, seconds_a_day * scale)))
            continue
        block, life, restarts, changes = case
        installs = [Install("collector-%d" % number, wait, share * scale, block, life, restarts) for number, (wait, share) in enumerate(team)]
        rules = Endpoint(world, installs, changes)
        rules.take_over(before, installs[:3])
        collect(rules, installs, now, end, counts)
        print("%-46s %s" % (label, measure(world, rules, counts, end, seconds_a_day * scale, block or PAGE)))
        rules.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("--matches-a-day", type=int, default=550, help="size of the made-up world (HN1 has about 11,000)")
    parser.add_argument("--days", type=int, default=30, help="days played on from 25 Nov")
    parser.add_argument("--seed", type=int, default=1)
    parser.add_argument("--real", type=int, default=REAL_MATCHES_A_DAY, help="HN1's Arena matches a day to assume")
    parser.add_argument("--quick", action="store_true", help="fewer cases, 14 days")
    parser.add_argument("--place-seconds", type=float, default=T_PLACE,
                        help="seconds a client takes for each match of a list it fetches afresh (measured on OC1: 0.25)")
    args = parser.parse_args()
    globals()["T_PLACE"] = args.place_seconds
    days = 14 if args.quick else args.days
    world = World(int(NOW) + 2 + days, args.matches_a_day, args.seed)
    show_world(world)
    state = replay(world, args.real)
    cases = [("rules until 25 Nov (as if copies did not matter)", None),
             ("one page of 20 (collectors up to 0.3.7)", (None, HOUR, None, None)),
             ("up to 200 in one request (0.3.8)", (200, HOUR, None, None)),
             ("  the same, clients keep copies until restarted *", (200, 999, 3, None)),
             ("  the same, and they never restart", (200, 999, None, None))]
    more = [("up to 100 in one request", (100, HOUR, None, None)),
            ("up to 60 in one request", (60, HOUR, None, None)),
            ("up to 1,000, if a client gave that many", (1000, HOUR, None, {"DEFAULT_BLOCK": 1000, "PLACES_PER_ROUND": 4000})),
            ("up to 200, a first read always 200", (200, HOUR, None, {"FIRST_PLACES": 10 ** 6})),
            ("up to 200, due again after 5 more matches", (200, HOUR, None, {"TARGET_SEEN": 5}))]
    hn1 = [(NORMAL, 0.9)] * 4                                # four collectors at the normal pace, on 9 hours in 10
    print("\n   A client keeps its copy of a list for an hour, except in the lines marked *: there until it is restarted,"
          "\n   every 3 days. On 25 Nov the first three collectors' clients hold a page of 20 of every list read so far.")
    table(world, "3. From 25 Nov on HN1: 4 collectors at the normal pace", state, cases if args.quick else cases + more,
          days, hn1, 4 * 0.9 * DAY)
    if not args.quick:
        table(world, "4. The same with 12 collectors", state, cases[:3], days, hn1 * 3, 12 * 0.9 * DAY)
    small = fresh(world, args.real * 0.54, 36000)
    one = [(FAST, 0.83)]                                     # one collector in fast mode, on 20 hours a day
    table(world, "5. A new server like HN10 (about %d matches a day, 3%% held): 1 collector in fast mode" % (args.real * 0.54),
          small, cases, days, one, 0.83 * DAY)
    if not args.quick:
        table(world, "6. The same with 3 collectors in fast mode", small, cases[:3], days, one * 3, 3 * 0.83 * DAY)


if __name__ == "__main__":
    main()
