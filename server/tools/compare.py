#!/usr/bin/env python3
"""One case of simulate.py's made-up HN1 from 25 Nov 2026, with collectors as they were on 26 Nov: in fast mode, one
request at a time (policy fast_flight 1), 0.4 s between requests and 0.08 s a match (measured).

    python server/tools/compare.py --block 200 --days 14 --installs 2
    git show 6f146eb:server/ingest.py > /tmp/rules-25nov/ingest.py
    python server/tools/compare.py --rules /tmp/rules-25nov/ingest.py --block 200     # the rules until 26 Nov

--rules plays another ingest.py (a folder holding one named ingest.py, or the file). --only-arena 0.15 plays a world
whose regulars play more other modes (15% of them nothing but Arena, not 40%): its lists hold about as many places back
to the season's start as HN1's measured on 26 Nov (see server/README.md). About half a minute a case.
"""

import argparse
import bisect
import collections
import importlib.util
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))


def main():
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("--rules", help="another ingest.py to play")
    parser.add_argument("--block", type=int, default=200, help="what collectors and the policy allow a list (200)")
    parser.add_argument("--days", type=int, default=14)
    parser.add_argument("--installs", type=int, default=2)
    parser.add_argument("--only-arena", type=float, help="share of regulars who play nothing but Arena (0.4)")
    args = parser.parse_args()
    if args.rules:              # loaded as "ingest" before simulate.py imports it, so that one is played
        path = os.path.join(args.rules, "ingest.py") if os.path.isdir(args.rules) else args.rules
        spec = importlib.util.spec_from_file_location("ingest", path)
        sys.modules["ingest"] = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(sys.modules["ingest"])
    import simulate
    simulate.FLIGHT, simulate.T_MATCH = 1, 0.08
    if args.only_arena is not None:
        simulate.PURE = args.only_arena
    started = time.time()
    world = simulate.World(int(simulate.NOW) + 2 + args.days, 550, 1)
    before, scale, now, _ = simulate.replay(world, simulate.REAL_MATCHES_A_DAY, show=False)
    installs = [simulate.Install("collector-%d" % number, simulate.FAST, 0.83 * scale, args.block, simulate.HOUR)
                for number in range(args.installs)]
    changes = {"DEFAULT_BLOCK": args.block, "PLACES_PER_ROUND": 4000} if args.block > 200 else {}
    rules = simulate.Endpoint(world, installs, changes)
    rules.take_over(before, installs[:3])
    counts = collections.defaultdict(collections.Counter)
    end = simulate.NOW + args.days
    simulate.collect(rules, installs, now, end, counts)
    fresh, reached, new = rules.db.execute("SELECT SUM(fresh), SUM(reached), SUM(new) FROM size_days").fetchone()
    states = dict(rules.db.execute("SELECT back, COUNT(*) FROM players WHERE checked_at > 0 GROUP BY back"))
    seconds = sum(day["seconds"] for day in counts.values())
    fetches = max(1, sum(day["fetches"] for day in counts.values()))
    places = sum(day["places"] for day in counts.values())
    played, got = sorted(world.mtime), sorted(rules.held_at.values())
    share = lambda moment: bisect.bisect_right(got, moment) / bisect.bisect_right(played, moment)
    october = [match for match, when in enumerate(world.mtime) if when < 25]
    meanwhile = [match for match, when in enumerate(world.mtime) if simulate.NOW + 2 <= when < end - 2]
    waits = sorted(24 * (rules.held_at[match] - world.mtime[match]) for match in meanwhile if rules.held[match]) or [0]
    print("%s, up to %d a list, %d installs, %d days (%.0f s to play)" % (
        "rules of " + args.rules if args.rules else "rules now", args.block, args.installs, args.days, time.time() - started))
    print("  client seconds a new match %.2f (lists %.2f of it); listed places a new match %.1f" % (
        seconds / fetches, places * simulate.T_PLACE / fetches, places / fetches))
    print("  lists fetched afresh %d, back to the season's start %d; new matches a list %.1f; lists by state at the end %s"
          % (fresh or 0, reached or 0, (new or 0) / max(fresh or 0, 1), states))
    print("  season held after 3, 7, %d days: %.1f%% %.1f%% %.1f%%; 6-31 Oct held at the end %.1f%%" % (
        args.days, 100 * share(simulate.NOW + 3), 100 * share(simulate.NOW + 7), 100 * share(end),
        100 * sum(rules.held[match] for match in october) / len(october)))
    print("  matches played meanwhile held %.1f%%, hours until held: half %.1f, 9 in 10 %.1f" % (
        100 * sum(rules.held[match] for match in meanwhile) / max(len(meanwhile), 1), waits[len(waits) // 2],
        waits[int(len(waits) * 0.9)]))
    rules.close()


if __name__ == "__main__":
    main()
