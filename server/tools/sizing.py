#!/usr/bin/env python3
"""How many places list reads ask for, played on the lists a server has really read.

    INGEST_DB=~/arenasweats-collector-data/ingest.db python server/tools/sizing.py [--grid]

Every list read in one request that listed 150 matches or more tells how many places its player's list holds back to
the season's start (its pace, all modes). For each of them this plays how many places a first read asks for and, while
that falls short, the reads further back, under the rules until 26 Nov 2026 (20 places a held match, the list left at
that when it falls short) and under ingest.py's now (what similar lists needed, FIRST_QUANTILE; then the pace, times
SPARE_DEEP): places listed, reads, and how many get back to the season's start. A list's own players are left out of
the lists its first read is judged by (half the lists judge the other half). The database is only read.

A read further back is sized by the pace the read before measured, and that is off by more the fewer matches it
listed. How much is not measured: it is taken as a spread of sqrt(4 / listed + 0.15^2) (a game session of about
4 matches, and a 15% change in how much a player plays between the part read and the part not read yet).
--grid also plays other values of FIRST_QUANTILE and SPARE_DEEP.
"""

import argparse
import math
import os
import random
import sqlite3
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import ingest  # noqa: E402

A_PLACE, A_READ = 0.08, 0.2     # client seconds a listed match (measured 25-26 Nov 2026), and a read (taken)


def pages(places, ceiling):
    return min(ceiling, 20 * max(1, -int(-places // 20)))


def fitted(lists, quantile, days):
    """What list_rates gives, from these lists: (server, band) -> matches a day."""
    rates = {}
    for platform in {platform for platform, _, _ in lists}:
        here = [(ingest.band(games), need / days) for server, games, need in lists if server == platform]

        def pick(values):
            values = sorted(values)
            return values[min(len(values) - 1, int(quantile * len(values)))]
        whole = pick([rate for _, rate in here])
        for number in range(1, 11):
            values = [rate for one, rate in here if one == number]
            rates[platform, number] = pick(values) if len(values) >= ingest.FIT_MIN else whole
    return rates


def play(lists, days, rule, ceiling, quantile, spare, seed=1):
    random_numbers = random.Random(seed)
    halves = [lists[0::2], lists[1::2]]
    totals = dict(places=0, reads=0, reached=0, first=0)
    for number, half in enumerate(halves):
        rates = fitted(halves[1 - number], quantile, days) if rule == "now" else None
        for platform, games, need in half:
            asked = min(ceiling, 20 * games) if rule == "until 26 Nov" else pages(
                rates.get((platform, ingest.band(games)), ingest.DEFAULT_RATE) * days, ceiling)
            reads, places, reached = 1, asked, need <= asked
            totals["first"] += reached
            while rule == "now" and not reached and asked < ceiling:
                judged = need * math.exp(random_numbers.gauss(0, math.sqrt(4 / asked + 0.15 ** 2)))
                asked = pages(judged * spare + 20, ceiling)
                reads, places, reached = reads + 1, places + asked, need <= asked
            totals["places"] += places
            totals["reads"] += reads
            totals["reached"] += reached
    count = len(lists)
    seconds = (totals["places"] * A_PLACE + totals["reads"] * A_READ) / count
    return ("%5.0f places, %4.1f client s, %.2f reads a list; %3.0f%% back to the season's start (%3.0f%% at the first "
            "read)" % (totals["places"] / count, seconds, totals["reads"] / count, 100 * totals["reached"] / count,
                       100 * totals["first"] / count))


def main():
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("--grid", action="store_true", help="also other values of FIRST_QUANTILE and SPARE_DEEP")
    args = parser.parse_args()
    db = sqlite3.connect("file:%s?mode=ro" % os.path.abspath(ingest.db_path()), uri=True, timeout=5)
    now = ingest.clock()
    season = int(dict(db.execute("SELECT name, value FROM settings")).get(
        "season_start", ingest.date_ms(ingest.DEFAULT_SEASON_START))) // 1000
    days = (now - season) / 86400
    lists = [(platform, games, (now - season) / pace) for platform, games, pace in db.execute(
        "SELECT platform, games, pace FROM players WHERE pace > 0 AND depth >= 150")]
    if not lists:
        print("No list has been read 150 matches deep yet.")
        return
    print("%d lists read 150 matches deep or more; back to the season's start they hold %.0f matches on average" % (
        len(lists), sum(need for _, _, need in lists) / len(lists)))
    for ceiling in (200, 1000):
        print("Up to %d a list:" % ceiling)
        for rule in ("until 26 Nov", "now"):
            print("  %-14s %s" % (rule, play(lists, days, rule, ceiling, ingest.FIRST_QUANTILE, ingest.SPARE_DEEP)))
    if args.grid:
        print("Up to 1000, other values:")
        for quantile in (0.25, 0.5, 0.6, 0.75, 0.9):
            for spare in (1.1, 1.25, 1.4):
                print("  FIRST_QUANTILE %.2f SPARE_DEEP %.2f  %s" % (quantile, spare,
                                                                    play(lists, days, "now", 1000, quantile, spare)))


if __name__ == "__main__":
    main()
