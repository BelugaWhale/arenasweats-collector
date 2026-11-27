# Collector ingest

`ingest.py` tells collectors what to read and receives the matches they send. It is a
separate process with its own SQLite file, so load here cannot slow the website.

Live at `https://arenasweats.lol/collector/` on the website server.

## How it works

- **Collectors are trusted.** Each install has its own key, which it gets from
  `POST /v1/register` the first time it runs (at most 10 a day per internet address, as many homes
  in China share one, and 500 a day in all; `policy --signups off` closes sign-ups). Keys can also be made by hand with
  `add-key`. The first copy of a match is kept and the match is never asked for again. There
  is no second opinion.
- **No match is fetched twice.** A collector reports which Arena matches it saw in a
  player's match list. Those not held go on a "wanted" list and are handed out, each to one
  collector at a time for 15 minutes. If that collector disappears, the match goes to
  another. A collector never takes on work while its owner is playing, sends what it has
  the moment a game starts, and drops work it held past the 15 minutes. A match nobody
  could fetch after 6 tries is set apart and not asked for again.
- **Nothing from before the season.** Lists are read back to the season's start only, and
  an older match that is sent anyway is not kept. The start is 2026-10-06 UTC until
  changed with `policy --season-start`. Moving it earlier makes every list read so far due
  again at once, back to the new date.
- **A list is read in one request, and the first request decides how much of it a client
  gives.** Measured on a real League client (OC1, 25 Nov 2026): the first request for a
  player's match list decides how many of their latest matches (all modes together) that
  client will give. Asked for 20, 50, 100 or 200, it listed 20, 50, 100 and 200, going back
  to mid-October for 100. It keeps a copy and answers every later request for that player
  from the copy, in under 0.2 s, whatever places are asked for, also minutes later. A list
  fetched afresh took about 0.25 s for each match listed. So reading a page of 20 and then
  the next page, as collectors did up to 0.3.7, never got past the first 20: on HN1, of the
  51,865 reads that went on past the first page, 51,619 found nothing there, and the extra
  pages were 31% of all client requests until 25 Nov (156,600 of 510,000).
  - **A Tencent client lists 200 at most.** Measured on 27 Nov 2026 from the live endpoint:
    asked for 540-1,000, every read fetched afresh listed 200 at most (HN1 asked for 740: 131
    reads, 26,200 listed; HN10 asked for 1,000: 97 reads, 19,400; TJ100 asked for 540: 39
    reads, 7,592). With the first request fixing the client's copy, no list can be read
    further back than its latest 200 matches (all modes), by anybody. So nobody is asked
    for more (`LIST_CAP`), and no list is handed out again to be read further back (the
    `back` 3 of 26 Nov is gone; the lists left at it became 2, or 0 when their read listed
    fewer than 200).
  - A collector from 0.3.8 on says the most it can ask for in one request (`block`: 200;
    1,000 from 0.3.9) and how many lists it has room for (`lists`). Each list it is handed
    says how many matches to ask for (`count`). A collector up to 0.3.7 is handed one page
    of 20, as before.
  - `policy --block N` sets the most any collector is asked for (200, which is also the
    most it can be; a setting made before 27 Nov above that counts as 200). It is in force
    from the next request for work, with no restart: `--block 20` is one page for everybody
    again. Raising it has the lists that one request listed fewer matches of read in one
    request again (by another install), which goes further back.
  - How many are asked for. Every listed match costs the volunteer's client about 0.08 s,
    whatever its mode:
    - a list never read in one request: all one request gives (the block: 200);
    - a list read in one request before: what should have come in since its last read and
      half as many again, 20 at least.
  - Only a read whose oldest match is from before the season counts as back to the
    season's start (`back` 1). Any other read in one request is as far back as one request
    goes (`back` 2, with how many it listed in `depth`). A read of 200 whose matches are all
    newer than the last read stays at that too: no request reaches the ones in between. A
    shorter one is read in one request again (`back` 0), by another install. A client
    answers every later request for a list from the copy it made at the first, so a list
    is not handed for a read in one request to the install that read it last for a day
    (`COPY_SECONDS`: that also keeps a list a collector up to 0.3.7 read as a page of 20 off
    that install's client). A failed read, an empty list, or an answer from the client's
    saved copy (10 matches or more in under 0.3 s) is no read at all: it is tried again
    after 10 minutes, then 20, 40 and so on (and that install keeps away from it for a day).
  - While a client keeps its copy of a list it cannot fetch that list again, not even for
    what is new; only another collector can. How long it keeps it is not known.
    `size_days` (below) shows what clients give, how long they take and how often a copy
    answers.
- **Whose list to read next:**
  0. ArenaSweats Events games that the rankings server is waiting for go out before anything
     else, until they are held here, every 5 minutes (one may not have ended yet), however
     many tries that takes. The top of the lists of the players in them is read before
     any other list, each every 10 minutes, until the game is held. Only the rankings
     server's relay key can name these games (`relay urgent`, below); collectors cannot.
  1. Matches on the wanted list go out first, newest first.
  2. Then lists read before that are due again, the most overdue first. They show what was
     played since their last read, so with step 1 new matches keep arriving while older
     ones are caught up on. One place of the 10 handed out per request is kept for step 3,
     and while there is a full load of matches to fetch, only these lists go out. For a
     collector from 0.3.8 on these are the lists read in one request before; the others are
     read in step 3, which shows what is new at their top as well.
  3. Then lists not read in one request yet (never read, or only as a page of 20), the ones
     whose latest 200 matches should show the most Arena matches not held yet first
     (`worths`, judged again every 10 minutes for each server):
     - how far back 200 matches reach: 200 over the matches a day of the lists read in one
       request of players in about as many held matches (1, 2-3, 4-7, ... 512 or more; the
       middle of 30 or more, else the nearest such band's, else the server's, all servers',
       8 a day), to the season's start at most. Lighter players' lists reach further back:
       on HN1 on 27 Nov 200 matches went back 50 days for players in 32-63 held matches, 39
       for 64-127, 35 for 128-255, 18 for 256-511 and 12 for more;
     - each day in that reach counts for the share of its matches not held yet (from
       `seen_days`, this week's reads; a day not judged takes the nearest judged day
       before it). On HN1 on 27 Nov 6-15 Oct were 12-18% held, 16-31 Oct 23-66%, 1-9 Nov
       71-96% and the days since 91-100%;
     - times the player's Arena matches a day: their held matches spread over the season
       as it is held, but at most 80% of their matches a day (of the matches reads of 200
       listed on HN1 and HN10 on 25-27 Nov, 74-85% were Arena).
     Players are looked up by their held matches, in that order, one index look-up each;
     players in a number of held matches not judged yet come after, the ones in the most
     first. A collector up to 0.3.7 cannot see further back than a page and gets the
     players never read, the ones in the most held matches first.
     Until 27 Nov the order was the most held matches first. Under the cap of 200 those are
     the worst first reads: they play the most, so their 200 reach back only 12-17 days,
     which are held already. On 27 Nov such reads found 4.6 (HN1) and 6.2 (HN10) new Arena
     matches each. Played on that day's players table (480,000 players), this order puts
     players in about 60-130 held matches first on HN1 (their 200 reach back 40-52 days),
     where the old one put players in 254-374. Judged the same way, the next 1,000 reads
     hold about 38 (HN1) and 23 (HN10) new matches each, against about 5 under the old order
     on HN1. Checked on the reads of 25-27 Nov, whose paces are known, the way of judging
     gave 6-41 new matches a read where 5-39 were found; judged from held matches alone
     (as for players never read) it gave up to twice what was found. So expect fewer than
     38, but several times what the old order found.
  4. A list is due again when about 10 more Arena matches are expected in it
     (`TARGET_SEEN`), judging by how many its last read showed and how long those took to
     come in: between 1 hour and 30 days later. One that showed no Arena match comes back
     after 30 days. A collector up to 0.3.7 that finds nothing newer than at the last read
     cannot tell whether its client answered from a saved copy: that is not counted as a
     read, and the list is tried again after as long again as since its last read.
- **What the reads found is counted.**
  - `size_days`, per server, day and number of matches asked for (collectors from 0.3.8
    on): lists handed out, failed, answered from a saved copy, fetched afresh; and of the
    ones fetched afresh how many listed fewer than asked, how many matches they listed, how
    many reached the season's start, how many days back their oldest match was, and how
    long the client took (answers of up to 1, 2, 5, 10, 20, 40, 80 seconds, and longer;
    and all of them together, `took_ms`). They also count the Arena matches those reads
    showed that no list had shown before. `stats` prints three days of it in one line a
    size (sizes up to 20, 100, 200, 500 and 1,000 together, as "lists of 60-100 places"),
    `relay status` seven days under `reads`, `sizes`, one entry a size. Whether a server's
    clients give more than 20 is read off it at once:
    "lists of 200 places: ... 950 fetched afresh; those listed 196 matches each (40 fewer
    than asked), back 31 days" is a client that gives what it is asked for; "listed 20
    matches each (950 fewer than asked)" is one that does not, and then `policy --block 20`
    saves the asking. Many failed: it refuses or times out at that size. Many from the
    saved copy: copies live long, and lists come back to collectors that read them before.
    Once a server is caught up, long lists show few new matches each: then a smaller
    block (60) spares the clients.
  - How far each server's lists are from being read, in `stats` (four lines a server) and
    in `relay status` under `reads`, `progress`: lists seen back to the season's start
    (`reached`), as far back as one request goes (`as_far`), and not read in one request
    yet (`not_back`; of them `not_back_regulars`, of players in 20 or more held matches).
    Then the places collectors would be asked for to read those once each (`places_left`,
    `places_left_regulars`: 200 each at most), what a listed match took the clients in the
    last 3 days (`seconds_a_place`), so the client hours (`hours_left`,
    `hours_left_regulars`), the client hours a day lists took in those 3 days
    (`list_hours_a_day`; only the days with timed reads count, today as far as it has
    gone) and the days the regulars' lists take at that (`days_left_regulars`). Then what
    those reads should show, as step 3 above judges it: the Arena matches not held yet a
    list for the next 1,000 handed out (`worth_next`) and in all of them (`worth_left`). And
    `season_held`: for each day played, the share held of the matches this week's list
    reads showed that were played 2 days or more before the read (see `seen_days`),
    averaged over the days (each counted alike: lists show the latest days most, and those
    are held most; on 26 Nov HN1's were 79% held, its days 55% on average). An estimate
    that leans high. Where nothing was measured the value is null.
  - `read_days`, per server and day: lists handed out and the places asked for in them,
    reads reported, failed and from a saved copy, matches listed, lists that reached the
    season's start, the Arena matches shown, and how many of those were held already or
    had not been seen in any list.
  - `seen_days` splits the matches that were at least two days old when a list showed them
    by the day they were played: the share of them held already says how complete that day
    is (it leans high, as the lists read belong to players found through held matches).
- **Out-of-date reports change nothing.** Each list is handed out with a token, and its
  progress is only taken from the report carrying the current token. A report that arrives
  after the list went to someone else still adds the matches it saw to the wanted list,
  but cannot move the list's progress backwards or forwards.

### What the order and the sizes were tried on

`python server/tools/simulate.py` (about half an hour; `--quick` for fewer cases) plays
`ingest.py` itself with made-up collectors on a made-up world of players shaped like HN1's:
how much they play, who queues together, lists that fill with all modes. Each made-up
collector has a League client that behaves as the real one measured on 25 Nov: it gives as
many matches as the first request for a player asks for, answers from its copy after that,
and takes as long as the list is long. Before that it replays the rules until 25 Nov on the
requests HN1's collectors really made on 13-24 Nov and prints the result beside what was
measured, which is how the world was fitted. A small run of it is one of the tests.

#### How many places a read asks for

From 26 Nov first reads were sized to reach back to the season's start, up to 1,000 (played
with `server/tools/sizing.py`, now removed). A Tencent client lists 200 at most (27 Nov, see
above), so a first read asks for all one request gives and there is nothing left to size.

In the made-up world (below; `server/tools/compare.py`, which also plays an older ingest.py:
14 days from 25 Nov, two collectors in fast mode, one request at a time, 0.08 s a match), the
rules until 26 Nov (ingest.py at 6f146eb) against the rules now. Two worlds: the one
fitted on 25 Nov, and one whose regulars play more other modes (15% play only Arena, not 40%),
so that its lists of players in 100 or more Arena matches hold 150, 212, 338, 572 and 936
places back to the season's start (10th to 90th percentile; HN1's measured: 148, 235, 338,
498, 700). Its client gives as many as it is asked for; a Tencent client gives 200 at most, so
the rows of 1,000 do not hold there. (The order of first reads of 27 Nov, step 3 above, was
not played in this table.)

| | client s a new match | season held after 3, 7, 14 days | a new match held after: half, 9 in 10 |
| --- | --- | --- | --- |
| up to 200, until 26 Nov | 4.43 / 4.34 | 55, 72, 97% / 53, 69, 96% | 16 h, 52 h / 18 h, 53 h |
| up to 200, now | 4.29 / 4.15 | 88, 99, 99% / 80, 98, 99% | 6 h, 19 h / 5 h, 20 h |
| up to 1,000, until 26 Nov | 8.79 / 8.79 | 48, 55, 65% / 45, 52, 63% | 39 h, 116 h / 36 h, 117 h |
| up to 1,000, now | 4.34 / 4.19 | 85, 97, 99% / 85, 96, 99% | 12 h, 45 h / 11 h, 38 h |

What it says: the order (the most valuable lists first, lists not read back yet only in step
3) is most of the gain at 200. Raising the policy to 1,000 under the rules until 26 Nov would
have asked nearly every regular for 1,000 places, most of them from before the season. Under
the rules now 1,000 costs about what 200 does per new match and fills the season about as fast;
in this world matches of early October are also in enough shorter lists that reach that far.
Whether the real servers are like that is not known: on HN1, early October was 7-16% held on
26 Nov after 1,400 reads of 200.

#### The tables until 26 Nov

At HN1's size, from 25 Nov, when 42% of the season was held. Four collectors at the normal
pace unless said; a client keeps its copies for an hour unless said (these were played with the
rules until 26 Nov):

| | days until 50%, 80%, 95% of the season is held | held after 30 days | a new match is held after: half of them, 9 in 10 |
| --- | --- | --- | --- |
| rules until 25 Nov | 1.8, 9.5, never | 88% | 11 h, 41 h |
| one page of 20 (collectors up to 0.3.7) | 1.2, 7.5, never | 89% | 3 h, 13 h |
| up to 200 in one request (0.3.8) | 1.5, 6.8, 9.8 | 99% | 7 h, 29 h |
| the same, clients keep copies until restarted, every 3 days | 1.5, 6.8, 9.8 | 99% | 7 h, 29 h |
| the same, and they are never restarted | 1.2, 5.8, 8.2 | 98% | 11 h, 43 h |
| up to 100 | 1.0, 5.0, 9.0 | 99% | 5 h, 20 h |
| up to 60 | 1.2, 5.2, 9.0 | 99% | 4 h, 18 h |
| up to 200, 12 collectors | 0.5, 1.5, 2.0 | 100% | 3 h, 17 h |
| a new server like HN10, one collector in fast mode, one page of 20 | 5.0, 10.0, never | 87% | 3 h, 13 h |
| the same, up to 200 | 4.5, 8.0, 10.2 | 100% | 6 h, 20 h |
| the same, its client never restarted | 4.5, 7.8, 10.0 | 95% | 23 h, 85 h |
| the same, three collectors in fast mode | 1.5, 2.5, 3.0 | 100% | 3 h, 16 h |

What it says, and how far to trust it:

- With one page of 20, a match can only be had while it is among the latest 20 of one of
  its 18 players: 84% of the season on 25 Nov, which is why those lines stop under 90%.
  With 100 or more, every match of the season was still among the latest matches of one of
  its players. That is the made-up world. How much players play other modes was fitted to
  reads of 20 only, so how far back a real list of 200 goes is not known until `size_days`
  shows it (the days back and how many reached the season's start).
- Fetching the matches is most of the work, so the number asked for decides how far back a
  read sees, not how fast the season fills: 60, 100 and 200 fill it about as fast here, and
  1,000 (if a client gave that many) more slowly, with nothing gained. 200 is asked for
  because it is what collectors can ask for and nobody knows yet how far 100 goes back.
- Long lists delay new matches a little while a server is being caught up on, and keep
  collectors busy after that, reading players whose matches are held by then (28 client
  seconds for each new match at the end, against 14 with pages). Then `policy --block 60`
  spares the clients.
- A client that keeps its copies until restarted costs nothing if it restarts every few
  days. One that never restarts cannot read a list twice: with a single collector on a
  server, 93% of the new matches were held, most of them a day late.
- The times are the weakest part. A page of 20 was taken to cost a client 1.7 s (inferred
  from how fast collectors went on the Chinese servers), a match 1.5 s. If a client there
  takes as long for a list as the one measured on OC1 (0.25 s for each match listed, so
  50 s for 200), reading lists becomes most of the work: with lists of 200 the four
  collectors on HN1 need 19 days to 80% and 28 to 95%, and new matches come a day and a
  half late; with lists of 100, 9.5 and 22 days, and 10 hours. One collector in fast mode
  (three lists at once) on a new server is hardly slowed: 14 days to 95%. The middle
  answer time in `size_days` says which it is on the first day; if lists of 200 take over
  20 seconds, `policy --block 100`. (`simulate.py --place-seconds 0.25` plays that.)
- HN1 has about 10,000-11,000 Arena matches a day (0.3% of its match-id counter), not the
  30,000-39,000 that 1% of the counter gives. Only near 11,000 does the replay give the fall
  in new matches per list (4.0 to 1.1) and the age of the matches received (22 days on
  24 Nov) that were measured; at 20,000 it gives 2.5 per list and 2 days. The days of
  12-18 Nov, of which lists read a week later showed 96-100% as held, have 8,500-12,800
  held each. Those lists were pages of 20, so this leans on players who play little; the
  share held in `seen_days`, once lists of 200 are read, is the better measure.

## What is still checked

Trust covers what a match says, not its size or shape:

- The key is checked before the body is read. Keys are stored only as hashes.
- Limits: 2 MB per request (4 MB after decompression), 50 matches per upload, 300 KB per
  match, 60 requests a minute and 220,000 matches a day per key (a fast-mode collector reads about
  100,000 a day and can never pass 216,000; one at the normal pace about 30,000).
- Uploads wait (503, collectors keep their matches and retry) while more than 300,000 matches are
  waiting for the rankings server or the disk has less than 5 GB free, so a stalled relay or a
  flood cannot fill the disk the website uses.
- Every match must be an Arena record with sane ids, times and participant lists. Others
  are refused with a reason; the collector keeps them and offers them again on its next run.
- `disable-key` stops a contributor. `purge` deletes everything they sent and puts those
  matches back on the wanted list, so other collectors fetch them again.

## Download and last seen

- `https://arenasweats.lol/collector/download/ArenaSweatsCollector.exe` serves the newest release
  straight from the web server (GitHub downloads are unreliable in China). `deploy/fetch_download.sh`
  keeps it equal to the newest GitHub release, checking the release's SHA-256, from the normal
  user's cron every 30 minutes.
- Every request records when that install was last seen and what it was doing
  (`X-Collector-State`: starting (signed up, no League client found yet; collectors up to 0.3.7
  say collecting here), collecting, paused, waiting_for_client, client_unreachable,
  client_failing, switched_off, and as it stops error or stopped; with `_fast` added when its
  user turned the beta fast mode on, such as collecting_fast). Collectors after 0.3.7 also say
  why they are not collecting (`X-Collector-Detail`), as a short fixed code, never a path, a
  name or an error message:
  - paused: `phase_` and the client's state, such as `phase_endofgame` or `phase_inprogress`,
    or `phase_status_` and the status when the client would not say (0: no answer).
  - waiting_for_client: `no_client` (no LeagueClient program running), with `.launcher` when
    WeGame, TCLS or the Riot Client is running, and `.lock_` and a result when a lockfile was
    found anyway.
  - client_unreachable: `path_seen` or `path_hidden` (whether Windows says where the client
    runs from; hidden when it runs as administrator), then `lock_` and the result for the first
    lockfile found and `cmd_` and the result for the client's command line. Results: `none`
    (nothing found), `login` and the client's status (it answered, nobody logged in yet, such
    as `login404`), `forbidden` (it answered 403: something refuses this program; China's
    anti-cheat may do that to programs it does not know, unconfirmed), `empty` (an empty
    lockfile: the Tencent client leaves it so), `riotclient` (the Riot Client's lockfile, not
    the League client's), `badfile` (another lockfile without a port and password), `refused`
    (nothing listening: a lockfile left from an earlier start), `reset`, `tls`, `denied`,
    `gone`, `oserror`, `failed`. `path_hidden.lock_none.cmd_none` or
    `path_seen.lock_empty.cmd_none` means Windows does not let a normal program reach the
    client: the collector has to run as administrator.
  - collecting: `found_` and the step of the collector's own search that found the client in
    use: `running` (the running client's folder), `installer` (Windows' list of installed
    programs), `usual` (Riot's or Tencent's records of the install, or a usual place:
    `英雄联盟...`, `LOL` or `League of Legends` in a drive's top, `WeGameApps`, `腾讯游戏`,
    `Program Files`, ...), `scan` (the drives searched), `cmdline` (the client's command line),
    `typed` (only the folder the person typed in led to it). Never the folder itself.
  - Administrator rights (asked at start), at the end of the waiting_for_client and
    client_unreachable codes and as the detail of starting: `admin` (this copy runs as
    administrator), `admin_no` (asked, and answered no or nothing; or started with
    `--no-admin`), `admin_declined` (answered yes, but Windows' permission box was declined or
    failed). A yes is remembered, so that install's next copies report `admin`.
  - error: `error.`, the kind of error and the collector's function it happened in, such as
    `error.keyerror.work_round` (the details are in the volunteer's collector.log); stopped:
    `ctrl_c`; closed: `window`, `logoff` or `shutdown` (sent in the seconds Windows gives a
    program whose window is closed).

  Until an install has collected anything, a change is sent at once (at most every 10 seconds),
  so an install that is closed within a minute still shows why. After that, collectors check in
  every 10 minutes, also while paused. `ingest.py keys` shows the last state and detail
  (`why: ...`), and `relay status` reports installs seen in the last hour and day by state.
- Each change of what an install says (state, detail or version) is also kept, with its time, in
  `contributor_history`: the latest 100 changes an install, for 30 days (it was started from what
  each install said last, at the time it was last seen). `ingest.py keys --history NAME` prints one
  install's. `ingest.py newcomers [--days 7]` shows how far the installs that signed up in those
  days got: whether they answered the administrator question (the last answer they told: `admin`,
  `admin_no` or `admin_declined`), found the League client (`found_` and the step), sent a first
  match, and were still collecting an hour after they began; and for the others what they said
  last, or before they closed (`closed window` after `waiting_for_client no_client.admin_no`
  is somebody who closed the collector while it waited for a League client it never found).
  `relay status` gives the counts of the last 7 days under `newcomers`: `signed_up`, `admin`
  (each answer), `found_client`, `first_match`, `collecting_an_hour` and `gave_up_at` (what the
  others said last, or before closing, with how many said it; the 8 most common; `never seen`:
  signed up and never got in touch again).
- Every 5 minutes while it collects, a collector newer than 0.3.7 sends a few numbers about its own pace with
  one of its requests (`X-Collector-Pace`): how many requests it made to the League client, how many
  failed or were answered 429, how long the client took to answer (matches, and match lists by the
  number of matches asked for, with how many matches they listed and how many answers came from the
  client's saved copy), how long requests to ArenaSweats took, the seconds it was paused, at the normal
  pace after the client pushed back in fast mode, or had nothing to read, and how many requests fast mode
  has on their way at once. Nothing but numbers. They are kept for 14 days; `ingest.py pace` shows them.

## Endpoints

| | |
| --- | --- |
| `POST /v1/register` | No key needed. A new install gets its own key (201 `{"key", "name"}`). |
| `GET /v1/policy` | Whether to collect, the pace, whether fast mode is allowed, its shortest gap and how many requests it may have on their way at once, a short message, the season's start. |
| `POST /v1/work` | Takes match-list reports; answers with matches to fetch and lists to read. |
| `POST /v1/games` | `{"games": [...]}`, gzip allowed. Answers with accepted / duplicates / skipped / rejected. |
| `GET /v1/me` | What this install has done, for the collector's own window (below). |
| `GET /healthz` | No key needed. |

`GET /v1/me` answers, about the install whose key it carries and nobody else's:

| field | |
| --- | --- |
| `days` | 30 entries, oldest first, the last one today (UTC days): `{"day": "2026-11-26", "matches": 412}`, the matches this install sent that day (each counted once, on the day it arrived). |
| `total` | All the matches this install has sent. |
| `server` | The server it last asked for work on, a platform code such as `"HN1"`; `null` until it has asked. |
| `season_start` | The season's first day, `"2026-10-06"`. |
| `server_held` | Matches held of that server played since the season began, from every install. |
| `server_held_share_estimate` | An estimate between 0 and 1 of the share of that server's season held: for each day played, the share held of the matches this week's list reads showed that were played at least 2 days before, averaged over the days (`season_held` in `relay status`; it leans high). `null` until lists there have been read. |
| `server_helpers` | Installs that sent matches of that server in the last 24 hours (this one included). |
| `servers` | China's eight servers, the fewest helpers first: `{"server": "GZ100", "helpers": 0}`, helpers as in `server_helpers`. Platform codes only; the collector names them. |

The install's own numbers come from a count kept as matches arrive (`contributor_days`); the server's
are worked out again every 5 minutes. It may be asked once a minute.

## Commands

```
python server/ingest.py add-key NAME          # prints the key once
python server/ingest.py keys                  # contributors and how much each sent
python server/ingest.py keys --history NAME   # each change of what one install said it was doing
python server/ingest.py newcomers --days 7    # how far the installs that signed up in 7 days got, and where the others stopped
python server/ingest.py disable-key NAME
python server/ingest.py purge NAME
python server/ingest.py policy --season-start 2026-10-06
python server/ingest.py policy --off          # every collector pauses within 10 minutes
python server/ingest.py policy --fast off     # fast-mode collectors go back to the normal pace (--delay)
python server/ingest.py policy --fast-delay 1 # fast mode keeps at least 1 s between requests (default 0.4)
python server/ingest.py policy --fast-flight 2 # fast mode has at most 2 requests on their way at once (default 4, 1 = one at a time)
python server/ingest.py policy --signups off  # new installs can no longer get a key
python server/ingest.py policy --block 100    # collectors are asked for at most 100 matches of a list at once (20: a page)
python server/ingest.py policy --on --delay 3 --message "thanks!"
python server/ingest.py stats                 # per server: matches held, still to fetch, players to read
python server/ingest.py pace                  # the newest pace numbers installs sent (pace NAME --last 50: one install's)
python server/ingest.py export out.jsonl.gz --platform HN1   # records still held here
python server/ingest.py relay status          # what is waiting for the rankings server, uploads per hour
python server/ingest.py serve                 # development only
```

The database is `server/data/ingest.db`, or the path in `INGEST_DB`.

## Deployment

On the website server, as the normal user:

```
git clone https://github.com/BelugaWhale/arenasweats-collector.git ~/arenasweats-collector
python3.10 -m venv ~/arenasweats-collector-venv        # the server's python3.12 has no venv support installed
~/arenasweats-collector-venv/bin/pip install -r ~/arenasweats-collector/server/requirements.txt
mkdir -m 700 ~/arenasweats-collector-data
```

Then once as root: `sudo bash ~/arenasweats-collector/server/deploy/install.sh`. It installs the
`arenasweats-collector` service (gunicorn on 127.0.0.1:8870, capped at 600 MB and one CPU core),
adds an nginx rate limit and a `/collector/` location, and adds one `include` line to the site
config, which it backs up first and restores if `nginx -t` fails.

Admin commands on the server need the database path:

```
export INGEST_DB=~/arenasweats-collector-data/ingest.db
~/arenasweats-collector-venv/bin/python ~/arenasweats-collector/server/ingest.py stats
```

Updating: `git pull --ff-only` in `~/arenasweats-collector`, then
`sudo bash ~/arenasweats-collector/server/deploy/update.sh` (reinstalls the service and nginx files,
restarts the service, reloads nginx, and puts the nginx files back if nginx rejects them).

Collectors send to `https://arenasweats.lol/collector` unless their `config.json` says otherwise.

The per-key request limit is counted per gunicorn worker, so the nginx limit is the real ceiling.

## Certificate

The site's certificate is from Let's Encrypt (certbot, nginx plugin, `/etc/letsencrypt/renewal/arenasweats.lol.conf`;
names arenasweats.lol, www and euw-api; no CAA records). Collectors check it against the roots they carry (0.3.6:
ISRG Root X1 and X2; 0.3.7 adds Root YE and YR and Google's GTS Root R1 and R4) plus the roots Windows holds. Up to 0.3.4 they had
only Windows' roots, and a Windows that has never updated its roots holds none for websites at all (checked on
fresh Windows Server 2019 and 2022 images, the Windows 10 1809 and 21H2 code): only Microsoft's own roots and
old VeriSign ones that no longer issue. So no certificate from any CA is sure to reach a stuck 0.3.4.

**Optional gamble for stuck 0.3.4 installs:** a Google Trust Services certificate whose chain ends at the old
GlobalSign Root CA: of the roots a free certificate can lead to, the one a Chinese PC is most likely to hold
(Baidu's and Taobao's certificates lead to it too). It helps only PCs whose Windows happens to hold it; nobody can
tell how many. Steps, as root:

1. First release 0.3.7 and wait until `ingest.py keys` shows every install seen in the last days on 0.3.7 or
   later. Older collectors on a PC without GlobalSign's or Google's roots would lose the connection.
2. With gcloud, in any Google Cloud project: `gcloud services enable publicca.googleapis.com`, then
   `gcloud publicca external-account-keys create` (gives keyId and b64MacKey; use within 7 days).
3. On the web server:
   ```
   certbot certonly --nginx --non-interactive --agree-tos -m ADMIN_EMAIL --cert-name arenasweats.lol \
     -d arenasweats.lol -d www.arenasweats.lol -d euw-api.arenasweats.lol --key-type ecdsa --force-renewal \
     --server https://dv.acme-v02.api.pki.goog/directory --eab-kid KEY_ID --eab-hmac-key B64_MAC_KEY \
     --preferred-chain "GlobalSign Root CA" --deploy-hook "systemctl reload nginx"
   systemctl reload nginx
   certbot renew --dry-run --cert-name arenasweats.lol
   ```
   The paths in the nginx config stay the same. Check the chain with
   `echo | openssl s_client -connect arenasweats.lol:443 -servername arenasweats.lol -showcerts | grep -E ' s:| i:'`:
   leaf, WE1, then GTS Root R4 issued by GlobalSign Root CA.
4. Watch the nginx access log for `POST /collector/v1/register` answered 201 to `arenasweats-collector/0.3.4`.
   To go back: the same command with `--server https://acme-v02.api.letsencrypt.org/directory` and without the
   `--eab-*` and `--preferred-chain` options.

Risks: that chain ends by 2028-01-28 (the cross-signature and GlobalSign Root CA expire; Google may stop offering
it sooner), after which it ends at GTS Root R4, which 0.3.7 carries. Renewals keep coming from Google with the
stored account (90-day certificates, renewed by the certbot timer). Google's revocation addresses (o.pki.goog,
c.pki.goog) may be unreachable in mainland China; browsers do not need them, a few Windows programs may wait on
them. HSTS (one year, includeSubDomains, preload) only needs a valid certificate, so it is unaffected. If CAA
records are ever added they must name both `letsencrypt.org` and `pki.goog`. bot.arenasweats.lol keeps its own
certificate. Before switching to any other CA, release a collector that carries its root first.

## Relay to the rankings server

This server is only a buffer. The rankings server (Eagle) pulls every match from here and keeps it;
its address is never given to collectors.

- Every match received goes into the `outbox` table, filled by a trigger on `games` (the matches held
  before the relay existed were queued once, when it was added).
- Eagle logs in over SSH with a key that may only run `ingest.py relay` (the `command=`, `from=` and
  `restrict` options on its line in `~/.ssh/authorized_keys`). It asks for `export N`, stores the
  matches, reads them back to check them, then sends `ack` with their numbers. Only then do they
  leave the queue. A match handed out but never confirmed is simply handed out again.
- A confirmed match's record is emptied here 3 days later. The row in `games` stays (with an empty
  record), so the match is still known, never asked for again, and duplicates are still recognised.
- `relay status` gives what is waiting, the last pull and confirmation, and uploads per hour per
  server, for the health check.
- `relay urgent` takes `{"games": [{"platform", "game_id", "players": [PUUIDs]}]}` on standard input:
  the ArenaSweats Events games Eagle is waiting for (they have a result deadline), each with the
  players whose lists may show it. Each list replaces the last. Eagle sends it every minute, and a
  list it stops sending lapses after an hour. `relay status` gives when it was last sent
  (`last_urgent_at`) and how many of its games are not held yet (`urgent_not_held`).

The code on Eagle is `collector_ingest.py` in ArenaSweats-database.

## Size

A real match record is about 6 KB compressed (54 KB raw). Records are emptied 3 days after Eagle
confirmed them, so `games` holds about 3 days of records plus a small row per match ever received.
The freed space is reused by SQLite; the file does not shrink.
