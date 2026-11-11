# Collector ingest

`ingest.py` tells collectors what to read and receives the matches they send. It is a
separate process with its own SQLite file, so load here cannot slow the website.

**Not deployed anywhere yet.**

## How it works

- **Collectors are trusted.** Each has a key. The first copy of a match is kept and the
  match is never asked for again. There is no second opinion.
- **No match is fetched twice.** A collector reports which Arena matches it saw in a
  player's match list. Those not held go on a "wanted" list and are handed out, each to one
  collector at a time for 15 minutes. If that collector disappears, the match goes to
  another. A collector never takes on work while its owner is playing, sends what it has
  the moment a game starts, and drops work it held past the 15 minutes. A match nobody
  could fetch after 6 tries is set apart and not asked for again.
- **Nothing from before the season.** Lists are read back to the season's start only, and
  an older match that is sent anyway is not kept. The start is 2026-10-06 UTC until
  changed with `policy --season-start`. Moving it earlier has every list read so far read
  again back to the new date, when there is room.
- **Whose list to read next:**
  1. Matches on the wanted list go out first, newest first. While there is a full load of
     them, no new lists are started.
  2. Then the top of the lists that are most overdue. A player never read yet is due at
     once, and moves earlier with every further match they are seen in, so the players in
     the most matches are read first.
  3. A list is read one page (20 matches) at first, then three pages at a time for as
     long as that turns up matches not held. After a stretch with nothing new, the older
     part is set aside and read only when steps 1 and 2 leave room. Nothing is skipped
     for good.
  4. A player is read again when about 8 new matches are expected, judging by how many
     the last read found: between 1 hour and 7 days later. A read that fails is tried
     again after 10 minutes, then 20, 40 and so on.
- **Out-of-date reports change nothing.** Each stretch of a list is handed out with a
  token, and the list's progress is only taken from the report carrying the current token.
  A report that arrives after its stretch went to someone else still adds the matches it
  saw to the wanted list, but cannot move the list's progress backwards or forwards.

In a simulation (1,500 matches, three collectors), every match was captured and none was
fetched twice. Client requests per match captured:

| | 90% held | all held | reading every list in full |
| --- | --- | --- | --- |
| Lists mostly Arena (300 players) | 1.5 | 2.2 | 2.9 |
| Arena a small share (1,500 players) | 2.8 | 4.4 | 8.3 |

Finishing the set-aside older parts afterwards costs about as much as reading every list in
full, but that only happens when there is nothing fresher to do.

## What is still checked

Trust covers what a match says, not its size or shape:

- The key is checked before the body is read. Keys are stored only as hashes.
- Limits: 2 MB per request (4 MB after decompression), 50 matches per upload, 300 KB per
  match, 60 requests a minute and 60,000 matches a day per key.
- Every match must be an Arena record with sane ids, times and participant lists. Others
  are refused with a reason; the collector keeps them and offers them again on its next run.
- `disable-key` stops a contributor. `purge` deletes everything they sent and puts those
  matches back on the wanted list, so other collectors fetch them again.

## Endpoints

| | |
| --- | --- |
| `GET /v1/policy` | Whether to collect, the pace, a short message, the season's start. |
| `POST /v1/work` | Takes match-list reports; answers with matches to fetch and lists to read. |
| `POST /v1/games` | `{"games": [...]}`, gzip allowed. Answers with accepted / duplicates / skipped / rejected. |
| `GET /healthz` | No key needed. |

## Commands

```
python server/ingest.py add-key NAME          # prints the key once
python server/ingest.py keys                  # contributors and how much each sent
python server/ingest.py disable-key NAME
python server/ingest.py purge NAME
python server/ingest.py policy --season-start 2026-10-06
python server/ingest.py policy --off          # every collector pauses within 10 minutes
python server/ingest.py policy --on --delay 3 --message "thanks!"
python server/ingest.py stats                 # per server: matches held, still to fetch, players to read
python server/ingest.py export out.jsonl.gz --platform HN1
python server/ingest.py serve                 # development only
```

The database is `server/data/ingest.db`, or the path in `INGEST_DB`.

## Deploying (when approved)

Run under gunicorn on its own port, behind nginx with its own limits:

```
INGEST_DB=/var/lib/arenasweats-collector/ingest.db gunicorn -w 2 -b 127.0.0.1:8870 --chdir server ingest:app
```

```nginx
limit_req_zone $binary_remote_addr zone=collector:10m rate=120r/m;

location /collector/ {
    limit_req zone=collector burst=30 nodelay;
    client_max_body_size 2m;
    proxy_pass http://127.0.0.1:8870/;
}
```

Collectors then use `"server": "https://arenasweats.lol/collector"`.

The per-key request limit is counted per gunicorn worker, so the nginx limit is the real
ceiling.

## Size

A real OCE match record is about 6 KB compressed (54 KB raw). At 100,000 matches a day
that is about 600 MB a day in `games`, so records should be deleted once they have been turned into
ArenaSweats' own format. That step does not exist yet.

## Not built yet

- Turning the held records (the client's format) into ArenaSweats' own match format and
  feeding rankings.
- Deleting held records after that.
