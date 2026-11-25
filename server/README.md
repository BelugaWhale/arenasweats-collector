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
  - A collector from 0.3.8 on says the most it can ask for in one request (`block`: 200)
    and how many lists it has room for (`lists`). Each list it is handed says how many
    matches to ask for (`count`). A collector up to 0.3.7 is handed one page of 20, as
    before.
  - `policy --block N` sets the most any collector is asked for (200 until changed). It is
    in force from the next request for work, with no restart: `--block 20` is one page for
    everybody again. Raising it has the lists that were cut short read back again.
  - How many are asked for. A list not seen back to the season's start yet: 20 for every
    held match its player is in (200 from 10 held matches on, one page for somebody seen
    once); once a read has told how fast the list fills, enough for the whole season and
    half as many again. A list seen that far back before: what should have come in since
    its last read and half as many again, 20 at least.
  - Only a read whose oldest match is from before the season counts as back to the
    season's start. One that listed all it was asked for and still goes on, or fewer than
    it was asked for (the list's own end, or all that client gives), is left at that
    (`back` 2, with how many it listed in `depth`). A failed read, an empty list, or an
    answer from the client's saved copy (10 matches or more in under 0.3 s) is no read at
    all: it is tried again after 10 minutes, then 20, 40 and so on.
  - Not known until a client on a Chinese server has reported: whether it gives more than
    20, how long it takes, and how long it keeps its copy (minutes, or until it is
    restarted). While a client keeps its copy of a list it cannot fetch that list again,
    neither further back nor for what is new; only another collector can. `size_days`
    (below) shows all three.
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
     and while there is a full load of matches to fetch, only these lists go out.
  3. Then lists not seen back to the season's start yet: the players in the most held
     matches first, and of those the latest found. For a collector from 0.3.8 on that is
     every player never read and every player only ever read as a page of 20 (on HN1 on
     25 Nov: 181,000 never read, in 2.6 held matches on average, and 121,000 read as a
     page, in 30: so those come first). A collector up to 0.3.7 cannot see further back
     than a page and gets the players never read.
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
    long the client took (answers of up to 1, 2, 5, 10, 20, 40, 80 seconds, and longer).
    They also count the Arena matches those reads showed that no list had shown before.
    `stats` prints three days of it in one line a size, `relay status` seven days under
    `reads`, `sizes`. Whether a server's clients give more than 20 is read off it at once:
    "lists of 200 places: ... 950 fetched afresh; those listed 196 matches each (40 fewer
    than asked), back 31 days" is a client that gives what it is asked for; "listed 20
    matches each (950 fewer than asked)" is one that does not, and then `policy --block 20`
    saves the asking. Many failed: it refuses or times out at that size. Many from the
    saved copy: copies live long, and lists come back to collectors that read them before.
    Once a server is caught up, long lists show few new matches each: then a smaller
    block (60) spares the clients.
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

`python server/tools/simulate.py` (several minutes; `--quick` for fewer cases) plays
`ingest.py` itself with made-up collectors on a made-up world of players shaped like HN1's:
how much they play, who queues together, lists that fill with all modes. Each made-up
collector has a League client that behaves as the real one measured on 25 Nov: it gives as
many matches as the first request for a player asks for, answers from its copy after that,
and takes as long as the list is long. Before that it replays the rules until 25 Nov on the
requests HN1's collectors really made on 13-24 Nov and prints the result beside what was
measured, which is how the world was fitted. A small run of it is one of the tests.

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
  (`X-Collector-State`: collecting, paused, waiting_for_client, client_unreachable, client_failing;
  with `_fast` added when its user turned the beta fast mode on, such as collecting_fast).
  Collectors check in every 10 minutes even while paused. `ingest.py keys` shows it, and
  `relay status` reports installs seen in the last hour and day by state.

## Endpoints

| | |
| --- | --- |
| `POST /v1/register` | No key needed. A new install gets its own key (201 `{"key", "name"}`). |
| `GET /v1/policy` | Whether to collect, the pace, whether fast mode is allowed and its shortest gap, a short message, the season's start. |
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
python server/ingest.py policy --fast off     # fast-mode collectors go back to the normal pace (--delay)
python server/ingest.py policy --fast-delay 1 # fast mode keeps at least 1 s between requests (default 0.4)
python server/ingest.py policy --signups off  # new installs can no longer get a key
python server/ingest.py policy --block 100    # collectors are asked for at most 100 matches of a list at once (20: a page)
python server/ingest.py policy --on --delay 3 --message "thanks!"
python server/ingest.py stats                 # per server: matches held, still to fetch, players to read
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
