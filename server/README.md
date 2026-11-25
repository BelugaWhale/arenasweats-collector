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
  again at once, back to the new date (as far as its latest 20 matches go).
- **A player's list is their latest 20 matches, and no more.** The League client shows a
  player's latest 20 matches (all modes together). Asked for older places (`begIndex=20`
  and up) it answers with the same 20, except for the account that is logged in. Measured
  on 25 Nov 2026 on the 121,500 lists read on HN1 (HN10 and NA1 show the same):
  - of the 51,865 reads that went on past the first page, 51,619 found nothing there;
  - of the 154,600 matches that one list read alone can have found, 99.5% were among that
    player's 20 newest Arena matches: places 15 to 20 brought in about 6,900 matches each,
    place 21 brought 482 and place 23 brought 20;
  - 11 players' reads on HN1 and 1 on HN10 did bring in older matches (90 and 12 of them):
    on HN10, with one collector, that is its own account.

  So a list is read one page at a time (`LIST_PAGES`). Until that day the endpoint asked for
  three more pages whenever the first showed something new, which was 31% of all client
  requests on HN1 (156,600 of 510,000). And a match can only be found while it is among the
  latest 20 of one of its 18 players. A busy player's list turns over in a day or two;
  someone who stopped playing keeps theirs for good. What no list shows any more cannot be
  had.
- **Whose list to read next:**
  0. ArenaSweats Events games that the rankings server is waiting for go out before anything
     else, until they are held here, every 5 minutes (one may not have ended yet), however
     many tries that takes. The lists of the players in them are read from the top before
     any other list, each every 10 minutes, until the game is held. Only the rankings
     server's relay key can name these games (`relay urgent`, below); collectors cannot.
  1. Matches on the wanted list go out first, newest first. While there is a full load of
     them, no new lists are started.
  2. Then lists read before that are due again, the most overdue first.
  3. Then players never read: the ones in the most held matches first, and of those the
     latest found. While both kinds are waiting, each gets half of the 10 lists handed out
     per request (`NEW_PLAYERS_PER_ROUND`); when only one kind is, it gets them all.
  4. A list is due again when about 10 more Arena matches are expected in it
     (`TARGET_SEEN`: half of its 20 places), judging by how many its last read showed and
     how long those took to come in: between 1 hour and 30 days later. One that showed no
     Arena match comes back after 30 days. A read that fails is tried again after 10
     minutes, then 20, 40 and so on.
- **What the reads found is counted.** `read_days` has, per server and day: list pages
  asked for, reads reported and failed, the Arena matches they showed, and how many of
  those were held already or had not been seen in any list. `seen_days` splits the matches
  that were at least two days old when a list showed them by the day they were played:
  the share of them held already says how complete that day is (it leans high, as the
  lists read belong to players found through held matches). `stats` prints the last days;
  `relay status` gives the last 7 days under `reads`.
- **Out-of-date reports change nothing.** Each stretch of a list is handed out with a
  token, and the list's progress is only taken from the report carrying the current token.
  A report that arrives after its stretch went to someone else still adds the matches it
  saw to the wanted list, but cannot move the list's progress backwards or forwards.

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
