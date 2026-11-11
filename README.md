# ArenaSweats collector

A small open-source program that reads **Arena match history** from the League client on
your own computer, so ArenaSweats can build leaderboards for regions Riot's public API
does not cover (China).

The whole program is one file, [`collector.py`](collector.py), using only Python's
standard library. Read it before you run it.

## What it does

1. Finds the League client that is running on your computer.
2. While you are **not playing**, slowly asks that client for this season's Arena match
   history: the match lists of players, and the full record of Arena matches.
3. With an upload address and key, ArenaSweats tells it which players and matches to read
   next, so that no two collectors read the same thing, and it sends the matches there.
   Without one, it starts from your own matches, moves on to the players you met, and
   saves everything in a `data` folder beside the program.

## What it never does

- **It never changes anything.** Every request to the League client is a read (`GET`) of
  one of five addresses. The list is `LCU_ALLOWED` near the top of `collector.py`, and the
  program refuses any address that is not on it.
- **It never touches the game itself** — not its process, its memory or its files.
- **It never sends your login token, password or account details anywhere.** What leaves
  your computer, and only to the upload address, is: match records, which Arena matches a
  player's list contained, and your own player id (the same id that is in your match
  records; it is how a server with no players yet gets one to start from).
- It needs no administrator rights and installs nothing.

| Address it reads from the League client | Why |
| --- | --- |
| `/lol-gameflow/v1/gameflow-phase` | Are you in queue, champion select or a game? If so, it waits. |
| `/lol-summoner/v1/current-summoner` | Which account is logged in, to know whose history to start from. |
| `/riotclient/region-locale` | Which region. Written to the log only. |
| `/lol-match-history/v1/products/lol/{player}/matches` | A player's recent match list. |
| `/lol-match-history/v1/games/{match}` | The full record of one Arena match. |

## How gentle it is

- One match-history request about every 2 seconds, one at a time. There is a hard floor of
  one per second that no setting can lower.
- It checks the client's state before every request. In queue, ready check, champion
  select, a game or the post-game screen it stops, and it resumes about 20 seconds after
  you are back in the client doing nothing (or sitting in a lobby).
- When requests fail it waits longer and longer, up to 10 minutes between tries. A player
  or match that keeps failing is asked for less and less often.
- Only one copy can run at a time.
- ArenaSweats chooses which players' lists and which matches are read, and can tell every
  collector to slow down or stop (they obey within about 10 minutes). It cannot make one
  go faster, read anything else, or do anything else.

## How it avoids wasted requests

- **Nothing from before the season.** Lists are only read back to the season's start
  (2026-10-06 UTC unless ArenaSweats says otherwise), and older matches are never fetched.
- **No match twice.** A collector tells ArenaSweats which Arena matches it saw in a list;
  it is only asked to fetch the ones ArenaSweats does not hold yet. A match being fetched
  by one collector is not given to another.
- **The most useful lists first.** Players seen in the most matches are read first. A list
  is read one page at first and further back only while that turns up matches not held
  yet; the rest is left for when nothing fresher is waiting. A player is read again when
  they are expected to have played several new matches.

## Is this allowed?

Read-only tools that use the client's local address are widely used and Riot tolerates
them, but Riot does not formally approve any third-party tool. In China the rules are
Tencent's and we cannot promise how they are applied. This program only asks for what the
client itself asks for when you browse match history, slowly, and only while you are not
playing. Decide for yourself whether you are comfortable running it.

## Running it

**Windows, packaged:** download `ArenaSweatsCollector.exe` from the
[latest release](https://github.com/BelugaWhale/arenasweats-collector/releases/latest),
open the League client and log in, then double-click it. Windows may warn that the
program is from an unknown publisher; choose "More info" then "Run anyway".

**From source (any system with Python 3.8+):**

```
python collector.py
```

With no upload address set, it does a **test run**: it collects for 15 minutes, then
writes one zip file into the `data` folder and tells you where it is. Send that file back
so we can see whether collection works on your server. It holds:

- the matches it collected (other players' match records, as the client shows them);
- its log: every request it made and how long it took, the folder League is installed in,
  the names of running Riot or Tencent programs, and the first 8 characters of your own
  player id. Nothing else about your account.

To work with ArenaSweats instead, put a `config.json` beside the program:

```json
{ "server": "https://…", "key": "as_…" }
```

Options (`python collector.py --help`):

| Option | Meaning |
| --- | --- |
| `--minutes N` | Stop after N minutes. `0` = run until closed. |
| `--league-dir PATH` | Folder League is installed in, if it is not found automatically. |
| `--delay S` | Seconds between match-history requests (default 2, never below 1). |
| `--since YYYY-MM-DD` | Read nothing older than this date, the season's start. With an upload address, ArenaSweats sets it. |
| `--export FILE` | Write the match records held on this computer to a file you can read. |

## Files it creates

Everything is in the `data` folder beside the program: `collector-XXXXXXXX.db` (matches
not yet uploaded and, in a test run, which players and matches it has seen),
`collector.log`, and the zip from a test run. Delete the folder to remove everything.

## Checking it yourself

- Read `collector.py`. It is about 1,200 lines, a good part of them comments.
- The `.exe` on the releases page is built by GitHub from the code at that release's tag
  ([how](.github/workflows/build.yml)), and the release notes give its SHA-256. If
  someone sends you the file instead, run `certutil -hashfile ArenaSweatsCollector.exe SHA256`
  in its folder and compare.
- Or build the `.exe` yourself: `py -m pip install pyinstaller`, then
  `py -m PyInstaller --onefile --console --name ArenaSweatsCollector collector.py`.
- Run the tests, which drive the collector against a fake League client:
  `python -m unittest discover tests` (the server tests need `pip install flask`).

The receiving side is in [`server/`](server/README.md).

MIT licence; see [`LICENSE`](LICENSE).
