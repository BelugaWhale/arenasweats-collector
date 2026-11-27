# ArenaSweats collector

A small, open-source program that pulls **Arena match data** from the League client on
your computer and shares it with ArenaSweats.

## Download

Get `ArenaSweatsCollector.exe` from the
[latest release](https://github.com/BelugaWhale/arenasweats-collector/releases/latest).
Open the League client and log in, then double-click it.

Windows may warn that the program is from an unknown publisher. Choose "More info", then
"Run anyway".

## Safe by design

- **Read-only.** It only reads match history, the same history you can browse in the client
  yourself. It never changes anything in the client and never touches the game. It refuses
  any request that isn't on its short list of allowed reads.
- **Never while you play.** It pauses the moment you queue up, and starts again only once
  you're back in the client and not in a match.
- **Gentle.** It sends one request about two seconds after the client's last answer (never
  more than one a second), one at a time, and slows down further if the client is busy. A
  request is for one match, or for up to 1,000 matches of one player's match history at
  once: their latest, or (when ArenaSweats asks) older ones further down the history. The
  client can take a minute or more to answer it.
  ArenaSweats can tell it to slow down or stop, but can never make it go faster or do
  anything else. Only you can, with fast mode.
- **Fast mode is off unless you turn it on.** Fast mode collects about 2-3 times faster:
  up to five requests a second, each sent at least 0.2 seconds after the last one was sent,
  not after its answer. **BETA: Please only use with unimportant accounts for now.**
  On real clients it collected about 2.2 times as many matches an hour as the normal pace
  (version 0.3.7, which kept 0.4 seconds between requests); with 0.2 seconds we estimate
  about 3 times. Up to four requests can be on their way at once, as many as ArenaSweats
  allows, and at most three of them match lists, which take the client longest. The Chinese
  client answers one request at a time, so there that gains nothing.
  It starts with one request at a time and allows more only while the client keeps answering
  as quickly as before. Two failed requests within a hundred, or slower answers, mean fewer
  at once again. An answer of "too many requests", or three failures in a row, bring back
  the normal pace for an hour. It still never runs while you play: when you queue up it
  sends nothing more, and drops the requests that are still on their way. ArenaSweats can
  still slow it down, allow fewer requests at once or switch it off, but can never allow
  more than four at once, or less than 0.2 seconds between them.
- **Your account stays private.** It never reads or sends your password or login. It sends
  Arena match results, your player ID, which is already in your match records, and a
  little about the collector itself: what it is doing and how fast.
- **Nothing to install.** No background service. Close the window and it stops. Delete its
  folder and it's gone. It runs as administrator only if you say yes when it asks (see
  below).
- **Open source.** The download is built by GitHub straight from the code in this
  repository, so what you run is exactly what you can read here.

Riot doesn't officially approve any third-party tool, but read-only tools that use the
client like this are common. Decide for yourself whether you're comfortable running it.

## What happens when you run it

It sends the Arena matches it reads to ArenaSweats and keeps going until you close the
window. While it runs it keeps your PC from going to sleep; the screen can still turn off. The first time, it gets its own upload key from ArenaSweats and saves it in
`config.json` next to the program.

When a new version is out, it asks whether to update before it starts, counting down
10 seconds; with no answer, it updates. Then it asks whether to turn on fast mode, again
counting down 10 seconds; pressing Enter, or no answer, keeps it off. So it still starts by itself when nobody is at
the PC. To skip the fast mode question, add `--fast` or `--no-fast` to the end of a
shortcut's target. While fast mode is on, it says so every time it starts.

Before those questions it asks whether to run as administrator, which League on Chinese
servers needs (it runs as administrator, and Windows then hides it from other programs),
counting down 30 seconds; no answer means no, and it asks again at the next start. A
yes is remembered in `config.json`, and from then on it starts as administrator by itself
(Windows asks your permission each time); to undo that, start it once with `--no-admin`, or
delete the `"admin"` line from `config.json`. As administrator it makes exactly the same
read-only requests, and nothing else.

MIT licence; see [`LICENSE`](LICENSE).
