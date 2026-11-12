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
- **Gentle.** It sends one request every couple of seconds, one at a time, and slows down
  further if the client is busy. ArenaSweats can tell it to slow down or stop, but can
  never make it go faster or do anything else.
- **Your account stays private.** It never reads or sends your password or login. It sends
  only Arena match results and your player ID, which is already in your match records.
- **Nothing to install.** No administrator rights and no background service. Close the
  window and it stops. Delete its folder and it's gone.
- **Open source.** The download is built by GitHub straight from the code in this
  repository, so what you run is exactly what you can read here.

Riot doesn't officially approve any third-party tool, but read-only tools that use the
client like this are common. Decide for yourself whether you're comfortable running it.

## What happens when you run it

Unless you've been given an upload key, it does a **15-minute test run**. It collects for
15 minutes, then saves one file in the `data` folder next to the program for you to send
back. That file holds the Arena matches it collected and a log of what it did. Nothing is
uploaded.

If it can't find League by itself, it asks you which folder League is installed in.

MIT licence; see [`LICENSE`](LICENSE).
