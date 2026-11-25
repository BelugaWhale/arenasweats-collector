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
  further if the client is busy. A request is for one match, or for one player's latest
  matches: up to 200 of them at once, which the client can take many seconds to answer.
  ArenaSweats can tell it to slow down or stop, but can never make it go faster or do
  anything else. Only you can, with fast mode.
- **Fast mode is off unless you turn it on.** Fast mode collects about 3 times faster:
  about two requests a second, sent without waiting for the last one's answer, so up to four
  can be on their way at once. **BETA: Please only use with unimportant accounts for now.**
  At most three of the four are match lists, which take the client longest.
  It starts with one request at a time and allows more only while the client keeps answering
  as quickly as before. Two failed requests within a hundred, or slower answers, mean fewer
  at once again. An answer of "too many requests", or three failures in a row, bring back
  the normal pace for an hour. It still never runs while you play: when you queue up it
  sends nothing more, and drops the requests that are still on their way. ArenaSweats can
  still slow it down, allow fewer requests at once or switch it off, but can never allow
  more than four.
- **Your account stays private.** It never reads or sends your password or login. It sends
  only Arena match results, your player ID, which is already in your match records,
  short fixed codes for what it is doing, such as "paused while you play" or "League
  client not found", and a few numbers about its own pace: how many requests it made,
  how many failed, and how long the client took to answer. Never a file or folder name,
  nor anything that identifies you or your PC.
- **Nothing to install.** No background service. Close the window and it stops. Delete its
  folder and it's gone. It runs as administrator only if you allow it when Windows asks, which
  happens only when Windows hides the League client from it (see below).
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

If Windows hides the League client from it (this happens when League itself runs as
administrator), it asks Windows to restart it as administrator, never during a match, and
Windows shows its own permission request. If you allow it, that is remembered in
`config.json` and it starts as administrator by itself from then on; to undo that, start it
once with `--no-admin`, or delete the `"admin"` line from `config.json`. If the request is
declined or nobody answers, it does not ask Windows again by itself: it asks you in its
window whether to, and keeps looking for League meanwhile. As administrator it makes exactly
the same read-only requests, and nothing else. Where knowing the folder League is installed
in can help (Riot's own client; the Chinese client's folder cannot help), a line in its
window offers to take it, and it keeps looking meanwhile.

MIT licence; see [`LICENSE`](LICENSE).
