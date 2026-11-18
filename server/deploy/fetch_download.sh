#!/usr/bin/env bash
# Keeps a copy of the newest collector release on this server for
# https://arenasweats.lol/collector/download/ArenaSweatsCollector.exe (GitHub is unreliable in China).
# Runs as the normal user from cron:
#   */30 * * * * bash ~/arenasweats-collector/server/deploy/fetch_download.sh >> ~/arenasweats-collector-data/fetch_download.log 2>&1
set -euo pipefail
dir="$HOME/arenasweats-collector-downloads"
name=ArenaSweatsCollector.exe
mkdir -p "$dir"
chmod 755 "$dir"
info="$(curl -fsSL --max-time 30 https://api.github.com/repos/BelugaWhale/arenasweats-collector/releases/latest)"
read -r url digest tag < <(python3 -c '
import json, sys
release = json.load(sys.stdin)
asset = next(a for a in release["assets"] if a["name"] == "ArenaSweatsCollector.exe")
print(asset["browser_download_url"], (asset.get("digest") or "").replace("sha256:", "") or "-", release["tag_name"])
' <<<"$info")
[ "$digest" != "-" ] || { echo "$(date -u +%FT%TZ) release $tag has no checksum; not updating"; exit 1; }
if [ -f "$dir/$name" ] && [ "$(sha256sum "$dir/$name" | cut -d' ' -f1)" = "$digest" ]; then
  exit 0
fi
tmp="$(mktemp "$dir/.$name.XXXXXX")"
trap 'rm -f "$tmp"' EXIT
curl -fsSL --max-time 300 -o "$tmp" "$url"
[ "$(sha256sum "$tmp" | cut -d' ' -f1)" = "$digest" ] || { echo "$(date -u +%FT%TZ) checksum mismatch for $tag; not updating"; exit 1; }
chmod 644 "$tmp"
mv -f "$tmp" "$dir/$name"
trap - EXIT
echo "$digest  $name" > "$dir/$name.sha256"
echo "$(date -u +%FT%TZ) now serving $tag ($digest)"
