#!/usr/bin/env bash
# Applies a new version of the collector endpoint on the website server. Run as root after
# `git pull --ff-only` in ~/arenasweats-collector:
#   sudo bash ~/arenasweats-collector/server/deploy/update.sh
# It reinstalls the service and nginx files from this folder, restarts the service and reloads nginx.
# The nginx files are put back as they were if nginx rejects the new ones.
set -euo pipefail
here="$(cd "$(dirname "$0")" && pwd)"
zone=/etc/nginx/conf.d/arenasweats-collector.conf
location=/etc/nginx/snippets/arenasweats-collector.conf
[ -f "$zone" ] && [ -f "$location" ] || { echo "the endpoint was never installed; run install.sh instead"; exit 1; }

install -m 0644 "$here/arenasweats-collector.service" /etc/systemd/system/arenasweats-collector.service
systemctl daemon-reload
systemctl restart arenasweats-collector.service
for _ in $(seq 1 20); do curl -fsS http://127.0.0.1:8870/healthz >/dev/null 2>&1 && break; sleep 1; done
curl -fsS http://127.0.0.1:8870/healthz >/dev/null && echo "collector service: running ($(systemctl show -p NRestarts --value arenasweats-collector) restarts)"

backup="$(mktemp -d /var/backups/nginx-arenasweats-collector-XXXXXX)"
cp -p "$zone" "$location" "$backup/"
install -m 0644 "$here/nginx-zone.conf" "$zone"
install -m 0644 "$here/nginx-location.conf" "$location"
if ! nginx -t; then
  cp -p "$backup/$(basename "$zone")" "$zone"
  cp -p "$backup/$(basename "$location")" "$location"
  echo "nginx rejected the new files; the old ones were put back (copies in $backup)"
  exit 1
fi
systemctl reload nginx
sleep 1
curl -fsS https://arenasweats.lol/collector/healthz >/dev/null && echo "collector endpoint: live at https://arenasweats.lol/collector/"
