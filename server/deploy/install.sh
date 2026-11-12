#!/usr/bin/env bash
# Puts the collector endpoint live on the website server. Run once as root:
#   sudo bash ~/arenasweats-collector/server/deploy/install.sh
# It only adds files and one include line; the website's own config is backed up first
# and put back if nginx rejects the result.
set -euo pipefail
here="$(cd "$(dirname "$0")" && pwd)"
site=/etc/nginx/sites-available/arenasweats
anchor='^    server_name arenasweats.lol;$'
include='include snippets/arenasweats-collector.conf;'

install -m 0644 "$here/arenasweats-collector.service" /etc/systemd/system/arenasweats-collector.service
systemctl daemon-reload
systemctl enable --now arenasweats-collector.service
for _ in $(seq 1 20); do curl -fsS http://127.0.0.1:8870/healthz >/dev/null 2>&1 && break; sleep 1; done
curl -fsS http://127.0.0.1:8870/healthz >/dev/null && echo "collector service: running"

install -m 0644 "$here/nginx-zone.conf" /etc/nginx/conf.d/arenasweats-collector.conf
install -m 0644 "$here/nginx-location.conf" /etc/nginx/snippets/arenasweats-collector.conf
backup=""
if ! grep -qF "$include" "$site"; then
  [ "$(grep -c "$anchor" "$site")" = 1 ] || { echo "could not find the arenasweats.lol server block; nothing changed in $site"; exit 1; }
  backup="/var/backups/nginx-arenasweats-before-collector-$(date +%Y%m%d%H%M%S)"
  cp -p "$site" "$backup"
  sed -i "s|$anchor|&\n    $include|" "$site"
fi
if ! nginx -t; then
  [ -n "$backup" ] && cp -p "$backup" "$site"
  rm -f /etc/nginx/conf.d/arenasweats-collector.conf /etc/nginx/snippets/arenasweats-collector.conf
  echo "nginx rejected the change; everything was put back"
  exit 1
fi
systemctl reload nginx
sleep 1
curl -fsS https://arenasweats.lol/collector/healthz >/dev/null && echo "collector endpoint: live at https://arenasweats.lol/collector/"
