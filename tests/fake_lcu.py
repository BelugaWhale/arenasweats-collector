"""A stand-in for the League client's local API, with a small made-up world of players and matches.

It answers the same addresses the real client does, in the same shapes, over plain
HTTP on 127.0.0.1. Tests point the collector at it with a lockfile.
"""

import base64
import json
import os
import random
import socket
import threading
import time
import uuid
from collections import defaultdict
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse


class Clock:
    """Pretend time, so tests that 'wait' an hour finish at once."""

    def __init__(self):
        self.time = 1000.0
        self.events = []

    def now(self):
        return self.time

    def sleep(self, seconds):
        self.time += max(0.0, seconds)
        for event in [e for e in self.events if e[0] <= self.time]:
            self.events.remove(event)
            event[1]()

    def at(self, when, action):
        self.events.append((when, action))


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.server.fake.handle(self, "GET")

    def other(self):
        self.server.fake.handle(self, self.command)

    do_POST = do_PUT = do_DELETE = do_PATCH = other

    def log_message(self, *args):
        pass


class FakeClient:
    def __init__(self, players=24, arena=40, other=4, seed=7, platform="OC1", clock=None):
        self.random = random.Random(seed)
        self.clock = clock
        self.token = "fake-token"
        self.platform = platform
        self.players = [str(uuid.UUID(int=self.random.getrandbits(128))) for _ in range(players)]
        self.me = self.players[0]
        self.games = {}                       # match id -> full record
        self.history = defaultdict(list)      # player -> match ids
        self.next_id = 600000000
        self.phase = "None"
        self.fail = lambda path: None         # tests set this to return a status code for a path
        self.wrong_record = {}                # match id -> a different record to answer with
        self.drop = lambda path: False        # tests set this to cut the connection for a path
        self.list_hook = None                 # tests set this to change a match-list page: hook(player, page)
        self.requests = []                    # (method, path, pretend time)
        for _ in range(arena):
            self.add_game("CHERRY", 1700, self.random.sample(self.players, 16))
        for player in self.players:
            for _ in range(other):
                others = self.random.sample([p for p in self.players if p != player], 9)
                self.add_game("CLASSIC", 420, [player] + others)
        self.httpd = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.httpd.fake = self
        self.port = self.httpd.server_address[1]
        threading.Thread(target=self.httpd.serve_forever, daemon=True).start()

    def close(self):
        self.httpd.shutdown()
        self.httpd.server_close()

    def write_lockfile(self, folder):
        os.makedirs(folder, exist_ok=True)
        with open(os.path.join(folder, "lockfile"), "w") as handle:
            handle.write("LeagueClient:4321:%d:%s:http" % (self.port, self.token))
        return folder

    # -- the made-up world ---------------------------------------------------
    def add_game(self, mode, queue, lobby, age_seconds=None):
        self.next_id += 1
        age = age_seconds if age_seconds is not None else self.random.randint(600, 20 * 86400)
        created = int((time.time() - age) * 1000)
        identities, participants = [], []
        for seat, puuid in enumerate(lobby, start=1):
            identities.append({"participantId": seat, "player": {
                "puuid": puuid, "gameName": "Player" + puuid[:4], "tagLine": "OCE",
                "platformId": self.platform, "summonerId": seat}})
            team = (seat - 1) // 2 + 1
            participants.append({"participantId": seat, "championId": self.random.randint(1, 900), "teamId": 100,
                                 "stats": {"win": team == 1, "kills": self.random.randint(0, 20),
                                           "deaths": self.random.randint(0, 12), "assists": self.random.randint(0, 20),
                                           "subteamPlacement": team, "playerSubteamId": team,
                                           "playerAugment1": self.random.randint(1, 300)}})
        self.games[self.next_id] = {
            "gameId": self.next_id, "platformId": self.platform, "gameCreation": created,
            "gameCreationDate": time.strftime("%Y-%m-%dT%H:%M:%S.000Z", time.gmtime(created / 1000)),
            "gameDuration": 900 + self.random.randint(0, 600), "gameMode": mode, "gameType": "MATCHED_GAME",
            "gameVersion": "16.21.700.1234", "mapId": 30 if mode == "CHERRY" else 11, "queueId": queue,
            "participantIdentities": identities, "participants": participants, "teams": []}
        for puuid in lobby:
            self.history[puuid].append(self.next_id)
        return self.next_id

    def arena_ids(self):
        return {game_id for game_id, game in self.games.items() if game["gameMode"] == "CHERRY"}

    def summary(self, game_id, puuid):
        """A match as it appears in one player's match list: only that player's own row."""
        game = dict(self.games[game_id])
        seat = next(i["participantId"] for i in game["participantIdentities"] if i["player"]["puuid"] == puuid)
        game["participantIdentities"] = [i for i in game["participantIdentities"] if i["participantId"] == seat]
        game["participants"] = [p for p in game["participants"] if p["participantId"] == seat]
        return game

    # -- answering requests ----------------------------------------------------
    def handle(self, handler, method):
        self.requests.append((method, handler.path, self.clock.now() if self.clock else time.monotonic()))
        expected = "Basic " + base64.b64encode(("riot:" + self.token).encode()).decode()
        if handler.headers.get("Authorization") != expected:
            return self.reply(handler, 401, {"errorCode": "RPC_ERROR", "httpStatus": 401, "message": "Unauthorized"})
        if method != "GET":
            return self.reply(handler, 405, {"message": "this fake only expects GET"})
        if self.drop(handler.path):           # hang up without answering, like a client that is shutting down
            handler.close_connection = True
            handler.connection.shutdown(socket.SHUT_RDWR)
            return None
        forced = self.fail(handler.path)
        if forced:
            return self.reply(handler, forced, {"errorCode": "RPC_ERROR", "httpStatus": forced, "message": "forced"})
        url = urlparse(handler.path)
        parts = url.path.strip("/").split("/")
        if url.path == "/lol-gameflow/v1/gameflow-phase":
            return self.reply(handler, 200, self.phase)
        if url.path == "/lol-summoner/v1/current-summoner":
            return self.reply(handler, 200, {"puuid": self.me, "gameName": "Me", "tagLine": "OCE", "summonerLevel": 30})
        if url.path == "/riotclient/region-locale":
            return self.reply(handler, 200, {"locale": "en_AU", "region": self.platform, "webRegion": "oce"})
        if parts[:4] == ["lol-match-history", "v1", "products", "lol"] and parts[-1] == "matches":
            query = parse_qs(url.query)
            begin, end = int(query["begIndex"][0]), int(query["endIndex"][0])
            ids = sorted(self.history.get(parts[4], []), key=lambda i: -self.games[i]["gameCreation"])
            page = [self.summary(i, parts[4]) for i in ids[begin:end + 1]]
            if self.list_hook:
                page = self.list_hook(parts[4], page)
            return self.reply(handler, 200, {"accountId": 1, "platformId": self.platform, "games": {
                "gameCount": len(page), "gameIndexBegin": begin, "gameIndexEnd": end, "games": page}})
        if parts[:3] == ["lol-match-history", "v1", "games"] and int(parts[3]) in self.games:
            return self.reply(handler, 200, self.wrong_record.get(int(parts[3]), self.games[int(parts[3])]))
        return self.reply(handler, 404, {"errorCode": "RESOURCE_NOT_FOUND", "httpStatus": 404, "message": "not found"})

    @staticmethod
    def reply(handler, status, data):
        body = json.dumps(data).encode()
        handler.send_response(status)
        handler.send_header("Content-Type", "application/json")
        handler.send_header("Content-Length", str(len(body)))
        handler.end_headers()
        handler.wfile.write(body)
