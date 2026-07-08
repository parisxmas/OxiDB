#!/usr/bin/env python3
"""Market-data feeder — streams live trade ticks from three real crypto
exchanges (Binance, Coinbase, Kraken) over WebSocket and writes them
into OxiDB. 20 symbols total. Each tick upserts a `prices` row (latest
price per symbol) and appends to a `ticks` collection.

Run inside the venv:  tests/live-exchange/.venv/bin/python feeder.py
Stop with Ctrl-C.
"""
import asyncio
import json
import sys
import time

import websockets

sys.path.insert(0, __file__.rsplit("/", 1)[0])
from oxidb_client import OxiDB  # noqa: E402

# 20 DISTINCT canonical symbols, split across three real venues.
BINANCE = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "XRPUSDT",
           "ADAUSDT", "DOGEUSDT", "AVAXUSDT", "LINKUSDT", "TRXUSDT"]
COINBASE = ["LTC-USD", "DOT-USD", "ATOM-USD", "ALGO-USD", "UNI-USD"]
KRAKEN = ["FIL/USD", "GRT/USD", "SAND/USD", "MANA/USD", "AAVE/USD"]

def canon(venue, raw):
    if venue == "binance":
        return raw[:-4] if raw.endswith("USDT") else raw
    if venue == "coinbase":
        return raw.split("-")[0]
    return raw.split("/")[0]  # kraken

# The canonical universe traders trade against.
SYMBOLS = ([canon("binance", s) for s in BINANCE]
           + [canon("coinbase", s) for s in COINBASE]
           + [canon("kraken", s) for s in KRAKEN])

# One shared writer per feeder process (single connection, serialized by asyncio).
_db = OxiDB()
_stats = {"ticks": 0, "errors": 0}


def on_trade(venue, raw_sym, price, qty):
    sym = canon(venue, raw_sym)
    try:
        # `prices` is pre-seeded with all canonical symbols, so a plain
        # $set update always matches — no upsert needed.
        _db.call({"cmd": "update", "collection": "prices",
                  "query": {"sym": sym},
                  "update": {"$set": {"price": price, "venue": venue, "ts": time.time()}}})
        _db.insert("ticks", {"sym": sym, "venue": venue, "price": price,
                             "qty": qty, "ts": time.time()})
        _stats["ticks"] += 1
        if _stats["ticks"] % 200 == 0:
            print(f"[feeder] {_stats['ticks']} ticks written "
                  f"({_stats['errors']} errors)", flush=True)
    except Exception as e:
        _stats["errors"] += 1
        if _stats["errors"] % 50 == 1:
            print(f"[feeder] db error: {e}", flush=True)


async def binance():
    streams = "/".join(f"{s.lower()}@trade" for s in BINANCE)
    url = f"wss://stream.binance.com:9443/stream?streams={streams}"
    async for ws in websockets.connect(url, ping_interval=20):
        try:
            async for msg in ws:
                d = json.loads(msg).get("data", {})
                if d.get("e") == "trade":
                    on_trade("binance", d["s"], float(d["p"]), float(d["q"]))
        except websockets.ConnectionClosed:
            print("[binance] reconnecting", flush=True)
            continue


async def coinbase():
    url = "wss://ws-feed.exchange.coinbase.com"
    sub = {"type": "subscribe", "product_ids": COINBASE, "channels": ["matches"]}
    async for ws in websockets.connect(url, ping_interval=20):
        try:
            await ws.send(json.dumps(sub))
            async for msg in ws:
                d = json.loads(msg)
                if d.get("type") in ("match", "last_match"):
                    on_trade("coinbase", d["product_id"],
                                 float(d["price"]), float(d["size"]))
        except websockets.ConnectionClosed:
            print("[coinbase] reconnecting", flush=True)
            continue


async def kraken():
    url = "wss://ws.kraken.com"
    sub = {"event": "subscribe", "pair": KRAKEN, "subscription": {"name": "trade"}}
    async for ws in websockets.connect(url, ping_interval=20):
        try:
            await ws.send(json.dumps(sub))
            async for msg in ws:
                d = json.loads(msg)
                # trade payload: [channelID, [[price, vol, time, side, ...]], "trade", pair]
                if isinstance(d, list) and len(d) >= 4 and d[2] == "trade":
                    pair = d[3]
                    for t in d[1]:
                        on_trade("kraken", pair, float(t[0]), float(t[1]))
        except websockets.ConnectionClosed:
            print("[kraken] reconnecting", flush=True)
            continue


async def main():
    print("[feeder] connecting to Binance + Coinbase + Kraken (20 symbols)…", flush=True)
    await asyncio.gather(binance(), coinbase(), kraken())


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print(f"\n[feeder] stopped after {_stats['ticks']} ticks", flush=True)
