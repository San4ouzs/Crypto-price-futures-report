# -*- coding: utf-8 -*-
import time
import requests
import pandas as pd
from datetime import timezone
from dateutil import parser as dtparser

BINANCE_SPOT = "https://api.binance.com"
BINANCE_FUT = "https://fapi.binance.com"
COINGECKO = "https://api.coingecko.com/api/v3"

_INTERVAL_MAP = {
    "1m":"1m","3m":"3m","5m":"5m","15m":"15m","30m":"30m",
    "1h":"1h","2h":"2h","4h":"4h","6h":"6h","8h":"8h","12h":"12h",
    "1d":"1d","3d":"3d","1w":"1w","1M":"1M"
}

def _to_ms(ts):
    if ts is None:
        return None
    if isinstance(ts, (int, float)):
        return int(ts)
    dt = dtparser.parse(ts)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1000)

def _klines_to_df(rows):
    cols = ["open_time","open","high","low","close","volume",
            "close_time","quote_asset_volume","trades",
            "taker_base","taker_quote","ignore"]
    df = pd.DataFrame(rows, columns=cols)
    for c in ["open","high","low","close","volume","quote_asset_volume","taker_base","taker_quote"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    df["open_time"] = pd.to_datetime(df["open_time"], unit="ms", utc=True)
    df["close_time"] = pd.to_datetime(df["close_time"], unit="ms", utc=True)
    df["symbol"] = None
    return df[["symbol","open_time","open","high","low","close","volume","close_time","trades","quote_asset_volume","taker_base","taker_quote"]]

def fetch_binance_spot_klines(symbol_base="BTC", quote="USDT", interval="1h", start=None, end=None, limit=1000):
    assert interval in _INTERVAL_MAP, f"Unsupported interval: {interval}"
    sym = f"{symbol_base.upper()}{quote.upper()}"
    params = {"symbol": sym, "interval": interval, "limit": min(limit, 1000)}
    if start:
        params["startTime"] = _to_ms(start)
    if end:
        params["endTime"] = _to_ms(end)
    url = f"{BINANCE_SPOT}/api/v3/klines"
    out = []
    while True:
        r = requests.get(url, params=params, timeout=30)
        r.raise_for_status()
        data = r.json()
        if not data:
            break
        out.extend(data)
        if len(data) < params["limit"]:
            break
        params["startTime"] = data[-1][6] + 1
        time.sleep(0.2)
    df = _klines_to_df(out)
    df["symbol"] = sym
    return df

def fetch_binance_futures_klines(symbol_base="BTC", quote="USDT", interval="1h", start=None, end=None, contract_type="PERPETUAL", limit=1000):
    assert interval in _INTERVAL_MAP, f"Unsupported interval: {interval}"
    sym = f"{symbol_base.upper()}{quote.upper()}"
    params = {"symbol": sym, "interval": interval, "limit": min(limit, 1500)}
    if start:
        params["startTime"] = _to_ms(start)
    if end:
        params["endTime"] = _to_ms(end)
    url = f"{BINANCE_FUT}/fapi/v1/klines"
    out = []
    while True:
        r = requests.get(url, params=params, timeout=30)
        r.raise_for_status()
        data = r.json()
        if not data:
            break
        out.extend(data)
        if len(data) < params["limit"]:
            break
        params["startTime"] = data[-1][6] + 1
        time.sleep(0.2)
    df = _klines_to_df(out)
    df["symbol"] = sym + "_FUT_PERP"
    return df

def fetch_coingecko_ohlc(symbol_id="bitcoin", vs_currency="usd", days=365):
    url = f"{COINGECKO}/coins/{symbol_id}/ohlc"
    params = {"vs_currency": vs_currency, "days": days}
    r = requests.get(url, params=params, timeout=30)
    r.raise_for_status()
    arr = r.json()
    if not arr:
        return pd.DataFrame()
    df = pd.DataFrame(arr, columns=["ts","open","high","low","close"])
    df["open_time"] = pd.to_datetime(df["ts"], unit="ms", utc=True)
    df["volume"] = None
    df["close_time"] = df["open_time"]
    df["symbol"] = f"{symbol_id.upper()}_{vs_currency.upper()}"
    return df[["symbol","open_time","open","high","low","close","volume","close_time"]]
