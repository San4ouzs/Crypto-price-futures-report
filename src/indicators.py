# -*- coding: utf-8 -*-
import pandas as pd
import numpy as np

def _ema(series, span):
    return series.ewm(span=span, adjust=False, min_periods=span).mean()

def add_basic_indicators(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    close = out["close"]
    high = out["high"]
    low = out["low"]
    volume = out["volume"].fillna(0)

    delta = close.diff()
    up = np.where(delta > 0, delta, 0.0)
    down = np.where(delta < 0, -delta, 0.0)
    roll_up = pd.Series(up, index=out.index).ewm(alpha=1/14, adjust=False).mean()
    roll_down = pd.Series(down, index=out.index).ewm(alpha=1/14, adjust=False).mean()
    rs = roll_up / (roll_down + 1e-12)
    out["rsi_14"] = 100 - (100 / (1 + rs))

    ema12 = _ema(close, 12)
    ema26 = _ema(close, 26)
    macd = ema12 - ema26
    signal = _ema(macd, 9)
    out["macd"] = macd
    out["macd_signal"] = signal
    out["macd_hist"] = macd - signal

    ma20 = close.rolling(20).mean()
    std20 = close.rolling(20).std()
    out["bb_mid_20"] = ma20
    out["bb_up_20_2"] = ma20 + 2*std20
    out["bb_lo_20_2"] = ma20 - 2*std20

    tr = pd.concat([
        (high - low),
        (high - close.shift()).abs(),
        (low - close.shift()).abs()
    ], axis=1).max(axis=1)
    out["atr_14"] = tr.rolling(14).mean()

    direction = np.sign(close.diff().fillna(0))
    out["obv"] = (volume * direction).fillna(0).cumsum()

    tp = (high + low + close) / 3.0
    raw_money = tp * volume
    pmf = np.where(tp > tp.shift(), raw_money, 0.0)
    nmf = np.where(tp < tp.shift(), raw_money, 0.0)
    mfr = (pd.Series(pmf).rolling(14).sum()) / (pd.Series(nmf).rolling(14).sum() + 1e-12)
    out["mfi_14"] = 100 - (100 / (1 + mfr.values))

    out["flag_overbought"] = (out["rsi_14"] >= 70) | (out["mfi_14"] >= 80)
    out["flag_oversold"] = (out["rsi_14"] <= 30) | (out["mfi_14"] <= 20)
    return out

def resample_ohlcv(df: pd.DataFrame, rule: str) -> pd.DataFrame:
    if "open_time" in df.columns:
        df = df.set_index("open_time")
    o = df["open"].resample(rule).first()
    h = df["high"].resample(rule).max()
    l = df["low"].resample(rule).min()
    c = df["close"].resample(rule).last()
    v = df["volume"].resample(rule).sum()
    out = pd.concat([o,h,l,c,v], axis=1).dropna(how="all")
    out.columns = ["open","high","low","close","volume"]
    out = out.reset_index()
    return out
