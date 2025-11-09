# -*- coding: utf-8 -*-
import os
import argparse
import yaml
from datetime import datetime
import pandas as pd

from exchanges import fetch_binance_spot_klines, fetch_binance_futures_klines, fetch_coingecko_ohlc
from indicators import add_basic_indicators
from excel_export import save_to_excel
from charts_dashboard import make_dashboard_html

def _parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--symbol", default=None)
    p.add_argument("--quote", default="USDT")
    p.add_argument("--timeframes", nargs="+", default=["1h","4h","1d"])
    p.add_argument("--start", default="2024-01-01")
    p.add_argument("--end", default=None)
    p.add_argument("--exchange", default="binance", choices=["binance","coingecko"])
    p.add_argument("--fetch-spot", action="store_true")
    p.add_argument("--fetch-futures", action="store_true")
    p.add_argument("--out-dir", default="out")
    p.add_argument("--html", action="store_true")
    p.add_argument("--config", default=None)
    return p.parse_args()

def load_config(args):
    cfg = {}
    if args.config and os.path.exists(args.config):
        with open(args.config, "r", encoding="utf-8") as f:
            cfg = yaml.safe_load(f) or {}
    def pick(key, default):
        v = getattr(args, key, None)
        return cfg.get(key, v if v is not None else default)
    merged = {
        "symbol": pick("symbol", "BTC") or cfg.get("symbol","BTC"),
        "quote": pick("quote", "USDT"),
        "timeframes": pick("timeframes", ["1h","4h","1d"]),
        "start": pick("start", "2024-01-01"),
        "end": pick("end", None),
        "exchange": pick("exchange", "binance"),
        "fetch_spot": cfg.get("fetch_spot", args.fetch_spot or True),
        "fetch_futures": cfg.get("fetch_futures", args.fetch_futures or True),
        "out_dir": pick("out_dir", "out"),
        "generate_html": cfg.get("generate_html", args.html or True),
    }
    return merged

def fetch_spot(symbol, quote, tf, start, end, exchange):
    if exchange == "binance":
        return fetch_binance_spot_klines(symbol, quote, tf, start, end)
    elif exchange == "coingecko":
        return fetch_coingecko_ohlc(symbol_id=symbol.lower(), vs_currency=quote.lower(), days=365*2)
    else:
        raise ValueError("Unsupported exchange")

def fetch_fut(symbol, quote, tf, start, end):
    return fetch_binance_futures_klines(symbol, quote, tf, start, end)

def prepare_frames_with_indicators(df):
    if df is None or df.empty:
        return df
    out = df.copy().sort_values("open_time").reset_index(drop=True)
    out = add_basic_indicators(out)
    return out

def main():
    args = _parse_args()
    cfg = load_config(args)
    symbol = cfg["symbol"]
    quote = cfg["quote"]
    timeframes = cfg["timeframes"]
    start = cfg["start"]
    end = cfg["end"]
    exchange = cfg["exchange"]
    want_spot = cfg["fetch_spot"]
    want_fut = cfg["fetch_futures"]
    out_dir = cfg["out_dir"]
    gen_html = cfg["generate_html"]
    os.makedirs(out_dir, exist_ok=True)

    excel_sheets = {}
    html_datasets = {}

    for tf in timeframes:
        if want_spot:
            spot = fetch_spot(symbol, quote, tf, start, end, exchange)
            spot = prepare_frames_with_indicators(spot)
            excel_sheets[f"spot_{tf}"] = spot
            html_datasets[f"spot_{tf}"] = spot
        if want_fut:
            fut = fetch_fut(symbol, quote, tf, start, end)
            fut = prepare_frames_with_indicators(fut)
            excel_sheets[f"futures_{tf}"] = fut
            html_datasets[f"futures_{tf}"] = fut

    summary_rows = []
    for name, df in excel_sheets.items():
        if df is None or df.empty:
            continue
        last = df.iloc[-1]
        summary_rows.append({
            "series": name,
            "time": last["open_time"],
            "close": last["close"],
            "rsi_14": last.get("rsi_14", None),
            "mfi_14": last.get("mfi_14", None),
            "macd": last.get("macd", None),
            "macd_signal": last.get("macd_signal", None),
            "flag_overbought": last.get("flag_overbought", None),
            "flag_oversold": last.get("flag_oversold", None),
            "atr_14": last.get("atr_14", None),
            "obv": last.get("obv", None),
        })
    excel_sheets["summary"] = pd.DataFrame(summary_rows)

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    excel_path = os.path.join(out_dir, f"{symbol.upper()}_{quote.upper()}_report_{ts}.xlsx")
    save_to_excel(excel_path, excel_sheets)

    html_path = None
    if gen_html:
        html_path = os.path.join(out_dir, f"{symbol.upper()}_{quote.upper()}_dashboard_{ts}.html")
        make_dashboard_html(html_path, html_datasets)

    print("Saved:", excel_path)
    if html_path:
        print("HTML dashboard:", html_path)

if __name__ == "__main__":
    main()
