# -*- coding: utf-8 -*-
import os, json
import pandas as pd
import plotly.graph_objects as go
import plotly.io as pio

def make_ohlc_fig(df: pd.DataFrame, title: str):
    fig = go.Figure()
    fig.add_trace(go.Candlestick(
        x=df["open_time"],
        open=df["open"], high=df["high"], low=df["low"], close=df["close"],
        name="OHLC"
    ))
    fig.update_layout(title=title, xaxis_rangeslider_visible=False, template="plotly_white", height=540)
    return fig

def add_indicator_traces(fig, df: pd.DataFrame):
    if {"bb_up_20_2","bb_mid_20","bb_lo_20_2"}.issubset(df.columns):
        fig.add_trace(go.Scatter(x=df["open_time"], y=df["bb_up_20_2"], mode="lines", name="BB Up (20,2)", opacity=0.5))
        fig.add_trace(go.Scatter(x=df["open_time"], y=df["bb_mid_20"], mode="lines", name="BB Mid (20)", opacity=0.5))
        fig.add_trace(go.Scatter(x=df["open_time"], y=df["bb_lo_20_2"], mode="lines", name="BB Lo (20,2)", opacity=0.5))

def make_dashboard_html(output_html: str, datasets: dict):
    figs = []
    for key, df in datasets.items():
        if df is None or df.empty:
            continue
        title = f"{key}"
        fig = make_ohlc_fig(df, title)
        add_indicator_traces(fig, df)
        figs.append({"key": key, "html": pio.to_html(fig, include_plotlyjs=False, full_html=False)})

    os.makedirs(os.path.dirname(output_html), exist_ok=True)
    keys_json = json.dumps([f["key"] for f in figs])
    with open(output_html, "w", encoding="utf-8") as f:
        f.write("<html><head><meta charset='utf-8'><title>Crypto Report</title>")
        f.write("<script src='https://cdn.plot.ly/plotly-latest.min.js'></script>")
        f.write("</head><body style='font-family:Arial,sans-serif'>")
        f.write("<h2>Crypto Dashboard</h2><p>Выберите серию ниже.</p>")
        f.write("<div id='buttons'></div>")
        for i, item in enumerate(figs):
            display = "block" if i==0 else "none"
            f.write(f"<div id='fig_{i}' style='display:{display}'>")
            f.write(item["html"])
            f.write("</div>")
        f.write("<script>")
        f.write("const keys = " + keys_json + ";\n")
        f.write("function showFig(idx){\n  for (let i=0;i<keys.length;i++){\n    const el = document.getElementById('fig_'+i);\n    if(el) el.style.display = (i===idx)?'block':'none';\n  }\n}\n")
        f.write("(function(){\n  const container = document.getElementById('buttons');\n  keys.forEach((k, idx) => {\n    const btn = document.createElement('button');\n    btn.textContent = k;\n    btn.style.marginRight = '8px';\n    btn.onclick = () => showFig(idx);\n    container.appendChild(btn);\n  });\n})();\n")
        f.write("</script></body></html>")
    return output_html
