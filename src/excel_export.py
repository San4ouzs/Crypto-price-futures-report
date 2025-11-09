# -*- coding: utf-8 -*-
import os
import pandas as pd

def save_to_excel(filepath: str, sheets: dict):
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    with pd.ExcelWriter(filepath, engine="xlsxwriter", datetime_format="yyyy-mm-dd hh:mm:ss") as writer:
        for name, df in sheets.items():
            if df is None or df.empty:
                continue
            sheet = name[:31]
            df.to_excel(writer, sheet_name=sheet, index=False)
    return filepath
