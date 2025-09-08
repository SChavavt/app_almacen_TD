"""Script to convert stored S3 URLs in Google Sheets to plain S3 keys.

Uses environment variable `GOOGLE_SHEETS_CREDENTIALS` containing the JSON
service account credentials.

Run manually: `python migrate_s3_urls_to_keys.py`
"""

import os
import json
from urllib.parse import urlparse, unquote

import gspread
from oauth2client.service_account import ServiceAccountCredentials

SHEET_KEY = "1aWkSelodaz0nWfQx7FZAysGnIYGQFJxAN7RO3YgCiZY"


def extract_s3_key(url_or_key: str) -> str:
    if not isinstance(url_or_key, str):
        return url_or_key
    parsed = urlparse(url_or_key)
    if parsed.scheme and parsed.netloc:
        return unquote(parsed.path.lstrip("/"))
    return url_or_key


def partir_urls(value):
    if value is None:
        return []
    s = str(value).strip()
    if not s or s.lower() in ("nan", "none", "n/a"):
        return []
    try:
        obj = json.loads(s)
        if isinstance(obj, list):
            return [str(it).strip() for it in obj if str(it).strip()]
    except Exception:
        pass
    parts = [p.strip() for p in re.split(r"[,;\n]", s) if p.strip()]
    return parts


import re

def combine_keys(keys):
    return "; ".join(keys)


def convert_sheet(sheet, columns):
    header = sheet.row_values(1)
    for col_name in columns:
        if col_name not in header:
            continue
        col_idx = header.index(col_name) + 1
        values = sheet.col_values(col_idx)[1:]  # skip header
        for i, cell in enumerate(values, start=2):
            keys = [extract_s3_key(u) for u in partir_urls(cell)]
            new_val = combine_keys(keys) if keys else ""
            if new_val != cell:
                sheet.update_cell(i, col_idx, new_val)


def main():
    creds_json = os.environ.get("GOOGLE_SHEETS_CREDENTIALS")
    if not creds_json:
        raise RuntimeError("GOOGLE_SHEETS_CREDENTIALS env var missing")
    credentials_dict = json.loads(creds_json)
    credentials_dict["private_key"] = credentials_dict["private_key"].replace("\\n", "\n")
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    creds = ServiceAccountCredentials.from_json_keyfile_dict(credentials_dict, scope)
    client = gspread.authorize(creds)

    pedidos = client.open_by_key(SHEET_KEY).worksheet("datos_pedidos")
    convert_sheet(pedidos, ["Adjuntos_Guia", "Adjuntos", "Adjuntos_Surtido"])

    casos = client.open_by_key(SHEET_KEY).worksheet("casos_especiales")
    convert_sheet(casos, ["Hoja_Ruta_Mensajero", "Adjuntos", "Adjuntos_Surtido"])


if __name__ == "__main__":
    main()
