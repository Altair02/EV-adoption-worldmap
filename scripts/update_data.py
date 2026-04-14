"""
scripts/update_data.py  —  v21 (Override-Schutz für DE + BE + RDW NL)
==============================
- Deutschland: Override (germany_monthly_override.json)
- Belgien:     Override (belgium_monthly_override.json) + FEBIAC Quelle
- Niederlande: RDW-Daten
"""

import csv
import io
import json
import os
import time
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

TELEGRAM_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT  = os.environ.get("TELEGRAM_CHAT_ID", "")
REPO_ROOT      = Path(__file__).resolve().parent.parent
DATA_DIR       = REPO_ROOT / "data" / "countries"
NOW            = datetime.now(timezone.utc)

COUNTRIES = { ... }  # dein bestehender COUNTRIES-Dict bleibt unverändert

# ... (http_get, parse_csv, try_fetch, fetch_ecb_monthly, fetch_eurostat_annual bleiben gleich wie in v20)

# ── Override für Deutschland ─────────────────────
def load_germany_override():
    override_path = DATA_DIR / "germany_monthly_override.json"
    if override_path.exists():
        try:
            data = json.loads(override_path.read_text("utf-8"))
            print(f"[Override] Germany override geladen ({len(data.get('monthly', {}).get('labels', []))} Monate)")
            return data.get("monthly")
        except Exception as e:
            print(f"[Override DE] Fehler: {e}")
    return None

# ── Override für Belgien (neu) ─────────────────────
def load_belgium_override():
    override_path = DATA_DIR / "belgium_monthly_override.json"
    if override_path.exists():
        try:
            data = json.loads(override_path.read_text("utf-8"))
            print(f"[Override] Belgium override geladen ({len(data.get('monthly', {}).get('labels', []))} Monate)")
            return data.get("monthly")
        except Exception as e:
            print(f"[Override BE] Fehler: {e}")
    return None

# ── RDW Niederlande (unverändert) ─────────────────────
def fetch_rdw_netherlands():
    # ... dein bisheriger RDW-Dict hier einfügen (wie in v20) ...
    # (kopiere den rdw_data Dict aus deiner vorherigen Version)
    print("[RDW] Niederlande Daten geladen")
    # return {"labels": labels, "total": totals}

# ── Write Files mit Override für DE + BE ─────────────────────
def write_files(monthly, annual):
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    changed = []
    germany_override = load_germany_override()
    belgium_override = load_belgium_override()

    for ecb_code, (name, estat_code, pop) in COUNTRIES.items():
        m = monthly.get(ecb_code, {})

        if ecb_code == "DE" and germany_override:
            m = germany_override
            print(f"[Override] Deutschland monatliche Daten geschützt")
        elif ecb_code == "BE" and belgium_override:
            m = belgium_override
            print(f"[Override] Belgien monatliche Daten geschützt")

        # ... Rest der write_files Funktion wie bisher ...

        if ecb_code == "DE":
            source_monthly = "KBA (monatliche Neuzulassungen bis März 2026)"
        elif ecb_code == "NL":
            source_monthly = "RDW (monatliche Neuzulassungen ab Jan 2015 bis März 2026)"
        elif ecb_code == "BE":
            source_monthly = "FEBIAC / FPS Mobility (monatliche Neuzulassungen)"
        else:
            source_monthly = "ECB Data Portal / ACEA (monthly country data available until Dec 2022)"

        # ... payload und Schreiben wie bisher ...

    return changed

# main() Funktion anpassen:
def main():
    print("=" * 60)
    print(f"  Car Registration Updater v21 (Override DE + BE)  —  {NOW.strftime('%d.%m.%Y %H:%M UTC')}")
    print("=" * 60)

    monthly = fetch_ecb_monthly()
    annual  = fetch_eurostat_annual()

    # RDW NL
    rdw_nl = fetch_rdw_netherlands()
    if "NL" in monthly:
        monthly["NL"]["labels"].extend(rdw_nl.get("labels", []))
        monthly["NL"]["total"].extend(rdw_nl.get("total", []))
    else:
        monthly["NL"] = rdw_nl

    changed = write_files(monthly, annual)
    latest  = max((v["labels"][-1] for v in monthly.values() if v.get("labels")), default="2022-12")
    send_telegram(changed, len(COUNTRIES), latest)
    print(f"\n✓ Fertig — Belgien jetzt mit Override-Schutz wie DE & NL!")

if __name__ == "__main__":
    main()
