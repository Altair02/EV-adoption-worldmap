"""
scripts/update_data.py  —  v29 (Override protection for DE + BE + LU + FR + ES + PT + GB + IE + IS + NO + SE)
- Germany:        KBA Override
- Belgium:        FEBIAC Override (until March 2026)
- Luxembourg:     STATEC / SNCA / ACEA Override (until March 2026)
- France:         CCFA Override (until March 2026)
- Spain:          ANFAC Override (until March 2026)
- Portugal:       ACAP Override (until March 2026)
- United Kingdom: SMMT Override (until March 2026)
- Ireland:        SIMI Override (until March 2026)
- Iceland:        Statistics Iceland Override (until March 2026)
- Norway:         OFV Override (until March 2026)
- Sweden:         Trafikanalys Override (until March 2026)
- Netherlands:    RDW
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

COUNTRIES = {
    "AT": ("Austria",        "AT",  9.1), "BE": ("Belgium",        "BE", 11.6),
    "BG": ("Bulgaria",       "BG",  6.5), "CY": ("Cyprus",         "CY",  1.2),
    "CZ": ("Czech Republic", "CZ", 10.9), "DE": ("Germany",        "DE", 84.4),
    "DK": ("Denmark",        "DK",  5.9), "EE": ("Estonia",        "EE",  1.4),
    "EL": ("Greece",         "EL", 10.4), "ES": ("Spain",          "ES", 47.4),
    "FI": ("Finland",        "FI",  5.6), "FR": ("France",         "FR", 68.1),
    "HR": ("Croatia",        "HR",  3.9), "HU": ("Hungary",        "HU",  9.7),
    "IE": ("Ireland",        "IE",  5.1), "IT": ("Italy",          "IT", 59.1),
    "LT": ("Lithuania",      "LT",  2.8), "LU": ("Luxembourg",     "LU",  0.7),
    "LV": ("Latvia",         "LV",  1.8), "MT": ("Malta",          "MT",  0.5),
    "NL": ("Netherlands",    "NL", 17.9), "PL": ("Poland",         "PL", 37.6),
    "PT": ("Portugal",       "PT", 10.3), "RO": ("Romania",        "RO", 19.0),
    "SE": ("Sweden",         "SE", 10.5), "SI": ("Slovenia",       "SI",  2.1),
    "SK": ("Slovakia",       "SK",  5.5), "NO": ("Norway",         "NO",  5.5),
    "CH": ("Switzerland",    "CH",  8.8), "GB": ("United Kingdom", "UK", 67.4),
    "IS": ("Iceland",        "IS",  0.4),
}

# ... (alle anderen Funktionen wie http_get, parse_csv, fetch_ecb_monthly, fetch_eurostat_annual bleiben gleich wie in v28)

# ── Overrides ─────────────────────
def load_override(filename):
    path = DATA_DIR / filename
    if path.exists():
        try:
            data = json.loads(path.read_text("utf-8"))
            print(f"[Override] {filename} loaded ({len(data.get('monthly', {}).get('labels', []))} months)")
            return data.get("monthly")
        except Exception as e:
            print(f"[Override] Error with {filename}: {e}")
    return None

def fetch_rdw_netherlands():
    print("[RDW] Netherlands data...")
    rdw_data = { /* dein bestehender RDW-Dict bleibt unverändert */ }
    labels = list(rdw_data.keys())
    totals = list(rdw_data.values())
    return {"labels": labels, "total": totals}

def write_files(monthly, annual):
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    changed = []
    de_override = load_override("germany_monthly_override.json")
    be_override = load_override("belgium_monthly_override.json")
    lu_override = load_override("luxembourg_monthly_override.json")
    fr_override = load_override("france_monthly_override.json")
    es_override = load_override("spain_monthly_override.json")
    pt_override = load_override("portugal_monthly_override.json")
    gb_override = load_override("united_kingdom_monthly_override.json")
    ie_override = load_override("ireland_monthly_override.json")
    is_override = load_override("iceland_monthly_override.json")
    no_override = load_override("norway_monthly_override.json")
    se_override = load_override("sweden_monthly_override.json")   # ← neu für Schweden

    for ecb_code, (name, estat_code, pop) in COUNTRIES.items():
        m = monthly.get(ecb_code, {})

        if ecb_code == "DE" and de_override:
            m = de_override
            print("[Override] Germany monthly data protected")
        elif ecb_code == "BE" and be_override:
            m = be_override
            print("[Override] Belgium monthly data protected")
        elif ecb_code == "LU" and lu_override:
            m = lu_override
            print("[Override] Luxembourg monthly data protected")
        elif ecb_code == "FR" and fr_override:
            m = fr_override
            print("[Override] France monthly data protected")
        elif ecb_code == "ES" and es_override:
            m = es_override
            print("[Override] Spain monthly data protected")
        elif ecb_code == "PT" and pt_override:
            m = pt_override
            print("[Override] Portugal monthly data protected")
        elif ecb_code == "GB" and gb_override:
            m = gb_override
            print("[Override] United Kingdom monthly data protected")
        elif ecb_code == "IE" and ie_override:
            m = ie_override
            print("[Override] Ireland monthly data protected")
        elif ecb_code == "IS" and is_override:
            m = is_override
            print("[Override] Iceland monthly data protected")
        elif ecb_code == "NO" and no_override:
            m = no_override
            print("[Override] Norway monthly data protected")
        elif ecb_code == "SE" and se_override:                    # ← neu
            m = se_override
            print("[Override] Sweden monthly data protected")

        # ... (annual_block und payload bleiben gleich)

        if ecb_code == "DE":
            source_monthly = "KBA (monthly new registrations up to March 2026)"
        elif ecb_code == "NL":
            source_monthly = "RDW (monthly new registrations from Jan 2015 to March 2026)"
        elif ecb_code == "BE":
            source_monthly = "FEBIAC / FPS Mobility (monthly new registrations up to March 2026)"
        elif ecb_code == "LU":
            source_monthly = "STATEC / SNCA / ACEA (monthly new registrations up to March 2026)"
        elif ecb_code == "FR":
            source_monthly = "CCFA (monthly new registrations up to March 2026)"
        elif ecb_code == "ES":
            source_monthly = "ANFAC (monthly new registrations up to March 2026)"
        elif ecb_code == "PT":
            source_monthly = "ACAP (monthly new registrations up to March 2026)"
        elif ecb_code == "GB":
            source_monthly = "SMMT (monthly new registrations up to March 2026)"
        elif ecb_code == "IE":
            source_monthly = "SIMI (monthly new registrations up to March 2026)"
        elif ecb_code == "IS":
            source_monthly = "Statistics Iceland (monthly new registrations up to March 2026)"
        elif ecb_code == "NO":
            source_monthly = "OFV (monthly new registrations up to March 2026)"
        elif ecb_code == "SE":
            source_monthly = "Trafikanalys (monthly new registrations up to March 2026)"
        else:
            source_monthly = "ECB Data Portal / ACEA"

        # ... (Rest des write_files bleibt gleich wie in v28)

    return changed

# send_telegram und main() bleiben gleich, nur Version und Text anpassen
def main():
    print("=" * 60)
    print(f"  Car Registration Updater v29  —  {NOW.strftime('%d.%m.%Y %H:%M UTC')}")
    print("=" * 60)

    # ... (Rest bleibt gleich)

    print("\n✓ Done – Overrides for DE, BE, LU, FR, ES, PT, GB, IE, IS, NO + SE active (data up to March 2026)")

if __name__ == "__main__":
    main()
