"""
scripts/update_data.py  —  v11 (final)
==============================
WICHTIGER HINWEIS (seit 2023):
ECB stellt KEINE monatlichen Zulassungszahlen MEHR pro Land zur Verfügung.
Nur noch Euro-Gesamt (EA21). 
Monatliche Daten enden daher bei Dezember 2022.
Annual powertrain data weiterhin von Eurostat.
"""

import csv, io, json, os, sys, time, urllib.error, urllib.request
from datetime import datetime, timezone
from pathlib import Path

TELEGRAM_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT  = os.environ.get("TELEGRAM_CHAT_ID", "")
REPO_ROOT      = Path(__file__).resolve().parent.parent
DATA_DIR       = REPO_ROOT / "data" / "countries"
NOW            = datetime.now(timezone.utc)

COUNTRIES = {
    "AT": ("Austria",        "AT",  9.1),
    "BE": ("Belgium",        "BE", 11.6),
    "BG": ("Bulgaria",       "BG",  6.5),
    "CY": ("Cyprus",         "CY",  1.2),
    "CZ": ("Czech Republic", "CZ", 10.9),
    "DE": ("Germany",        "DE", 84.4),
    "DK": ("Denmark",        "DK",  5.9),
    "EE": ("Estonia",        "EE",  1.4),
    "EL": ("Greece",         "EL", 10.4),
    "ES": ("Spain",          "ES", 47.4),
    "FI": ("Finland",        "FI",  5.6),
    "FR": ("France",         "FR", 68.1),
    "HR": ("Croatia",        "HR",  3.9),
    "HU": ("Hungary",        "HU",  9.7),
    "IE": ("Ireland",        "IE",  5.1),
    "IT": ("Italy",          "IT", 59.1),
    "LT": ("Lithuania",      "LT",  2.8),
    "LU": ("Luxembourg",     "LU",  0.7),
    "LV": ("Latvia",         "LV",  1.8),
    "MT": ("Malta",          "MT",  0.5),
    "NL": ("Netherlands",    "NL", 17.9),
    "PL": ("Poland",         "PL", 37.6),
    "PT": ("Portugal",       "PT", 10.3),
    "RO": ("Romania",        "RO", 19.0),
    "SE": ("Sweden",         "SE", 10.5),
    "SI": ("Slovenia",       "SI",  2.1),
    "SK": ("Slovakia",       "SK",  5.5),
    "NO": ("Norway",         "NO",  5.5),
    "CH": ("Switzerland",    "CH",  8.8),
    "GB": ("United Kingdom", "UK", 67.4),
}

# ... (http_get, parse_csv, try_fetch bleiben gleich wie in deiner v10 – kopiere sie einfach aus deiner aktuellen Datei)

def fetch_ecb_monthly():
    STS_KEYS = ["M.XX.W.CREG.PC0000.3.ABS", "M.XX.N.CREG.PC0000.3.ABS"]
    print("[ECB] Teste STS-Keys (2015–2022)…")
    sts_key = None
    for k in STS_KEYS:
        d = try_fetch("STS", k, "DE", "2020-01")
        if d:
            sts_key = k
            print(f"[ECB] ✅ STS-Key gefunden: {k}")
            break
    if not sts_key:
        print("[ECB] ❌ Kein STS-Key gefunden")
        return {}

    print(f"\n[ECB] Hole monatliche Daten für {len(COUNTRIES)} Länder…")
    results = {}
    for geo in COUNTRIES:
        merged = try_fetch("STS", sts_key, geo, "2015-01")
        if merged:
            months = sorted(merged.keys())
            results[geo] = {"labels": months, "total": [merged[m] for m in months]}
            print(f"[ECB]   {geo}: {len(months)} Monate (bis {months[-1]})")
        time.sleep(0.2)

    print("[ECB] Fertig – Monatsdaten enden bei Dezember 2022 (ECB hat Länderdaten eingestellt)")
    return results

# fetch_eurostat_annual() bleibt 100 % gleich wie bisher (kopiere den kompletten Block aus deiner v10)

def write_files(monthly, annual):
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    changed = []
    for ecb_code, (name, estat_code, pop) in COUNTRIES.items():
        m = monthly.get(ecb_code, {})
        a = annual.get(ecb_code, {})

        years = sorted(a.keys()) if a else []
        annual_block = {
            "labels": years,
            "bev":    [a[y].get("bev",    0) for y in years],
            "phev":   [a[y].get("phev",   0) for y in years],
            "hybrid": [a[y].get("hybrid", 0) for y in years],
            "petrol": [a[y].get("petrol", 0) for y in years],
            "diesel": [a[y].get("diesel", 0) for y in years],
            "other":  [a[y].get("other",  0) for y in years],
        }

        payload = {
            "name": name,
            "ecb_code": ecb_code,
            "population_mio": pop,
            "source_monthly": "ECB Data Portal / ACEA (country-level monthly data only until Dec 2022)",
            "source_annual":  "Eurostat road_eqr_carpda",
            "last_updated":   NOW.isoformat(),
            "monthly": m,
            "annual": annual_block,
        }

        fname = name.lower().replace(" ", "_") + ".json"
        path = DATA_DIR / fname
        old = {}
        if path.exists():
            try:
                old = json.loads(path.read_text("utf-8"))
            except Exception:
                pass

        def no_ts(d):
            d2 = dict(d)
            d2.pop("last_updated", None)
            return d2

        path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), "utf-8")

        if no_ts(payload) != no_ts(old):
            changed.append(name)
            print(f"[Write] {name} ✓ (aktualisiert)")
        else:
            print(f"[Write] {name} (nur Timestamp)")

    return changed

# send_telegram und main() bleiben gleich (kopiere aus deiner v10)

def main():
    print("=" * 60)
    print(f"  Car Registration Updater v11 (final)  —  {NOW.strftime('%d.%m.%Y %H:%M UTC')}")
    print("=" * 60)
    monthly = fetch_ecb_monthly()
    annual  = fetch_eurostat_annual()
    changed = write_files(monthly, annual)
    latest  = max((v["labels"][-1] for v in monthly.values() if v.get("labels")), default="2022-12")
    send_telegram(changed, len(COUNTRIES), latest)
    print(f"\n\u2713 Fertig — Monatsdaten enden bei 2022-12 (keine neueren Länderdaten verfügbar)")

if __name__ == "__main__":
    main()
