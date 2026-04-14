"""
scripts/update_data.py  —  v21 (Override-Schutz für DE + BE + RDW NL)
- Deutschland: KBA Override
- Belgien:     FEBIAC Override (bis März 2026)
- Niederlande: RDW
"""

import csv, io, json, os, time, urllib.request
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
}

FUEL_MAP_GRANULAR = {
    "ELC": "bev", "ELC_PET_PI": "phev", "ELC_DIE_PI": "phev",
    "ELC_PET_HYB": "hybrid", "ELC_DIE_HYB": "hybrid",
    "PET_X_HYB": "petrol", "DIE_X_HYB": "diesel",
    "LPG": "other", "GAS": "other", "HYD_FCELL": "other",
    "BIOETH": "other", "BIODIE": "other", "BIFUEL": "other", "OTH": "other",
}
FUEL_MAP_FALLBACK = {
    "ELC": "bev", "PET": "petrol", "DIE": "diesel",
    "LPG": "other", "GAS": "other", "OTH": "other",
}

def http_get(url, timeout=30):
    req = urllib.request.Request(url, headers={"User-Agent": "EV-Map-Bot/21.0"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read()

def parse_csv(raw):
    result = {}
    try:
        reader = csv.DictReader(io.StringIO(raw))
        for row in reader:
            row_u = {k.upper(): v for k, v in row.items()}
            period = row_u.get("TIME_PERIOD", "").strip()
            value = row_u.get("OBS_VALUE", "").strip()
            if period and value:
                try:
                    v = int(float(value))
                    if v > 0:
                        result[period] = v
                except ValueError:
                    pass
    except Exception:
        pass
    return result

def try_fetch(dataset, key_template, geo, start_period):
    key = key_template.replace("XX", geo)
    url = f"https://data-api.ecb.europa.eu/service/data/{dataset}/{key}?format=csvdata&startPeriod={start_period}&detail=dataonly"
    try:
        raw = http_get(url, timeout=20).decode("utf-8")
        if "TIME_PERIOD" not in raw.upper():
            return {}
        return parse_csv(raw)
    except Exception:
        return {}

def fetch_ecb_monthly():
    STS_KEYS = ["M.XX.W.CREG.PC0000.3.ABS", "M.XX.N.CREG.PC0000.3.ABS"]
    print("[ECB] Teste STS-Keys...")
    sts_key = None
    for k in STS_KEYS:
        d = try_fetch("STS", k, "DE", "2020-01")
        if d:
            sts_key = k
            print(f"[ECB] STS-Key gefunden: {k}")
            break
    if not sts_key:
        print("[ECB] Kein STS-Key gefunden")
        return {}

    results = {}
    for geo in COUNTRIES:
        merged = try_fetch("STS", sts_key, geo, "2015-01")
        if merged:
            months = sorted(merged.keys())
            results[geo] = {"labels": months, "total": [merged[m] for m in months]}
            print(f"[ECB]   {geo}: {len(months)} Monate")
        time.sleep(0.2)
    return results

def fetch_eurostat_annual():
    url = "https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/road_eqr_carpda?format=JSON&lang=EN&unit=NR"
    print("[Eurostat] Hole jährliche Daten...")
    try:
        raw = json.loads(http_get(url, timeout=60).decode("utf-8"))
    except Exception as e:
        print(f"[Eurostat] Fehler: {e}")
        return {}

    # ... (die komplette Eurostat-Funktion bleibt wie in der vorherigen Version – ich lasse sie hier der Kürze halber aus, kopiere sie 1:1 aus deiner v20 Version)

    # [Einfügen der gesamten fetch_eurostat_annual Funktion aus deiner letzten funktionierenden Version hier]

    print(f"[Eurostat] Fertig – {len(result)} Länder")
    return result   # <-- result muss hier definiert sein (aus der vollständigen Funktion)

# ── Overrides ─────────────────────
def load_override(filename):
    path = DATA_DIR / filename
    if path.exists():
        try:
            data = json.loads(path.read_text("utf-8"))
            print(f"[Override] {filename} geladen ({len(data.get('monthly', {}).get('labels', []))} Monate)")
            return data.get("monthly")
        except Exception as e:
            print(f"[Override] Fehler bei {filename}: {e}")
    return None

def fetch_rdw_netherlands():
    print("[RDW] Niederlande Daten...")
    rdw_data = { ... dein voller RDW-Dict aus v20 ... }   # bitte hier deinen bisherigen rdw_data Dict einfügen
    labels = list(rdw_data.keys())
    totals = list(rdw_data.values())
    return {"labels": labels, "total": totals}

def write_files(monthly, annual):
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    changed = []
    de_override = load_override("germany_monthly_override.json")
    be_override = load_override("belgium_monthly_override.json")

    for ecb_code, (name, estat_code, pop) in COUNTRIES.items():
        m = monthly.get(ecb_code, {})

        if ecb_code == "DE" and de_override:
            m = de_override
            print("[Override] Deutschland geschützt")
        elif ecb_code == "BE" and be_override:
            m = be_override
            print("[Override] Belgien geschützt")

        a = annual.get(ecb_code, {})
        years = sorted(a.keys()) if a else []

        annual_block = {
            "labels": years,
            "bev": [a[y].get("bev", 0) for y in years],
            "phev": [a[y].get("phev", 0) for y in years],
            "hybrid": [a[y].get("hybrid", 0) for y in years],
            "petrol": [a[y].get("petrol", 0) for y in years],
            "diesel": [a[y].get("diesel", 0) for y in years],
            "other": [a[y].get("other", 0) for y in years],
        }

        if ecb_code == "DE":
            source_monthly = "KBA (monatliche Neuzulassungen bis März 2026)"
        elif ecb_code == "NL":
            source_monthly = "RDW (monatliche Neuzulassungen ab Jan 2015 bis März 2026)"
        elif ecb_code == "BE":
            source_monthly = "FEBIAC / FPS Mobility (monatliche Neuzulassungen bis März 2026)"
        else:
            source_monthly = "ECB Data Portal / ACEA"

        payload = {
            "name": name,
            "ecb_code": ecb_code,
            "population_mio": pop,
            "source_monthly": source_monthly,
            "source_annual": "Eurostat road_eqr_carpda",
            "last_updated": NOW.isoformat(),
            "monthly": m,
            "annual": annual_block,
        }

        fname = name.lower().replace(" ", "_") + ".json"
        path = DATA_DIR / fname

        old = {}
        if path.exists():
            try:
                old = json.loads(path.read_text("utf-8"))
            except:
                pass

        def no_ts(d):
            d2 = dict(d)
            d2.pop("last_updated", None)
            return d2

        path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), "utf-8")

        if no_ts(payload) != no_ts(old):
            changed.append(name)
            print(f"[Write] {name} ✓")
        else:
            print(f"[Write] {name} (nur Timestamp)")

    return changed

def send_telegram(changed, n_countries, latest_month):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT:
        return
    date_str = NOW.strftime("%d.%m.%Y %H:%M UTC")
    body = f"🚗 *Car Registrations — Updated*\n📅 {date_str}\n🌍 {n_countries} countries\n📈 Monthly up to: {latest_month}"
    if changed:
        body += f"\n\n*Changed ({len(changed)}):*\n" + "\n".join(f"  • {c}" for c in changed[:25])
    body += "\n\n🗺️ [Open Map](https://altair02.github.io/EV-adoption-worldmap/)"

    data = json.dumps({"chat_id": TELEGRAM_CHAT, "text": body, "parse_mode": "Markdown"}).encode("utf-8")
    req = urllib.request.Request(f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage", data=data, headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=10) as r:
            print("[Telegram] Sent")
    except Exception as e:
        print(f"[Telegram] Error: {e}")

def main():
    print("=" * 60)
    print(f"  Car Registration Updater v21  —  {NOW.strftime('%d.%m.%Y %H:%M UTC')}")
    print("=" * 60)

    monthly = fetch_ecb_monthly()
    annual = fetch_eurostat_annual()

    rdw_nl = fetch_rdw_netherlands()
    if "NL" in monthly:
        monthly["NL"]["labels"].extend(rdw_nl["labels"])
        monthly["NL"]["total"].extend(rdw_nl["total"])
    else:
        monthly["NL"] = rdw_nl

    changed = write_files(monthly, annual)
    latest = max((v["labels"][-1] for v in monthly.values() if v.get("labels")), default="2022-12")
    send_telegram(changed, len(COUNTRIES), latest)
    print("\n✓ Fertig – Belgien jetzt mit FEBIAC Quelle und vollständigen Monatsdaten!")

if __name__ == "__main__":
    main()
