"""
scripts/update_data.py  —  v14 (stabil, ECB post-2022)
==============================
WICHTIG: Seit 2023 liefert der ECB keine monatlichen Zulassungszahlen pro Land mehr.
Monatliche Daten enden bei Dezember 2022.
Jährliche Powertrain-Daten kommen weiterhin von Eurostat.
"""

import csv, io, json, os, sys, time, urllib.request
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
    req = urllib.request.Request(
        url,
        headers={"User-Agent": "EV-Map-Bot/14.0 (github.com/Altair02/EV-adoption-worldmap)"}
    )
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read()

def parse_csv(raw):
    result = {}
    try:
        reader = csv.DictReader(io.StringIO(raw))
        for row in reader:
            row_u = {k.upper(): v for k, v in row.items()}
            period = row_u.get("TIME_PERIOD", "").strip()
            value  = row_u.get("OBS_VALUE", "").strip()
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

# ── ECB monthly: nur noch STS (bis 2022) ────────────────────────
def fetch_ecb_monthly():
    STS_KEYS = ["M.XX.W.CREG.PC0000.3.ABS", "M.XX.N.CREG.PC0000.3.ABS"]
    print("[ECB] Teste STS-Keys (2015–2022)…")
    sts_key = None
    for k in STS_KEYS:
        d = try_fetch("STS", k, "DE", "2020-01")
        if d:
            sts_key = k
            print(f"[ECB] STS-Key gefunden: {k}")
            break
    if not sts_key:
        print("[ECB] Kein STS-Key gefunden – monatliche Daten nur bis 2022 möglich")
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

    print("[ECB] Fertig – Monatsdaten enden bei Dezember 2022 (keine neueren Länderdaten verfügbar)")
    return results

# ── Eurostat annual powertrain (unverändert) ─────────────────────
def fetch_eurostat_annual():
    url = ("https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/"
           "road_eqr_carpda?format=JSON&lang=EN&unit=NR")
    print(f"\n[Eurostat] Hole jährliche Powertrain-Daten…")
    try:
        raw = json.loads(http_get(url, timeout=60).decode("utf-8"))
    except Exception as e:
        print(f"[Eurostat] Fehler: {e}")
        return {}

    dims      = raw.get("dimension", {})
    values    = raw.get("value", {})
    dim_order = raw.get("id",   [])
    dim_sizes = raw.get("size", [])

    def idx(dim):
        return dims.get(dim, {}).get("category", {}).get("index", {})

    geo_idx  = idx("geo")
    time_idx = idx("time")
    mot_idx  = idx("mot_nrg")

    strides = {}
    for i, d in enumerate(dim_order):
        s = 1
        for j in range(i+1, len(dim_order)):
            s *= dim_sizes[j]
        strides[d] = s

    years = sorted([y for y in time_idx if int(y) >= 2015])
    estat_to_ecb = {v[1]: k for k, v in COUNTRIES.items()}

    def get_val(g, t, fuel):
        f = mot_idx.get(fuel)
        if f is None: return 0
        linear = g*strides.get("geo",1) + t*strides.get("time",1) + f*strides.get("mot_nrg",1)
        v = values.get(str(linear)) or values.get(linear)
        return int(v) if v is not None else 0

    result = {}
    for estat_geo, g_pos in geo_idx.items():
        ecb_key = estat_to_ecb.get(estat_geo)
        if not ecb_key: continue
        country_data = {}
        for year in years:
            t_pos = time_idx.get(year)
            if t_pos is None: continue
            yv = {f: 0 for f in ["bev","phev","hybrid","petrol","diesel","other"]}
            for fuel_code, field in FUEL_MAP_GRANULAR.items():
                yv[field] += get_val(g_pos, t_pos, fuel_code)
            if yv["petrol"] == 0 and yv["diesel"] == 0:
                for fuel_code, field in FUEL_MAP_FALLBACK.items():
                    v = get_val(g_pos, t_pos, fuel_code)
                    if v > 0: yv[field] += v
            country_data[year] = yv
        if country_data:
            result[ecb_key] = country_data

    print(f"[Eurostat] Fertig – {len(result)} Länder, Jahre {years[0]}–{years[-1]}")
    return result

# ── Write JSON files ──────────────────────────────────────────────────────────
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
            "source_monthly": "ECB Data Portal / ACEA (monthly country data available until Dec 2022)",
            "source_annual":  "Eurostat road_eqr_carpda",
            "last_updated":   NOW.isoformat(),
            "monthly": m,
            "annual": annual_block,
        }
        fname = name.lower().replace(" ", "_") + ".json"
        path  = DATA_DIR / fname
        old   = {}
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

def send_telegram(changed, n_countries, latest_month):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT:
        return
    date_str = NOW.strftime("%d.%m.%Y %H:%M UTC")
    if changed:
        lines = "\n".join(f"  \u2022 {c}" for c in changed[:25])
        body  = (f"\U0001F697 *Car Registrations \u2014 Updated*\n"
                 f"\U0001F4C5 {date_str}\n"
                 f"\U0001F30D {n_countries} countries\n"
                 f"\U0001F4C8 Monthly data up to: {latest_month}\n\n"
                 f"*Changed ({len(changed)}):*\n{lines}")
    else:
        body = (f"\U0001F697 *Car Registrations \u2014 No Changes*\n"
                f"\U0001F4C5 {date_str}\n"
                f"\U0001F30D {n_countries} countries \u2014 all up to date.")
    body += ("\n\n\U0001F5FA\uFE0F [Open Map](https://altair02.github.io/EV-adoption-worldmap/)\n"
             "\U0001F4C1 [GitHub](https://github.com/Altair02/EV-adoption-worldmap)")
    data = json.dumps({
        "chat_id": TELEGRAM_CHAT, "text": body,
        "parse_mode": "Markdown", "disable_web_page_preview": False,
    }).encode("utf-8")
    req = urllib.request.Request(
        f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
        data=data, headers={"Content-Type": "application/json"}
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as r:
            msg_id = json.loads(r.read()).get("result", {}).get("message_id", "?")
        print(f"[Telegram] Sent ✓ (id={msg_id})")
    except Exception as e:
        print(f"[Telegram] Error: {e}")

def main():
    print("=" * 60)
    print(f"  Car Registration Updater v14  —  {NOW.strftime('%d.%m.%Y %H:%M UTC')}")
    print("=" * 60)
    monthly = fetch_ecb_monthly()
    annual  = fetch_eurostat_annual()
    changed = write_files(monthly, annual)
    latest  = max(
        (v["labels"][-1] for v in monthly.values() if v.get("labels")),
        default="2022-12"
    )
    send_telegram(changed, len(COUNTRIES), latest)
    print(f"\n\u2713 Fertig — Monatsdaten enden bei Dezember 2022")

if __name__ == "__main__":
    main()
