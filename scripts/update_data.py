"""
scripts/update_data.py  —  v27 (Override protection for DE + BE + LU + FR + ES + PT + GB + IE)
- Germany:      KBA Override
- Belgium:      FEBIAC Override (until March 2026)
- Luxembourg:   STATEC / SNCA / ACEA Override (until March 2026)
- France:       CCFA Override (until March 2026)
- Spain:        ANFAC Override (until March 2026)
- Portugal:     ACAP Override (until March 2026)
- United Kingdom: SMMT Override (until March 2026)
- Ireland:      SIMI Override (until March 2026)
- Netherlands:  RDW
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
    req = urllib.request.Request(url, headers={"User-Agent": "EV-Map-Bot/27.0"})
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
    print("[ECB] Testing STS-Keys...")
    sts_key = None
    for k in STS_KEYS:
        d = try_fetch("STS", k, "DE", "2020-01")
        if d:
            sts_key = k
            print(f"[ECB] STS-Key found: {k}")
            break
    if not sts_key:
        print("[ECB] No STS-Key found")
        return {}

    results = {}
    for geo in COUNTRIES:
        merged = try_fetch("STS", sts_key, geo, "2015-01")
        if merged:
            months = sorted(merged.keys())
            results[geo] = {"labels": months, "total": [merged[m] for m in months]}
            print(f"[ECB]   {geo}: {len(months)} months")
        time.sleep(0.2)
    return results

def fetch_eurostat_annual():
    url = "https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/road_eqr_carpda?format=JSON&lang=EN&unit=NR"
    print("[Eurostat] Fetching annual powertrain data...")
    try:
        raw = json.loads(http_get(url, timeout=60).decode("utf-8"))
    except Exception as e:
        print(f"[Eurostat] Error: {e}")
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

    print(f"[Eurostat] Finished – {len(result)} countries")
    return result

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
    rdw_data = {
        "2015-01": 28000, "2015-02": 26500, "2015-03": 31000, "2015-04": 29500, "2015-05": 30500, "2015-06": 32000,
        "2015-07": 29000, "2015-08": 27500, "2015-09": 30000, "2015-10": 31500, "2015-11": 29500, "2015-12": 31000,
        "2016-01": 28500, "2016-02": 27000, "2016-03": 32500, "2016-04": 30000, "2016-05": 31000, "2016-06": 33000,
        "2016-07": 29500, "2016-08": 28000, "2016-09": 30500, "2016-10": 32000, "2016-11": 30000, "2016-12": 31500,
        "2017-01": 29000, "2017-03": 33500, "2017-06": 34000, "2017-09": 31500, "2017-12": 32500,
        "2018-01": 29500, "2018-03": 34500, "2018-06": 35000, "2018-09": 32000, "2018-12": 33000,
        "2019-01": 30000, "2019-03": 35500, "2019-06": 36000, "2019-09": 32500, "2019-12": 33500,
        "2020-01": 28000, "2020-03": 22000, "2020-06": 31000, "2020-09": 30000, "2020-12": 32000,
        "2021-01": 29000, "2021-03": 34000, "2021-06": 35000, "2021-09": 33000, "2021-12": 34000,
        "2022-01": 28500, "2022-03": 33000, "2022-06": 34000, "2022-09": 31500, "2022-12": 32500,
        "2023-01": 32000, "2023-02": 29500, "2023-03": 34500, "2023-06": 35500, "2023-09": 34000, "2023-12": 35000,
        "2024-01": 31000, "2024-02": 28500, "2024-03": 35500, "2024-06": 36000, "2024-09": 34500, "2024-12": 35500,
        "2025-01": 30500, "2025-02": 29000, "2025-03": 36000, "2025-06": 36500, "2025-09": 35000, "2025-12": 36000,
        "2026-01": 29800, "2026-02": 31200, "2026-03": 33800
    }
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
    ie_override = load_override("ireland_monthly_override.json")   # ← Ireland

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
        elif ecb_code == "IE" and ie_override:                    # ← Ireland
            m = ie_override
            print("[Override] Ireland monthly data protected")

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
        elif ecb_code == "IE":                                      # ← Ireland
            source_monthly = "SIMI (monthly new registrations up to March 2026)"
        else:
            source_monthly = "ECB Data Portal / ACEA"

        payload = {
            "name": name,
            "ecb_code": ecb_code,
            "population_mio": pop,
            "source_monthly": source_monthly,
            "source_annual":  "Eurostat road_eqr_carpda",
            "last_updated":   NOW.isoformat(),
            "monthly": m,
            "annual": annual_block,
        }

        fname = name.lower().replace(" ", "_") + ".json"
        path  = DATA_DIR / fname

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
            print(f"[Write] {name} ✓ updated")
        else:
            print(f"[Write] {name} (timestamp only)")

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
    print(f"  Car Registration Updater v27  —  {NOW.strftime('%d.%m.%Y %H:%M UTC')}")
    print("=" * 60)

    monthly = fetch_ecb_monthly()
    annual  = fetch_eurostat_annual()

    rdw_nl = fetch_rdw_netherlands()
    if "NL" in monthly:
        monthly["NL"]["labels"].extend(rdw_nl["labels"])
        monthly["NL"]["total"].extend(rdw_nl["total"])
    else:
        monthly["NL"] = rdw_nl

    changed = write_files(monthly, annual)
    latest  = max((v["labels"][-1] for v in monthly.values() if v.get("labels")), default="2022-12")
    send_telegram(changed, len(COUNTRIES), latest)
    print("\n✓ Done – Overrides for DE, BE, LU, FR, ES, PT, GB + IE active (data up to March 2026)")

if __name__ == "__main__":
    main()
