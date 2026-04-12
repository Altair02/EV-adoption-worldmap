# “””
scripts/update_data.py  —  v10

Key fix vs v9:
The ECB CAR dataset does NOT support lastNObservations parameter.
v9 used it for probing → always returned empty → fell back to old STS.
v10 fetches directly with startPeriod=2023-01 to check for recent data,
then fetches full history if found.
Working key confirmed: CAR / M.{GEO}.N.NEWCARS.ABS
“””

import csv, io, json, os, sys, time, urllib.error, urllib.request
from datetime import datetime, timezone
from pathlib import Path

TELEGRAM_TOKEN = os.environ.get(“TELEGRAM_BOT_TOKEN”, “”)
TELEGRAM_CHAT  = os.environ.get(“TELEGRAM_CHAT_ID”, “”)
REPO_ROOT      = Path(**file**).resolve().parent.parent
DATA_DIR       = REPO_ROOT / “data” / “countries”
NOW            = datetime.now(timezone.utc)
START_YEAR     = 2015

COUNTRIES = {
“AT”: (“Austria”,        “AT”,  9.1),
“BE”: (“Belgium”,        “BE”, 11.6),
“BG”: (“Bulgaria”,       “BG”,  6.5),
“CY”: (“Cyprus”,         “CY”,  1.2),
“CZ”: (“Czech Republic”, “CZ”, 10.9),
“DE”: (“Germany”,        “DE”, 84.4),
“DK”: (“Denmark”,        “DK”,  5.9),
“EE”: (“Estonia”,        “EE”,  1.4),
“EL”: (“Greece”,         “EL”, 10.4),
“ES”: (“Spain”,          “ES”, 47.4),
“FI”: (“Finland”,        “FI”,  5.6),
“FR”: (“France”,         “FR”, 68.1),
“HR”: (“Croatia”,        “HR”,  3.9),
“HU”: (“Hungary”,        “HU”,  9.7),
“IE”: (“Ireland”,        “IE”,  5.1),
“IT”: (“Italy”,          “IT”, 59.1),
“LT”: (“Lithuania”,      “LT”,  2.8),
“LU”: (“Luxembourg”,     “LU”,  0.7),
“LV”: (“Latvia”,         “LV”,  1.8),
“MT”: (“Malta”,          “MT”,  0.5),
“NL”: (“Netherlands”,    “NL”, 17.9),
“PL”: (“Poland”,         “PL”, 37.6),
“PT”: (“Portugal”,       “PT”, 10.3),
“RO”: (“Romania”,        “RO”, 19.0),
“SE”: (“Sweden”,         “SE”, 10.5),
“SI”: (“Slovenia”,       “SI”,  2.1),
“SK”: (“Slovakia”,       “SK”,  5.5),
“NO”: (“Norway”,         “NO”,  5.5),
“CH”: (“Switzerland”,    “CH”,  8.8),
“GB”: (“United Kingdom”, “UK”, 67.4),
}

FUEL_MAP_GRANULAR = {
“ELC”:         “bev”,
“ELC_PET_PI”:  “phev”,
“ELC_DIE_PI”:  “phev”,
“ELC_PET_HYB”: “hybrid”,
“ELC_DIE_HYB”: “hybrid”,
“PET_X_HYB”:   “petrol”,
“DIE_X_HYB”:   “diesel”,
“LPG”:         “other”,
“GAS”:         “other”,
“HYD_FCELL”:   “other”,
“BIOETH”:      “other”,
“BIODIE”:      “other”,
“BIFUEL”:      “other”,
“OTH”:         “other”,
}
FUEL_MAP_FALLBACK = {
“ELC”: “bev”,
“PET”: “petrol”,
“DIE”: “diesel”,
“LPG”: “other”,
“GAS”: “other”,
“OTH”: “other”,
}

def http_get(url, timeout=30):
req = urllib.request.Request(
url,
headers={“User-Agent”: “EV-Map-Bot/10.0 (github.com/Altair02/EV-adoption-worldmap)”}
)
with urllib.request.urlopen(req, timeout=timeout) as r:
return r.read()

def parse_csv(raw):
result = {}
try:
reader = csv.DictReader(io.StringIO(raw))
for row in reader:
row_u = {k.upper(): v for k, v in row.items()}
period = row_u.get(“TIME_PERIOD”, “”).strip()
value  = row_u.get(“OBS_VALUE”,   “”).strip()
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

def fetch_url(url):
“”“Fetch URL, return parsed CSV dict or {} on any error.”””
try:
raw = http_get(url).decode(“utf-8”)
if “TIME_PERIOD” not in raw.upper():
return {}
return parse_csv(raw)
except Exception:
return {}

# ── ECB: per-country, direct fetch ───────────────────────────────────────────

def fetch_ecb_one_country(geo):
“””
Strategy:
1. Try CAR dataset with confirmed working key M.{GEO}.N.NEWCARS.ABS
- First check if recent data (2023+) exists
- If yes, fetch full history from 2015
2. Fall back to alternative CAR keys
3. Last resort: old STS key (data until ~2022)
“””
base = “https://data-api.ecb.europa.eu/service/data”

```
# Step 1: Try primary CAR key — check recent data first (no lastNObservations!)
car_primary = f"M.{geo}.N.NEWCARS.ABS"
recent = fetch_url(f"{base}/CAR/{car_primary}?format=csvdata&startPeriod=2023-01&detail=dataonly")
if recent:
    # Recent data found — now fetch full history
    full = fetch_url(f"{base}/CAR/{car_primary}?format=csvdata&startPeriod={START_YEAR}-01&detail=dataonly")
    if full:
        latest = max(full.keys())
        return full, "CAR", car_primary, latest
time.sleep(0.15)

# Step 2: Try alternative CAR keys (also without lastNObservations)
car_alternatives = [
    f"M.{geo}.N.NEWCARS.N",
    f"M.{geo}.N.NEWCARS.NSA",
    f"M.{geo}.N.CARS.ABS",
    f"M.{geo}.N.CARS.N",
    f"M.{geo}.N.CREG.PC0000.ABS",
    f"M.{geo}.N.CREG.PC0000.3.ABS",
]
for key in car_alternatives:
    recent = fetch_url(f"{base}/CAR/{key}?format=csvdata&startPeriod=2023-01&detail=dataonly")
    if recent:
        full = fetch_url(f"{base}/CAR/{key}?format=csvdata&startPeriod={START_YEAR}-01&detail=dataonly")
        if full:
            latest = max(full.keys())
            return full, "CAR", key, latest
    time.sleep(0.1)

# Step 3: Old STS fallback (will only have data until ~2022)
sts_keys = [
    f"M.{geo}.N.CREG.PC0000.3.ABS",
    f"M.{geo}.W.CREG.PC0000.3.ABS",
]
for key in sts_keys:
    data = fetch_url(f"{base}/STS/{key}?format=csvdata&startPeriod={START_YEAR}-01&detail=dataonly")
    if data:
        latest = max(data.keys())
        return data, "STS", key, latest
    time.sleep(0.1)

return {}, None, None, None
```

def fetch_ecb_monthly():
print(f”[ECB] Fetching {len(COUNTRIES)} countries…”)
results = {}
for geo in COUNTRIES:
data, dataset, key, latest = fetch_ecb_one_country(geo)
if data:
months = sorted(data.keys())
results[geo] = {“labels”: months, “total”: [data[m] for m in months]}
is_recent = latest and latest >= “2024-01”
flag = “✓” if is_recent else “⚠ old”
print(f”[ECB]   {geo} {flag}: {dataset}/{key} → {months[-1]}”)
else:
print(f”[ECB]   {geo}: no data”)
time.sleep(0.15)

```
ok = len(results)
recent = sum(1 for v in results.values() if v["labels"][-1] >= "2024-01")
print(f"[ECB] Done — {ok}/{len(COUNTRIES)} total, {recent} with data up to 2024+")
return results
```

# ── Eurostat annual ───────────────────────────────────────────────────────────

def fetch_eurostat_annual():
url = (“https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/”
“road_eqr_carpda?format=JSON&lang=EN&unit=NR”)
print(f”\n[Eurostat] Fetching annual breakdown…”)
try:
raw = json.loads(http_get(url, timeout=60).decode(“utf-8”))
except Exception as e:
print(f”[Eurostat] Error: {e}”)
return {}

```
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
    for j in range(i + 1, len(dim_order)):
        s *= dim_sizes[j]
    strides[d] = s

years = sorted([y for y in time_idx if int(y) >= START_YEAR])
estat_to_ecb = {v[1]: k for k, v in COUNTRIES.items()}

def get_val(g_pos, t_pos, fuel_code):
    f_pos = mot_idx.get(fuel_code)
    if f_pos is None:
        return 0
    linear = (g_pos * strides.get("geo",     1) +
              t_pos * strides.get("time",    1) +
              f_pos * strides.get("mot_nrg", 1))
    v = values.get(str(linear)) or values.get(linear)
    return int(v) if v is not None else 0

result = {}
for estat_geo, g_pos in geo_idx.items():
    ecb_key = estat_to_ecb.get(estat_geo)
    if not ecb_key:
        continue
    country_data = {}
    for year in years:
        t_pos = time_idx.get(year)
        if t_pos is None:
            continue
        year_vals = {f: 0 for f in ["bev","phev","hybrid","petrol","diesel","other"]}
        for fuel_code, field in FUEL_MAP_GRANULAR.items():
            year_vals[field] += get_val(g_pos, t_pos, fuel_code)
        if year_vals["petrol"] == 0 and year_vals["diesel"] == 0:
            for fuel_code, field in FUEL_MAP_FALLBACK.items():
                v = get_val(g_pos, t_pos, fuel_code)
                if v > 0:
                    year_vals[field] += v
        country_data[year] = year_vals
    if country_data:
        result[ecb_key] = country_data

print(f"[Eurostat] Done — {len(result)} countries")
return result
```

# ── Write files ───────────────────────────────────────────────────────────────

def write_files(monthly, annual):
DATA_DIR.mkdir(parents=True, exist_ok=True)
changed = []

```
for ecb_code, (name, estat_code, pop) in COUNTRIES.items():
    new_monthly = monthly.get(ecb_code, {})
    new_annual  = annual.get(ecb_code, {})

    if not new_monthly and not new_annual:
        continue

    # Load existing file
    old_payload = {}
    fname = name.lower().replace(" ", "_") + ".json"
    path  = DATA_DIR / fname
    if path.exists():
        try:
            old_payload = json.loads(path.read_text("utf-8"))
        except Exception:
            pass

    # Merge monthly: keep whichever is more recent
    old_monthly = old_payload.get("monthly", {})
    old_latest  = old_monthly.get("labels", [""])[-1] if old_monthly.get("labels") else ""
    new_latest  = new_monthly.get("labels", [""])[-1] if new_monthly.get("labels") else ""

    if new_monthly and new_latest >= old_latest:
        merged_monthly = new_monthly
    else:
        merged_monthly = old_monthly

    # Annual block
    years = sorted(new_annual.keys()) if new_annual else []
    annual_block = old_payload.get("annual", {})
    if years:
        annual_block = {
            "labels": years,
            "bev":    [new_annual[y].get("bev",    0) for y in years],
            "phev":   [new_annual[y].get("phev",   0) for y in years],
            "hybrid": [new_annual[y].get("hybrid", 0) for y in years],
            "petrol": [new_annual[y].get("petrol", 0) for y in years],
            "diesel": [new_annual[y].get("diesel", 0) for y in years],
            "other":  [new_annual[y].get("other",  0) for y in years],
        }

    payload = {
        "name": name, "ecb_code": ecb_code, "population_mio": pop,
        "source_monthly": "ECB Data Portal / ACEA",
        "source_annual":  "Eurostat road_eqr_carpda",
        "last_updated":   NOW.isoformat(),
        "monthly": merged_monthly,
        "annual":  annual_block,
    }

    def no_ts(d):
        d2 = dict(d); d2.pop("last_updated", None); return d2

    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), "utf-8")
    if no_ts(payload) != no_ts(old_payload):
        changed.append(name)
        m_end = merged_monthly.get("labels", ["?"])[-1] if merged_monthly.get("labels") else "none"
        print(f"[Write] {name} ✓  monthly → {m_end}")
    else:
        print(f"[Write] {name} (no change)")

return changed
```

# ── Telegram ──────────────────────────────────────────────────────────────────

def send_telegram(changed, n_countries):
if not TELEGRAM_TOKEN or not TELEGRAM_CHAT:
return
date_str = NOW.strftime(”%d.%m.%Y %H:%M UTC”)
if changed:
lines = “\n”.join(f”  • {c}” for c in changed[:25])
body = (f”🚗 *Car Registrations — Updated*\n📅 {date_str}\n”
f”🌍 {n_countries} countries\n\n*Changed ({len(changed)}):*\n{lines}”)
else:
body = (f”🚗 *Car Registrations — No Changes*\n📅 {date_str}\n”
f”🌍 {n_countries} — all up to date.”)
body += (”\n\n🗺️ [Open Map](https://altair02.github.io/EV-adoption-worldmap/)\n”
“📁 [GitHub](https://github.com/Altair02/EV-adoption-worldmap)”)
data = json.dumps({
“chat_id”: TELEGRAM_CHAT, “text”: body,
“parse_mode”: “Markdown”, “disable_web_page_preview”: False,
}).encode(“utf-8”)
req = urllib.request.Request(
f”https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage”,
data=data, headers={“Content-Type”: “application/json”}
)
try:
with urllib.request.urlopen(req, timeout=10) as r:
msg_id = json.loads(r.read()).get(“result”, {}).get(“message_id”, “?”)
print(f”[Telegram] Sent ✓ (id={msg_id})”)
except Exception as e:
print(f”[Telegram] Error: {e}”)

# ── Main ──────────────────────────────────────────────────────────────────────

def main():
print(”=” * 60)
print(f”  Car Registration Updater v10  —  {NOW.strftime(’%d.%m.%Y %H:%M UTC’)}”)
print(”=” * 60)
monthly = fetch_ecb_monthly()
annual  = fetch_eurostat_annual()
if not monthly and not annual:
print(“⚠️  No data from either source”)
send_telegram([], 0)
sys.exit(1)
changed = write_files(monthly, annual)
send_telegram(changed, len(COUNTRIES))
print(f”\n✓ Done — {len(changed)} countries updated”)

if **name** == “**main**”:
main()
