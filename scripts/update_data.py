# “””
scripts/update_data.py  —  v7

Key change vs v6:
ECB: prioritises new CAR dataset (has data up to March 2026).
Old STS dataset only goes to ~end 2022 — avoid it unless nothing else works.
New approach: use JSON format + wildcard to discover the actual series key,
then switch to CSV for bulk download.
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
headers={“User-Agent”: “EV-Map-Bot/7.0 (github.com/Altair02/EV-adoption-worldmap)”}
)
with urllib.request.urlopen(req, timeout=timeout) as r:
return r.read()

# ── ECB: discover key via JSON format ─────────────────────────────────────────

ECB_WORKING_KEY = None

def find_ecb_key():
“””
Try every plausible key for the new CAR dataset first,
then fall back to old STS (which only has data until ~2022).
Logs the latest observation date so we know which is current.
“””
global ECB_WORKING_KEY
if ECB_WORKING_KEY:
return ECB_WORKING_KEY

```
# ── New CAR dataset keys (data up to 2026) ──────────────────────────────
# ECB renamed the dataset in March 2025. The DSD is ECB_CAR1.
# Key format: FREQ.REF_AREA.ADJUSTMENT.INDICATOR.SUFFIX
# We try every combination we can think of:
car_keys = [
    "M.DE.N.NEWCARS.ABS",      # most likely new format
    "M.DE.N.NEWCARS.N",
    "M.DE.N.NEWCARS.NSA",
    "M.DE.N.NEWCARS.RAW",
    "M.DE.N.NEWCARS.3.ABS",
    "M.DE.N.CARS.ABS",
    "M.DE.N.CARS.N",
    "M.DE.N.CARS.NSA",
    "M.DE.N.CREG.NEWCARS.ABS",
    "M.DE.N.CREG.NEWCARS.N",
    "M.DE.N.REG.PC.ABS",
    "M.DE.N.REG.CARS.ABS",
    "M.DE.N.CREG.PC0000.ABS",  # without version number
    "M.DE.N.CREG.PC0000.N",
    "M.DE..NEWCARS.ABS",       # wildcard adjustment
    "M.DE..NEWCARS.N",
    "M.DE..CARS.ABS",
    "M.DE.N.CREG.PC0000.3.ABS", # with version
    "M.DE....",                 # full wildcard
]
# ── Old STS dataset keys (data only until ~2022) ─────────────────────
sts_keys = [
    "M.DE.N.CREG.PC0000.3.ABS",
    "M.DE.W.CREG.PC0000.3.ABS",
]

print("[ECB] Testing CAR dataset keys (prefer recent data up to 2026)…")

best_result = None  # (dataset, key, latest_period)

for dataset, keys in [("CAR", car_keys), ("STS", sts_keys)]:
    for key in keys:
        url = (f"https://data-api.ecb.europa.eu/service/data/{dataset}/{key}"
               f"?format=csvdata&lastNObservations=3&detail=dataonly")
        try:
            raw = http_get(url, timeout=15).decode("utf-8")
            if "TIME_PERIOD" not in raw.upper():
                continue
            rows = [r for r in raw.split("\n")[1:] if r.strip()]
            if not rows:
                continue

            # Parse latest period from data
            reader = csv.DictReader(io.StringIO(raw))
            periods = []
            for row in reader:
                row_u = {k.upper(): v for k, v in row.items()}
                p = row_u.get("TIME_PERIOD", "").strip()
                if p:
                    periods.append(p)

            if not periods:
                continue

            latest = max(periods)
            print(f"[ECB]   ✓ {dataset}/{key} → latest: {latest}")

            # Prefer the key with the most recent data
            if best_result is None or latest > best_result[2]:
                best_result = (dataset, key, latest)

            # If we found recent data (2024+), stop searching CAR keys
            if dataset == "CAR" and latest >= "2024":
                print(f"[ECB] ✓ Using {dataset}/{key} (data up to {latest})")
                ECB_WORKING_KEY = (dataset, key)
                return ECB_WORKING_KEY

        except urllib.error.HTTPError:
            pass
        except Exception as e:
            pass
        time.sleep(0.15)

    # After trying all CAR keys, if we found something recent enough, use it
    if best_result and best_result[0] == "CAR" and best_result[2] >= "2023":
        print(f"[ECB] ✓ Best CAR key: {best_result[1]} (latest: {best_result[2]})")
        ECB_WORKING_KEY = (best_result[0], best_result[1])
        return ECB_WORKING_KEY

# Use whatever we found, even if it's old STS data
if best_result:
    print(f"[ECB] ⚠ Using {best_result[0]}/{best_result[1]} (latest: {best_result[2]}) — may be outdated")
    ECB_WORKING_KEY = (best_result[0], best_result[1])
    return ECB_WORKING_KEY

print("[ECB] ✗ No working key found")
return None
```

def fetch_ecb_one(geo, dataset, key_template):
key = key_template.replace(”.DE.”, f”.{geo}.”)
url = (f”https://data-api.ecb.europa.eu/service/data/{dataset}/{key}”
f”?format=csvdata&startPeriod={START_YEAR}-01&detail=dataonly”)
try:
raw = http_get(url, timeout=25).decode(“utf-8”)
result = {}
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
return result
except Exception:
return {}

def fetch_ecb_monthly():
working = find_ecb_key()
if not working:
print(”[ECB] Skipping monthly fetch — no working key”)
return {}

```
dataset, key_template = working
print(f"\n[ECB] Fetching all {len(COUNTRIES)} countries…")
results = {}
ok = 0
for geo in COUNTRIES:
    data = fetch_ecb_one(geo, dataset, key_template)
    if data:
        months = sorted(data.keys())
        results[geo] = {"labels": months, "total": [data[m] for m in months]}
        ok += 1
    time.sleep(0.2)

print(f"[ECB] Done — {ok}/{len(COUNTRIES)} countries with monthly data")
if results:
    # Show date range for a sample country
    sample_geo = "DE" if "DE" in results else next(iter(results))
    m = results[sample_geo]
    print(f"[ECB] {sample_geo}: {len(m['labels'])} months "
          f"({m['labels'][0]} → {m['labels'][-1]}), "
          f"latest value: {m['total'][-1]:,}")
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

if "DE" in result:
    de23 = result["DE"].get("2023", {})
    print(f"[Eurostat] DE 2023 check: BEV={de23.get('bev',0):,} "
          f"Petrol={de23.get('petrol',0):,} Diesel={de23.get('diesel',0):,}")

print(f"[Eurostat] Done — {len(result)} countries")
return result
```

# ── Write files ───────────────────────────────────────────────────────────────

def write_files(monthly, annual):
DATA_DIR.mkdir(parents=True, exist_ok=True)
changed = []
for ecb_code, (name, estat_code, pop) in COUNTRIES.items():
m = monthly.get(ecb_code, {})
a = annual.get(ecb_code, {})
if not m and not a:
continue
years = sorted(a.keys()) if a else []
annual_block = {}
if years:
annual_block = {
“labels”: years,
“bev”:    [a[y].get(“bev”,    0) for y in years],
“phev”:   [a[y].get(“phev”,   0) for y in years],
“hybrid”: [a[y].get(“hybrid”, 0) for y in years],
“petrol”: [a[y].get(“petrol”, 0) for y in years],
“diesel”: [a[y].get(“diesel”, 0) for y in years],
“other”:  [a[y].get(“other”,  0) for y in years],
}
payload = {
“name”: name, “ecb_code”: ecb_code, “population_mio”: pop,
“source_monthly”: “ECB Data Portal / ACEA”,
“source_annual”:  “Eurostat road_eqr_carpda”,
“last_updated”:   NOW.isoformat(),
“monthly”: m,
“annual”:  annual_block,
}
fname = name.lower().replace(” “, “_”) + “.json”
path  = DATA_DIR / fname
old = {}
if path.exists():
try:
old = json.loads(path.read_text(“utf-8”))
except Exception:
pass
def no_ts(d):
d2 = dict(d); d2.pop(“last_updated”, None); return d2
path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), “utf-8”)
if no_ts(payload) != no_ts(old):
changed.append(name)
print(f”[Write] {name} ✓ updated”)
else:
print(f”[Write] {name} (no change)”)
return changed

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
print(f”  Car Registration Updater v7  —  {NOW.strftime(’%d.%m.%Y %H:%M UTC’)}”)
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
