# “””
scripts/update_data.py  —  v7

Fix: Monthly data was cut off at 2022 because the old STS dataset
was used (discontinued end-2022). New CAR dataset has 2023-present.

Solution: fetch BOTH datasets per country and merge them:
STS → historical data (2015-2022)
CAR → recent data (2023-present)
Result → complete series 2015-March 2026

Note on powertrain charts: Eurostat road_eqr_carpda is ANNUAL only.
Monthly breakdown by fuel type is not available as a free API.
The 3 powertrain charts remain annual — this is a data limitation.
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

def parse_ecb_csv(raw):
“”“Parse ECB CSV response into {period: value} dict.”””
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

def fetch_dataset(dataset, key, geo, params):
“”“Try one dataset/key combination. Returns {period: value} or {}.”””
actual_key = key.replace(“XX”, geo)
url = f”https://data-api.ecb.europa.eu/service/data/{dataset}/{actual_key}?{params}”
try:
raw = http_get(url, timeout=20).decode(“utf-8”)
if “TIME_PERIOD” not in raw.upper():
return {}
return parse_ecb_csv(raw)
except Exception:
return {}

# ── ECB monthly: merge STS (historic) + CAR (recent) ─────────────────────────

def fetch_ecb_monthly():
params = f”format=csvdata&startPeriod={START_YEAR}-01&detail=dataonly”

```
# STS keys (historic, works up to ~end 2022 when STS was discontinued)
STS_KEYS = [
    "M.XX.N.CREG.PC0000.3.ABS",   # not seasonally adjusted
    "M.XX.W.CREG.PC0000.3.ABS",   # working day adjusted
]

# CAR keys (new dataset, from ~Jan 2023 onwards)
CAR_KEYS = [
    "M.XX.N.CREG.PC0000.3.ABS",
    "M.XX.N.CREG.PC0000..ABS",
    "M.XX..CREG.PC0000.3.ABS",
    "M.XX.N.NEWCARS.N",
    "M.XX.N.NEWCARS.NSA",
    "M.XX.....",
]

# First: find working STS key using Germany as test
print("[ECB] Testing STS keys with Germany…")
sts_key = None
for k in STS_KEYS:
    d = fetch_dataset("STS", k, "DE", params)
    if d:
        sts_key = k
        latest = max(d.keys())
        print(f"[ECB] STS working key: {k} → {len(d)} months, latest={latest}")
        break

# Find working CAR key using Germany as test
print("[ECB] Testing CAR keys with Germany…")
car_key = None
for k in CAR_KEYS:
    d = fetch_dataset("CAR", k, "DE", params)
    if d:
        car_key = k
        latest = max(d.keys())
        print(f"[ECB] CAR working key: {k} → {len(d)} months, latest={latest}")
        break

if not sts_key and not car_key:
    print("[ECB] No working key found — skipping monthly data")
    return {}

print(f"\n[ECB] Fetching all {len(COUNTRIES)} countries…")
results = {}

for geo in COUNTRIES:
    merged = {}

    # Fetch STS (historic)
    if sts_key:
        sts_data = fetch_dataset("STS", sts_key, geo, params)
        merged.update(sts_data)

    # Fetch CAR (recent) — overwrites STS where overlap exists
    if car_key:
        car_data = fetch_dataset("CAR", car_key, geo, params)
        merged.update(car_data)

    if merged:
        months = sorted(merged.keys())
        results[geo] = {
            "labels": months,
            "total":  [merged[m] for m in months],
        }
        latest = months[-1]
        print(f"[ECB]   {geo}: {len(months)} months ({months[0]}→{latest})")
    else:
        print(f"[ECB]   {geo}: no data")

    time.sleep(0.2)

# Summary
countries_with_2025 = sum(
    1 for v in results.values()
    if any(l >= "2025" for l in v["labels"])
)
print(f"\n[ECB] Done — {len(results)} countries, "
      f"{countries_with_2025} with data up to 2025+")
return results
```

# ── Eurostat annual powertrain breakdown ──────────────────────────────────────

def fetch_eurostat_annual():
url = (“https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/”
“road_eqr_carpda?format=JSON&lang=EN&unit=NR”)
print(f”\n[Eurostat] Fetching annual powertrain breakdown…”)
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
        # Fallback to aggregates if granular petrol+diesel = 0
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
    print(f"[Eurostat] DE 2023: BEV={de23.get('bev',0):,} "
          f"PHEV={de23.get('phev',0):,} Hybrid={de23.get('hybrid',0):,} "
          f"Petrol={de23.get('petrol',0):,} Diesel={de23.get('diesel',0):,}")

print(f"[Eurostat] Done — {len(result)} countries, years: {years[0]}–{years[-1]}")
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

def send_telegram(changed, n_countries, monthly_coverage):
if not TELEGRAM_TOKEN or not TELEGRAM_CHAT:
return
date_str = NOW.strftime(”%d.%m.%Y %H:%M UTC”)
if changed:
lines = “\n”.join(f”  • {c}” for c in changed[:25])
body = (f”🚗 *Car Registrations — Updated*\n”
f”📅 {date_str}\n”
f”🌍 {n_countries} countries\n”
f”📈 Monthly data: {monthly_coverage} countries up to date\n\n”
f”*Changed ({len(changed)}):*\n{lines}”)
else:
body = (f”🚗 *Car Registrations — No Changes*\n”
f”📅 {date_str}\n”
f”🌍 {n_countries} countries — all up to date.”)
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
send_telegram([], 0, 0)
sys.exit(1)
changed = write_files(monthly, annual)
coverage = sum(
1 for v in monthly.values()
if any(l >= “2025” for l in v.get(“labels”, []))
)
send_telegram(changed, len(COUNTRIES), coverage)
print(f”\n✓ Done — {len(changed)} countries updated, “
f”{coverage} with 2025+ monthly data”)

if **name** == “**main**”:
main()
