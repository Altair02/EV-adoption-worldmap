"""
scripts/update_data.py  —  v4
==============================
Fixes vs v3:
  1. Fuel mapping corrected:
       PET_X_HYB  = Petrol EXCLUDING hybrids → petrol  (NOT hybrid)
       DIE_X_HYB  = Diesel EXCLUDING hybrids → diesel  (NOT hybrid)
       ELC_PET_HYB = Hybrid electric-petrol  → hybrid  ✓
       ELC_DIE_HYB = Hybrid diesel-electric  → hybrid  ✓
       ELC_PET_PI  = Plug-in hybrid petrol   → phev    ✓
       ELC_DIE_PI  = Plug-in hybrid diesel   → phev    ✓
       LPG, GAS    → other (not petrol)
  2. ECB: one request per country (avoids HTTP 400 from URL length limit)
  3. ECB: tries both 5-dim and 4-dim key variants per country
"""

import csv, io, json, os, sys, time, urllib.error, urllib.request
from datetime import datetime, timezone
from pathlib import Path

TELEGRAM_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT  = os.environ.get("TELEGRAM_CHAT_ID", "")
REPO_ROOT      = Path(__file__).resolve().parent.parent
DATA_DIR       = REPO_ROOT / "data" / "countries"
NOW            = datetime.now(timezone.utc)
START_YEAR     = 2015

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

# Correct fuel code → category mapping
# Key insight: PET_X_HYB = petrol EXCLUDING hybrids = still petrol
#              DIE_X_HYB = diesel EXCLUDING hybrids = still diesel
#              We use the granular codes and skip the aggregates (TOTAL, ALT, PET, DIE)
# This way we avoid double-counting.
FUEL_MAP = {
    # --- BEV ---
    "ELC":         "bev",      # Electricity (pure BEV)
    # --- PHEV ---
    "ELC_PET_PI":  "phev",     # Plug-in hybrid petrol-electric
    "ELC_DIE_PI":  "phev",     # Plug-in hybrid diesel-electric
    # --- Hybrid (non-plug) ---
    "ELC_PET_HYB": "hybrid",   # Hybrid electric-petrol (non-plug)
    "ELC_DIE_HYB": "hybrid",   # Hybrid diesel-electric (non-plug)
    # --- Petrol ---
    "PET_X_HYB":   "petrol",   # Petrol EXCLUDING hybrids (pure ICE petrol)
    # --- Diesel ---
    "DIE_X_HYB":   "diesel",   # Diesel EXCLUDING hybrids (pure ICE diesel)
    # --- Other ---
    "LPG":         "other",    # Liquefied petroleum gas
    "GAS":         "other",    # Natural gas
    "HYD_FCELL":   "other",    # Hydrogen / fuel cell
    "BIOETH":      "other",    # Bioethanol
    "BIODIE":      "other",    # Biodiesel
    "BIFUEL":      "other",    # Bi-fuel
    "OTH":         "other",    # Other
    # SKIP these aggregates to avoid double-counting:
    # TOTAL, PET, DIE, ALT, ELC (when granular codes present)
}

def http_get(url, timeout=30):
    req = urllib.request.Request(
        url,
        headers={"User-Agent": "EV-Map-Bot/4.0 (github.com/Altair02/EV-adoption-worldmap)"}
    )
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read()

# ── ECB monthly: one request per country ─────────────────────────────────────
def fetch_ecb_one(ecb_code):
    """Fetch monthly registrations for a single country. Returns {period: value}."""
    base = "https://data-api.ecb.europa.eu/service/data/CAR"
    start = f"{START_YEAR}-01"
    params = "format=csvdata&detail=dataonly"

    # Try different key formats — ECB changed naming in March 2025
    keys = [
        f"M.{ecb_code}.N.NEWCARS.NSA",   # new format with adjustment dim
        f"M.{ecb_code}.N.CARS.NSA",       # alternative indicator name
        f"M.{ecb_code}..NEWCARS.NSA",     # wildcard adjustment
        f"M.{ecb_code}..CARS.NSA",        # wildcard + alt name
    ]

    for key in keys:
        url = f"{base}/{key}?startPeriod={start}&{params}"
        try:
            raw = http_get(url).decode("utf-8")
            if "TIME_PERIOD" not in raw and "time_period" not in raw.lower():
                continue
            result = {}
            reader = csv.DictReader(io.StringIO(raw))
            for row in reader:
                period = (row.get("TIME_PERIOD") or row.get("time_period") or "").strip()
                value  = (row.get("OBS_VALUE")   or row.get("obs_value")   or "").strip()
                if period and value:
                    try:
                        v = int(float(value))
                        if v > 0:
                            result[period] = v
                    except ValueError:
                        pass
            if result:
                return result
        except urllib.error.HTTPError:
            pass
        except Exception:
            pass
    return {}

def fetch_ecb_monthly():
    print("[ECB] Fetching monthly data — one request per country…")
    all_results = {}
    ok = 0
    for ecb_code in COUNTRIES:
        data = fetch_ecb_one(ecb_code)
        if data:
            sorted_months = sorted(data.keys())
            all_results[ecb_code] = {
                "labels": sorted_months,
                "total":  [data[m] for m in sorted_months],
            }
            ok += 1
        else:
            print(f"[ECB]   {ecb_code}: no data")
        time.sleep(0.3)   # be polite to the API

    print(f"[ECB] Done — {ok}/{len(COUNTRIES)} countries with monthly data")
    if "DE" in all_results:
        m = all_results["DE"]
        print(f"[ECB] DE sample: {len(m['labels'])} months, "
              f"latest {m['labels'][-1]} = {m['total'][-1]:,}")
    return all_results

# ── Eurostat annual ───────────────────────────────────────────────────────────
def fetch_eurostat_annual():
    url = ("https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/"
           "road_eqr_carpda?format=JSON&lang=EN&unit=NR")
    print(f"\n[Eurostat] Fetching annual breakdown…")
    try:
        raw = json.loads(http_get(url, timeout=60).decode("utf-8"))
    except Exception as e:
        print(f"[Eurostat] Error: {e}")
        return {}

    dims      = raw.get("dimension", {})
    values    = raw.get("value", {})
    dim_order = raw.get("id",   [])
    dim_sizes = raw.get("size", [])

    # Index lookups
    def idx(dim):
        return dims.get(dim, {}).get("category", {}).get("index", {})

    geo_idx = idx("geo")
    time_idx = idx("time")
    mot_idx  = idx("mot_nrg")

    # Strides for linear index
    strides = {}
    for i, d in enumerate(dim_order):
        s = 1
        for j in range(i + 1, len(dim_order)):
            s *= dim_sizes[j]
        strides[d] = s

    years = sorted([y for y in time_idx if int(y) >= START_YEAR])
    estat_to_ecb = {v[1]: k for k, v in COUNTRIES.items()}

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
            for fuel_code, our_field in FUEL_MAP.items():
                f_pos = mot_idx.get(fuel_code)
                if f_pos is None:
                    continue
                linear = (g_pos  * strides.get("geo",     1) +
                          t_pos  * strides.get("time",    1) +
                          f_pos  * strides.get("mot_nrg", 1))
                v = values.get(str(linear)) or values.get(linear)
                if v is not None:
                    year_vals[our_field] += int(v)
            country_data[year] = year_vals
        if country_data:
            result[ecb_key] = country_data

    # Sanity check
    if "DE" in result and "2023" in result["DE"]:
        de = result["DE"]["2023"]
        print(f"[Eurostat] DE 2023 — BEV={de['bev']:,}  PHEV={de['phev']:,}  "
              f"Hybrid={de['hybrid']:,}  Petrol={de['petrol']:,}  Diesel={de['diesel']:,}")
        print(f"[Eurostat] Expected: BEV~524k PHEV~228k Hybrid~700k Petrol~1.6M Diesel~700k")

    print(f"[Eurostat] Done — {len(result)} countries")
    return result

# ── Write JSON files ──────────────────────────────────────────────────────────
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
                "labels": years,
                "bev":    [a[y].get("bev",    0) for y in years],
                "phev":   [a[y].get("phev",   0) for y in years],
                "hybrid": [a[y].get("hybrid", 0) for y in years],
                "petrol": [a[y].get("petrol", 0) for y in years],
                "diesel": [a[y].get("diesel", 0) for y in years],
                "other":  [a[y].get("other",  0) for y in years],
            }
        payload = {
            "name": name, "ecb_code": ecb_code,
            "population_mio": pop,
            "source_monthly": "ECB Data Portal / ACEA",
            "source_annual":  "Eurostat road_eqr_carpda",
            "last_updated":   NOW.isoformat(),
            "monthly": m,
            "annual":  annual_block,
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
            d2 = dict(d); d2.pop("last_updated", None); return d2
        path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), "utf-8")
        if no_ts(payload) != no_ts(old):
            changed.append(name)
            print(f"[Write] {name} ✓ updated")
        else:
            print(f"[Write] {name} (no change)")
    return changed

# ── Telegram ──────────────────────────────────────────────────────────────────
def send_telegram(changed, n_countries):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT:
        return
    date_str = NOW.strftime("%d.%m.%Y %H:%M UTC")
    if changed:
        lines = "\n".join(f"  • {c}" for c in changed[:25])
        body = (f"🚗 *Car Registrations — Updated*\n📅 {date_str}\n"
                f"🌍 {n_countries} countries\n\n*Changed ({len(changed)}):*\n{lines}")
    else:
        body = (f"🚗 *Car Registrations — No Changes*\n📅 {date_str}\n"
                f"🌍 {n_countries} countries — all up to date.")
    body += ("\n\n🗺️ [Open Map](https://altair02.github.io/EV-adoption-worldmap/)\n"
             "📁 [GitHub](https://github.com/Altair02/EV-adoption-worldmap)")
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

# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    print("=" * 60)
    print(f"  Car Registration Updater v4  —  {NOW.strftime('%d.%m.%Y %H:%M UTC')}")
    print("=" * 60)
    monthly = fetch_ecb_monthly()
    annual  = fetch_eurostat_annual()
    if not monthly and not annual:
        print("⚠️  No data from either source")
        send_telegram([], 0)
        sys.exit(1)
    changed = write_files(monthly, annual)
    send_telegram(changed, len(COUNTRIES))
    print(f"\n✓ Done — {len(changed)} countries updated")

if __name__ == "__main__":
    main()
