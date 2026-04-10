"""
scripts/update_data.py
======================
Fetches data from two sources and writes one JSON file per country
into data/countries/<country_code>.json

SOURCE 1 — ECB Data Portal API (monthly total registrations per country)
  URL: https://data-api.ecb.europa.eu/service/data/CAR/M.{GEO}.NEWCARS.NSA
  Format: CSV, no authentication required
  Coverage: ~30 EU + EEA countries, monthly since 1990
  Published: ~3rd week of the following month

SOURCE 2 — Eurostat Statistics API (annual registrations by fuel type)
  Dataset: road_eqr_carpda
  URL: https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/road_eqr_carpda
  Format: JSON-stat, no authentication required
  Coverage: ~37 European countries, annual
  Fuel types: BEV, PHEV, hybrid, petrol, diesel, LPG, others

Output JSON format per country:
{
  "name": "Germany",
  "source_monthly": "ECB/ACEA",
  "source_annual": "Eurostat/road_eqr_carpda",
  "last_updated": "2026-04-10T09:00:00Z",

  "monthly": {
    "labels": ["2020-01", "2020-02", ...],   // ISO year-month
    "total":  [240000, 198000, ...]           // total registrations
  },

  "annual": {
    "labels": ["2015", "2016", ..., "2024"],
    "bev":    [...],
    "phev":   [...],
    "hybrid": [...],
    "petrol": [...],
    "diesel": [...],
    "other":  [...]
  }
}

GitHub Secrets required:
  TELEGRAM_BOT_TOKEN  e.g. 123456789:ABC-xyz...
  TELEGRAM_CHAT_ID    e.g. -1001234567890 (group) or 123456789 (private)
"""

import csv
import io
import json
import os
import sys
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

# ── Configuration ─────────────────────────────────────────────────────────────

TELEGRAM_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT  = os.environ.get("TELEGRAM_CHAT_ID", "")
REPO_ROOT      = Path(__file__).resolve().parent.parent
DATA_DIR       = REPO_ROOT / "data" / "countries"
NOW            = datetime.now(timezone.utc)
START_YEAR     = 2015   # how far back to fetch data

# ── Country definitions ───────────────────────────────────────────────────────
# ECB code → (display name, Eurostat code, population in millions)
# Note: ECB uses EL for Greece (not GR), GB for United Kingdom
COUNTRIES = {
    "AT": ("Austria",         "AT",  9.1),
    "BE": ("Belgium",         "BE", 11.6),
    "BG": ("Bulgaria",        "BG",  6.5),
    "CY": ("Cyprus",          "CY",  1.2),
    "CZ": ("Czech Republic",  "CZ", 10.9),
    "DE": ("Germany",         "DE", 84.4),
    "DK": ("Denmark",         "DK",  5.9),
    "EE": ("Estonia",         "EE",  1.4),
    "EL": ("Greece",          "EL", 10.4),
    "ES": ("Spain",           "ES", 47.4),
    "FI": ("Finland",         "FI",  5.6),
    "FR": ("France",          "FR", 68.1),
    "HR": ("Croatia",         "HR",  3.9),
    "HU": ("Hungary",         "HU",  9.7),
    "IE": ("Ireland",         "IE",  5.1),
    "IT": ("Italy",           "IT", 59.1),
    "LT": ("Lithuania",       "LT",  2.8),
    "LU": ("Luxembourg",      "LU",  0.7),
    "LV": ("Latvia",          "LV",  1.8),
    "MT": ("Malta",           "MT",  0.5),
    "NL": ("Netherlands",     "NL", 17.9),
    "PL": ("Poland",          "PL", 37.6),
    "PT": ("Portugal",        "PT", 10.3),
    "RO": ("Romania",         "RO", 19.0),
    "SE": ("Sweden",          "SE", 10.5),
    "SI": ("Slovenia",        "SI",  2.1),
    "SK": ("Slovakia",        "SK",  5.5),
    "NO": ("Norway",          "NO",  5.5),
    "CH": ("Switzerland",     "CH",  8.8),
    "GB": ("United Kingdom",  "UK", 67.4),
}

# Eurostat fuel type codes → our field names
FUEL_MAP = {
    "ELC":   "bev",     # Battery electric
    "PHEV":  "phev",    # Plug-in hybrid
    "HEV":   "hybrid",  # Hybrid non-plug
    "PET":   "petrol",  # Petrol / gasoline
    "DIE":   "diesel",  # Diesel
    "LPG":   "other",   # LPG → grouped into "other"
    "NAT":   "other",   # Natural gas
    "H2":    "other",   # Hydrogen
    "OTH":   "other",   # Other
    "ALT":   "other",   # Alternative (aggregate, skip if BEV/PHEV present)
}

# ── HTTP helper ───────────────────────────────────────────────────────────────

def http_get(url: str, timeout: int = 30) -> bytes:
    req = urllib.request.Request(
        url,
        headers={"User-Agent": "EV-Map-Bot/2.0 (github.com/Altair02/EV-adoption-worldmap)"},
    )
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read()


# ── SOURCE 1: ECB monthly total registrations ─────────────────────────────────

def fetch_ecb_monthly() -> dict:
    """
    Returns { "DE": {"labels": ["2015-01",...], "total": [240000,...]}, ... }
    One API call fetches all countries at once using the OR (+) operator.
    """
    ecb_codes = "+".join(COUNTRIES.keys())
    url = (
        "https://data-api.ecb.europa.eu/service/data/CAR/"
        f"M.{ecb_codes}.NEWCARS.NSA"
        f"?format=csvdata&startPeriod={START_YEAR}-01&detail=dataonly"
    )
    print(f"[ECB] Fetching monthly data for {len(COUNTRIES)} countries…")
    print(f"[ECB] URL: {url[:100]}…")

    try:
        raw = http_get(url).decode("utf-8")
    except urllib.error.HTTPError as e:
        print(f"[ECB] HTTP error {e.code}: {e.reason}")
        return {}
    except Exception as e:
        print(f"[ECB] Error: {e}")
        return {}

    # Parse CSV: columns include KEY, FREQ, REF_AREA, ..., TIME_PERIOD, OBS_VALUE
    result = {}
    reader = csv.DictReader(io.StringIO(raw))

    for row in reader:
        geo  = row.get("REF_AREA", "").strip()
        period = row.get("TIME_PERIOD", "").strip()   # e.g. "2024-03"
        value  = row.get("OBS_VALUE", "").strip()

        if not geo or not period or not value:
            continue
        try:
            v = int(float(value))
        except ValueError:
            continue

        if geo not in result:
            result[geo] = {}
        result[geo][period] = v

    # Convert dict → sorted lists
    final = {}
    for geo, months in result.items():
        sorted_months = sorted(months.keys())
        final[geo] = {
            "labels": sorted_months,
            "total":  [months[m] for m in sorted_months],
        }
        print(f"[ECB]   {geo}: {len(sorted_months)} months "
              f"({sorted_months[0]} – {sorted_months[-1]})")

    print(f"[ECB] Done — {len(final)} countries with monthly data")
    return final


# ── SOURCE 2: Eurostat annual breakdown by fuel type ─────────────────────────

def fetch_eurostat_annual() -> dict:
    """
    Returns {
      "DE": {"2020": {"bev": 194163, "phev": 200469, ...}, ...},
      ...
    }
    """
    # Fetch all fuel types + all countries in one call
    # mot_nrg codes that map cleanly to our categories
    fuel_codes = "+".join(FUEL_MAP.keys())
    url = (
        "https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/"
        "road_eqr_carpda"
        f"?format=JSON&lang=EN&unit=NR"
        # no mot_nrg filter → get everything, then filter in code
    )
    print(f"\n[Eurostat] Fetching annual fuel-type breakdown…")
    print(f"[Eurostat] URL: {url[:100]}…")

    try:
        raw = json.loads(http_get(url).decode("utf-8"))
    except Exception as e:
        print(f"[Eurostat] Error: {e}")
        return {}

    dims      = raw.get("dimension", {})
    values    = raw.get("value", {})

    geo_cat   = dims.get("geo",     {}).get("category", {})
    time_cat  = dims.get("time",    {}).get("category", {})
    fuel_cat  = dims.get("mot_nrg", {}).get("category", {})

    geo_idx   = geo_cat.get("index",  {})   # {"DE": 5, "FR": 12, ...}
    time_idx  = time_cat.get("index", {})   # {"2020": 0, ...}
    fuel_idx  = fuel_cat.get("index", {})   # {"ELC": 0, "PET": 1, ...}

    n_geo  = len(geo_idx)
    n_time = len(time_idx)
    n_fuel = len(fuel_idx)

    years = [y for y in sorted(time_idx.keys()) if int(y) >= START_YEAR]
    print(f"[Eurostat] Years available: {years}")
    print(f"[Eurostat] Fuel codes: {list(fuel_idx.keys())[:8]}…")

    result = {}
    eurostat_to_ecb = {v_info[1]: k for k, v_info in COUNTRIES.items()}
    # e.g. "AT" → "AT", "UK" → "GB", "EL" → "EL"

    for estat_geo, g_pos in geo_idx.items():
        # Map Eurostat geo to our ECB key
        ecb_key = None
        for ecb_code, (name, estat_code, pop) in COUNTRIES.items():
            if estat_code == estat_geo:
                ecb_key = ecb_code
                break
        if not ecb_key:
            continue

        country_data = {}
        for year in years:
            if year not in time_idx:
                continue
            t_pos = time_idx[year]
            year_data = {f: 0 for f in set(FUEL_MAP.values())}

            for fuel_code, our_field in FUEL_MAP.items():
                if fuel_code not in fuel_idx:
                    continue
                # Skip aggregate "ALT" if we already have BEV/PHEV
                if fuel_code == "ALT":
                    continue
                f_pos = fuel_idx[fuel_code]
                # Linear index: geo * (n_time * n_fuel) + time * n_fuel + fuel
                linear = g_pos * (n_time * n_fuel) + t_pos * n_fuel + f_pos
                v = values.get(str(linear)) or values.get(linear)
                if v is not None:
                    year_data[our_field] = year_data[our_field] + int(v)

            country_data[year] = year_data

        if country_data:
            result[ecb_key] = country_data

    print(f"[Eurostat] Done — {len(result)} countries with annual fuel data")
    return result


# ── Merge & write JSON files ──────────────────────────────────────────────────

def write_country_files(monthly: dict, annual: dict) -> list:
    """
    Merges both sources and writes one JSON per country.
    Returns list of changed country names.
    """
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    changed = []

    for ecb_code, (name, estat_code, pop) in COUNTRIES.items():
        m_data = monthly.get(ecb_code, {})
        a_data = annual.get(ecb_code, {})

        if not m_data and not a_data:
            print(f"[Write] {name}: no data from either source — skip")
            continue

        # Build annual arrays from the dict
        all_years = sorted(a_data.keys()) if a_data else []
        annual_block = {}
        if all_years:
            annual_block = {
                "labels": all_years,
                "bev":    [a_data[y].get("bev",    0) for y in all_years],
                "phev":   [a_data[y].get("phev",   0) for y in all_years],
                "hybrid": [a_data[y].get("hybrid", 0) for y in all_years],
                "petrol": [a_data[y].get("petrol", 0) for y in all_years],
                "diesel": [a_data[y].get("diesel", 0) for y in all_years],
                "other":  [a_data[y].get("other",  0) for y in all_years],
            }

        payload = {
            "name":           name,
            "ecb_code":       ecb_code,
            "population_mio": pop,
            "source_monthly": "ECB Data Portal / ACEA",
            "source_annual":  "Eurostat road_eqr_carpda",
            "last_updated":   NOW.isoformat(),
            "monthly":        m_data,    # {labels:[...], total:[...]}
            "annual":         annual_block,
        }

        # File path uses lowercase country name
        fname = name.lower().replace(" ", "_").replace("(", "").replace(")", "") + ".json"
        path  = DATA_DIR / fname

        # Check if anything changed
        old_payload = {}
        if path.exists():
            try:
                old_payload = json.loads(path.read_text("utf-8"))
            except Exception:
                pass

        # Compare without last_updated timestamp
        def strip_ts(d):
            d2 = dict(d)
            d2.pop("last_updated", None)
            return d2

        if strip_ts(payload) != strip_ts(old_payload):
            path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), "utf-8")
            changed.append(name)
            print(f"[Write] {name} → {fname} ✓ (updated)")
        else:
            # Still update timestamp
            path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), "utf-8")
            print(f"[Write] {name} → {fname} (no data change)")

    return changed


# ── Telegram notification ─────────────────────────────────────────────────────

def send_telegram(changed: list, n_countries: int):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT:
        print("[Telegram] No token/chat configured — skipping")
        return

    date_str = NOW.strftime("%d.%m.%Y %H:%M UTC")

    if changed:
        lines = "\n".join(f"  • {c}" for c in changed[:25])
        body = (
            f"🚗 *Car Registrations — Data Updated*\n"
            f"📅 {date_str}\n"
            f"🌍 {n_countries} countries processed\n\n"
            f"*Updated ({len(changed)} countries):*\n{lines}"
        )
    else:
        body = (
            f"🚗 *Car Registrations — No Changes*\n"
            f"📅 {date_str}\n"
            f"🌍 {n_countries} countries checked\n\n"
            f"ℹ️ All data already up to date."
        )

    body += (
        f"\n\n"
        f"🗺️ [Open Map](https://altair02.github.io/EV-adoption-worldmap/)\n"
        f"📁 [GitHub](https://github.com/Altair02/EV-adoption-worldmap)"
    )

    payload = json.dumps({
        "chat_id":    TELEGRAM_CHAT,
        "text":       body,
        "parse_mode": "Markdown",
        "disable_web_page_preview": False,
    }).encode("utf-8")

    req = urllib.request.Request(
        f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
        data=payload,
        headers={"Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as r:
            msg_id = json.loads(r.read()).get("result", {}).get("message_id", "?")
        print(f"[Telegram] Sent ✓ (message_id={msg_id})")
    except Exception as e:
        print(f"[Telegram] Error: {e}")


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    print("=" * 65)
    print(f"  Car Registration Updater  —  {NOW.strftime('%d.%m.%Y %H:%M UTC')}")
    print("=" * 65)

    # 1. Fetch monthly totals from ECB
    monthly = fetch_ecb_monthly()

    # 2. Fetch annual fuel breakdown from Eurostat
    annual = fetch_eurostat_annual()

    if not monthly and not annual:
        print("\n⚠️  No data received from either source.")
        send_telegram([], 0)
        sys.exit(1)

    # 3. Merge and write JSON files
    changed = write_country_files(monthly, annual)

    # 4. Telegram notification
    send_telegram(changed, len(COUNTRIES))

    print(f"\n✓ Done — {len(changed)} countries updated")


if __name__ == "__main__":
    main()
