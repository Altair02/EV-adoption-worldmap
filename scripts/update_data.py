"""
scripts/update_data.py  —  v3 FINAL
=====================================
Key fixes vs v2:
  1. ECB: correct series key M.{GEO}.N.NEWCARS.NSA (with N dimension)
  2. ECB: wildcard fallback M.{GEO}..NEWCARS.NSA if first attempt fails
  3. Eurostat: script discovers ALL available fuel codes from the API response
               and logs them — no more hardcoded guessing
  4. Eurostat: robust linear index using strides computed from actual dim order
  5. Better debug logging so you can see exactly what's happening in GitHub Actions
"""

import csv, io, json, os, sys, urllib.error, urllib.request
from datetime import datetime, timezone
from pathlib import Path

TELEGRAM_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT  = os.environ.get("TELEGRAM_CHAT_ID", "")
REPO_ROOT      = Path(__file__).resolve().parent.parent
DATA_DIR       = REPO_ROOT / "data" / "countries"
NOW            = datetime.now(timezone.utc)
START_YEAR     = 2015

# ECB code → (display name, Eurostat code, population Mio)
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

def http_get(url, timeout=45):
    req = urllib.request.Request(
        url, headers={"User-Agent": "EV-Map-Bot/3.0 (github.com/Altair02/EV-adoption-worldmap)"}
    )
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read()

# ── ECB monthly ───────────────────────────────────────────────────────────────
def fetch_ecb_monthly():
    codes = "+".join(COUNTRIES.keys())

    # Try correct 5-dimension key first, then wildcard fallback
    attempts = [
        f"https://data-api.ecb.europa.eu/service/data/CAR/M.{codes}.N.NEWCARS.NSA?format=csvdata&startPeriod={START_YEAR}-01&detail=dataonly",
        f"https://data-api.ecb.europa.eu/service/data/CAR/M.{codes}..NEWCARS.NSA?format=csvdata&startPeriod={START_YEAR}-01&detail=dataonly",
        f"https://data-api.ecb.europa.eu/service/data/CAR/M.{codes}.N.CARS.NSA?format=csvdata&startPeriod={START_YEAR}-01&detail=dataonly",
    ]

    raw = None
    for url in attempts:
        print(f"[ECB] Trying: {url[:100]}…")
        try:
            raw = http_get(url).decode("utf-8")
            # Check if we got actual CSV data (not an error page)
            if "TIME_PERIOD" in raw or "OBS_VALUE" in raw:
                print(f"[ECB] Success with this URL ✓")
                break
            else:
                print(f"[ECB] Response has no data columns, trying next…")
                print(f"[ECB] Response preview: {raw[:200]}")
                raw = None
        except urllib.error.HTTPError as e:
            print(f"[ECB] HTTP {e.code} — trying next URL…")
        except Exception as e:
            print(f"[ECB] Error: {e} — trying next URL…")

    if not raw:
        print("[ECB] All attempts failed — no monthly data")
        return {}

    # Log first few lines to understand the CSV structure
    lines = raw.split("\n")
    print(f"[ECB] CSV header: {lines[0][:200]}")
    print(f"[ECB] First data row: {lines[1][:200] if len(lines) > 1 else 'none'}")

    result = {}
    reader = csv.DictReader(io.StringIO(raw))
    headers = reader.fieldnames or []
    print(f"[ECB] CSV columns: {headers}")

    rows_read = 0
    for row in reader:
        rows_read += 1
        # Try all common column name variants
        geo    = (row.get("REF_AREA") or row.get("ref_area") or
                  row.get("AREA") or row.get("COUNTRY") or "").strip()
        period = (row.get("TIME_PERIOD") or row.get("time_period") or
                  row.get("TIME") or row.get("PERIOD") or "").strip()
        value  = (row.get("OBS_VALUE") or row.get("obs_value") or
                  row.get("VALUE") or "").strip()

        if not geo or not period or not value:
            continue
        try:
            v = int(float(value))
        except ValueError:
            continue
        if v <= 0:
            continue
        if geo not in result:
            result[geo] = {}
        result[geo][period] = v

    print(f"[ECB] Read {rows_read} data rows → {len(result)} countries")

    final = {}
    for geo, months in result.items():
        sorted_months = sorted(months.keys())
        if sorted_months:
            final[geo] = {
                "labels": sorted_months,
                "total":  [months[m] for m in sorted_months],
            }
            print(f"[ECB]   {geo}: {len(sorted_months)} months "
                  f"({sorted_months[0]}→{sorted_months[-1]}) "
                  f"latest={final[geo]['total'][-1]:,}")

    return final

# ── Eurostat annual ───────────────────────────────────────────────────────────
def fetch_eurostat_annual():
    url = ("https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/"
           "road_eqr_carpda?format=JSON&lang=EN&unit=NR")
    print(f"\n[Eurostat] Fetching: {url[:80]}…")

    try:
        raw = json.loads(http_get(url).decode("utf-8"))
    except Exception as e:
        print(f"[Eurostat] Error: {e}")
        return {}

    dims       = raw.get("dimension", {})
    values     = raw.get("value", {})
    dim_order  = raw.get("id", [])
    dim_sizes  = raw.get("size", [])

    print(f"[Eurostat] Dimensions in order: {dim_order}")
    print(f"[Eurostat] Sizes: {dim_sizes}")
    print(f"[Eurostat] Total values in dataset: {len(values)}")

    # Log ALL available fuel codes so we know what's actually in the data
    fuel_cat    = dims.get("mot_nrg", {}).get("category", {})
    fuel_labels = fuel_cat.get("label", {})
    fuel_index  = fuel_cat.get("index", {})
    print(f"\n[Eurostat] Available mot_nrg codes ({len(fuel_labels)}):")
    for code, label in sorted(fuel_labels.items(), key=lambda x: fuel_index.get(x[0], 99)):
        print(f"  [{fuel_index.get(code, '?'):2}] {code:12s} → {label}")

    # Now build our fuel mapping based on what's ACTUALLY available
    # We map discovered codes to our 6 display categories
    FUEL_MAP = {}
    for code in fuel_index:
        cl = code.upper()
        label = fuel_labels.get(code, "").upper()
        # Battery electric
        if cl in ("ELC", "BEV", "ELEC") or "BATTERY" in label or "PURE ELECTRIC" in label:
            FUEL_MAP[code] = "bev"
        # Plug-in hybrid
        elif cl in ("PHEV", "PHEV_TEV", "TEV") or "PLUG-IN" in label or "PLUGIN" in label:
            FUEL_MAP[code] = "phev"
        # Hybrid non-plug (HEV, mild hybrid)
        elif cl in ("HEV", "MHEV", "FHEV", "HYBRID") or ("HYBRID" in label and "PLUG" not in label):
            FUEL_MAP[code] = "hybrid"
        # Petrol
        elif cl in ("PET", "PETROL", "GAS", "GASOLINE") or "PETROL" in label or "GASOLINE" in label:
            FUEL_MAP[code] = "petrol"
        # Diesel
        elif cl in ("DIE", "DIESEL") or "DIESEL" in label:
            FUEL_MAP[code] = "diesel"
        # Skip aggregates (TOTAL, ALT = alternative aggregate, etc.)
        elif cl in ("TOTAL", "ALT", "OTHER_NEC", "UNK"):
            pass  # skip aggregates
        # Everything else → other
        elif cl not in ("TOTAL", "ALT"):
            FUEL_MAP[code] = "other"

    print(f"\n[Eurostat] Fuel code mapping:")
    for code, field in FUEL_MAP.items():
        print(f"  {code:12s} → {field}")

    # Build index lookups
    dim_idx = {}
    for d in dim_order:
        dim_idx[d] = dims.get(d, {}).get("category", {}).get("index", {})

    geo_idx  = dim_idx.get("geo",     {})
    time_idx = dim_idx.get("time",    {})
    mot_idx  = dim_idx.get("mot_nrg", {})

    # Compute strides (row-major)
    strides = {}
    for i, d in enumerate(dim_order):
        s = 1
        for j in range(i + 1, len(dim_order)):
            s *= dim_sizes[j]
        strides[d] = s

    print(f"\n[Eurostat] Strides: {strides}")

    years = sorted([y for y in time_idx if int(y) >= START_YEAR])
    print(f"[Eurostat] Years {START_YEAR}+: {years}")

    # Map Eurostat geo codes → ECB codes
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
                linear = sum(
                    pos * strides.get(dim, 1)
                    for dim, pos in [
                        ("geo", g_pos), ("time", t_pos), ("mot_nrg", f_pos)
                    ]
                    if dim in strides
                )
                v = values.get(str(linear)) or values.get(linear)
                if v is not None:
                    year_vals[our_field] += int(v)

            country_data[year] = year_vals

        if country_data:
            result[ecb_key] = country_data

    # Sanity check for Germany 2023
    if "DE" in result and "2023" in result["DE"]:
        de23 = result["DE"]["2023"]
        bev = de23.get("bev", 0)
        pet = de23.get("petrol", 0)
        hyb = de23.get("hybrid", 0)
        phev = de23.get("phev", 0)
        print(f"\n[Sanity DE 2023] BEV={bev:,}  PHEV={phev:,}  "
              f"Hybrid={hyb:,}  Petrol={pet:,}")
        print(f"[Sanity DE 2023] BEV expected ~524,000 → "
              f"{'OK ✓' if bev > 200000 else 'WRONG ✗'}")
        print(f"[Sanity DE 2023] PHEV expected ~228,000 → "
              f"{'OK ✓' if phev > 100000 else 'likely still 0 — check fuel codes above'}")

    print(f"\n[Eurostat] Done — {len(result)} countries")
    return result

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
        old   = {}
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
        print("[Telegram] No credentials — skip")
        return
    date_str = NOW.strftime("%d.%m.%Y %H:%M UTC")
    if changed:
        lines = "\n".join(f"  • {c}" for c in changed[:25])
        body = (f"🚗 *Car Registrations — Updated*\n📅 {date_str}\n"
                f"🌍 {n_countries} countries\n\n*Changed ({len(changed)}):*\n{lines}")
    else:
        body = (f"🚗 *Car Registrations — No Changes*\n📅 {date_str}\n"
                f"🌍 {n_countries} countries checked — all up to date.")
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
    print(f"  Car Registration Updater v3  —  {NOW.strftime('%d.%m.%Y %H:%M UTC')}")
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
