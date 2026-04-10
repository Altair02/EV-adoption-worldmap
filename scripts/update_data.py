import csv, io, json, os, sys, urllib.error, urllib.request
from datetime import datetime, timezone
from pathlib import Path

# ── Config ────────────────────────────────────────────────────────────────────
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

# Eurostat fuel codes → our field names
FUEL_MAP = {
    "ELC":  "bev",
    "PHEV": "phev",
    "HEV":  "hybrid",
    "PET":  "petrol",
    "DIE":  "diesel",
    "LPG":  "other",
    "NAT":  "other",
    "H2":   "other",
    "OTH":  "other",
}

# ── HTTP ──────────────────────────────────────────────────────────────────────
def http_get(url, timeout=45):
    req = urllib.request.Request(
        url, headers={"User-Agent": "EV-Map-Bot/2.1 (github.com/Altair02/EV-adoption-worldmap)"}
    )
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read()

# ── ECB monthly ───────────────────────────────────────────────────────────────
def fetch_ecb_monthly():
    """
    Returns { "DE": {"labels": ["2015-01",...], "total": [12345,...]}, ... }

    Series key format (ECB_CAR1 DSD, updated March 2025):
      CAR / M . {REF_AREA} . N . NEWCARS . NSA
      ─────────────────────────────────────────
      M         = monthly
      {REF_AREA} = ISO country code (DE, FR, ...)
      N         = not adjusted (adjustment dimension — was missing before!)
      NEWCARS   = new passenger car registrations
      NSA       = not seasonally adjusted
    """
    codes = "+".join(COUNTRIES.keys())
    url = (
        f"https://data-api.ecb.europa.eu/service/data/CAR/"
        f"M.{codes}.N.NEWCARS.NSA"
        f"?format=csvdata&startPeriod={START_YEAR}-01&detail=dataonly"
    )
    print(f"[ECB] Fetching: {url[:100]}…")
    try:
        raw = http_get(url).decode("utf-8")
    except urllib.error.HTTPError as e:
        print(f"[ECB] HTTP {e.code}: {e.reason}")
        # Try wildcard fallback
        url2 = (
            f"https://data-api.ecb.europa.eu/service/data/CAR/"
            f"M.{codes}..NEWCARS.NSA"
            f"?format=csvdata&startPeriod={START_YEAR}-01&detail=dataonly"
        )
        print(f"[ECB] Trying wildcard fallback: {url2[:100]}…")
        try:
            raw = http_get(url2).decode("utf-8")
        except Exception as e2:
            print(f"[ECB] Fallback also failed: {e2}")
            return {}
    except Exception as e:
        print(f"[ECB] Error: {e}")
        return {}

    result = {}
    reader = csv.DictReader(io.StringIO(raw))
    rows_read = 0
    for row in reader:
        rows_read += 1
        # CSV columns vary — try common column names
        geo    = (row.get("REF_AREA") or row.get("AREA") or "").strip()
        period = (row.get("TIME_PERIOD") or row.get("TIME") or "").strip()
        value  = (row.get("OBS_VALUE") or row.get("VALUE") or "").strip()
        if not geo or not period or not value:
            continue
        try:
            v = int(float(value))
        except ValueError:
            continue
        if geo not in result:
            result[geo] = {}
        result[geo][period] = v

    print(f"[ECB] Read {rows_read} CSV rows")

    # Convert to sorted lists
    final = {}
    for geo, months in result.items():
        sorted_months = sorted(months.keys())
        final[geo] = {
            "labels": sorted_months,
            "total":  [months[m] for m in sorted_months],
        }

    print(f"[ECB] {len(final)} countries with monthly data")
    if final:
        sample = next(iter(final.items()))
        print(f"[ECB] Sample — {sample[0]}: {len(sample[1]['labels'])} months, "
              f"latest={sample[1]['labels'][-1]}, value={sample[1]['total'][-1]:,}")
    return final

# ── Eurostat annual ───────────────────────────────────────────────────────────
def fetch_eurostat_annual():
    """
    Returns { "DE": {"2020": {"bev": 194163, ...}, ...}, ... }

    JSON-stat index formula:
      linear_idx = geo_pos * (n_time * n_fuel) + time_pos * n_fuel + fuel_pos

    This was wrong in the previous version — the dimension order must match
    exactly what Eurostat returns in the 'size' array.
    """
    url = (
        "https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/"
        "road_eqr_carpda?format=JSON&lang=EN&unit=NR"
    )
    print(f"\n[Eurostat] Fetching: {url[:80]}…")
    try:
        raw = json.loads(http_get(url).decode("utf-8"))
    except Exception as e:
        print(f"[Eurostat] Error: {e}")
        return {}

    dims   = raw.get("dimension", {})
    values = raw.get("value", {})

    # Get dimension order from the 'id' array — CRITICAL for correct indexing
    dim_order = raw.get("id", [])       # e.g. ["freq","unit","mot_nrg","geo","time"]
    dim_sizes = raw.get("size", [])     # e.g. [1, 1, 12, 45, 25]
    print(f"[Eurostat] Dimension order: {dim_order}")
    print(f"[Eurostat] Dimension sizes: {dim_sizes}")

    # Build index lookups per dimension
    dim_idx = {}
    for d in dim_order:
        dim_idx[d] = dims.get(d, {}).get("category", {}).get("index", {})

    geo_idx  = dim_idx.get("geo",     {})
    time_idx = dim_idx.get("time",    {})
    fuel_idx = dim_idx.get("mot_nrg", {})

    # Calculate stride for each dimension (row-major order)
    # stride[i] = product of all sizes after position i
    strides = {}
    for i, d in enumerate(dim_order):
        stride = 1
        for j in range(i + 1, len(dim_order)):
            stride *= dim_sizes[j]
        strides[d] = stride

    print(f"[Eurostat] Strides: { {k: strides[k] for k in ['geo','time','mot_nrg'] if k in strides} }")

    years = sorted([y for y in time_idx.keys() if int(y) >= START_YEAR])
    print(f"[Eurostat] Years {START_YEAR}+: {years}")

    # Build Eurostat-code → ECB-code mapping
    estat_to_ecb = {}
    for ecb_code, (name, estat_code, pop) in COUNTRIES.items():
        estat_to_ecb[estat_code] = ecb_code

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
            year_vals = {f: 0 for f in set(FUEL_MAP.values())}

            for fuel_code, our_field in FUEL_MAP.items():
                f_pos = fuel_idx.get(fuel_code)
                if f_pos is None:
                    continue
                # Compute linear index using strides
                linear = (
                    g_pos  * strides.get("geo",     1) +
                    t_pos  * strides.get("time",    1) +
                    f_pos  * strides.get("mot_nrg", 1)
                )
                v = values.get(str(linear)) or values.get(linear)
                if v is not None:
                    year_vals[our_field] += int(v)

            country_data[year] = year_vals

        if country_data:
            # Sanity check — Germany BEV 2023 should be ~500k
            if ecb_key == "DE" and "2023" in country_data:
                bev_2023 = country_data["2023"].get("bev", 0)
                pet_2023 = country_data["2023"].get("petrol", 0)
                print(f"[Eurostat] DE 2023 BEV={bev_2023:,}  Petrol={pet_2023:,}  "
                      f"({'OK ✓' if bev_2023 > 100000 else 'WRONG — index error'})")
            result[ecb_key] = country_data

    print(f"[Eurostat] {len(result)} countries with annual data")
    return result

# ── Write JSON files ──────────────────────────────────────────────────────────
def write_files(monthly, annual):
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    changed = []

    for ecb_code, (name, estat_code, pop) in COUNTRIES.items():
        m = monthly.get(ecb_code, {})
        a = annual.get(ecb_code, {})

        if not m and not a:
            print(f"[Write] {name}: no data — skip")
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
            "name":           name,
            "ecb_code":       ecb_code,
            "population_mio": pop,
            "source_monthly": "ECB Data Portal / ACEA",
            "source_annual":  "Eurostat road_eqr_carpda",
            "last_updated":   NOW.isoformat(),
            "monthly":        m,
            "annual":         annual_block,
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
            print(f"[Write] {name} → {fname} ✓ updated")
        else:
            print(f"[Write] {name} → {fname} (no change)")

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
                f"🌍 {n_countries} countries processed\n\n"
                f"*Changed ({len(changed)}):*\n{lines}")
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
    print(f"  Car Registration Updater  —  {NOW.strftime('%d.%m.%Y %H:%M UTC')}")
    print("=" * 60)
    monthly = fetch_ecb_monthly()
    annual  = fetch_eurostat_annual()
    if not monthly and not annual:
        print("⚠️  No data received from either source")
        send_telegram([], 0)
        sys.exit(1)
    changed = write_files(monthly, annual)
    send_telegram(changed, len(COUNTRIES))
    print(f"\n✓ Done — {len(changed)} countries updated")

if __name__ == "__main__":
    main()
