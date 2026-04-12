"""
scripts/update_data.py  —  v9
==============================
Key fix vs v8:
  Instead of finding ONE key for Germany and applying it to all countries,
  v9 searches for the best key PER COUNTRY from the new CAR dataset.
  Falls back to STS only if CAR returns nothing.
  This ensures Austria, France etc. get data up to 2026.
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

FUEL_MAP_GRANULAR = {
    "ELC":         "bev",
    "ELC_PET_PI":  "phev",
    "ELC_DIE_PI":  "phev",
    "ELC_PET_HYB": "hybrid",
    "ELC_DIE_HYB": "hybrid",
    "PET_X_HYB":   "petrol",
    "DIE_X_HYB":   "diesel",
    "LPG":         "other",
    "GAS":         "other",
    "HYD_FCELL":   "other",
    "BIOETH":      "other",
    "BIODIE":      "other",
    "BIFUEL":      "other",
    "OTH":         "other",
}
FUEL_MAP_FALLBACK = {
    "ELC": "bev",
    "PET": "petrol",
    "DIE": "diesel",
    "LPG": "other",
    "GAS": "other",
    "OTH": "other",
}

def http_get(url, timeout=25):
    req = urllib.request.Request(
        url,
        headers={"User-Agent": "EV-Map-Bot/9.0 (github.com/Altair02/EV-adoption-worldmap)"}
    )
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read()

def parse_csv(raw):
    """Parse ECB CSV response into {period: value} dict."""
    result = {}
    try:
        reader = csv.DictReader(io.StringIO(raw))
        for row in reader:
            row_u = {k.upper(): v for k, v in row.items()}
            period = row_u.get("TIME_PERIOD", "").strip()
            value  = row_u.get("OBS_VALUE",   "").strip()
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

def try_ecb_key(dataset, key, last_n=None, start=None):
    """
    Try one ECB key. Returns (data_dict, latest_period) or ({}, None).
    """
    params = "format=csvdata&detail=dataonly"
    if last_n:
        params += f"&lastNObservations={last_n}"
    if start:
        params += f"&startPeriod={start}"
    url = f"https://data-api.ecb.europa.eu/service/data/{dataset}/{key}?{params}"
    try:
        raw = http_get(url).decode("utf-8")
        if "TIME_PERIOD" not in raw.upper():
            return {}, None
        data = parse_csv(raw)
        if not data:
            return {}, None
        latest = max(data.keys())
        return data, latest
    except urllib.error.HTTPError:
        return {}, None
    except Exception:
        return {}, None

# ── ECB: per-country key search ───────────────────────────────────────────────
def fetch_ecb_one_country(geo):
    """
    Find the best ECB monthly series for a single country.
    Returns {period: value} with data as recent as possible.
    """
    # Key templates to try for this country
    # CAR dataset first (recent data), then STS (older)
    attempts = [
        ("CAR", f"M.{geo}.N.NEWCARS.ABS"),
        ("CAR", f"M.{geo}.N.NEWCARS.N"),
        ("CAR", f"M.{geo}.N.NEWCARS.NSA"),
        ("CAR", f"M.{geo}.N.NEWCARS.3.ABS"),
        ("CAR", f"M.{geo}.N.CARS.ABS"),
        ("CAR", f"M.{geo}.N.CARS.N"),
        ("CAR", f"M.{geo}.N.CARS.NSA"),
        ("CAR", f"M.{geo}.N.CREG.PC0000.ABS"),
        ("CAR", f"M.{geo}.N.CREG.PC0000.N"),
        ("CAR", f"M.{geo}.N.CREG.PC0000.3.ABS"),
        ("CAR", f"M.{geo}...."),           # full wildcard
        # Old STS as last resort (data only to ~2022)
        ("STS", f"M.{geo}.N.CREG.PC0000.3.ABS"),
        ("STS", f"M.{geo}.W.CREG.PC0000.3.ABS"),
    ]

    best_data   = {}
    best_latest = None

    for dataset, key in attempts:
        # Quick probe: last 3 observations
        probe, latest = try_ecb_key(dataset, key, last_n=3)
        if not probe:
            time.sleep(0.1)
            continue

        # If we found recent CAR data (2024+) → fetch full history and use it
        if dataset == "CAR" and latest and latest >= "2024-01":
            full, _ = try_ecb_key(dataset, key, start=f"{START_YEAR}-01")
            if full:
                return full, dataset, key, latest

        # Otherwise keep as best candidate if more recent than what we have
        if best_latest is None or (latest and latest > best_latest):
            best_data   = probe
            best_latest = latest
            best_key    = (dataset, key)

        time.sleep(0.1)

    # Fetch full history for best candidate
    if best_data and best_latest:
        dataset, key = best_key
        full, _ = try_ecb_key(dataset, key, start=f"{START_YEAR}-01")
        if full:
            return full, dataset, key, best_latest

    return {}, None, None, None


def fetch_ecb_monthly():
    print(f"[ECB] Fetching {len(COUNTRIES)} countries (per-country key search)…")
    results = {}
    for geo in COUNTRIES:
        data, dataset, key, latest = fetch_ecb_one_country(geo)
        if data:
            months = sorted(data.keys())
            results[geo] = {"labels": months, "total": [data[m] for m in months]}
            flag = "✓" if latest and latest >= "2024-01" else "⚠ old"
            print(f"[ECB]   {geo} {flag}: {dataset}/{key} → {months[0]}..{months[-1]}")
        else:
            print(f"[ECB]   {geo}: no data found")
        time.sleep(0.15)

    ok = len(results)
    recent = sum(1 for v in results.values() if v["labels"][-1] >= "2024-01")
    print(f"[ECB] Done — {ok}/{len(COUNTRIES)} countries, {recent} with data up to 2024+")
    return results


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


# ── Write files ───────────────────────────────────────────────────────────────
def write_files(monthly, annual):
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    changed = []

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

        # MERGE monthly: keep old data if new fetch is empty or older
        old_monthly = old_payload.get("monthly", {})
        old_latest  = old_monthly.get("labels", [""])[-1] if old_monthly.get("labels") else ""
        new_latest  = new_monthly.get("labels", [""])[-1] if new_monthly.get("labels") else ""

        if new_monthly and new_latest >= old_latest:
            merged_monthly = new_monthly
        else:
            merged_monthly = old_monthly  # keep existing

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
                f"🌍 {n_countries} — all up to date.")
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
    print(f"  Car Registration Updater v9  —  {NOW.strftime('%d.%m.%Y %H:%M UTC')}")
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
