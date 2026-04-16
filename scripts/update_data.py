# scripts/update_data.py
# Version: v49 - April 2026
# Unterstützt alle EU/EEA + Asien/Pazifik + Nordamerika Länder mit monatlichen Overrides

import csv
import json
import os
from datetime import datetime
from urllib.request import urlopen, Request
from urllib.error import URLError, HTTPError

# ====================== KONFIGURATION ======================
DATA_DIR = "data/countries"

ECB_BASE_URL = "https://data.ecb.europa.eu/api/v2.1/data/STS.M.CAR.REG.{country}.M?format=csvdata&detail=dataonly"

# Länder-Mapping: ECB-Code → (Dateiname, Anzeigename)
COUNTRIES = {
    # Europa
    "DE": ("germany", "Germany"),
    "FR": ("france", "France"),
    "IT": ("italy", "Italy"),
    "ES": ("spain", "Spain"),
    "NL": ("netherlands", "Netherlands"),
    "BE": ("belgium", "Belgium"),
    "AT": ("austria", "Austria"),
    "CH": ("switzerland", "Switzerland"),
    "PL": ("poland", "Poland"),
    "CZ": ("czech_republic", "Czech Republic"),
    "SK": ("slovakia", "Slovakia"),
    "HU": ("hungary", "Hungary"),
    "RO": ("romania", "Romania"),
    "BG": ("bulgaria", "Bulgaria"),
    "HR": ("croatia", "Croatia"),
    "SI": ("slovenia", "Slovenia"),
    "GR": ("greece", "Greece"),
    "PT": ("portugal", "Portugal"),
    "IE": ("ireland", "Ireland"),
    "LU": ("luxembourg", "Luxembourg"),
    "FI": ("finland", "Finland"),
    "SE": ("sweden", "Sweden"),
    "DK": ("denmark", "Denmark"),
    "NO": ("norway", "Norway"),
    "MT": ("malta", "Malta"),
    "CY": ("cyprus", "Cyprus"),
    "EE": ("estonia", "Estonia"),
    "LV": ("latvia", "Latvia"),
    "LT": ("lithuania", "Lithuania"),
    "IS": ("iceland", "Iceland"),
    "GB": ("united_kingdom", "United Kingdom"),

    # Asien & Pazifik
    "IN": ("india", "India"),
    "TH": ("thailand", "Thailand"),
    "MY": ("malaysia", "Malaysia"),
    "ID": ("indonesia", "Indonesia"),
    "AU": ("australia", "Australia"),
    "NZ": ("new_zealand", "New Zealand"),
    "JP": ("japan", "Japan"),
    "KR": ("south_korea", "South Korea"),
    "CN": ("china", "China"),

    # Nordamerika (neu)
    "CA": ("canada", "Canada"),
    "US": ("united_states", "United States"),
}

# ====================== OVERRIDES ======================
def load_override(filename):
    path = os.path.join(DATA_DIR, filename)
    if not os.path.exists(path):
        return None, None, None
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        monthly = data.get("monthly")
        last_updated = data.get("last_updated")
        source = data.get("source_monthly")
        return monthly, last_updated, source
    except Exception as e:
        print(f"  Warnung: Konnte Override {filename} nicht laden: {e}")
        return None, None, None

overrides = {
    # Europa
    "DE": load_override("germany_monthly_override.json"),
    "FR": load_override("france_monthly_override.json"),
    "IT": load_override("italy_monthly_override.json"),
    "ES": load_override("spain_monthly_override.json"),
    "NL": load_override("netherlands_monthly_override.json"),
    "BE": load_override("belgium_monthly_override.json"),
    "AT": load_override("austria_monthly_override.json"),
    "CH": load_override("switzerland_monthly_override.json"),
    "PL": load_override("poland_monthly_override.json"),
    "CZ": load_override("czech_republic_monthly_override.json"),
    "SK": load_override("slovakia_monthly_override.json"),
    "HU": load_override("hungary_monthly_override.json"),
    "RO": load_override("romania_monthly_override.json"),
    "BG": load_override("bulgaria_monthly_override.json"),
    "HR": load_override("croatia_monthly_override.json"),
    "SI": load_override("slovenia_monthly_override.json"),
    "GR": load_override("greece_monthly_override.json"),
    "PT": load_override("portugal_monthly_override.json"),
    "IE": load_override("ireland_monthly_override.json"),
    "LU": load_override("luxembourg_monthly_override.json"),
    "FI": load_override("finland_monthly_override.json"),
    "SE": load_override("sweden_monthly_override.json"),
    "DK": load_override("denmark_monthly_override.json"),
    "NO": load_override("norway_monthly_override.json"),
    "MT": load_override("malta_monthly_override.json"),
    "CY": load_override("cyprus_monthly_override.json"),
    "EE": load_override("estonia_monthly_override.json"),
    "LV": load_override("latvia_monthly_override.json"),
    "LT": load_override("lithuania_monthly_override.json"),
    "IS": load_override("iceland_monthly_override.json"),
    "GB": load_override("united_kingdom_monthly_override.json"),

    # Asien & Pazifik
    "IN": load_override("india_monthly_override.json"),
    "TH": load_override("thailand_monthly_override.json"),
    "MY": load_override("malaysia_monthly_override.json"),
    "ID": load_override("indonesia_monthly_override.json"),
    "AU": load_override("australia_monthly_override.json"),
    "NZ": load_override("new_zealand_monthly_override.json"),
    "JP": load_override("japan_monthly_override.json"),
    "KR": load_override("south_korea_monthly_override.json"),
    "CN": load_override("china_monthly_override.json"),

    # Nordamerika (neu)
    "CA": load_override("canada_monthly_override.json"),
    "US": load_override("united_states_monthly_override.json"),
}

# ====================== ECB FETCH (mit urllib) ======================
def fetch_ecb_monthly(ecb_code):
    url = ECB_BASE_URL.format(country=ecb_code)
    headers = {"User-Agent": "Mozilla/5.0 (compatible; GitHub-Actions EV-Map)"}
    req = Request(url, headers=headers)

    try:
        with urlopen(req, timeout=30) as response:
            data = response.read().decode("utf-8").strip().split("\n")
        
        reader = csv.DictReader(data)
        result = []
        for row in reader:
            if row.get("OBS_VALUE") and row.get("TIME_PERIOD"):
                try:
                    value = int(float(row["OBS_VALUE"]))
                    result.append((row["TIME_PERIOD"], value))
                except (ValueError, TypeError):
                    continue
        result.sort(key=lambda x: x[0])
        return result
    except (HTTPError, URLError) as e:
        print(f"  Fehler beim Laden von ECB {ecb_code}: {e}")
        return []
    except Exception as e:
        print(f"  Unerwarteter Fehler bei {ecb_code}: {e}")
        return []

# ====================== WRITE JSON ======================
def write_country_json(country_code, ecb_data):
    monthly_override, last_upd_override, source_override = overrides.get(country_code, (None, None, None))
    display_name = COUNTRIES[country_code][1]

    if monthly_override and monthly_override.get("labels") and monthly_override.get("total"):
        labels = monthly_override["labels"]
        total = monthly_override["total"]
        last_updated = last_upd_override or datetime.utcnow().isoformat() + "Z"
        source_monthly = source_override or f"National statistics (monthly up to March 2026)"
        print(f"  → Override verwendet für {display_name}")
    else:
        labels = [date for date, _ in ecb_data]
        total = [val for _, val in ecb_data]
        last_updated = datetime.utcnow().isoformat() + "Z"
        source_monthly = "ECB Data Portal / ACEA"

    # Längen synchronisieren
    min_len = min(len(labels), len(total))
    labels = labels[:min_len]
    total = total[:min_len]

    output = {
        "monthly": {
            "labels": labels,
            "total": total
        },
        "last_updated": last_updated,
        "source_monthly": source_monthly
    }

    filename = os.path.join(DATA_DIR, f"{COUNTRIES[country_code][0]}.json")
    os.makedirs(DATA_DIR, exist_ok=True)

    with open(filename, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2, ensure_ascii=False)

    print(f"  ✓ Geschrieben: {filename} ({len(labels)} Monate)")

# ====================== MAIN ======================
def main():
    print("=== Car Registration Data Update gestartet ===")
    print(f"Zeit: {datetime.utcnow().isoformat()}Z\n")

    for ecb_code, (filename, display_name) in COUNTRIES.items():
        print(f"Verarbeite {display_name} ({ecb_code}) ...")
        
        # Für Länder ohne ECB-Daten nur Override verwenden
        if ecb_code in ["IN", "TH", "MY", "ID", "AU", "NZ", "JP", "KR", "CN", "CA", "US"]:
            ecb_data = []
        else:
            ecb_data = fetch_ecb_monthly(ecb_code)
        
        write_country_json(ecb_code, ecb_data)

    print("\n=== Update abgeschlossen ===")

if __name__ == "__main__":
    main()
