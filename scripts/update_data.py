# scripts/update_data.py
# Version: v45 - April 2026
# Unterstützt alle EU/EEA + ausgewählte Länder mit monatlichen Overrides

import csv
import json
import os
import requests
from datetime import datetime

# ====================== KONFIGURATION ======================
DATA_DIR = "data/countries"
ECB_URL = "https://data.ecb.europa.eu/api/v2.1/data/STS.M.CAR.REG.{country}.M?format=csvdata&detail=dataonly"

# Länder-Mapping: ECB-Code → Dateiname + Anzeigename
COUNTRIES = {
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
}

# ====================== OVERRIDES ======================
def load_override(filename):
    path = os.path.join(DATA_DIR, filename)
    if not os.path.exists(path):
        return None, None, None
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    monthly = data.get("monthly")
    last_updated = data.get("last_updated")
    source = data.get("source_monthly")
    return monthly, last_updated, source

overrides = {
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
}

# ====================== HELFER ======================
def fetch_ecb_monthly(ecb_code):
    url = ECB_URL.format(country=ecb_code)
    try:
        r = requests.get(url, timeout=30)
        r.raise_for_status()
        lines = r.text.strip().split("\n")
        reader = csv.DictReader(lines)
        data = []
        for row in reader:
            if row.get("OBS_VALUE") and row.get("TIME_PERIOD"):
                try:
                    value = int(float(row["OBS_VALUE"]))
                    data.append((row["TIME_PERIOD"], value))
                except:
                    continue
        data.sort(key=lambda x: x[0])
        return data
    except Exception as e:
        print(f"  Fehler beim Laden von ECB {ecb_code}: {e}")
        return []

def write_country_json(country_code, ecb_data):
    monthly_override, last_upd_override, source_override = overrides.get(country_code, (None, None, None))

    if monthly_override and monthly_override.get("labels") and monthly_override.get("total"):
        labels = monthly_override["labels"]
        total = monthly_override["total"]
        last_updated = last_upd_override or datetime.utcnow().isoformat() + "Z"
        source_monthly = source_override or f"National statistics / ACEA (monthly up to March 2026)"
        print(f"  → Override verwendet für {COUNTRIES[country_code][1]}")
    else:
        labels = [date for date, _ in ecb_data]
        total = [val for _, val in ecb_data]
        last_updated = datetime.utcnow().isoformat() + "Z"
        source_monthly = "ECB Data Portal / ACEA"

    # Sicherstellen, dass Längen übereinstimmen
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
        ecb_data = fetch_ecb_monthly(ecb_code)
        write_country_json(ecb_code, ecb_data)

    print("\n=== Update abgeschlossen ===")

if __name__ == "__main__":
    main()
