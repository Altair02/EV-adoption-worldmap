# scripts/update_data.py
# Version: v52.1 - April 2026 - VOLLAUTOMATISCH + Bugfix

import csv
import json
import os
import re
from datetime import datetime
from urllib.request import urlopen, Request
import requests
from bs4 import BeautifulSoup

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

    # Nordamerika
    "CA": ("canada", "Canada"),
    "US": ("united_states", "United States"),

    # Neue Länder (v51)
    "BR": ("brazil", "Brazil"),
    "RU": ("russia", "Russia"),
    "TR": ("turkey", "Turkey"),
    "MX": ("mexico", "Mexico"),
    "AR": ("argentina", "Argentina"),
    "CL": ("chile", "Chile"),
    "IR": ("iran", "Iran"),
    "SA": ("saudi_arabia", "Saudi Arabia"),
    "ZA": ("south_africa", "South Africa"),
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

    "IN": load_override("india_monthly_override.json"),
    "TH": load_override("thailand_monthly_override.json"),
    "MY": load_override("malaysia_monthly_override.json"),
    "ID": load_override("indonesia_monthly_override.json"),
    "AU": load_override("australia_monthly_override.json"),
    "NZ": load_override("new_zealand_monthly_override.json"),
    "JP": load_override("japan_monthly_override.json"),
    "KR": load_override("south_korea_monthly_override.json"),
    "CN": load_override("china_monthly_override.json"),

    "CA": load_override("canada_monthly_override.json"),
    "US": load_override("united_states_monthly_override.json"),

    "BR": load_override("brazil_monthly_override.json"),
    "RU": load_override("russia_monthly_override.json"),
    "TR": load_override("turkey_monthly_override.json"),
    "MX": load_override("mexico_monthly_override.json"),
    "AR": load_override("argentina_monthly_override.json"),
    "CL": load_override("chile_monthly_override.json"),
    "IR": load_override("iran_monthly_override.json"),
    "SA": load_override("saudi_arabia_monthly_override.json"),
    "ZA": load_override("south_africa_monthly_override.json"),
}

# ====================== ECB FETCH ======================
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
    except Exception as e:
        print(f"  Fehler beim Laden von ECB {ecb_code}: {e}")
        return []

# ====================== AUTO-FETCHER (Argentinien) ======================
def fetch_latest_argentina():
    """Holt den neuesten Monat automatisch von Trading Economics / ADEFA"""
    url = "https://tradingeconomics.com/argentina/car-registrations"
    headers = {"User-Agent": "Mozilla/5.0 (compatible; GitHub-Actions EV-Map)"}
    try:
        resp = requests.get(url, headers=headers, timeout=20)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")
        text = soup.get_text()

        # Verbesserte Suche nach aktuellem Monat + Zahl
        match = re.search(r'Car Registrations.*?increased to ([\d,]+) Units in (\w+)', text, re.IGNORECASE)
        if not match:
            match = re.search(r'([\d,]+).*?in (\w+)', text, re.IGNORECASE)

        if match:
            value = int(match.group(1).replace(",", ""))
            month_name = match.group(2)
            month_map = {"january": "01", "february": "02", "march": "03", "april": "04",
                         "may": "05", "june": "06", "july": "07", "august": "08",
                         "september": "09", "october": "10", "november": "11", "december": "12"}
            year = datetime.now().year
            month_num = month_map.get(month_name.lower(), "01")
            date_label = f"{year}-{month_num}"

            print(f"  → Argentina Auto-Fetch: {date_label} = {value} Einheiten")
            return date_label, value
    except Exception as e:
        print(f"  Warnung: Argentina Auto-Fetch fehlgeschlagen: {e}")
    return None, None

# ====================== WRITE JSON ======================
def write_country_json(country_code, ecb_data):
    monthly_override, last_upd_override, source_override = overrides.get(country_code, (None, None, None))
    display_name = COUNTRIES[country_code][1]

    # === Auto-Update für Argentinien ===
    if country_code == "AR":
        new_label, new_value = fetch_latest_argentina()
        if new_label and new_value is not None:
            if monthly_override and monthly_override.get("labels"):
                if new_label not in monthly_override["labels"]:
                    monthly_override["labels"].append(new_label)
                    monthly_override["total"].append(new_value)
                    print(f"  → Argentina: Neuer Monat {new_label} automatisch hinzugefügt ({new_value})")
                else:
                    print(f"  → Argentina: {new_label} bereits aktuell")
            else:
                monthly_override = {"labels": [new_label], "total": [new_value]}

    # Normale Verarbeitung
    if monthly_override and monthly_override.get("labels") and monthly_override.get("total"):
        labels = monthly_override["labels"]
        total = monthly_override["total"]
        last_updated = last_upd_override or datetime.now(datetime.UTC).isoformat().replace("+00:00", "Z")
        source_monthly = source_override or "National statistics + Auto-Fetch (ADEFA)"
        print(f"  → Override (ggf. auto-updated) verwendet für {display_name}")
    else:
        labels = [date for date, _ in ecb_data]
        total = [val for _, val in ecb_data]
        last_updated = datetime.now(datetime.UTC).isoformat().replace("+00:00", "Z")
        source_monthly = "ECB Data Portal / ACEA"

    # Längen synchronisieren
    min_len = min(len(labels), len(total))
    labels = labels[:min_len]
    total = total[:min_len]

    output = {
        "monthly": {"labels": labels, "total": total},
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
    print("=== Car Registration Data Update gestartet (v52.1) ===")
    print(f"Zeit: {datetime.now(datetime.UTC).isoformat().replace('+00:00', 'Z')}\n")

    for ecb_code, (filename, display_name) in COUNTRIES.items():
        print(f"Verarbeite {display_name} ({ecb_code}) ...")
        
        if ecb_code in ["IN", "TH", "MY", "ID", "AU", "NZ", "JP", "KR", "CN", "CA", "US", 
                        "BR", "RU", "TR", "MX", "AR", "CL", "IR", "SA", "ZA"]:
            ecb_data = []
        else:
            ecb_data = fetch_ecb_monthly(ecb_code)
        
        write_country_json(ecb_code, ecb_data)

    print("\n=== Update abgeschlossen ===")

if __name__ == "__main__":
    main()
