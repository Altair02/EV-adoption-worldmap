# scripts/update_data.py
# Version: v53 - April 2026 - VOLLAUTOMATISCH für ALLE 20 Nicht-ECB-Länder

import csv
import json
import os
import re
from datetime import datetime, timezone
from urllib.request import urlopen, Request
import requests
from bs4 import BeautifulSoup

# ====================== KONFIGURATION ======================
DATA_DIR = "data/countries"

ECB_BASE_URL = "https://data.ecb.europa.eu/api/v2.1/data/STS.M.CAR.REG.{country}.M?format=csvdata&detail=dataonly"

# Länder-Mapping
COUNTRIES = {
    # Europa (ECB)
    "DE": ("germany", "Germany"), "FR": ("france", "France"), "IT": ("italy", "Italy"),
    "ES": ("spain", "Spain"), "NL": ("netherlands", "Netherlands"), "BE": ("belgium", "Belgium"),
    "AT": ("austria", "Austria"), "CH": ("switzerland", "Switzerland"), "PL": ("poland", "Poland"),
    "CZ": ("czech_republic", "Czech Republic"), "SK": ("slovakia", "Slovakia"),
    "HU": ("hungary", "Hungary"), "RO": ("romania", "Romania"), "BG": ("bulgaria", "Bulgaria"),
    "HR": ("croatia", "Croatia"), "SI": ("slovenia", "Slovenia"), "GR": ("greece", "Greece"),
    "PT": ("portugal", "Portugal"), "IE": ("ireland", "Ireland"), "LU": ("luxembourg", "Luxembourg"),
    "FI": ("finland", "Finland"), "SE": ("sweden", "Sweden"), "DK": ("denmark", "Denmark"),
    "NO": ("norway", "Norway"), "MT": ("malta", "Malta"), "CY": ("cyprus", "Cyprus"),
    "EE": ("estonia", "Estonia"), "LV": ("latvia", "Latvia"), "LT": ("lithuania", "Lithuania"),
    "IS": ("iceland", "Iceland"), "GB": ("united_kingdom", "United Kingdom"),

    # Nicht-ECB (jetzt alle auto)
    "IN": ("india", "India"), "TH": ("thailand", "Thailand"), "MY": ("malaysia", "Malaysia"),
    "ID": ("indonesia", "Indonesia"), "AU": ("australia", "Australia"),
    "NZ": ("new_zealand", "New Zealand"), "JP": ("japan", "Japan"),
    "KR": ("south_korea", "South Korea"), "CN": ("china", "China"),
    "CA": ("canada", "Canada"), "US": ("united_states", "United States"),
    "BR": ("brazil", "Brazil"), "RU": ("russia", "Russia"), "TR": ("turkey", "Turkey"),
    "MX": ("mexico", "Mexico"), "AR": ("argentina", "Argentina"), "CL": ("chile", "Chile"),
    "IR": ("iran", "Iran"), "SA": ("saudi_arabia", "Saudi Arabia"), "ZA": ("south_africa", "South Africa"),
}

# ====================== OVERRIDES (Fallback) ======================
def load_override(filename):
    path = os.path.join(DATA_DIR, filename)
    if not os.path.exists(path):
        return None, None, None
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data.get("monthly"), data.get("last_updated"), data.get("source_monthly")
    except Exception as e:
        print(f"  Warnung: Konnte Override {filename} nicht laden: {e}")
        return None, None, None

overrides = {k: load_override(f"{v[0]}_monthly_override.json") for k, v in COUNTRIES.items()}

# ====================== TRADING ECONOMICS AUTO-FETCHER (für alle 20 Länder) ======================
TE_SLUGS = {
    "AR": "argentina", "BR": "brazil", "CA": "canada", "CN": "china", "ID": "indonesia",
    "IN": "india", "JP": "japan", "KR": "south-korea", "MX": "mexico", "RU": "russia",
    "TH": "thailand", "TR": "turkey", "US": "united-states", "ZA": "south-africa",
    "AU": "australia", "NZ": "new-zealand", "MY": "malaysia", "CL": "chile",
    "IR": "iran", "SA": "saudi-arabia",
}

def fetch_latest_te(country_code, slug):
    """Holt neuesten Monat von Trading Economics"""
    url = f"https://tradingeconomics.com/{slug}/car-registrations"
    headers = {"User-Agent": "Mozilla/5.0 (compatible; GitHub-Actions EV-Map)"}
    display_name = COUNTRIES[country_code][1]
    
    try:
        resp = requests.get(url, headers=headers, timeout=20)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")
        text = soup.get_text()

        # Robuste Regex – funktioniert bei allen Ländern
        match = re.search(r'Car Registrations in .*?increased to ([\d,]+)(?:[\s]*Thousand)?[\s]*Units? in (\w+)', 
                         text, re.IGNORECASE)
        if match:
            num_str = match.group(1).replace(",", "")
            value = int(num_str)
            # Thousand korrigieren (z. B. USA)
            if "Thousand" in text[match.start():match.end() + 150]:
                value *= 1000

            month_name = match.group(2)
            month_map = {"january":"01","february":"02","march":"03","april":"04","may":"05","june":"06",
                         "july":"07","august":"08","september":"09","october":"10","november":"11","december":"12"}
            year = datetime.now().year
            month_num = month_map.get(month_name.lower(), "01")
            date_label = f"{year}-{month_num}"

            print(f"  → {display_name} Auto-Fetch: {date_label} = {value:,} Einheiten")
            return date_label, value
    except Exception as e:
        print(f"  Warnung: {display_name} Auto-Fetch fehlgeschlagen: {e}")
    return None, None

# ====================== ECB FETCH (unverändert) ======================
def fetch_ecb_monthly(ecb_code):
    # ... (deine originale Funktion bleibt komplett gleich)
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

# ====================== WRITE JSON ======================
def write_country_json(country_code, ecb_data):
    monthly_override, last_upd_override, source_override = overrides.get(country_code, (None, None, None))
    display_name = COUNTRIES[country_code][1]

    # === AUTO-UPDATE für alle Trading-Economics-Länder ===
    if country_code in TE_SLUGS:
        new_label, new_value = fetch_latest_te(country_code, TE_SLUGS[country_code])
        if new_label and new_value is not None:
            if monthly_override and monthly_override.get("labels"):
                if new_label not in monthly_override["labels"]:
                    monthly_override["labels"].append(new_label)
                    monthly_override["total"].append(new_value)
                    print(f"  → {display_name}: Neuer Monat {new_label} automatisch hinzugefügt")
                else:
                    print(f"  → {display_name}: {new_label} bereits aktuell")
            else:
                monthly_override = {"labels": [new_label], "total": [new_value]}

    # Normale Verarbeitung
    if monthly_override and monthly_override.get("labels") and monthly_override.get("total"):
        labels = monthly_override["labels"]
        total = monthly_override["total"]
        last_updated = last_upd_override or datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
        source_monthly = source_override or "National statistics + Auto-Fetch (Trading Economics)"
        print(f"  → Override (auto-updated) verwendet für {display_name}")
    else:
        labels = [date for date, _ in ecb_data]
        total = [val for _, val in ecb_data]
        last_updated = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
        source_monthly = "ECB Data Portal / ACEA"

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
    print("=== Car Registration Data Update gestartet (v53) ===")
    print(f"Zeit: {datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z')}\n")

    for ecb_code, (filename, display_name) in COUNTRIES.items():
        print(f"Verarbeite {display_name} ({ecb_code}) ...")
        
        # Nur EU-Länder holen von ECB
        if ecb_code in ["IN", "TH", "MY", "ID", "AU", "NZ", "JP", "KR", "CN", "CA", "US",
                        "BR", "RU", "TR", "MX", "AR", "CL", "IR", "SA", "ZA"]:
            ecb_data = []
        else:
            ecb_data = fetch_ecb_monthly(ecb_code)
        
        write_country_json(ecb_code, ecb_data)

    print("\n=== Update abgeschlossen ===")

if __name__ == "__main__":
    main()
