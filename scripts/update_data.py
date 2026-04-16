# scripts/update_data.py
# Version: v52 - April 2026 - VOLLAUTOMATISCH (wo möglich)

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

COUNTRIES = { ... }   # ← bleibt exakt gleich wie bei dir (DE, FR, ..., AR, BR, ...)

# ====================== OVERRIDES + AUTO-FETCH ======================
def load_override(filename):
    path = os.path.join(DATA_DIR, filename)
    if not os.path.exists(path):
        return None, None, None
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    return data.get("monthly"), data.get("last_updated"), data.get("source_monthly")

overrides = { ... }   # ← bleibt gleich (alle _monthly_override.json)

# ====================== NEU: AUTO-FETCHER ======================
def fetch_latest_argentina():
    """Holt den neuesten Monat automatisch von Trading Economics (ADEFA-Quelle)"""
    url = "https://tradingeconomics.com/argentina/car-registrations"
    headers = {"User-Agent": "Mozilla/5.0 (compatible; GitHub-Actions EV-Map)"}
    try:
        resp = requests.get(url, headers=headers, timeout=20)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")
        
        # Sucht nach dem aktuellen Wert + Monat (Text auf der Seite)
        text = soup.get_text()
        match = re.search(r'Car Registrations in Argentina increased to ([\d,]+) Units in (\w+) from', text)
        if not match:
            match = re.search(r'Car Registrations.*?([\d,]+).*?in (\w+)', text)
        
        if match:
            value = int(match.group(1).replace(",", ""))
            month_str = match.group(2)
            # Monat in YYYY-MM umwandeln (sehr robust)
            month_map = {"January": "01", "February": "02", "March": "03", "April": "04", "May": "05", "June": "06",
                         "July": "07", "August": "08", "September": "09", "October": "10", "November": "11", "December": "12"}
            year = datetime.now().year
            if month_map[month_str] == "12" and datetime.now().month == 1:
                year -= 1
            date_label = f"{year}-{month_map.get(month_str, '01')}"
            return date_label, value
    except Exception as e:
        print(f"  Warnung: Argentina Auto-Fetch fehlgeschlagen: {e}")
    return None, None

# Weitere Fetcher kannst du hier später hinzufügen (z. B. für Brasilien, USA, Japan...)

# ====================== WRITE JSON ======================
def write_country_json(country_code, ecb_data):
    monthly_override, last_upd_override, source_override = overrides.get(country_code, (None, None, None))
    display_name = COUNTRIES[country_code][1]

    # === NEU: Auto-Update für Argentinien ===
    if country_code == "AR":
        new_label, new_value = fetch_latest_argentina()
        if new_label and new_value:
            if monthly_override and monthly_override.get("labels"):
                # Prüfen, ob der Monat schon drin ist
                if new_label not in monthly_override["labels"]:
                    monthly_override["labels"].append(new_label)
                    monthly_override["total"].append(new_value)
                    print(f"  → Argentina: Neuer Monat {new_label} automatisch hinzugefügt ({new_value})")
                else:
                    print(f"  → Argentina: {new_label} schon aktuell")
            else:
                # Falls keine Override existiert (sehr unwahrscheinlich)
                monthly_override = {"labels": [new_label], "total": [new_value]}

    # Rest bleibt gleich wie bei dir
    if monthly_override and monthly_override.get("labels") and monthly_override.get("total"):
        labels = monthly_override["labels"]
        total = monthly_override["total"]
        last_updated = last_upd_override or datetime.utcnow().isoformat() + "Z"
        source_monthly = source_override or "National statistics + Auto-Fetch"
        print(f"  → Override (ggf. auto-updated) verwendet für {display_name}")
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
    print("=== Car Registration Data Update gestartet (v52) ===")
    print(f"Zeit: {datetime.utcnow().isoformat()}Z\n")

    for ecb_code, (filename, display_name) in COUNTRIES.items():
        print(f"Verarbeite {display_name} ({ecb_code}) ...")
        
        if ecb_code in ["IN", "TH", "MY", "ID", "AU", "NZ", "JP", "KR", "CN", "CA", "US", 
                        "BR", "RU", "TR", "MX", "AR", "CL", "IR", "SA", "ZA"]:
            ecb_data = []
        else:
            ecb_data = fetch_ecb_monthly(ecb_code)   # deine alte Funktion bleibt gleich
        
        write_country_json(ecb_code, ecb_data)

    print("\n=== Update abgeschlossen ===")

if __name__ == "__main__":
    main()
