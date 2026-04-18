# scripts/update_data.py
# Version: v64 - April 2026 - Stündliches Update mit APScheduler

import json
import os
import re
from datetime import datetime, timezone
import requests
from bs4 import BeautifulSoup
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger

# ==================== KONFIGURATION ====================
DATA_DIR = "data/countries"

COUNTRIES = {
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
    "IN": ("india", "India"), "TH": ("thailand", "Thailand"), "MY": ("malaysia", "Malaysia"),
    "ID": ("indonesia", "Indonesia"), "AU": ("australia", "Australia"), "NZ": ("new_zealand", "New Zealand"),
    "JP": ("japan", "Japan"), "KR": ("south_korea", "South Korea"), "CN": ("china", "China"),
    "CA": ("canada", "Canada"), "US": ("united_states", "United States"), "BR": ("brazil", "Brazil"),
    "RU": ("russia", "Russia"), "TR": ("turkey", "Turkey"), "MX": ("mexico", "Mexico"),
    "AR": ("argentina", "Argentina"), "CL": ("chile", "Chile"), "IR": ("iran", "Iran"),
    "SA": ("saudi_arabia", "Saudi Arabia"), "ZA": ("south_africa", "South Africa"),
    "VN": ("vietnam", "Vietnam"), "TW": ("taiwan", "Taiwan"), "PH": ("philippines", "Philippines"),
    "EG": ("egypt", "Egypt"), "AE": ("united_arab_emirates", "United Arab Emirates"),
    "PK": ("pakistan", "Pakistan"), "NG": ("nigeria", "Nigeria"), "KE": ("kenya", "Kenya"),
    "BD": ("bangladesh", "Bangladesh"), "ET": ("ethiopia", "Ethiopia"), "CO": ("colombia", "Colombia"),
    "PE": ("peru", "Peru"), "VE": ("venezuela", "Venezuela"),
    # Afrikanische Länder (gekürzt für Übersichtlichkeit)
    "DZ": ("algeria", "Algeria"), "AO": ("angola", "Angola"), "GQ": ("equatorial_guinea", "Equatorial Guinea"),
    "BJ": ("benin", "Benin"), "BW": ("botswana", "Botswana"), "BF": ("burkina_faso", "Burkina Faso"),
    "BI": ("burundi", "Burundi"), "DJ": ("djibouti", "Djibouti"), "CI": ("ivory_coast", "Ivory Coast"),
    "ER": ("eritrea", "Eritrea"), "SZ": ("eswatini", "Eswatini"), "GA": ("gabon", "Gabon"),
    "GM": ("gambia", "Gambia"), "GH": ("ghana", "Ghana"), "GN": ("guinea", "Guinea"),
    "GW": ("guinea_bissau", "Guinea-Bissau"), "CM": ("cameroon", "Cameroon"), "CV": ("cape_verde", "Cape Verde"),
    "KM": ("comoros", "Comoros"), "CD": ("democratic_republic_of_the_congo", "DR Congo"),
    "CG": ("republic_of_the_congo", "Congo"), "LS": ("lesotho", "Lesotho"), "LR": ("liberia", "Liberia"),
    "LY": ("libya", "Libya"), "MG": ("madagascar", "Madagascar"), "MW": ("malawi", "Malawi"),
    "ML": ("mali", "Mali"), "MA": ("morocco", "Morocco"), "MR": ("mauritania", "Mauritania"),
    "MU": ("mauritius", "Mauritius"), "MZ": ("mozambique", "Mozambique"), "NA": ("namibia", "Namibia"),
    "NE": ("niger", "Niger"), "RW": ("rwanda", "Rwanda"), "ST": ("sao_tome_and_principe", "São Tomé and Príncipe"),
    "SN": ("senegal", "Senegal"), "SC": ("seychelles", "Seychelles"), "SL": ("sierra_leone", "Sierra Leone"),
    "ZW": ("zimbabwe", "Zimbabwe"), "SD": ("sudan", "Sudan"), "SS": ("south_sudan", "South Sudan"),
    "TZ": ("tanzania", "Tanzania"), "TG": ("togo", "Togo"), "TD": ("chad", "Chad"),
    "TN": ("tunisia", "Tunisia"), "UG": ("uganda", "Uganda"), "CF": ("central_african_republic", "Central African Republic"),
}

TE_SLUGS = {k: v[0].replace("_", "-") for k, v in COUNTRIES.items()}

YEARLY_ONLY = {"EG", "TW", "PH", "DZ", "AO", "GQ", "BJ", "BW", "BF", "BI", "DJ", "CI", "ER", "SZ",
               "GA", "GM", "GH", "GN", "GW", "CM", "CV", "KM", "CD", "CG", "LS", "LR", "LY", "MG",
               "MW", "ML", "MA", "MR", "MU", "MZ", "NA", "NE", "RW", "ST", "SN", "SC", "SL", "ZW",
               "SD", "SS", "TZ", "TG", "TD", "TN", "UG", "CF"}

MONTH_MAP = {
    "jan": "01", "feb": "02", "mar": "03", "apr": "04", "may": "05", "jun": "06",
    "jul": "07", "aug": "08", "sep": "09", "oct": "10", "nov": "11", "dec": "12"
}

# ==================== FALLBACK-DATEN (gekürzt) ====================
AFRICA_YEARLY_FALLBACK = {
    "EG": {2015:220000,2016:240000,2017:260000,2018:280000,2019:300000,2020:180000,2021:220000,2022:240000,2023:250000,2024:260000,2025:250000},
    "TW": {2015:380000,2016:390000,2017:410000,2018:430000,2019:420000,2020:380000,2021:400000,2022:410000,2023:430000,2024:440000,2025:450000},
    # Weitere Fallbacks können bei Bedarf ergänzt werden
}

def fetch_latest_te(country_code):
    slug = TE_SLUGS.get(country_code)
    if not slug:
        return None, None, None
    
    url = f"https://tradingeconomics.com/{slug}/car-registrations"
    try:
        headers = {"User-Agent": "Mozilla/5.0 (compatible; CarUpdateBot/1.0)"}
        r = requests.get(url, headers=headers, timeout=15)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "lxml")
        text = soup.get_text()

        match = re.search(r"Car Registrations.*?to\s+([\d,]+)\s*(?:Thousand|Units?)?\s+in\s+([A-Za-z]+)\s+(\d{4})", 
                         text, re.IGNORECASE | re.DOTALL)
        
        if match:
            value_str = match.group(1).replace(",", "")
            month_name = match.group(2).lower()[:3]
            year = match.group(3)
            month = MONTH_MAP.get(month_name)
            if month:
                label = f"{year}-{month}"
                value = int(value_str) * 1000 if "thousand" in text.lower() else int(value_str)
                return label, value, url
        return None, None, url
    except Exception as e:
        print(f"[{datetime.now():%H:%M:%S}] Fehler bei {country_code}: {e}")
        return None, None, None

def update_single_country(country_code):
    display_name = COUNTRIES[country_code][1]
    print(f"[{datetime.now():%H:%M:%S}] Prüfe {display_name} ...")
    
    label, value, url = fetch_latest_te(country_code)
    filename = os.path.join(DATA_DIR, f"{COUNTRIES[country_code][0]}.json")
    os.makedirs(DATA_DIR, exist_ok=True)

    if os.path.exists(filename):
        with open(filename, "r", encoding="utf-8") as f:
            data = json.load(f)
    else:
        data = {"monthly": {"labels": [], "total": []}, "last_updated": None, "source_monthly": None}

    monthly = data["monthly"]
    changed = False

    if label and value is not None and label not in monthly["labels"]:
        monthly["labels"].append(label)
        monthly["total"].append(value)
        changed = True
        print(f" → NEUER EINTRAG: {label} → {value:,} Fahrzeuge")

    if changed:
        data["last_updated"] = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
        with open(filename, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        print(f" ✓ {display_name} aktualisiert")
    else:
        print(f"   Keine neuen Daten")

def hourly_update():
    print(f"\n=== Stündliches Update gestartet um {datetime.now():%Y-%m-%d %H:%M:%S} ===")
    for code in COUNTRIES:
        update_single_country(code)
    print("=== Stündliches Update abgeschlossen ===\n")

# ==================== HAUPTSTART ====================
if __name__ == "__main__":
    print("=== Car Registration Updater v64 gestartet ===")
    print("Drücke Ctrl+C zum Beenden.\n")

    # Ordner sicherstellen
    os.makedirs(DATA_DIR, exist_ok=True)

    scheduler = BlockingScheduler(timezone="UTC")
    scheduler.add_job(hourly_update, CronTrigger(minute=0))   # jede volle Stunde

    # Sofort einmal ausführen
    hourly_update()

    print("=== Scheduler läuft jetzt im Hintergrund (jede volle Stunde) ===\n")

    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        scheduler.shutdown()
        print("\nUpdater wurde beendet.")
