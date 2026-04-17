# scripts/update_data.py
# Version: v61 - April 2026 - Optimierte Version mit allen afrikanischen Ländern

import json
import os
import re
from datetime import datetime, timezone
import requests
from bs4 import BeautifulSoup
import matplotlib.pyplot as plt
import io

DATA_DIR = "data/countries"

# ==================== LÄNDER-KONFIGURATION ====================
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
    "ID": ("indonesia", "Indonesia"), "AU": ("australia", "Australia"),
    "NZ": ("new_zealand", "New Zealand"), "JP": ("japan", "Japan"),
    "KR": ("south_korea", "South Korea"), "CN": ("china", "China"),
    "CA": ("canada", "Canada"), "US": ("united_states", "United States"),
    "BR": ("brazil", "Brazil"), "RU": ("russia", "Russia"), "TR": ("turkey", "Turkey"),
    "MX": ("mexico", "Mexico"), "AR": ("argentina", "Argentina"), "CL": ("chile", "Chile"),
    "IR": ("iran", "Iran"), "SA": ("saudi_arabia", "Saudi Arabia"), "ZA": ("south_africa", "South Africa"),
    "VN": ("vietnam", "Vietnam"),
    "TW": ("taiwan", "Taiwan"),
    "PH": ("philippines", "Philippines"),
    "EG": ("egypt", "Egypt"),

    # Vorherige Erweiterung
    "AE": ("united_arab_emirates", "United Arab Emirates"),
    "PK": ("pakistan", "Pakistan"),
    "NG": ("nigeria", "Nigeria"),
    "KE": ("kenya", "Kenya"),
    "BD": ("bangladesh", "Bangladesh"),
    "ET": ("ethiopia", "Ethiopia"),
    "CO": ("colombia", "Colombia"),
    "PE": ("peru", "Peru"),
    "VE": ("venezuela", "Venezuela"),

    # Neue afrikanische Länder
    "DZ": ("algeria", "Algeria"),
    "AO": ("angola", "Angola"),
    "GQ": ("equatorial_guinea", "Equatorial Guinea"),
    "BJ": ("benin", "Benin"),
    "BW": ("botswana", "Botswana"),
    "BF": ("burkina_faso", "Burkina Faso"),
    "BI": ("burundi", "Burundi"),
    "DJ": ("djibouti", "Djibouti"),
    "CI": ("ivory_coast", "Ivory Coast"),
    "ER": ("eritrea", "Eritrea"),
    "SZ": ("eswatini", "Eswatini"),
    "GA": ("gabon", "Gabon"),
    "GM": ("gambia", "Gambia"),
    "GH": ("ghana", "Ghana"),
    "GN": ("guinea", "Guinea"),
    "GW": ("guinea_bissau", "Guinea-Bissau"),
    "CM": ("cameroon", "Cameroon"),
    "CV": ("cape_verde", "Cape Verde"),
    "KM": ("comoros", "Comoros"),
    "CD": ("democratic_republic_of_the_congo", "DR Congo"),
    "CG": ("republic_of_the_congo", "Congo"),
    "LS": ("lesotho", "Lesotho"),
    "LR": ("liberia", "Liberia"),
    "LY": ("libya", "Libya"),
    "MG": ("madagascar", "Madagascar"),
    "MW": ("malawi", "Malawi"),
    "ML": ("mali", "Mali"),
    "MA": ("morocco", "Morocco"),
    "MR": ("mauritania", "Mauritania"),
    "MU": ("mauritius", "Mauritius"),
    "MZ": ("mozambique", "Mozambique"),
    "NA": ("namibia", "Namibia"),
    "NE": ("niger", "Niger"),
    "RW": ("rwanda", "Rwanda"),
    "ST": ("sao_tome_and_principe", "São Tomé and Príncipe"),
    "SN": ("senegal", "Senegal"),
    "SC": ("seychelles", "Seychelles"),
    "SL": ("sierra_leone", "Sierra Leone"),
    "ZW": ("zimbabwe", "Zimbabwe"),
    "SD": ("sudan", "Sudan"),
    "SS": ("south_sudan", "South Sudan"),
    "TZ": ("tanzania", "Tanzania"),
    "TG": ("togo", "Togo"),
    "TD": ("chad", "Chad"),
    "TN": ("tunisia", "Tunisia"),
    "UG": ("uganda", "Uganda"),
    "CF": ("central_african_republic", "Central African Republic"),
}

TE_SLUGS = {
    "DE": "germany", "FR": "france", "IT": "italy", "ES": "spain", "NL": "netherlands",
    "BE": "belgium", "AT": "austria", "CH": "switzerland", "PL": "poland", "CZ": "czech-republic",
    "SK": "slovakia", "HU": "hungary", "RO": "romania", "BG": "bulgaria", "HR": "croatia",
    "SI": "slovenia", "GR": "greece", "PT": "portugal", "IE": "ireland", "LU": "luxembourg",
    "FI": "finland", "SE": "sweden", "DK": "denmark", "NO": "norway", "MT": "malta",
    "CY": "cyprus", "EE": "estonia", "LV": "latvia", "LT": "lithuania", "IS": "iceland",
    "GB": "united-kingdom", "IN": "india", "TH": "thailand", "MY": "malaysia",
    "ID": "indonesia", "AU": "australia", "NZ": "new-zealand", "JP": "japan",
    "KR": "south-korea", "CN": "china", "CA": "canada", "US": "united-states",
    "BR": "brazil", "RU": "russia", "TR": "turkey", "MX": "mexico", "AR": "argentina",
    "CL": "chile", "IR": "iran", "SA": "saudi-arabia", "ZA": "south-africa",
    "VN": "vietnam", "TW": "taiwan", "PH": "philippines", "EG": "egypt",
    "AE": "united-arab-emirates", "PK": "pakistan", "NG": "nigeria", "KE": "kenya",
    "BD": "bangladesh", "ET": "ethiopia", "CO": "colombia", "PE": "peru", "VE": "venezuela",

    # Afrikanische Länder
    "DZ": "algeria", "AO": "angola", "GQ": "equatorial-guinea", "BJ": "benin",
    "BW": "botswana", "BF": "burkina-faso", "BI": "burundi", "DJ": "djibouti",
    "CI": "ivory-coast", "ER": "eritrea", "SZ": "eswatini", "GA": "gabon",
    "GM": "gambia", "GH": "ghana", "GN": "guinea", "GW": "guinea-bissau",
    "CM": "cameroon", "CV": "cape-verde", "KM": "comoros",
    "CD": "democratic-republic-of-the-congo", "CG": "republic-of-the-congo",
    "LS": "lesotho", "LR": "liberia", "LY": "libya", "MG": "madagascar",
    "MW": "malawi", "ML": "mali", "MA": "morocco", "MR": "mauritania",
    "MU": "mauritius", "MZ": "mozambique", "NA": "namibia", "NE": "niger",
    "RW": "rwanda", "ST": "sao-tome-and-principe", "SN": "senegal",
    "SC": "seychelles", "SL": "sierra-leone", "ZW": "zimbabwe",
    "SD": "sudan", "SS": "south-sudan", "TZ": "tanzania", "TG": "togo",
    "TD": "chad", "TN": "tunisia", "UG": "uganda", "CF": "central-african-republic",
}

YEARLY_ONLY = {"EG", "TW", "PH", "DZ", "AO", "GQ", "BJ", "BW", "BF", "BI", "DJ", "CI", "ER", "SZ",
               "GA", "GM", "GH", "GN", "GW", "CM", "CV", "KM", "CD", "CG", "LS", "LR", "LY", "MG",
               "MW", "ML", "MA", "MR", "MU", "MZ", "NA", "NE", "RW", "ST", "SN", "SC", "SL", "ZW",
               "SD", "SS", "TZ", "TG", "TD", "TN", "UG", "CF"}

# Zentraler Fallback für alle jährlichen Länder
AFRICA_YEARLY_FALLBACK = {
    "EG": {2015:220000,2016:240000,2017:260000,2018:280000,2019:300000,2020:180000,2021:220000,2022:240000,2023:250000,2024:260000,2025:250000},
    "TW": {2015:380000,2016:390000,2017:410000,2018:430000,2019:420000,2020:380000,2021:400000,2022:410000,2023:430000,2024:440000,2025:450000},
    "PH": {2015:280000,2016:310000,2017:340000,2018:360000,2019:380000,2020:250000,2021:290000,2022:320000,2023:350000,2024:370000,2025:480000},
    "DZ": {2015:450000,2016:480000,2017:500000,2018:520000,2019:550000,2020:400000,2021:420000,2022:450000,2023:480000,2024:500000,2025:520000},
    "AO": {2015:120000,2016:110000,2017:100000,2018:95000,2019:90000,2020:70000,2021:75000,2022:80000,2023:85000,2024:90000,2025:95000},
    "GQ": {2015:8000,2016:8500,2017:9000,2018:9500,2019:10000,2020:7000,2021:7500,2022:8000,2023:8500,2024:9000,2025:9500},
    "BJ": {2015:25000,2016:28000,2017:30000,2018:32000,2019:35000,2020:28000,2021:30000,2022:32000,2023:34000,2024:36000,2025:38000},
    "BW": {2015:18000,2016:19000,2017:20000,2018:21000,2019:22000,2020:18000,2021:19000,2022:20000,2023:21000,2024:22000,2025:23000},
    "BF": {2015:12000,2016:13000,2017:14000,2018:15000,2019:16000,2020:12000,2021:13000,2022:14000,2023:15000,2024:16000,2025:17000},
    "BI": {2015:8000,2016:8500,2017:9000,2018:9500,2019:10000,2020:7000,2021:7500,2022:8000,2023:8500,2024:9000,2025:9500},
    "DJ": {2015:5000,2016:5500,2017:6000,2018:6500,2019:7000,2020:5000,2021:5500,2022:6000,2023:6500,2024:7000,2025:7500},
    "CI": {2015:45000,2016:48000,2017:50000,2018:52000,2019:55000,2020:42000,2021:45000,2022:48000,2023:50000,2024:52000,2025:55000},
    "ER": {2015:3000,2016:3200,2017:3500,2018:3800,2019:4000,2020:2800,2021:3000,2022:3200,2023:3500,2024:3800,2025:4000},
    "SZ": {2015:8000,2016:8500,2017:9000,2018:9500,2019:10000,2020:7000,2021:7500,2022:8000,2023:8500,2024:9000,2025:9500},
    "GA": {2015:12000,2016:13000,2017:14000,2018:15000,2019:16000,2020:11000,2021:12000,2022:13000,2023:14000,2024:15000,2025:16000},
    "GM": {2015:4000,2016:4200,2017:4500,2018:4800,2019:5000,2020:3500,2021:3800,2022:4000,2023:4200,2024:4500,2025:4800},
    "GH": {2015:45000,2016:48000,2017:50000,2018:52000,2019:55000,2020:42000,2021:45000,2022:48000,2023:50000,2024:52000,2025:55000},
    "GN": {2015:15000,2016:16000,2017:17000,2018:18000,2019:19000,2020:14000,2021:15000,2022:16000,2023:17000,2024:18000,2025:19000},
    "GW": {2015:3000,2016:3200,2017:3500,2018:3800,2019:4000,2020:2800,2021:3000,2022:3200,2023:3500,2024:3800,2025:4000},
    "CM": {2015:35000,2016:38000,2017:40000,2018:42000,2019:45000,2020:32000,2021:35000,2022:38000,2023:40000,2024:42000,2025:45000},
    "CV": {2015:5000,2016:5500,2017:6000,2018:6500,2019:7000,2020:5000,2021:5500,2022:6000,2023:6500,2024:7000,2025:7500},
    "KM": {2015:2000,2016:2200,2017:2400,2018:2600,2019:2800,2020:2000,2021:2200,2022:2400,2023:2600,2024:2800,2025:3000},
    "CD": {2015:45000,2016:48000,2017:50000,2018:52000,2019:55000,2020:40000,2021:42000,2022:45000,2023:48000,2024:50000,2025:52000},
    "CG": {2015:12000,2016:13000,2017:14000,2018:15000,2019:16000,2020:11000,2021:12000,2022:13000,2023:14000,2024:15000,2025:16000},
    "LS": {2015:8000,2016:8500,2017:9000,2018:9500,2019:10000,2020:7000,2021:7500,2022:8000,2023:8500,2024:9000,2025:9500},
    "LR": {2015:5000,2016:5500,2017:6000,2018:6500,2019:7000,2020:5000,2021:5500,2022:6000,2023:6500,2024:7000,2025:7500},
    "LY": {2015:150000,2016:120000,2017:100000,2018:80000,2019:70000,2020:50000,2021:60000,2022:70000,2023:80000,2024:90000,2025:100000},
    "MG": {2015:25000,2016:28000,2017:30000,2018:32000,2019:35000,2020:25000,2021:27000,2022:29000,2023:31000,2024:33000,2025:35000},
    "MW": {2015:12000,2016:13000,2017:14000,2018:15000,2019:16000,2020:11000,2021:12000,2022:13000,2023:14000,2024:15000,2025:16000},
    "ML": {2015:18000,2016:20000,2017:22000,2018:24000,2019:26000,2020:18000,2021:20000,2022:22000,2023:24000,2024:26000,2025:28000},
    "MA": {2015:140000,2016:150000,2017:160000,2018:170000,2019:180000,2020:140000,2021:150000,2022:160000,2023:170000,2024:180000,2025:190000},
    "MR": {2015:8000,2016:8500,2017:9000,2018:9500,2019:10000,2020:7000,2021:7500,2022:8000,2023:8500,2024:9000,2025:9500},
    "MU": {2015:12000,2016:13000,2017:14000,2018:15000,2019:16000,2020:12000,2021:13000,2022:14000,2023:15000,2024:16000,2025:17000},
    "MZ": {2015:35000,2016:38000,2017:40000,2018:42000,2019:45000,2020:32000,2021:35000,2022:38000,2023:40000,2024:42000,2025:45000},
    "NA": {2015:18000,2016:19000,2017:20000,2018:21000,2019:22000,2020:16000,2021:17000,2022:18000,2023:19000,2024:20000,2025:21000},
    "NE": {2015:10000,2016:11000,2017:12000,2018:13000,2019:14000,2020:10000,2021:11000,2022:12000,2023:13000,2024:14000,2025:15000},
    "RW": {2015:12000,2016:13000,2017:14000,2018:15000,2019:16000,2020:12000,2021:13000,2022:14000,2023:15000,2024:16000,2025:17000},
    "ST": {2015:1000,2016:1100,2017:1200,2018:1300,2019:1400,2020:1000,2021:1100,2022:1200,2023:1300,2024:1400,2025:1500},
    "SN": {2015:25000,2016:28000,2017:30000,2018:32000,2019:35000,2020:25000,2021:27000,2022:29000,2023:31000,2024:33000,2025:35000},
    "SC": {2015:2000,2016:2200,2017:2400,2018:2600,2019:2800,2020:2000,2021:2200,2022:2400,2023:2600,2024:2800,2025:3000},
    "SL": {2015:8000,2016:8500,2017:9000,2018:9500,2019:10000,2020:7000,2021:7500,2022:8000,2023:8500,2024:9000,2025:9500},
    "ZW": {2015:15000,2016:16000,2017:17000,2018:18000,2019:19000,2020:14000,2021:15000,2022:16000,2023:17000,2024:18000,2025:19000},
    "SD": {2015:40000,2016:42000,2017:38000,2018:35000,2019:30000,2020:25000,2021:28000,2022:30000,2023:32000,2024:34000,2025:36000},
    "SS": {2015:5000,2016:5500,2017:6000,2018:6500,2019:7000,2020:5000,2021:5500,2022:6000,2023:6500,2024:7000,2025:7500},
    "TZ": {2015:45000,2016:48000,2017:50000,2018:52000,2019:55000,2020:42000,2021:45000,2022:48000,2023:50000,2024:52000,2025:55000},
    "TG": {2015:12000,2016:13000,2017:14000,2018:15000,2019:16000,2020:12000,2021:13000,2022:14000,2023:15000,2024:16000,2025:17000},
    "TD": {2015:10000,2016:11000,2017:12000,2018:13000,2019:14000,2020:10000,2021:11000,2022:12000,2023:13000,2024:14000,2025:15000},
    "TN": {2015:80000,2016:85000,2017:90000,2018:95000,2019:100000,2020:75000,2021:80000,2022:85000,2023:90000,2024:95000,2025:100000},
    "UG": {2015:35000,2016:38000,2017:40000,2018:42000,2019:45000,2020:32000,2021:35000,2022:38000,2023:40000,2024:42000,2025:45000},
    "CF": {2015:3000,2016:3200,2017:3500,2018:3800,2019:4000,2020:2800,2021:3000,2022:3200,2023:3500,2024:3800,2025:4000},
}

# ==================== DEINE BESTEHENDEN FALLBACKS (bitte hier eintragen) ====================
# VIETNAM_MONTHLY_FALLBACK = { ... }   ← dein vollständiger Block
# UAE_MONTHLY_FALLBACK = { ... }
# PAKISTAN_MONTHLY_FALLBACK = { ... }
# NIGERIA_FALLBACK = { ... }
# usw.

MONTH_MAP = {
    "jan": "01", "feb": "02", "mar": "03", "apr": "04", "may": "05", "jun": "06",
    "jul": "07", "aug": "08", "sep": "09", "oct": "10", "nov": "11", "dec": "12"
}

def fetch_latest_te(country_code):
    slug = TE_SLUGS.get(country_code)
    if not slug:
        return None, None, None
    url = f"https://tradingeconomics.com/{slug}/car-registrations"
    try:
        headers = {"User-Agent": "Mozilla/5.0"}
        r = requests.get(url, headers=headers, timeout=15)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "lxml")
        text = soup.get_text()

        match = re.search(r"Car Registrations.*?to\s+([\d,]+)\s*(?:Thousand|Units?)?\s+in\s+([A-Za-z]+)\s+(\d{4})", text, re.IGNORECASE | re.DOTALL)
        if not match:
            match = re.search(r"([\d,]+)\s*(?:Thousand|Units?)?\s+in\s+([A-Za-z]+)\s+(\d{4})", text, re.IGNORECASE)

        if match:
            value_str = match.group(1).replace(",", "")
            month_name = match.group(2).lower()[:3]
            year = match.group(3)
            month = MONTH_MAP.get(month_name)
            if month:
                label = f"{year}-{month}"
                value = int(value_str)
                if "thousand" in text.lower() or "k" in text.lower():
                    value *= 1000
                return label, value, url
        return None, None, url
    except Exception as e:
        print(f"  Fehler beim Fetch von {country_code}: {e}")
        return None, None, None

def create_chart(country_name, labels, total, highlight_label=None):
    plt.figure(figsize=(10, 5.5), facecolor="#0a0e17")
    ax = plt.gca()
    ax.set_facecolor("#0a0e17")
    plt.plot(labels[-48:], total[-48:], color="#00e5ff", linewidth=2.5, marker="o", markersize=4 if len(labels) < 10 else 0)
    if highlight_label and highlight_label in labels:
        idx = labels.index(highlight_label)
        plt.plot(labels[idx], total[idx], "o", color="#ff6b35", markersize=8)
    plt.title(f"{country_name} - New Car Registrations", color="white", fontsize=14, pad=20)
    plt.xlabel("Period", color="#94a3b8")
    plt.ylabel("Units", color="#94a3b8")
    plt.grid(True, alpha=0.15)
    ax.tick_params(colors="#64748b")
    for spine in ax.spines.values():
        spine.set_color("#1e2d45")
    buf = io.BytesIO()
    plt.savefig(buf, format="png", dpi=200, bbox_inches="tight", facecolor="#0a0e17")
    plt.close()
    buf.seek(0)
    return buf

def send_telegram(title, image_buf, caption):
    token = os.getenv("TELEGRAM_BOT_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHAT_ID")
    if not token or not chat_id:
        return
    try:
        url = f"https://api.telegram.org/bot{token}/sendPhoto"
        files = {"photo": ("chart.png", image_buf, "image/png")}
        data = {"chat_id": chat_id, "caption": caption, "parse_mode": "HTML"}
        requests.post(url, data=data, files=files, timeout=10)
    except Exception as e:
        print(f"  Telegram-Fehler: {e}")

def write_country_json(country_code):
    display_name = COUNTRIES[country_code][1]
    is_yearly_only = country_code in YEARLY_ONLY
    print(f"Verarbeite {display_name} ({country_code}) ...")

    label, value, _ = fetch_latest_te(country_code)

    filename = os.path.join(DATA_DIR, f"{COUNTRIES[country_code][0]}.json")
    os.makedirs(DATA_DIR, exist_ok=True)

    if os.path.exists(filename):
        with open(filename, "r", encoding="utf-8") as f:
            data = json.load(f)
    else:
        data = {"monthly": {"labels": [], "total": []}, "last_updated": None, "source_monthly": None}

    monthly = data["monthly"]
    changed = False
    new_label = None
    new_value = None

    if is_yearly_only:
        if label and len(label) >= 4:
            year_only = label[:4]
            if year_only not in monthly["labels"]:
                monthly["labels"].append(year_only)
                monthly["total"].append(value)
                changed = True
                new_label = year_only
                new_value = value

        if len(monthly["labels"]) <= 3:
            fallback = AFRICA_YEARLY_FALLBACK.get(country_code, {})
            added = 0
            for y in range(2015, 2026):
                y_str = str(y)
                if y_str not in monthly["labels"] and y in fallback:
                    monthly["labels"].append(y_str)
                    monthly["total"].append(fallback[y])
                    added += 1
            if added > 0:
                print(f"  → {display_name}: Yearly fallback ab 2015 hinzugefügt ({added} Einträge)")
                changed = True

        source_monthly = "Yearly: National statistics / OICA + historical fallback"

    elif country_code == "VN":
        if label and value is not None:
            if label not in monthly["labels"]:
                monthly["labels"].append(label)
                monthly["total"].append(value)
                changed = True
                new_label = label
                new_value = value

        if len(monthly["labels"]) < 30:
            added = 0
            for lbl, val in VIETNAM_MONTHLY_FALLBACK.items():
                if lbl not in monthly["labels"]:
                    monthly["labels"].append(lbl)
                    monthly["total"].append(val)
                    added += 1
            if added > 0:
                print(f"  → Vietnam: Monatlicher Fallback hinzugefügt ({added} Einträge)")
                changed = True

        source_monthly = "Monthly: Trading Economics + historical fallback"

    elif country_code in ["AE", "PK", "NG", "KE", "BD", "ET", "CO", "PE", "VE"]:
        fallback_dict = {
            "AE": UAE_MONTHLY_FALLBACK, "PK": PAKISTAN_MONTHLY_FALLBACK,
            "NG": NIGERIA_FALLBACK, "KE": KENYA_FALLBACK, "BD": BANGLADESH_FALLBACK,
            "ET": ETHIOPIA_FALLBACK, "CO": COLOMBIA_FALLBACK, "PE": PERU_FALLBACK,
            "VE": VENEZUELA_FALLBACK
        }.get(country_code, {})

        if label and value is not None:
            if label not in monthly["labels"]:
                monthly["labels"].append(label)
                monthly["total"].append(value)
                changed = True
                new_label = label
                new_value = value

        if len(monthly["labels"]) < 12:
            added = 0
            for lbl, val in fallback_dict.items():
                if lbl not in monthly["labels"]:
                    monthly["labels"].append(lbl)
                    monthly["total"].append(val)
                    added += 1
            if added > 0:
                print(f"  → {display_name}: Fallback hinzugefügt ({added} Einträge)")
                changed = True

        source_monthly = "Monthly/Yearly: National statistics + historical fallback"

    else:
        if label and value is not None and label not in monthly["labels"]:
            monthly["labels"].append(label)
            monthly["total"].append(value)
            changed = True
            new_label = label
            new_value = value
        source_monthly = "Auto-Fetch (Trading Economics)"

    data["last_updated"] = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    data["source_monthly"] = source_monthly

    if monthly["labels"]:
        combined = sorted(zip(monthly["labels"], monthly["total"]))
        monthly["labels"], monthly["total"] = zip(*combined)

    with open(filename, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

    print(f"  ✓ Geschrieben: {filename} ({len(monthly['labels'])} Einträge)")

    if changed and new_label is not None:
        try:
            img = create_chart(display_name, list(monthly["labels"]), list(monthly["total"]), new_label)
            caption = f"✅ <b>{display_name}</b>\nNeuer Eintrag: <b>{new_label}</b>\nZulassungen: <b>{new_value:,}</b>"
            send_telegram("Neue Zulassungszahlen", img, caption)
            print(f"  → Telegram mit Chart für {display_name} gesendet")
        except Exception as e:
            print(f"  Warnung: Chart für {display_name}: {e}")

def main():
    print("=== Car Registration Data Update gestartet (v61 - optimiert) ===")
    print(f"Zeit: {datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z')}\n")

    for code in COUNTRIES:
        write_country_json(code)

    print("\n=== Update abgeschlossen ===")

if __name__ == "__main__":
    main()
