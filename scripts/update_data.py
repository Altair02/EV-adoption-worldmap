# scripts/update_data.py
# Version: v54.5 - April 2026 - Täglich + Telegram mit Charts bei neuen Monaten

import json
import os
import re
from datetime import datetime, timezone
import requests
from bs4 import BeautifulSoup
import matplotlib.pyplot as plt
import io

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
    "ID": ("indonesia", "Indonesia"), "AU": ("australia", "Australia"),
    "NZ": ("new_zealand", "New Zealand"), "JP": ("japan", "Japan"),
    "KR": ("south_korea", "South Korea"), "CN": ("china", "China"),
    "CA": ("canada", "Canada"), "US": ("united_states", "United States"),
    "BR": ("brazil", "Brazil"), "RU": ("russia", "Russia"), "TR": ("turkey", "Turkey"),
    "MX": ("mexico", "Mexico"), "AR": ("argentina", "Argentina"), "CL": ("chile", "Chile"),
    "IR": ("iran", "Iran"), "SA": ("saudi_arabia", "Saudi Arabia"), "ZA": ("south_africa", "South Africa"),
}

TE_SLUGS = {
    "AR": "argentina", "AU": "australia", "BR": "brazil", "CA": "canada", "CH": "switzerland",
    "CL": "chile", "CN": "china", "CZ": "czech-republic", "DE": "germany", "DK": "denmark",
    "ES": "spain", "FI": "finland", "FR": "france", "GB": "united-kingdom", "GR": "greece",
    "HU": "hungary", "ID": "indonesia", "IE": "ireland", "IN": "india", "IR": "iran",
    "IS": "iceland", "IT": "italy", "JP": "japan", "KR": "south-korea", "LT": "lithuania",
    "LU": "luxembourg", "LV": "latvia", "MX": "mexico", "MY": "malaysia", "NL": "netherlands",
    "NO": "norway", "NZ": "new-zealand", "PL": "poland", "PT": "portugal", "RO": "romania",
    "RU": "russia", "SA": "saudi-arabia", "SE": "sweden", "SI": "slovenia", "SK": "slovakia",
    "TH": "thailand", "TR": "turkey", "US": "united-states", "ZA": "south-africa", "AT": "austria",
    "BE": "belgium", "BG": "bulgaria", "CY": "cyprus", "EE": "estonia", "HR": "croatia", "MT": "malta",
}

# Telegram
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

def send_telegram(text, photo_bytes=None, caption=None):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/"
    if photo_bytes:
        files = {'photo': ('chart.png', photo_bytes, 'image/png')}
        data = {'chat_id': TELEGRAM_CHAT_ID, 'caption': caption or text, 'parse_mode': 'HTML'}
        requests.post(url + "sendPhoto", data=data, files=files)
    else:
        requests.post(url + "sendMessage", json={"chat_id": TELEGRAM_CHAT_ID, "text": text, "parse_mode": "HTML"})

def create_chart(country_name, labels, total, highlight_label=None):
    plt.switch_backend('Agg')
    fig, ax = plt.subplots(figsize=(10, 5.5), facecolor='#0a0e17')
    ax.set_facecolor('#111827')
    
    ax.plot(labels[-60:], total[-60:], color='#00e5ff', linewidth=2.8)
    
    if highlight_label and highlight_label in labels:
        idx = labels.index(highlight_label)
        ax.plot(labels[idx], total[idx], 'o', color='#ff6b35', markersize=10)
    
    ax.set_title(f"{country_name} – New Car Registrations", color='white', fontsize=14, pad=15)
    ax.grid(True, alpha=0.2)
    ax.tick_params(colors='#64748b')
    plt.xticks(rotation=45, fontsize=9)
    plt.yticks(fontsize=9)
    
    buf = io.BytesIO()
    plt.tight_layout()
    plt.savefig(buf, format='png', dpi=180, facecolor=fig.get_facecolor())
    buf.seek(0)
    plt.close()
    return buf.read()

def fetch_latest_te(country_code, slug):
    url = f"https://tradingeconomics.com/{slug}/car-registrations"
    headers = {"User-Agent": "Mozilla/5.0 (compatible; GitHub-Actions EV-Map)"}
    display_name = COUNTRIES[country_code][1]
    
    try:
        resp = requests.get(url, headers=headers, timeout=20)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")
        text = soup.get_text()

        match = re.search(r'Car Registrations .*?(increased|decreased) to ([\d,]+)(?:[\s]*Thousand)?[\s]*Units? in (\w+)', 
                         text, re.IGNORECASE)
        if match:
            value = int(match.group(2).replace(",", ""))
            if "Thousand" in text[match.start():match.end() + 200]:
                value *= 1000

            month_name = match.group(3).lower()
            month_map = {"january":"01","february":"02","march":"03","april":"04","may":"05","june":"06",
                         "july":"07","august":"08","september":"09","october":"10","november":"11","december":"12"}
            if month_name not in month_map:
                return None, None

            year = datetime.now().year
            date_label = f"{year}-{month_map[month_name]}"

            current_month = datetime.now(timezone.utc).strftime("%Y-%m")
            if date_label > current_month:
                return None, None

            print(f"  → {display_name} Auto-Fetch: {date_label} = {value:,} Einheiten")
            return date_label, value
    except Exception as e:
        print(f"  Warnung: {display_name} Auto-Fetch fehlgeschlagen: {e}")
    return None, None

def load_override(filename):
    path = os.path.join(DATA_DIR, filename)
    if not os.path.exists(path):
        return None, None, None
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data.get("monthly"), data.get("last_updated"), data.get("source_monthly")
    except Exception:
        return None, None, None

overrides = {k: load_override(f"{v[0]}_monthly_override.json") for k, v in COUNTRIES.items()}

def write_country_json(country_code):
    monthly_override, last_upd_override, source_override = overrides.get(country_code, (None, None, None))
    display_name = COUNTRIES[country_code][1]

    new_label, new_value = fetch_latest_te(country_code, TE_SLUGS.get(country_code))

    changed = False
    if new_label and new_value is not None:
        if monthly_override and monthly_override.get("labels"):
            if new_label not in monthly_override["labels"]:
                monthly_override["labels"].append(new_label)
                monthly_override["total"].append(new_value)
                print(f"  → {display_name}: Neuer Monat {new_label} automatisch hinzugefügt")
                changed = True
            else:
                print(f"  → {display_name}: {new_label} bereits aktuell")
        else:
            monthly_override = {"labels": [new_label], "total": [new_value]}
            changed = True

    if monthly_override and monthly_override.get("labels") and monthly_override.get("total"):
        labels = monthly_override["labels"]
        total = monthly_override["total"]
        last_updated = last_upd_override or datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
        source_monthly = source_override or "Auto-Fetch (Trading Economics)"
    else:
        labels = total = []
        last_updated = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
        source_monthly = "No data available"

    min_len = min(len(labels), len(total))
    output = {
        "monthly": {"labels": labels[:min_len], "total": total[:min_len]},
        "last_updated": last_updated,
        "source_monthly": source_monthly
    }

    filename = os.path.join(DATA_DIR, f"{COUNTRIES[country_code][0]}.json")
    os.makedirs(DATA_DIR, exist_ok=True)

    with open(filename, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2, ensure_ascii=False)

    print(f"  ✓ Geschrieben: {filename} ({len(labels)} Monate)")

    # Telegram mit Chart senden, wenn neuer Monat hinzugefügt wurde
    if changed and new_label and new_value is not None:
        try:
            img = create_chart(display_name, labels, total, new_label)
            caption = f"✅ <b>{display_name}</b>\nNeuer Monat: <b>{new_label}</b>\nZulassungen: <b>{new_value:,}</b>"
            send_telegram("Neue Zulassungszahlen", img, caption)
            print(f"  → Telegram mit Chart für {display_name} gesendet")
        except Exception as e:
            print(f"  Warnung: Chart für {display_name} konnte nicht gesendet werden: {e}")

def main():
    print("=== Car Registration Data Update gestartet (v54.5) ===")
    print(f"Zeit: {datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z')}\n")

    for ecb_code in COUNTRIES:
        print(f"Verarbeite {COUNTRIES[ecb_code][1]} ({ecb_code}) ...")
        write_country_json(ecb_code)

    print("\n=== Update abgeschlossen ===")

if __name__ == "__main__":
    main()
