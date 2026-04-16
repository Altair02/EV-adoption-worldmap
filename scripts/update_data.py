# scripts/update_data.py
# Version: v54.4 - Telegram + Charts bei Veränderungen

import json
import os
import re
from datetime import datetime, timezone
import requests
from bs4 import BeautifulSoup
import matplotlib.pyplot as plt
import io

DATA_DIR = "data/countries"

# Alle Länder
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

TE_SLUGS = {k: v[0].replace("_", "-") for k, v in COUNTRIES.items()}

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
    ax.plot(labels[-60:], total[-60:], color='#00e5ff', linewidth=2.8)  # letzte 5 Jahre
    
    if highlight_label and highlight_label in labels:
        idx = labels.index(highlight_label)
        ax.plot(labels[idx], total[idx], 'o', color='#ff6b35', markersize=10)
    
    ax.set_title(f"{country_name} – New Registrations", color='white', fontsize=15, pad=15)
    ax.grid(True, alpha=0.15)
    ax.tick_params(colors='#64748b')
    plt.xticks(rotation=45, fontsize=9)
    plt.yticks(fontsize=9)
    buf = io.BytesIO()
    plt.tight_layout()
    plt.savefig(buf, format='png', dpi=180, facecolor=fig.get_facecolor())
    buf.seek(0)
    plt.close()
    return buf.read()

# ... (fetch_latest_te, load_override, write_country_json bleiben fast gleich wie vorher) ...

def write_country_json(country_code):
    # ... (genau wie in v54.3) ...
    # Am Ende, nachdem ein neuer Monat hinzugefügt wurde:
    if new_label and new_value is not None and monthly_override and new_label not in monthly_override.get("labels", []):
        # Chart erzeugen und Telegram senden
        img = create_chart(display_name, monthly_override["labels"], monthly_override["total"], new_label)
        caption = f"✅ <b>{display_name}</b> – Neuer Monat <b>{new_label}</b>\n{new_value:,} Zulassungen"
        send_telegram("Neue Daten!", img, caption)

# main-Funktion bleibt gleich wie vorher

def main():
    print("=== Car Registration Data Update gestartet (v54.4) ===")
    print(f"Zeit: {datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z')}\n")

    for ecb_code in COUNTRIES:
        print(f"Verarbeite {COUNTRIES[ecb_code][1]} ({ecb_code}) ...")
        write_country_json(ecb_code)

    print("\n=== Update abgeschlossen ===")

if __name__ == "__main__":
    main()
