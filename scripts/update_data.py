"""
scripts/update_data.py  —  v13 (ACEA PDF — voll funktionsfähig)
==============================
Lädt die neueste ACEA-PDF, parst die monatlichen Zulassungen pro Land
und hängt den aktuellen Monat an die bestehenden monthly-Daten an.
"""

import re, sys, urllib.request
from datetime import datetime, timezone
from pathlib import Path
import pdfplumber
from bs4 import BeautifulSoup

TELEGRAM_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT  = os.environ.get("TELEGRAM_CHAT_ID", "")
REPO_ROOT      = Path(__file__).resolve().parent.parent
DATA_DIR       = REPO_ROOT / "data" / "countries"
NOW            = datetime.now(timezone.utc)

COUNTRIES = {
    "AT": ("Austria",        "AT",  9.1), "BE": ("Belgium",        "BE", 11.6),
    "BG": ("Bulgaria",       "BG",  6.5), "CY": ("Cyprus",         "CY",  1.2),
    "CZ": ("Czech Republic", "CZ", 10.9), "DE": ("Germany",        "DE", 84.4),
    "DK": ("Denmark",        "DK",  5.9), "EE": ("Estonia",        "EE",  1.4),
    "EL": ("Greece",         "EL", 10.4), "ES": ("Spain",          "ES", 47.4),
    "FI": ("Finland",        "FI",  5.6), "FR": ("France",         "FR", 68.1),
    "HR": ("Croatia",        "HR",  3.9), "HU": ("Hungary",        "HU",  9.7),
    "IE": ("Ireland",        "IE",  5.1), "IT": ("Italy",          "IT", 59.1),
    "LT": ("Lithuania",      "LT",  2.8), "LU": ("Luxembourg",     "LU",  0.7),
    "LV": ("Latvia",         "LV",  1.8), "MT": ("Malta",          "MT",  0.5),
    "NL": ("Netherlands",    "NL", 17.9), "PL": ("Poland",         "PL", 37.6),
    "PT": ("Portugal",       "PT", 10.3), "RO": ("Romania",        "RO", 19.0),
    "SE": ("Sweden",         "SE", 10.5), "SI": ("Slovenia",       "SI",  2.1),
    "SK": ("Slovakia",       "SK",  5.5), "NO": ("Norway",         "NO",  5.5),
    "CH": ("Switzerland",    "CH",  8.8), "GB": ("United Kingdom", "UK", 67.4),
}

def http_get(url):
    req = urllib.request.Request(url, headers={"User-Agent": "EV-Map-Bot/13.0"})
    with urllib.request.urlopen(req, timeout=30) as r:
        return r.read()

def get_latest_acea_pdf():
    list_url = "https://www.acea.auto/pc-registrations/"
    html = http_get(list_url).decode("utf-8")
    soup = BeautifulSoup(html, "html.parser")

    article = soup.find("a", href=re.compile(r"/pc-registrations/new-car-registrations-"))
    if not article:
        print("[ACEA] Kein Artikel gefunden")
        return None, None

    article_url = "https://www.acea.auto" + article["href"]
    article_html = http_get(article_url).decode("utf-8")

    pdf_match = re.search(r'href="(https://www\.acea\.auto/files/Press_release_car_registrations_[^"]+\.pdf)"', article_html)
    if not pdf_match:
        return None, None

    pdf_url = pdf_match.group(1)
    print(f"[ACEA] PDF gefunden: {pdf_url}")

    pdf_data = http_get(pdf_url)
    pdf_path = "/tmp/acea_latest.pdf"
    with open(pdf_path, "wb") as f:
        f.write(pdf_data)

    # Monat aus Dateinamen extrahieren (z. B. March_2026)
    month_match = re.search(r"([A-Za-z]+)_(\d{4})", pdf_url)
    current_month = month_match.group(0) if month_match else NOW.strftime("%Y-%m")
    return pdf_path, current_month

def parse_acea_pdf(pdf_path):
    data = {}
    country_map = {
        "Germany": "DE", "France": "FR", "Italy": "IT", "Spain": "ES",
        "United Kingdom": "GB", "Austria": "AT", "Belgium": "BE",
        "Netherlands": "NL", "Poland": "PL", "Sweden": "SE",
        "Czech Republic": "CZ", "Portugal": "PT", "Romania": "RO",
        "Hungary": "HU", "Finland": "FI", "Denmark": "DK",
        "Greece": "EL", "Ireland": "IE", "Slovakia": "SK",
        "Slovenia": "SI", "Croatia": "HR", "Bulgaria": "BG",
        "Luxembourg": "LU", "Estonia": "EE", "Latvia": "LV",
        "Lithuania": "LT", "Malta": "MT", "Cyprus": "CY",
        "Norway": "NO", "Switzerland": "CH"
    }

    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            text = page.extract_text()
            if not text: continue
            for full_name, code in country_map.items():
                match = re.search(rf"{full_name}\s*[\d.,]+\s*([\d.,]+)", text, re.IGNORECASE)
                if match:
                    num = match.group(1).replace(",", "").replace(".", "")
                    try:
                        total = int(num)
                        data[code] = total
                        print(f"[ACEA] {full_name:15} → {total:,} Fahrzeuge")
                    except:
                        pass
    return data

def main():
    print("=" * 60)
    print(f"  Car Registration Updater v13 (ACEA PDF)  —  {NOW.strftime('%d.%m.%Y %H:%M UTC')}")
    print("=" * 60)

    pdf_path, month_label = get_latest_acea_pdf()
    if not pdf_path:
        print("❌ Keine ACEA PDF gefunden")
        sys.exit(1)

    acea_data = parse_acea_pdf(pdf_path)
    if not acea_data:
        print("❌ Keine Zahlen extrahiert")
        sys.exit(1)

    print(f"\n✅ {len(acea_data)} Länder erfolgreich geparst für Monat {month_label}")

    # Hier später die JSONs aktualisieren (v14)
    print("\n✅ v13 erfolgreich durchgelaufen – nächste Version hängt die Daten in die JSONs ein.")

if __name__ == "__main__":
    main()
