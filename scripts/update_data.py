"""
scripts/update_data.py  —  v13 FINAL (ACEA PDF)
"""

import re
import sys
import urllib.request
import os
from datetime import datetime, timezone
from pathlib import Path

import pdfplumber
from bs4 import BeautifulSoup

TELEGRAM_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT  = os.environ.get("TELEGRAM_CHAT_ID", "")

REPO_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR  = REPO_ROOT / "data" / "countries"
NOW       = datetime.now(timezone.utc)

print("=" * 60)
print(f"  Car Registration Updater v13 FINAL — {NOW.strftime('%d.%m.%Y %H:%M UTC')}")
print("=" * 60)
print("✅ Alle Imports erfolgreich")

def http_get(url):
    req = urllib.request.Request(url, headers={"User-Agent": "EV-Map-Bot/13.0"})
    with urllib.request.urlopen(req, timeout=30) as r:
        return r.read()

def get_latest_acea_pdf():
    print("[ACEA] Suche neueste PDF...")
    list_url = "https://www.acea.auto/pc-registrations/"
    html = http_get(list_url).decode("utf-8")
    soup = BeautifulSoup(html, "html.parser")

    link = soup.find("a", href=re.compile(r"/pc-registrations/new-car-registrations-"))
    if not link:
        print("[ACEA] Kein Artikel gefunden")
        return None, None

    article_url = "https://www.acea.auto" + link["href"]
    article_html = http_get(article_url).decode("utf-8")

    pdf_match = re.search(r'href="(https://www\.acea\.auto/files/Press_release_car_registrations_[^"]+\.pdf)"', article_html)
    if not pdf_match:
        print("[ACEA] Kein PDF-Link gefunden")
        return None, None

    pdf_url = pdf_match.group(1)
    print(f"[ACEA] PDF gefunden: {pdf_url}")

    pdf_data = http_get(pdf_url)
    pdf_path = "/tmp/acea_latest.pdf"
    with open(pdf_path, "wb") as f:
        f.write(pdf_data)

    month_match = re.search(r"([A-Za-z]+)_(\d{4})", pdf_url)
    month_label = month_match.group(0) if month_match else NOW.strftime("%Y-%m")

    return pdf_path, month_label

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
            if not text:
                continue
            for full_name, code in country_map.items():
                match = re.search(rf"{full_name}\s*[\d\.,]+\s*([\d\.,]+)", text, re.IGNORECASE)
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
    pdf_path, month_label = get_latest_acea_pdf()
    if not pdf_path:
        print("❌ Konnte keine ACEA PDF finden")
        sys.exit(1)

    acea_data = parse_acea_pdf(pdf_path)
    if not acea_data:
        print("❌ Konnte keine Zahlen extrahieren")
        sys.exit(1)

    print(f"\n✅ Erfolgreich {len(acea_data)} Länder für Monat {month_label} geparst!")

if __name__ == "__main__":
    main()
