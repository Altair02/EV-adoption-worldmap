"""
scripts/update_data.py  —  v12 (ACEA PDF)
==============================
Seit 2023: ECB liefert keine Länderdaten mehr.
Neue Quelle: ACEA monatliche PDF-Pressemitteilungen
→ Automatisch aktuelle monatliche Zulassungen pro Land
"""

import json, os, sys, time, urllib.request, re
from datetime import datetime, timezone
from pathlib import Path
import pdfplumber

TELEGRAM_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT  = os.environ.get("TELEGRAM_CHAT_ID", "")
REPO_ROOT      = Path(__file__).resolve().parent.parent
DATA_DIR       = REPO_ROOT / "data" / "countries"
NOW            = datetime.now(timezone.utc)

COUNTRIES = {
    "AT": ("Austria",        "AT",  9.1),
    "BE": ("Belgium",        "BE", 11.6),
    "BG": ("Bulgaria",       "BG",  6.5),
    "CY": ("Cyprus",         "CY",  1.2),
    "CZ": ("Czech Republic", "CZ", 10.9),
    "DE": ("Germany",        "DE", 84.4),
    "DK": ("Denmark",        "DK",  5.9),
    "EE": ("Estonia",        "EE",  1.4),
    "EL": ("Greece",         "EL", 10.4),
    "ES": ("Spain",          "ES", 47.4),
    "FI": ("Finland",        "FI",  5.6),
    "FR": ("France",         "FR", 68.1),
    "HR": ("Croatia",        "HR",  3.9),
    "HU": ("Hungary",        "HU",  9.7),
    "IE": ("Ireland",        "IE",  5.1),
    "IT": ("Italy",          "IT", 59.1),
    "LT": ("Lithuania",      "LT",  2.8),
    "LU": ("Luxembourg",     "LU",  0.7),
    "LV": ("Latvia",         "LV",  1.8),
    "MT": ("Malta",          "MT",  0.5),
    "NL": ("Netherlands",    "NL", 17.9),
    "PL": ("Poland",         "PL", 37.6),
    "PT": ("Portugal",       "PT", 10.3),
    "RO": ("Romania",        "RO", 19.0),
    "SE": ("Sweden",         "SE", 10.5),
    "SI": ("Slovenia",       "SI",  2.1),
    "SK": ("Slovakia",       "SK",  5.5),
    "NO": ("Norway",         "NO",  5.5),
    "CH": ("Switzerland",    "CH",  8.8),
    "GB": ("United Kingdom", "UK", 67.4),
}

def http_get(url, timeout=30):
    req = urllib.request.Request(url, headers={"User-Agent": "EV-Map-Bot/12.0"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read()

def fetch_latest_acea_pdf():
    """Findet und lädt die neueste ACEA PDF herunter"""
    base_url = "https://www.acea.auto/pc-registrations/"
    try:
        html = http_get(base_url).decode("utf-8")
        # Sucht nach dem neuesten PDF (z.B. Press_release_car_registrations_March_2026.pdf)
        match = re.search(r'href="(https://www\.acea\.auto/files/Press_release_car_registrations_[^"]+\.pdf)"', html, re.IGNORECASE)
        if match:
            pdf_url = match.group(1)
            print(f"[ACEA] Neueste PDF gefunden: {pdf_url}")
            pdf_data = http_get(pdf_url)
            pdf_path = "/tmp/acea_latest.pdf"
            with open(pdf_path, "wb") as f:
                f.write(pdf_data)
            return pdf_path
    except Exception as e:
        print(f"[ACEA] Fehler beim Finden der PDF: {e}")
    return None

def parse_acea_pdf(pdf_path):
    """Parst die ACEA PDF und gibt dict mit Land -> aktuelle Monatszulassungen zurück"""
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

    try:
        with pdfplumber.open(pdf_path) as pdf:
            for page in pdf.pages:
                text = page.extract_text()
                if not text:
                    continue
                # Suche nach Ländern und Zahlen (sehr robuste Suche)
                for country_name, ecb_code in country_map.items():
                    # Suche nach "Germany" gefolgt von einer Zahl (oft in der gleichen Zeile)
                    match = re.search(rf"{country_name}\s*[\d,]+\s*([\d,]+)", text, re.IGNORECASE)
                    if match:
                        num_str = match.group(1).replace(",", "")
                        try:
                            total = int(num_str)
                            data[ecb_code] = total
                            print(f"[ACEA] {country_name} → {total:,} Fahrzeuge (aktueller Monat)")
                        except:
                            pass
    except Exception as e:
        print(f"[ACEA] PDF-Parsing-Fehler: {e}")

    return data

# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    print("=" * 60)
    print(f"  Car Registration Updater v12 (ACEA PDF)  —  {NOW.strftime('%d.%m.%Y %H:%M UTC')}")
    print("=" * 60)

    pdf_path = fetch_latest_acea_pdf()
    if not pdf_path:
        print("❌ Konnte keine ACEA PDF finden")
        sys.exit(1)

    acea_data = parse_acea_pdf(pdf_path)
    if not acea_data:
        print("❌ Konnte keine Zahlen aus der PDF extrahieren")
        sys.exit(1)

    print(f"\n[ACEA] Erfolgreich {len(acea_data)} Länder geparst")

    # Hier könntest du später die Daten in die JSONs schreiben (aktuell nur Log)
    # Für den ersten Test reicht das schon

    print("\n✅ Fertig – monatliche Daten kommen jetzt aus der ACEA PDF")

if __name__ == "__main__":
    main()
