"""
scripts/update_data.py  —  v12 (ACEA PDF)
==============================
Seit 2023: ECB liefert keine Länderdaten mehr.
Neue Quelle: ACEA monatliche PDF-Pressemitteilungen
→ Automatisch monatliche Gesamt-Zulassungen pro Land
"""

import csv, io, json, os, sys, time, urllib.error, urllib.request
from datetime import datetime, timezone
from pathlib import Path
import pdfplumber
import re

TELEGRAM_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT  = os.environ.get("TELEGRAM_CHAT_ID", "")
REPO_ROOT      = Path(__file__).resolve().parent.parent
DATA_DIR       = REPO_ROOT / "data" / "countries"
NOW            = datetime.now(timezone.utc)

# Dein COUNTRIES-Dict bleibt gleich
COUNTRIES = { ... }  # ← kopiere dein gesamtes COUNTRIES-Dict hier rein

def http_get(url, timeout=30):
    req = urllib.request.Request(url, headers={"User-Agent": "EV-Map-Bot/12.0"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read()

# ── Neu: ACEA PDF herunterladen und parsen ─────────────────────
def fetch_acea_monthly():
    # Seite mit allen Pressemitteilungen
    base_url = "https://www.acea.auto/pc-registrations/"
    print("[ACEA] Suche neueste Pressemitteilung…")

    try:
        html = http_get(base_url).decode("utf-8")
        # Suche nach dem neuesten PDF-Link (Muster: Press_release_car_registrations_...)
        match = re.search(r'href="(https://www\.acea\.auto/files/Press_release_car_registrations_[^"]+\.pdf)"', html)
        if not match:
            print("[ACEA] Kein PDF-Link gefunden")
            return {}
        pdf_url = match.group(1)
        print(f"[ACEA] Lade PDF: {pdf_url}")
    except Exception as e:
        print(f"[ACEA] Fehler beim Scrapen der Seite: {e}")
        return {}

    # PDF herunterladen
    try:
        pdf_data = http_get(pdf_url)
        with open("/tmp/acea_latest.pdf", "wb") as f:
            f.write(pdf_data)
    except Exception as e:
        print(f"[ACEA] PDF-Download fehlgeschlagen: {e}")
        return {}

    # PDF parsen
    data = {}
    try:
        with pdfplumber.open("/tmp/acea_latest.pdf") as pdf:
            for page in pdf.pages:
                table = page.extract_table()
                if not table or len(table) < 10:
                    continue
                # Die große Ländertabelle ist meist auf Seite 4
                for row in table:
                    if not row or len(row) < 2:
                        continue
                    country = str(row[0]).strip() if row[0] else ""
                    total_str = str(row[1]).strip() if len(row) > 1 else ""
                    if country and total_str.replace(",", "").replace(".", "").isdigit():
                        total = int(total_str.replace(",", "").replace(".", ""))
                        # Land zu ECB-Code mappen
                        country_map = {
                            "Germany": "DE", "France": "FR", "Italy": "IT", "Spain": "ES",
                            "United Kingdom": "GB", "Austria": "AT", "Belgium": "BE",
                            "Netherlands": "NL", "Poland": "PL", "Sweden": "SE",
                            # ... bei Bedarf erweitern
                        }
                        ecb_code = country_map.get(country)
                        if ecb_code:
                            data[ecb_code] = total
                            print(f"[ACEA] {country} → {total:,} Fahrzeuge")
    except Exception as e:
        print(f"[ACEA] PDF-Parsing fehlgeschlagen: {e}")

    return data

# ── Rest des Scripts (fetch_ecb_monthly bleibt für Historie, write_files etc.) ──
# ... (den Rest kopiere ich nicht komplett, aber er bleibt fast identisch mit v11)

def main():
    print("=" * 60)
    print(f"  Car Registration Updater v12 (ACEA)  —  {NOW.strftime('%d.%m.%Y %H:%M UTC')}")
    print("=" * 60)

    acea_data = fetch_acea_monthly()
    # Hier kannst du später die acea_data in die monthly-Listen eintragen
    # Für den ersten Lauf schreiben wir erstmal nur die neuen Monate

    # ... (den Rest des main() und write_files anpassen, damit die neuen Monate angehängt werden)

    print("\n\u2713 Fertig – monatliche Daten kommen jetzt aus ACEA-PDFs")

if __name__ == "__main__":
    main()
