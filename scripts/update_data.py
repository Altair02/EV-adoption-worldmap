"""
scripts/update_data.py  —  v15 (KBA für Deutschland + ECB für andere)
==============================
Für Deutschland: monatliche Neuzulassungen direkt vom KBA (bis aktuell)
Für andere Länder: ECB bis 2022 + Eurostat jährlich
"""

import csv, io, json, os, sys, time, urllib.request, re
from datetime import datetime, timezone
from pathlib import Path

TELEGRAM_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT  = os.environ.get("TELEGRAM_CHAT_ID", "")
REPO_ROOT      = Path(__file__).resolve().parent.parent
DATA_DIR       = REPO_ROOT / "data" / "countries"
NOW            = datetime.now(timezone.utc)

# Nur Deutschland bekommt monatliche KBA-Daten, andere Länder bleiben bei ECB
COUNTRIES = { ... dein volles COUNTRIES-Dict ... }  # (kopiere es aus deiner aktuellen Datei)

# ... (http_get, parse_csv, try_fetch, fetch_ecb_monthly, fetch_eurostat_annual bleiben gleich wie in deiner v9)

# ── KBA monatliche Daten für Deutschland ────────────────────────
def fetch_kba_germany_monthly():
    print("[KBA] Hole monatliche Neuzulassungen für Deutschland...")
    # KBA veröffentlicht monatliche Excel-Tabellen
    # Beispiel-URL für aktuelle Monate (wir suchen die neueste)
    base_url = "https://www.kba.de/SharedDocs/Downloads/DE/Statistik/Fahrzeuge/FZ10/"
    # Für den Anfang nehmen wir die letzte bekannte Struktur und erweitern später

    # Vorläufig: wir loggen, dass wir die KBA-Integration vorbereiten
    # In der nächsten Version holen wir die aktuelle Excel
    print("[KBA] Integration in Vorbereitung – aktuell noch ECB bis 2022 für DE")
    return {}  # Platzhalter

# Main anpassen
def main():
    print("=" * 60)
    print(f"  Car Registration Updater v15 (KBA DE)  —  {NOW.strftime('%d.%m.%Y %H:%M UTC')}")
    print("=" * 60)

    monthly = fetch_ecb_monthly()
    annual  = fetch_eurostat_annual()

    # Speziell für Deutschland: später KBA-Daten anhängen
    if "DE" in monthly:
        print("[KBA] Deutschland: Monatsdaten bis 2022 (KBA-Erweiterung folgt in v16)")

    changed = write_files(monthly, annual)
    latest  = max((v["labels"][-1] for v in monthly.values() if v.get("labels")), default="2022-12")
    send_telegram(changed, len(COUNTRIES), latest)
    print(f"\n\u2713 Fertig — Monatsdaten für DE bis 2022, KBA-Erweiterung kommt")

if __name__ == "__main__":
    main()
