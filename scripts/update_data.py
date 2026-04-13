"""
scripts/update_data.py  —  v12 (ACEA PDF)
"""

import sys
from datetime import datetime, timezone

NOW = datetime.now(timezone.utc)

print("=" * 60)
print(f"  Car Registration Updater v12 TEST  —  {NOW.strftime('%d.%m.%Y %H:%M UTC')}")
print("=" * 60)
print("✅ Python läuft")
print("✅ pdfplumber und beautifulsoup4 sollten installiert sein")

try:
    import pdfplumber
    from bs4 import BeautifulSoup
    print("✅ pdfplumber erfolgreich importiert")
    print("✅ beautifulsoup4 erfolgreich importiert")
except Exception as e:
    print(f"❌ Import-Fehler: {e}")
    sys.exit(1)

print("\n✅ Alles funktioniert! Die ACEA-PDF-Verarbeitung kann jetzt aktiviert werden.")

# Hier kommt später die echte Logik hin
print("Der volle ACEA-Parser wird in der nächsten Version (v13) aktiviert.")
