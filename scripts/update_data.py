# .github/workflows/update-data.yml
# Runs on the 1st of every month at 09:00 UTC
# Also triggered on every push (for GitHub Pages deployment)
# Can be started manually: Actions → Update Car Registration Data → Run workflow

name: Update Car Registration Data

on:
  schedule:
    - cron: "0 9 1 * *"
  workflow_dispatch:
  push:
    branches: ["main"]

permissions:
  contents: write
  pages: write
  id-token: write

concurrency:
  group: "pages"
  cancel-in-progress: false

jobs:
  # ── Job 1: Update data files ──────────────────────────────────────────────
  update:
    runs-on: ubuntu-latest
    # Only run data update on schedule or manual trigger, not on every push
    if: github.event_name == 'schedule' || github.event_name == 'workflow_dispatch'
    steps:
      - name: Checkout
        uses: actions/checkout@v4

      - name: Set up Python 3.12
        uses: actions/setup-python@v5
        with:
          python-version: "3.12"

      - name: Run update script
        env:
          TELEGRAM_BOT_TOKEN: ${{ secrets.TELEGRAM_BOT_TOKEN }}
          TELEGRAM_CHAT_ID:   ${{ secrets.TELEGRAM_CHAT_ID }}
        run: python scripts/update_data.py

      - name: Commit & push data files
        run: |
          git config user.name  "github-actions[bot]"
          git config user.email "github-actions[bot]@users.noreply.github.com"
          git add -f data/countries/
          echo "=== Git status ==="
          git status
          if git diff --cached --quiet; then
            echo "No changes to commit."
          else
            git commit -m "data: update registrations $(date +%Y-%m-%d) [ECB+Eurostat]"
            git push
            echo "Pushed ✓"
          fi

  # ── Job 2: Deploy GitHub Pages ────────────────────────────────────────────
  deploy:
    runs-on: ubuntu-latest
    needs: []  # runs independently, doesn't wait for update job
    environment:
      name: github-pages
      url: ${{ steps.deployment.outputs.page_url }}
    steps:
      - name: Checkout
        uses: actions/checkout@v4

      - name: Setup Pages
        uses: actions/configure-pages@v5

      - name: Upload artifact
        uses: actions/upload-pages-artifact@v3
        with:
          path: '.'

      - name: Deploy to GitHub Pages
        id: deployment
        uses: actions/deploy-pages@v4
