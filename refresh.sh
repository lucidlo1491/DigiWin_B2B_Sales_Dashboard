#!/bin/bash
# Digiwin Sales Dashboard — Data Refresh Script
# Pulls from Google Sheet (Deals + Actions + Contacts) → generates data.js → pushes to GitHub
# Cron: */30 6-22 * * * /Users/peterlo/digiwin_automation/dashboard/refresh.sh

set -e
cd /Users/peterlo/digiwin_automation/dashboard

SHEET_ID="1eopRSWKw_SPPORmGQjqZ-pvob8AuUqnnFSOlfHYmD4c"
LOG="refresh.log"

echo "$(date '+%Y-%m-%d %H:%M:%S') Starting refresh..." >> "$LOG"

# Check gws auth
if ! gws drive about get --params '{"fields":"user"}' > /dev/null 2>&1; then
    echo "$(date) ERROR: gws auth failed" >> "$LOG"
    exit 1
fi

# Pull Contacts tab (Sheet1)
echo "Pulling contacts..."
gws sheets spreadsheets values get --params "{\"spreadsheetId\":\"$SHEET_ID\",\"range\":\"Sheet1!A:AL\"}" 2>/dev/null > raw_leads.json || echo "$(date) WARN: contacts pull failed" >> "$LOG"

# Pull Deals tab
echo "Pulling deals..."
gws sheets spreadsheets values get --params "{\"spreadsheetId\":\"$SHEET_ID\",\"range\":\"Deals!A:V\"}" 2>/dev/null > raw_deals_tab.json || echo "$(date) WARN: deals pull failed" >> "$LOG"

# Pull Actions tab
echo "Pulling actions..."
gws sheets spreadsheets values get --params "{\"spreadsheetId\":\"$SHEET_ID\",\"range\":\"Actions!A:H\"}" 2>/dev/null > raw_actions_tab.json || echo "$(date) WARN: actions pull failed" >> "$LOG"

# Pull ACP tab
echo "Pulling ACP..."
gws sheets spreadsheets values get --params "{\"spreadsheetId\":\"$SHEET_ID\",\"range\":\"ACP潛客代號新建作業底稿!A:BC\"}" 2>/dev/null > raw_acp.json || echo "$(date) WARN: acp pull failed" >> "$LOG"

# Generate data.js
echo "Generating data.js..."
python3 generate_data.py

# Push to GitHub if data.js changed
if git diff --quiet data.js 2>/dev/null; then
    echo "$(date '+%Y-%m-%d %H:%M:%S') No changes to push" >> "$LOG"
else
    echo "Pushing to GitHub..."
    git add data.js
    git commit -m "Auto-refresh: $(date '+%Y-%m-%d %H:%M')" --no-verify 2>/dev/null
    git push 2>/dev/null
    echo "$(date '+%Y-%m-%d %H:%M:%S') Pushed to GitHub" >> "$LOG"
fi

echo "$(date '+%Y-%m-%d %H:%M:%S') Refresh complete" >> "$LOG"
