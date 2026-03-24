#!/bin/bash
# Digiwin Sales Dashboard — Data Refresh Script
# Pulls from Google Sheets + Google Tasks via gws CLI → generates data.js
# Set up cron: */30 6-22 * * * /Users/peterlo/digiwin_automation/dashboard/refresh.sh

set -e
cd /Users/peterlo/digiwin_automation/dashboard

SHEET_ID="1eopRSWKw_SPPORmGQjqZ-pvob8AuUqnnFSOlfHYmD4c"
LOG="refresh.log"

echo "$(date '+%Y-%m-%d %H:%M:%S') Starting refresh..." >> "$LOG"

# Check gws auth
if ! gws drive about get --params '{"fields":"user"}' > /dev/null 2>&1; then
    echo "$(date) ERROR: gws auth failed" >> "$LOG"
    # Write error data.js so dashboard shows warning
    cat > data.js << 'EOF'
const DASHBOARD_DATA = {
  generated_at: "AUTH_ERROR",
  sources: { leads: "error", acp: "error", tasks: "error" },
  auth_error: true,
  deals: [], i_owe: [], they_owe: [], hit_list: [],
  target: { annual: 150000000, ytd_closed: 0, currency: "THB" },
  stage_weights: { E: 0.05, D: 0.10, C2: 0.25, C1: 0.50, B: 0.75, A: 0.90 },
  metrics: { calls_today: 0, contacts_added: 0, transcripts_processed: 0, emails_queued: 0 }
};
EOF
    exit 1
fi

# Pull Leads Automations Sheet
echo "Pulling leads sheet..."
gws sheets spreadsheets values get --params "{\"spreadsheetId\":\"$SHEET_ID\",\"range\":\"Sheet1!A:AL\"}" -o raw_leads.json 2>/dev/null || echo "$(date) WARN: leads pull failed" >> "$LOG"

# Pull ACP tab
echo "Pulling ACP tab..."
gws sheets spreadsheets values get --params "{\"spreadsheetId\":\"$SHEET_ID\",\"range\":\"ACP潛客代號新建作業底稿!A:BC\"}" -o raw_acp.json 2>/dev/null || echo "$(date) WARN: acp pull failed" >> "$LOG"

# Pull Google Tasks
TASK_LISTS=(
    "WmMzeHNMX2FlVlNrLTROcQ:Projects"
    "SW4xSmFrWHJ1ZHRMOW9aWQ:Calls"
    "UmxCODdJbW8yYlEtUE9rLQ:Computer"
    "b0ZTejZpWTJtMHJjdWxfcw:Office"
    "TVZMYV9RSWRJRmk2emhxVQ:WaitingFor"
    "bnR5U0YwbFdJZHVOenFtZw:SomedayMaybe"
)

for list_info in "${TASK_LISTS[@]}"; do
    IFS=: read list_id list_name <<< "$list_info"
    echo "Pulling tasks: $list_name..."
    gws tasks tasks list --params "{\"tasklist\":\"$list_id\",\"showCompleted\":false}" -o "raw_tasks_${list_name}.json" 2>/dev/null || echo "$(date) WARN: tasks $list_name pull failed" >> "$LOG"
done

# Generate data.js
echo "Generating data.js..."
python3 generate_data.py

echo "$(date '+%Y-%m-%d %H:%M:%S') Refresh complete" >> "$LOG"
