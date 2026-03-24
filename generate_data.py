#!/usr/bin/env python3
"""
Generate data.js for the Digiwin Sales Dashboard.
Reads from raw JSON exports (pulled by refresh.sh) and transforms into dashboard format.
"""
import json
import os
from datetime import datetime, timedelta

DASHBOARD_DIR = os.path.dirname(os.path.abspath(__file__))

def safe_load(filename):
    """Load JSON file, return None if not found or invalid."""
    path = os.path.join(DASHBOARD_DIR, filename)
    try:
        with open(path) as f:
            # gws output has "Using keyring backend: keyring" as first line sometimes
            content = f.read()
            # Find the first { and parse from there
            idx = content.find('{')
            if idx >= 0:
                return json.loads(content[idx:])
    except (FileNotFoundError, json.JSONDecodeError) as e:
        print(f"  WARN: Could not load {filename}: {e}")
    return None

def parse_leads(raw):
    """Parse the Leads Automations sheet into deal objects."""
    deals = []
    if not raw or 'values' not in raw:
        return deals

    rows = raw['values']
    if len(rows) < 2:
        return deals

    headers = rows[0]

    for i, row in enumerate(rows[1:], start=2):
        # Pad row to header length
        while len(row) < len(headers):
            row.append('')

        try:
            deal = {
                'id': i,
                'company': row[4] if len(row) > 4 else '',  # Company EN
                'person': row[13] if len(row) > 13 else '',  # First Name EN
                'last_name': row[15] if len(row) > 15 else '',  # Last Name EN
                'title': row[18] if len(row) > 18 else '',  # Job Title EN
                'industry': row[11] if len(row) > 11 else '',  # Industry
                'email': row[26] if len(row) > 26 else '',  # Email Primary
                'phone': row[24] if len(row) > 24 else '',  # Phone Mobile
                'event': row[37] if len(row) > 37 else '',  # Event
                'status': row[2] if len(row) > 2 else '',  # Status
            }

            # Combine first + last name
            if deal['last_name']:
                deal['person'] = f"{deal['person']} {deal['last_name']}"

            # Skip empty rows
            if not deal['company']:
                continue

            deals.append(deal)
        except (IndexError, KeyError) as e:
            print(f"  WARN: Row {i} parse error: {e}")
            continue

    return deals

def parse_tasks(raw, list_name):
    """Parse a Google Tasks list into task objects."""
    tasks = []
    if not raw or 'items' not in raw:
        return tasks

    for item in raw['items']:
        task = {
            'title': item.get('title', ''),
            'notes': item.get('notes', ''),
            'due': item.get('due', ''),
            'status': item.get('status', ''),
            'list': list_name,
            'updated': item.get('updated', '')
        }
        tasks.append(task)

    return tasks

def generate_dashboard_data():
    """Main function: read all sources, generate data.js."""
    now = datetime.now().strftime('%Y-%m-%dT%H:%M:%S+07:00')
    sources = {}

    # Load leads
    raw_leads = safe_load('raw_leads.json')
    sources['leads'] = 'ok' if raw_leads else 'error'
    leads = parse_leads(raw_leads)

    # Load ACP
    raw_acp = safe_load('raw_acp.json')
    sources['acp'] = 'ok' if raw_acp else 'error'

    # Load tasks
    all_tasks = {}
    for list_name in ['Projects', 'Calls', 'Computer', 'Office', 'WaitingFor', 'SomedayMaybe']:
        raw = safe_load(f'raw_tasks_{list_name}.json')
        sources[f'tasks_{list_name}'] = 'ok' if raw else 'error'
        all_tasks[list_name] = parse_tasks(raw, list_name)

    # Build i_owe from Calls + Computer + Office tasks
    i_owe = []
    for list_name in ['Calls', 'Computer', 'Office']:
        for task in all_tasks.get(list_name, []):
            i_owe.append({
                'what': task['title'][:80],
                'to': '',
                'company': '',
                'due': task.get('due', '')[:10] if task.get('due') else '',
                'status': 'upcoming',
                'why': (task.get('notes', '') or '')[:100]
            })

    # Build they_owe from WaitingFor tasks
    they_owe = []
    for task in all_tasks.get('WaitingFor', []):
        they_owe.append({
            'what': task['title'][:80],
            'from': '',
            'company': '',
            'follow_up': task.get('due', '')[:10] if task.get('due') else ''
        })

    # Build simplified deals from leads (without full six_elements — those are in data.js manually)
    dashboard_deals = []
    for lead in leads[:20]:  # Top 20
        dashboard_deals.append({
            'id': lead['id'],
            'company': lead['company'],
            'person': lead['person'],
            'title': lead['title'],
            'stage': 'E',  # Default — manual override in data.js
            'days_at_stage': 0,
            'days_since_contact': 0,
            'value': None,
            'confidence': 'warm',
            'six_elements': {
                'timeline': {'status': False, 'detail': ''},
                'budget': {'status': False, 'detail': ''},
                'requirements': {'status': False, 'detail': ''},
                'decision': {'status': False, 'detail': ''},
                'competitors': {'status': False, 'detail': ''},
                'motivation': {'status': False, 'detail': ''}
            },
            'must_act': 'weak',
            'must_act_detail': '',
            'must_choose_digiwin': 'weak',
            'must_choose_detail': '',
            'next_action': '',
            'pain_points': [],
            'stakeholders': [],
            'industry': lead['industry'],
            'fit': 'MEDIUM',
            'transcript_date': '',
            'call_summary': ''
        })

    # Build hit_list from i_owe (sorted by urgency)
    hit_list = []
    for i, item in enumerate(i_owe[:7], 1):
        hit_list.append({
            'rank': i,
            'action': item['what'],
            'company': item.get('company', ''),
            'why': item.get('why', ''),
            'score': max(1, 30 - i * 3),
            'type': 'call' if 'call' in item['what'].lower() else 'email' if 'email' in item['what'].lower() or 'send' in item['what'].lower() else 'computer'
        })

    # Assemble final data
    data = {
        'generated_at': now,
        'sources': sources,
        'target': {'annual': 150000000, 'ytd_closed': 0, 'currency': 'THB'},
        'stage_weights': {'E': 0.05, 'D': 0.10, 'C2': 0.25, 'C1': 0.50, 'B': 0.75, 'A': 0.90},
        'deals': dashboard_deals,
        'i_owe': i_owe,
        'they_owe': they_owe,
        'hit_list': hit_list,
        'metrics': {
            'calls_today': len(all_tasks.get('Calls', [])),
            'contacts_added': len(leads),
            'transcripts_processed': 0,
            'emails_queued': sum(1 for t in all_tasks.get('Computer', []) if 'email' in t['title'].lower() or 'send' in t['title'].lower()),
            'deals_at_d_or_above': 0
        }
    }

    # Write data.js
    output_path = os.path.join(DASHBOARD_DIR, 'data.js')
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write('// Dashboard data — auto-generated by generate_data.py\n')
        f.write(f'// Last updated: {now}\n')
        f.write('const DASHBOARD_DATA = ')
        json.dump(data, f, indent=2, ensure_ascii=False)
        f.write(';\n')

    print(f"  Generated data.js: {len(dashboard_deals)} deals, {len(i_owe)} tasks, {len(they_owe)} waiting-for")
    print(f"  Sources: {sources}")

if __name__ == '__main__':
    print("Generating dashboard data...")
    generate_dashboard_data()
    print("Done!")
