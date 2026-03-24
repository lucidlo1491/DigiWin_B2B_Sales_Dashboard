#!/usr/bin/env python3
"""
Generate data.js for the Digiwin Sales Dashboard.
Reads from Google Sheet tabs (Deals, Actions, Contacts) pulled by refresh.sh.
Sheet is the single source of truth → data.js is auto-generated → Dashboard displays it.
"""
import json
import os
from datetime import datetime, date

DASHBOARD_DIR = os.path.dirname(os.path.abspath(__file__))

def safe_load(filename):
    """Load JSON file, return None if not found or invalid."""
    path = os.path.join(DASHBOARD_DIR, filename)
    try:
        with open(path) as f:
            content = f.read()
            # gws output sometimes has "Using keyring backend" prefix
            idx = content.find('{')
            if idx >= 0:
                return json.loads(content[idx:])
    except (FileNotFoundError, json.JSONDecodeError) as e:
        print(f"  WARN: Could not load {filename}: {e}")
    return None

def rows_to_dicts(raw):
    """Convert Sheet values (header row + data rows) into list of dicts."""
    if not raw or 'values' not in raw:
        return []
    rows = raw['values']
    if len(rows) < 2:
        return []
    headers = rows[0]
    result = []
    for row in rows[1:]:
        while len(row) < len(headers):
            row.append('')
        result.append({h: row[i] for i, h in enumerate(headers)})
    return result

def parse_six_status(val):
    """Convert Sheet six_elements value to dashboard format."""
    v = str(val).strip().lower()
    if v in ('true', 'yes', '1', 'confirmed'):
        return True
    elif v in ('partial', 'some', '0.5'):
        return 'partial'
    return False

def build_contacts_lookup(contacts_data):
    """Build company → [contacts] lookup from Contacts tab."""
    lookup = {}
    for c in contacts_data:
        company = c.get('Company (EN)', '').strip()
        if not company:
            continue
        # Normalize company name for matching
        key = company.lower().split('/')[0].strip().split(' co')[0].strip().split(' ltd')[0].strip()
        if key not in lookup:
            lookup[key] = []

        first = c.get('First Name (EN)', '').strip()
        last = c.get('Last Name (EN)', '').strip()
        name = f"{first} {last}".strip()
        phone = c.get('Phone (Mobile)', '').strip()
        if phone and not phone.startswith('+'):
            phone = f"+{phone}"
        email = c.get('Email (Primary)', '').strip()

        if name:
            lookup[key].append({
                'name': name,
                'phone': phone or None,
                'email': email or None,
                'line': None,
                'role': c.get('Job Title (EN)', '')
            })
    return lookup

def match_contacts(company_name, contacts_lookup):
    """Find contacts for a company using fuzzy matching."""
    key = company_name.lower().split('(')[0].strip().split(' co')[0].strip().split(' ltd')[0].strip()
    # Try exact match first
    if key in contacts_lookup:
        return contacts_lookup[key]
    # Try partial match
    for k, v in contacts_lookup.items():
        if k in key or key in k:
            return v
    return []

def generate_dashboard_data():
    """Main: read Sheet tabs → generate data.js."""
    now = datetime.now().strftime('%Y-%m-%dT%H:%M:%S+07:00')
    today = date.today().isoformat()
    sources = {}

    # Load Deals tab
    raw_deals = safe_load('raw_deals_tab.json')
    sources['deals'] = 'ok' if raw_deals else 'error'
    deals_data = rows_to_dicts(raw_deals)

    # Load Actions tab
    raw_actions = safe_load('raw_actions_tab.json')
    sources['actions'] = 'ok' if raw_actions else 'error'
    actions_data = rows_to_dicts(raw_actions)

    # Load Contacts (Sheet1)
    raw_contacts = safe_load('raw_leads.json')
    sources['contacts'] = 'ok' if raw_contacts else 'error'
    contacts_data = rows_to_dicts(raw_contacts)
    contacts_lookup = build_contacts_lookup(contacts_data)

    # Load localStorage overrides for deal values (if saved locally)
    saved_values = {}
    values_path = os.path.join(DASHBOARD_DIR, 'saved_values.json')
    if os.path.exists(values_path):
        try:
            with open(values_path) as f:
                saved_values = json.load(f)
        except:
            pass

    # Build deals
    dashboard_deals = []
    for d in deals_data:
        deal_id = int(d.get('ID', 0)) if d.get('ID', '').isdigit() else 0
        if not deal_id:
            continue

        value_str = d.get('Value (THB)', '').strip()
        value = float(value_str) if value_str else None
        # Override from saved values
        if str(deal_id) in saved_values and saved_values[str(deal_id)]:
            value = saved_values[str(deal_id)]

        company = d.get('Company', '').strip()
        stage = d.get('Stage', 'E').strip()
        days_str = d.get('Days at Stage', '0').strip()
        days_at_stage = int(days_str) if days_str.isdigit() else 0

        # Build six_elements from individual columns
        six_elements = {
            'timeline': {'status': parse_six_status(d.get('Timeline', '')), 'detail': ''},
            'budget': {'status': parse_six_status(d.get('Budget', '')), 'detail': ''},
            'requirements': {'status': parse_six_status(d.get('Requirements', '')), 'detail': ''},
            'decision': {'status': parse_six_status(d.get('Decision', '')), 'detail': ''},
            'competitors': {'status': parse_six_status(d.get('Competitors', '')), 'detail': ''},
            'motivation': {'status': parse_six_status(d.get('Motivation', '')), 'detail': ''},
        }

        # Match contacts from Contacts tab
        matched_contacts = match_contacts(company, contacts_lookup)

        # Build stakeholders from contacts
        stakeholders = []
        for c in matched_contacts:
            stakeholders.append({
                'name': c['name'],
                'role': c['role'],
                'type': '聯絡窗口',
                'notes': f"Email: {c['email']}" if c['email'] else ''
            })

        deal = {
            'id': deal_id,
            'company': company,
            'person': d.get('Person', '').strip(),
            'title': d.get('Title', '').strip(),
            'stage': stage,
            'days_at_stage': days_at_stage,
            'days_since_contact': 0,
            'value': value,
            'confidence': d.get('Confidence', 'warm').strip(),
            'six_elements': six_elements,
            'must_act': d.get('Must Act', 'weak').strip(),
            'must_act_detail': d.get('Must Act Detail', '').strip(),
            'must_choose_digiwin': d.get('Must Choose DW', 'weak').strip(),
            'must_choose_detail': d.get('Must Choose Detail', '').strip(),
            'next_action': d.get('Next Action', '').strip(),
            'pain_points': [],
            'stakeholders': stakeholders,
            'contacts': matched_contacts if matched_contacts else [],
            'industry': d.get('Industry', '').strip(),
            'fit': d.get('Fit', 'MEDIUM').strip(),
            'transcript_date': '',
            'call_summary': d.get('Call Summary', '').strip()
        }
        dashboard_deals.append(deal)

    # Build I Owe and They Owe from Actions tab
    i_owe = []
    they_owe = []
    for a in actions_data:
        action_type = a.get('Type', '').strip()
        status = a.get('Status', '').strip()
        due = a.get('Due', '').strip()

        # Auto-calculate status based on date
        if status not in ('done',):
            if due and due < today:
                status = 'overdue'
            elif due == today:
                status = 'due_today'
            elif not status or status == 'pending':
                status = 'upcoming'

        if action_type == 'I Owe':
            i_owe.append({
                'what': a.get('What', '').strip(),
                'to': a.get('Who', '').strip(),
                'company': a.get('Company', '').strip(),
                'due': due,
                'status': status,
                'why': a.get('Why', '').strip()
            })
        elif action_type == 'They Owe':
            they_owe.append({
                'what': a.get('What', '').strip(),
                'from': a.get('Who', '').strip(),
                'company': a.get('Company', '').strip(),
                'follow_up': due
            })

    # Build hit_list from I Owe (not done, sorted by urgency)
    active_i_owe = [x for x in i_owe if x['status'] != 'done']
    status_order = {'overdue': 0, 'due_today': 1, 'upcoming': 2}
    active_i_owe.sort(key=lambda x: status_order.get(x['status'], 9))

    hit_list = []
    for i, item in enumerate(active_i_owe[:7], 1):
        what_lower = item['what'].lower()
        action_type = 'call' if 'call' in what_lower or 'phone' in what_lower else \
                      'email' if 'email' in what_lower or 'send' in what_lower else 'computer'
        hit_list.append({
            'rank': i,
            'action': item['what'],
            'company': item['company'],
            'why': item['why'],
            'score': max(1, 30 - i * 3),
            'type': action_type
        })

    # Metrics
    deals_at_d_plus = sum(1 for d in dashboard_deals if d['stage'] in ('D', 'C2', 'C1', 'B', 'A'))
    total_pipeline = sum(d['value'] or 0 for d in dashboard_deals)

    data = {
        'generated_at': now,
        'sources': sources,
        'target': {'annual': 150000000, 'ytd_closed': 0, 'currency': 'THB'},
        'stage_weights': {'E': 0.05, 'D': 0.10, 'C2': 0.25, 'C1': 0.50, 'B': 0.75, 'A': 0.90},
        'stage_sla': {'E': 14, 'D': 30, 'C2': 30, 'C1': 45, 'B': 30, 'A': 14},
        'deals': dashboard_deals,
        'i_owe': i_owe,
        'they_owe': they_owe,
        'hit_list': hit_list,
        'metrics': {
            'calls_today': 0,
            'contacts_added': len(contacts_data),
            'transcripts_processed': 0,
            'emails_queued': sum(1 for x in active_i_owe if 'email' in x['what'].lower() or 'send' in x['what'].lower()),
            'deals_at_d_or_above': deals_at_d_plus,
            'pipeline_total': total_pipeline if total_pipeline > 0 else None,
            'weighted_total': None
        }
    }

    # Write data.js
    output_path = os.path.join(DASHBOARD_DIR, 'data.js')
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write('// Dashboard data — auto-generated by generate_data.py\n')
        f.write(f'// Last updated: {now}\n')
        f.write('// Source: Google Sheet (Deals + Actions + Contacts tabs)\n')
        f.write('const DASHBOARD_DATA = ')
        json.dump(data, f, indent=2, ensure_ascii=False)
        f.write(';\n')

    print(f"  Generated data.js: {len(dashboard_deals)} deals, {len(i_owe)} I Owe, {len(they_owe)} They Owe, {len(hit_list)} hit list")
    print(f"  Sources: {sources}")

if __name__ == '__main__':
    print("Generating dashboard data from Google Sheet...")
    generate_dashboard_data()
    print("Done!")
