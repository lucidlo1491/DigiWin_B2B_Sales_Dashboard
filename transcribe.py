#!/usr/bin/env python3.13
"""
Digiwin Transcription Pipeline — transcribe.py
Single Gemini 3.1 Pro call with timestamps at speaker changes.
Supports Thai, Chinese (Mandarin), English, and mixed-language recordings.

Usage:
  python3 transcribe.py audio.m4a --company "BFC"
  python3 transcribe.py audio.m4a --language chinese --speakers "Eddie:Trainer,Peter:Sales"
  python3 transcribe.py ~/inbox/*.m4a --company "Digiwin Internal"
  python3 transcribe.py audio.m4a --language thai  # unknown speakers → Speaker A/B/C
"""

import argparse
import atexit
import json
import os
import re
import subprocess
import sys
import time
from pathlib import Path
from datetime import datetime

# Force unbuffered output so background runs show progress
os.environ["PYTHONUNBUFFERED"] = "1"

def log(msg):
    """Print with flush — always visible, even in background."""
    print(msg, flush=True)

LOCKFILE = os.path.expanduser("~/.transcribe/.lock")

# ─── Config ───────────────────────────────────────────────────────────────────

API_KEY = "AIzaSyA7hUTVdmhmzgNCZJ-O1pgJYZXrA1dnMLY"
MODEL = "gemini-3.1-pro-preview"
TEMPERATURE = 0.1  # Not 0.0 — allows multilingual code-switching
MAX_OUTPUT_TOKENS = 65536
MAX_SINGLE_CALL_MINUTES = 60  # Chunk if longer (Chinese is token-heavy)
CHUNK_MINUTES = 25  # Each chunk size when splitting
CHUNK_OVERLAP_SECONDS = 60  # Overlap between chunks

CONTACTS_PATH = os.path.expanduser("~/.transcribe/contacts.json")
COST_LOG_PATH = os.path.expanduser("~/.transcribe/cost_log.json")
UPLOAD_CACHE_PATH = os.path.expanduser("~/.transcribe/upload_cache.json")

DEFAULT_SPEAKER = "Peter Lo"


# ─── Lockfile (prevent multiple instances) ────────────────────────────────────

def acquire_lock():
    """Prevent multiple transcribe.py from running simultaneously."""
    os.makedirs(os.path.dirname(LOCKFILE), exist_ok=True)
    if os.path.exists(LOCKFILE):
        try:
            with open(LOCKFILE) as f:
                pid = int(f.read().strip())
            # Check if that process is still alive
            os.kill(pid, 0)
            log(f"  ERROR: Another transcribe.py is already running (PID {pid})")
            log(f"  If this is stale, delete {LOCKFILE}")
            sys.exit(1)
        except (ProcessLookupError, ValueError):
            pass  # Stale lock, proceed

    with open(LOCKFILE, "w") as f:
        f.write(str(os.getpid()))


def release_lock():
    """Remove lockfile on exit."""
    try:
        os.unlink(LOCKFILE)
    except:
        pass

# ─── Contacts ─────────────────────────────────────────────────────────────────

def load_contacts():
    """Load contacts from ~/.transcribe/contacts.json"""
    if os.path.exists(CONTACTS_PATH):
        with open(CONTACTS_PATH) as f:
            return json.load(f)
    return {"default_speaker": DEFAULT_SPEAKER, "companies": {}}


def get_speakers(args, contacts):
    """Resolve speakers from --company, --speakers, or default."""
    default = contacts.get("default_speaker", DEFAULT_SPEAKER)

    if args.speakers:
        # Manual: "Xiaoping:Trainer,Peter:Sales"
        speakers = [default]
        for s in args.speakers.split(","):
            parts = s.strip().split(":")
            name = parts[0].strip()
            if name.lower() != default.lower():
                speakers.append(s.strip())
            else:
                speakers[0] = s.strip()
        return speakers

    if args.company:
        company_speakers = contacts.get("companies", {}).get(args.company, [])
        if company_speakers:
            return [default] + company_speakers

    # Unknown speakers
    return None


def format_speakers_for_prompt(speakers):
    """Format speaker list for the transcription prompt."""
    if not speakers:
        return "Speakers are unknown. Label them as Speaker A, Speaker B, Speaker C, etc. based on voice characteristics."

    lines = []
    for s in speakers:
        if ":" in s:
            name, role = s.split(":", 1)
            lines.append(f"- {name.strip()} ({role.strip()})")
        else:
            lines.append(f"- {s.strip()}")
    return "Speakers:\n" + "\n".join(lines)


# ─── Audio Analysis ───────────────────────────────────────────────────────────

def get_audio_duration(filepath):
    """Get audio duration in seconds using ffprobe."""
    try:
        result = subprocess.run(
            ["ffprobe", "-v", "quiet", "-show_entries", "format=duration",
             "-of", "default=noprint_wrappers=1:nokey=1", filepath],
            capture_output=True, text=True, timeout=30
        )
        return float(result.stdout.strip())
    except Exception as e:
        log(f"  WARN: Could not get duration: {e}")
        return None


def preprocess_audio(filepath):
    """Convert to mono 64kbps m4a for optimal upload size."""
    output = filepath.replace(".m4a", "_processed.m4a").replace(".mp3", "_processed.m4a")
    if output == filepath:
        output = filepath + "_processed.m4a"

    try:
        subprocess.run(
            ["ffmpeg", "-i", filepath, "-ac", "1", "-b:a", "64k", "-y", output],
            capture_output=True, timeout=300
        )
        if os.path.exists(output) and os.path.getsize(output) > 0:
            return output
    except Exception:
        pass
    return filepath  # Fallback to original


def split_audio(filepath, duration_sec):
    """Split audio into chunks with overlap."""
    chunks = []
    chunk_sec = CHUNK_MINUTES * 60
    overlap = CHUNK_OVERLAP_SECONDS
    offset = 0
    i = 1

    while offset < duration_sec:
        chunk_path = filepath.replace(".m4a", f"_chunk{i}.m4a")
        end = min(offset + chunk_sec + overlap, duration_sec)

        subprocess.run(
            ["ffmpeg", "-i", filepath, "-ss", str(offset), "-t", str(end - offset),
             "-c", "copy", "-y", chunk_path],
            capture_output=True, timeout=120
        )
        chunks.append({"path": chunk_path, "offset_sec": offset, "index": i})
        offset += chunk_sec
        i += 1

    return chunks


# ─── Gemini API ───────────────────────────────────────────────────────────────

def load_upload_cache():
    """Load cached file URIs to avoid re-uploading."""
    if os.path.exists(UPLOAD_CACHE_PATH):
        try:
            with open(UPLOAD_CACHE_PATH) as f:
                return json.load(f)
        except:
            pass
    return {}


def save_upload_cache(cache):
    """Save upload cache."""
    os.makedirs(os.path.dirname(UPLOAD_CACHE_PATH), exist_ok=True)
    with open(UPLOAD_CACHE_PATH, "w") as f:
        json.dump(cache, f, indent=2)


def upload_to_gemini(filepath):
    """Upload audio file to Gemini File API with caching."""
    from google import genai
    from google.genai import types

    # Check cache — skip upload if same file was already uploaded
    file_size = os.path.getsize(filepath)
    cache_key = f"{Path(filepath).name}_{file_size}"
    cache = load_upload_cache()

    if cache_key in cache:
        cached = cache[cache_key]
        client = genai.Client(api_key=API_KEY)
        try:
            # Verify the cached file still exists on Gemini
            status = client.files.get(name=cached["name"])
            if status.state.name == "ACTIVE":
                log(f"  Using cached upload: {cached['uri']} (skipping re-upload)")
                return status
        except:
            pass  # Cache stale, re-upload

    client = genai.Client(api_key=API_KEY)
    log(f"  Uploading {Path(filepath).name} ({file_size / 1e6:.1f}MB)...")

    uploaded = client.files.upload(
        file=filepath,
        config=types.UploadFileConfig(mime_type="audio/mp4")
    )

    # Wait for ACTIVE state
    for _ in range(30):
        status = client.files.get(name=uploaded.name)
        if status.state.name == "ACTIVE":
            log(f"  Upload complete: {uploaded.uri}")
            # Cache it
            cache[cache_key] = {"uri": uploaded.uri, "name": uploaded.name, "timestamp": datetime.now().isoformat()}
            save_upload_cache(cache)
            return uploaded
        time.sleep(2)

    raise RuntimeError(f"File upload did not become ACTIVE: {uploaded.name}")


def build_prompt(language, speakers_text, duration_min, offset_min=0):
    """Build the transcription prompt."""
    lang_instruction = {
        "thai": "This audio is primarily in Thai with English technical terms (ERP, MES, AGV, WMS, IoT, SFT, APS, QC, BOI). Transcribe Thai in Thai script, English in Roman script. Do not translate.",
        "chinese": "This audio is primarily in Mandarin Chinese (中文) with English technical terms. Use Traditional Chinese characters (繁體中文). Do not translate English terms.",
        "english": "This audio is in English with possible Thai or Chinese terms. Preserve all languages as spoken.",
        "mixed": "This audio contains multiple languages (Thai, Chinese, English). Transcribe each language in its original script. Do not translate. When speakers switch languages, continue in the language spoken."
    }.get(language, "Transcribe in the original language spoken. Do not translate.")

    offset_note = ""
    if offset_min > 0:
        offset_note = f"\n\nIMPORTANT: This is a segment starting at {offset_min:.0f} minutes into the full recording. Add {offset_min:.0f} minutes to all timestamps. The first timestamp should be approximately [{int(offset_min):02d}:00:00]."

    return f"""Transcribe this entire {duration_min:.0f}-minute audio recording verbatim from start to finish.

Timestamp rules:
- Add [HH:MM:SS] ONLY when the speaker changes or after a significant pause
- Do NOT add timestamps every few seconds — only at speaker turns or pauses
- If one speaker talks for more than 3 minutes continuously, insert a timestamp anyway for navigation
- Each speaker turn gets ONE timestamp at the start

{speakers_text}

{lang_instruction}

Format:
[HH:MM:SS] **Speaker Name:** their complete utterance until the next person speaks

Rules:
- Transcribe the ENTIRE recording from beginning to end
- Do NOT stop early or summarize
- Do NOT create repeated/looping text — if the conversation ends, stop transcribing
- If audio is unintelligible, write [inaudible]
- Preserve filler words and natural speech patterns{offset_note}"""


def transcribe_audio(uploaded_file, prompt):
    """Call Gemini 3.1 Pro to transcribe."""
    from google import genai
    from google.genai import types

    client = genai.Client(api_key=API_KEY)

    log(f"  Transcribing with {MODEL}...")
    start = time.time()

    response = client.models.generate_content(
        model=MODEL,
        contents=[
            types.Part.from_uri(file_uri=uploaded_file.uri, mime_type="audio/mp4"),
            prompt
        ],
        config=types.GenerateContentConfig(
            temperature=TEMPERATURE,
            max_output_tokens=MAX_OUTPUT_TOKENS
        )
    )

    elapsed = time.time() - start
    text = response.text
    finish_reason = response.candidates[0].finish_reason.name if response.candidates else "UNKNOWN"

    # Token usage
    usage = {}
    if hasattr(response, 'usage_metadata') and response.usage_metadata:
        usage = {
            "input_tokens": getattr(response.usage_metadata, 'prompt_token_count', 0),
            "output_tokens": getattr(response.usage_metadata, 'candidates_token_count', 0),
        }

    log(f"  Done in {elapsed:.0f}s | {len(text):,} chars | Finish: {finish_reason}")
    if usage:
        log(f"  Tokens — in: {usage.get('input_tokens', '?'):,} | out: {usage.get('output_tokens', '?'):,}")

    return text, finish_reason, elapsed, usage


# ─── QA Validation ────────────────────────────────────────────────────────────

def validate_transcript(text, audio_duration_sec):
    """Run QA checks on the transcript. Returns (passed, issues)."""
    issues = []
    lines = [l.strip() for l in text.strip().split("\n") if l.strip()]

    # 1. Timestamp monotonicity
    timestamps = re.findall(r'\[(\d{1,2}):(\d{2}):(\d{2})\]', text)
    ts_seconds = [int(h) * 3600 + int(m) * 60 + int(s) for h, m, s in timestamps]

    frozen_count = {}
    for ts in ts_seconds:
        frozen_count[ts] = frozen_count.get(ts, 0) + 1
    max_frozen = max(frozen_count.values()) if frozen_count else 0
    if max_frozen > 5:
        issues.append(f"HALLUCINATION: Timestamp frozen {max_frozen} times")

    # Check monotonicity
    non_monotonic = sum(1 for i in range(1, len(ts_seconds)) if ts_seconds[i] < ts_seconds[i - 1])
    if non_monotonic > 2:
        issues.append(f"Timestamps not monotonic: {non_monotonic} reversals")

    # 2. Repetition detection
    seen_blocks = {}
    for i in range(len(lines) - 2):
        block = "\n".join(lines[i:i + 3])
        if len(block) > 50:
            seen_blocks[block] = seen_blocks.get(block, 0) + 1
    max_repeat = max(seen_blocks.values()) if seen_blocks else 0
    if max_repeat > 2:
        issues.append(f"HALLUCINATION: 3-line block repeated {max_repeat} times")

    # 3. Unique line ratio
    unique_ratio = len(set(lines)) / max(len(lines), 1)
    if unique_ratio < 0.7:
        issues.append(f"Low uniqueness: {unique_ratio:.0%} (threshold: 70%)")

    # 4. Coverage check
    if audio_duration_sec and ts_seconds:
        last_ts = max(ts_seconds)
        gap = audio_duration_sec - last_ts
        if gap > 120:
            issues.append(f"COVERAGE GAP: Last timestamp at {last_ts // 60}:{last_ts % 60:02d}, audio is {audio_duration_sec // 60:.0f}:{int(audio_duration_sec) % 60:02d} ({gap:.0f}s missing)")

    # 5. Domain glossary — catch common Gemini mishearings of ERP/MES/manufacturing terms
    KNOWN_TERMS = {
        # Correct term → common Gemini errors
        "Fixed asset": ["Free asset", "Freeze asset", "Fix asset"],
        "Fixed cost": ["Free cost", "Freeze cost"],
        "Bill of Materials": ["Bill of Material", "Bill of Minerals"],
        "BOM": ["BOMB", "BOOM"],
        "Work order": ["Walk order", "Word order"],
        "Subcontract": ["Sub contract", "Subtract"],
        "Production line": ["Production lying", "Production lion"],
        "Quality control": ["Quality controlled", "Quality controller"],
        "Inventory": ["In ventory", "Inventor"],
        "Warehouse": ["War house", "Wear house"],
        "Dispatch": ["This patch", "Dis patch"],
        "IoT": ["I OT", "IOT", "I.O.T"],
        "eMES": ["E-MES", "EMES", "e mess", "E mess"],
        "ERP": ["E.R.P", "EAP"],
        "WMS": ["W.M.S", "WMF"],
        "AGV": ["A.G.V", "ABV", "AGB"],
        "SFT": ["S.F.T", "SFC"],
        "APS": ["A.P.S", "APC"],
        "ROI": ["R.O.I", "ROY"],
        "KPI": ["K.P.I", "KBI"],
        "Pallet": ["Pellet", "Palette"],
        "Conveyor": ["Convey", "Con vayer"],
        "Barcode": ["Bar code", "Bark code"],
        "Throughput": ["Through put", "Threw put"],
        "Downtime": ["Down time", "Dawn time"],
        "Scrap": ["Scrub", "Strap"],
        "Rework": ["Re work", "Reward"],
        "Batch": ["Badge", "Bash"],
        "Forklift": ["Fork lift", "Four clip"],
        "BOI": ["B.O.I", "BOY", "Boy"],
        "EPE": ["E.P.E", "EPP"],
    }

    term_warnings = []
    text_lower = text.lower()
    for correct, errors in KNOWN_TERMS.items():
        for error in errors:
            if error.lower() in text_lower and correct.lower() not in text_lower:
                term_warnings.append(f"TERM: '{error}' found — did you mean '{correct}'?")
            elif error.lower() in text_lower:
                term_warnings.append(f"TERM WARNING: '{error}' found alongside '{correct}' — verify which is correct")

    if term_warnings:
        for w in term_warnings[:10]:  # Cap at 10 warnings
            issues.append(w)

    passed = len([i for i in issues if not i.startswith("TERM WARNING")]) == 0
    return passed, issues, term_warnings


# ─── Output Formatting ────────────────────────────────────────────────────────

def format_markdown(text, metadata):
    """Wrap transcript in a formatted markdown document."""
    header = f"""# {metadata.get('title', 'Audio Transcript')}

- **Speakers**: {metadata.get('speakers', 'Unknown')}
- **Duration**: {metadata.get('duration', '?')} minutes
- **Language**: {metadata.get('language', 'Unknown')}
- **Transcribed**: {datetime.now().strftime('%Y-%m-%d %H:%M')} by Gemini 3.1 Pro
- **Source**: {metadata.get('source', 'Unknown')}

---

"""
    return header + text


def upload_google_doc(markdown_text, title, folder_id=None):
    """Upload as a Google Doc via gws CLI."""
    import tempfile

    # Convert markdown to simple HTML for better formatting
    html = "<html><head><meta charset='UTF-8'></head><body style='font-family:sans-serif;max-width:720px;margin:auto;line-height:1.8;'>"
    for line in markdown_text.split("\n"):
        line = line.strip()
        if line.startswith("# "):
            html += f"<h1>{line[2:]}</h1>"
        elif line.startswith("- **"):
            html += f"<p>{line}</p>"
        elif line.startswith("---"):
            html += "<hr>"
        elif re.match(r'\[\d', line):
            # Timestamp line — format speaker name in bold color
            m = re.match(r'(\[\d{1,2}:\d{2}:\d{2}\])\s*\*?\*?(\w+)\*?\*?:(.*)', line)
            if m:
                ts, spk, txt = m.groups()
                html += f'<p style="margin:12px 0;"><code style="font-size:11px;color:#999;background:#f0f0f0;padding:2px 5px;border-radius:3px;">{ts}</code> <b style="color:#003CC8;">{spk}:</b>{txt}</p>'
            else:
                html += f"<p>{line}</p>"
        elif line:
            html += f"<p>{line}</p>"
    html += "</body></html>"

    # Write temp file and upload
    with tempfile.NamedTemporaryFile(mode='w', suffix='.html', delete=False, dir='.') as f:
        f.write(html)
        tmp_path = f.name

    try:
        parent = f',"parents":["{folder_id}"]' if folder_id else ""
        cmd = f'gws drive files create --json \'{{"name":"{title}","mimeType":"application/vnd.google-apps.document"{parent}}}\' --upload "{tmp_path}" --upload-content-type "text/html"'
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=120)
        output = result.stdout
        if '"id"' in output:
            doc_id = json.loads(output.split("keyring")[-1] if "keyring" in output else output)["id"]
            log(f"  Google Doc: https://docs.google.com/document/d/{doc_id}")
    except Exception as e:
        log(f"  WARN: Google Doc upload failed: {e}")
    finally:
        os.unlink(tmp_path)


# ─── Cost Tracking ────────────────────────────────────────────────────────────

def log_cost(filename, duration_min, usage, elapsed):
    """Log transcription cost for tracking."""
    os.makedirs(os.path.dirname(COST_LOG_PATH), exist_ok=True)

    log = []
    if os.path.exists(COST_LOG_PATH):
        try:
            with open(COST_LOG_PATH) as f:
                log = json.load(f)
        except:
            log = []

    log.append({
        "file": filename,
        "date": datetime.now().isoformat(),
        "duration_min": round(duration_min, 1),
        "input_tokens": usage.get("input_tokens", 0),
        "output_tokens": usage.get("output_tokens", 0),
        "elapsed_sec": round(elapsed, 1),
    })

    with open(COST_LOG_PATH, "w") as f:
        json.dump(log, f, indent=2)


# ─── Main Pipeline ────────────────────────────────────────────────────────────

def transcribe_file(filepath, args, contacts):
    """Main pipeline for a single file."""
    filepath = os.path.abspath(filepath)
    filename = Path(filepath).name
    log(f"\n{'=' * 60}")
    log(f"  Transcribing: {filename}")
    log(f"{'=' * 60}")

    # 1. Audio analysis
    duration = get_audio_duration(filepath)
    if duration:
        duration_min = duration / 60
        log(f"  Duration: {duration_min:.1f} minutes")
    else:
        duration_min = 0
        log(f"  Duration: unknown (proceeding anyway)")

    # 2. Resolve speakers
    speakers = get_speakers(args, contacts)
    speakers_text = format_speakers_for_prompt(speakers)
    speakers_display = ", ".join(s.split(":")[0] for s in speakers) if speakers else "Unknown"

    # 3. Determine if chunking is needed
    needs_chunking = duration_min > MAX_SINGLE_CALL_MINUTES

    if needs_chunking:
        log(f"  Audio is {duration_min:.0f} min (>{MAX_SINGLE_CALL_MINUTES}) — splitting into chunks")
        chunks = split_audio(filepath, duration)
        all_text = []

        for chunk in chunks:
            offset_min = chunk["offset_sec"] / 60
            chunk_dur = get_audio_duration(chunk["path"]) or (CHUNK_MINUTES * 60)
            chunk_dur_min = chunk_dur / 60

            uploaded = upload_to_gemini(chunk["path"])
            prompt = build_prompt(args.language, speakers_text, chunk_dur_min, offset_min)
            text, finish, elapsed, usage = transcribe_audio(uploaded, prompt)
            all_text.append(text)

            # Clean up chunk file
            os.unlink(chunk["path"])

        # Merge chunks (simple concatenation — overlap dedup could be added later)
        full_text = "\n\n---\n\n".join(all_text)
    else:
        # Single call — the proven approach
        uploaded = upload_to_gemini(filepath)
        prompt = build_prompt(args.language, speakers_text, duration_min)
        full_text, finish_reason, elapsed, usage = transcribe_audio(uploaded, prompt)

        if finish_reason == "MAX_TOKENS":
            log(f"  WARNING: Hit token limit! Transcript may be incomplete.")

    # 4. QA Validation
    log(f"\n  Running QA checks...")
    passed, issues, term_warnings = validate_transcript(full_text, duration)
    if passed and not term_warnings:
        log(f"  ✅ QA PASSED — all checks clean")
    elif passed and term_warnings:
        log(f"  ✅ QA PASSED — but {len(term_warnings)} terminology warnings:")
        for w in term_warnings:
            log(f"     📝 {w}")
    else:
        for issue in issues:
            log(f"  ⚠️  {issue}")

    # 5. Format output
    title = Path(filepath).stem.replace("_", " ")
    metadata = {
        "title": title,
        "speakers": speakers_display,
        "duration": f"{duration_min:.0f}" if duration_min else "?",
        "language": args.language,
        "source": filename,
    }
    markdown = format_markdown(full_text, metadata)

    # 6. Save markdown
    output_path = args.output or filepath.replace(".m4a", "_transcript.md").replace(".mp3", "_transcript.md")
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(markdown)
    log(f"  Saved: {output_path}")

    # 7. Google Doc (default unless --local-only)
    if not args.local_only:
        doc_title = f"{title} — Transcript"
        # Audio_Recordings folder ID
        upload_google_doc(markdown, doc_title, folder_id="1ZSQ2A1SSFBXkmUMCwdaLNT4ZCeLBEm_h")

    # 8. Log cost
    if not needs_chunking:
        log_cost(filename, duration_min, usage, elapsed)

    return output_path, passed, issues


# ─── CLI ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Digiwin Transcription Pipeline — Gemini 3.1 Pro",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python3 transcribe.py call.m4a --company "BFC"
  python3 transcribe.py training.m4a --language thai --speakers "Xiaoping:Trainer,Ray:Staff,Kitty:Staff"
  python3 transcribe.py meeting.m4a --language chinese
  python3 transcribe.py ~/inbox/*.m4a --company "Digiwin Internal"
  python3 transcribe.py call.m4a --local-only  # skip Google Doc upload
        """
    )
    parser.add_argument("files", nargs="+", help="Audio file(s) to transcribe (.m4a, .mp3, .wav)")
    parser.add_argument("--language", "-l", default="thai", choices=["thai", "chinese", "english", "mixed"],
                        help="Primary language (default: thai)")
    parser.add_argument("--company", "-c", help="Company name — auto-populates speakers from contacts.json")
    parser.add_argument("--speakers", "-s", help='Manual speakers: "Name:Role,Name2:Role2"')
    parser.add_argument("--output", "-o", help="Output path (default: same as input with _transcript.md)")
    parser.add_argument("--local-only", action="store_true", help="Save locally only, skip Google Doc upload")
    parser.add_argument("--retry", action="store_true", help="Retry failed transcriptions")

    args = parser.parse_args()

    # Lockfile — prevent multiple instances
    acquire_lock()
    atexit.register(release_lock)

    # Load contacts
    contacts = load_contacts()

    # Process each file
    results = []
    for filepath in args.files:
        if not os.path.exists(filepath):
            log(f"  ERROR: File not found: {filepath}")
            continue
        try:
            output, passed, issues = transcribe_file(filepath, args, contacts)
            results.append({"file": filepath, "output": output, "passed": passed, "issues": issues})
        except Exception as e:
            log(f"  ERROR: {e}")
            results.append({"file": filepath, "output": None, "passed": False, "issues": [str(e)]})

    # Summary
    if len(results) > 1:
        log(f"\n{'=' * 60}")
        log(f"  BATCH SUMMARY: {len(results)} files")
        log(f"{'=' * 60}")
        for r in results:
            status = "✅" if r["passed"] else "⚠️"
            log(f"  {status} {Path(r['file']).name} → {r['output'] or 'FAILED'}")


if __name__ == "__main__":
    main()
