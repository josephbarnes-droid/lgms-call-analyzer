"""
Little Guys Movers - Call Analyzer Server v8
=============================================
Required environment variables:
  ANTHROPIC_API_KEY  - Anthropic API key
  SUPABASE_URL       - Supabase project URL
  SUPABASE_KEY       - Supabase anon/publishable key
  OPENAI_API_KEY     - OpenAI API key for Whisper transcription
  PORT               - optional, defaults to 8765
"""

import os, json, urllib.request, urllib.error, urllib.parse, tempfile, mimetypes
import zipfile, io, secrets, re, uuid, threading, time
from http.server import HTTPServer, BaseHTTPRequestHandler
from datetime import datetime, timezone, timedelta

API_KEY      = os.environ.get("ANTHROPIC_API_KEY", "")
SUPABASE_URL = os.environ.get("SUPABASE_URL", "")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY", "")
OPENAI_KEY   = os.environ.get("OPENAI_API_KEY", "")
PORT         = int(os.environ.get("PORT", 8765))

# Global semaphore — max 3 concurrent Whisper/Claude calls across ALL requests
_analysis_semaphore = threading.Semaphore(3)

# Background re-analyze job state
_reanalyze_job = {
    "status": "idle",   # idle | running | complete | error
    "total": 0,
    "processed": 0,
    "current": "",
    "errors": 0,
    "started_at": None,
    "finished_at": None,
}
_reanalyze_lock = threading.Lock()

# ──────────────────────────────────────────────
# CLAUDE PROMPT
# ──────────────────────────────────────────────

def build_prompt(transcript, filename, corrections=None):
    """Build Claude prompt, optionally injecting recent corrections as few-shot examples."""

    corrections_block = ""
    if corrections:
        examples = []
        for c in corrections[:20]:  # max 20 examples
            note = c.get("manager_note", "")
            examples.append(
                f"- {c['category']}: scored {c['original_score']} but correct score is {c['corrected_score']}"
                + (f" — reason: {note}" if note else "")
            )
        if examples:
            corrections_block = "\n\nRECENT SCORING CORRECTIONS (use these to calibrate your scoring):\n" + "\n".join(examples) + "\n"

    prompt = f"""You are a sales call evaluator for Little Guys Movers (LGMS).
{corrections_block}
FIRST — classify this call:
- Count approximately how many words are in the transcript
- Determine the call type: "sales_estimate", "follow_up", "complaint", "booking_confirmation", "non_sales", "too_short", or "other"
- If the transcript has fewer than 80 words OR is clearly not a sales estimate call, set call_type accordingly and set exclude_from_scoring to true

SECOND — detect the rep name:
- Look for how the rep introduces themselves (e.g. "This is JD", "Hi this is Manning", "Thank you for calling Little Guys this is Sarah")
- Return FIRST NAME ONLY (e.g. "Caleb" not "Caleb Smith")
- Return "Unknown" if not found

THIRD — detect call quality:
- "disconnected": abrupt ending, "Hello? Hello?" patterns, very short (<60 sec of content)
- "poor_audio": heavy [inaudible] density (5+ occurrences), one-sided conversation
- "normal": otherwise
- Short disconnected calls: set exclude_from_scoring=true

FOURTH — detect call flags:
- availability_decline: customer said their date/situation doesn't work for THEM — they are declining or unavailable
- turned_away: LGMS could NOT accommodate the customer — rep said things like "we don't have availability", "we're booked up", "that date is full", "we can't do that weekend", "we're not available then"
- onsite_suggested: customer or rep mentioned an onsite visit/estimate would be preferred
- is_continuation: contains "calling back about", "as we discussed", "following up on", "this is a follow-up", "called earlier", "spoke earlier"

REQUIRED CALL SEQUENCE (22 steps):
1. Move date
2. Customer full name
3. Phone number
4. Cities/locations
5. House or apt (load AND unload)
6. Stairs/long walk both locations
7. Room-by-room inventory (rep controls pace)
8. Forgotten items: appliances, outdoor/garage, rugs/pictures/lamps/TVs
9. Boxes ("moving boxes")
10. Give price IMMEDIATELY — #1 priority
11. FVP pitch: declared value, $7 per $1,000, handle objections
12. Attempt to close
13. If booking: confirm time slot, email, 2 confirmation calls
14. If not booking: offer email estimate
15. Thank customer
16. Asked customer name at START of call
17. Led the estimate process (Navigator/Pilot) — rep controlled flow
18. Attempted to schedule onsite if customer preferred it
19. Offered alternative solutions if customer hesitated on full-service (pack-only, load-only, National Express) — evaluate as customer-first problem solving
20. Took rapport opportunities — acknowledged personal info customer shared
21. Booking wrap-up — reviewed service, confirmed nothing missing, explained crew call-ahead
22. Lead captured — name, number, notes logged even if no booking

KEY RULES:
- Simple moves (apt, 1-2BR): quote HOURLY with price RANGE
- Always attempt to close
- Give price on first contact
- Be confident, friendly, knowledgeable
- Rep should sound engaged and enthusiastic — not flat or bothered

TALK RATIO: Count rep lines vs customer lines as percentages. Ideal: rep 40% / customer 60%.

KEYWORDS — check if rep said:
- "Full Value Protection" or "FVP"
- "confirmation call"
- "hourly" or "per hour"
- "moving boxes"
- "fuel" or "fuel charge"
- "declared value"
- "schedule" or "get you on the calendar"
- "Little Guys" or "Little Guys Movers"

For each detected keyword and objection, return the CHARACTER POSITION (integer) of its first occurrence in the transcript:
  keyword_positions: {{"Full Value Protection": 342}}
  objection_positions: {{"Price too high": 891}}

OBJECTIONS: Price too high, Need to think about it, Already have another quote, Wrong timing, Need to check with partner, Other

SENTIMENT: positive / neutral / hesitant / negative
OUTCOME: booked / estimate_sent / lost / unknown

Score 1-10. Be honest and critical. 10 = near perfect.
For too_short or non_sales calls, score everything 0.

RAPPORT & TONE (1-10):
- 1-3: Flat, bothered, disengaged; missed rapport opportunities; customer didn't trust them
- 4-6: Polite but mechanical; some warmth but rapport opportunities missed
- 7-8: Warm and engaged; took most rapport opportunities; customer felt heard
- 9-10: Excellent — enthusiastic, built genuine trust, customer felt this was the right choice

Filename: {filename}

Transcript:
{transcript}

Respond ONLY with valid JSON, no markdown:

{{"rep_name_detected":"name or Unknown","caller_name":"name or Unknown","call_purpose":"short phrase","call_type":"sales_estimate","move_type":"local/long distance/unknown","call_outcome":"booked/estimate_sent/lost/unknown","word_count":150,"exclude_from_scoring":false,"exclusion_reason":"","call_quality":"normal","availability_decline":false,"turned_away":false,"onsite_suggested":false,"is_continuation":false,"call_summary":"3-5 sentences","key_details_captured":"details gathered","talk_ratio_rep":40,"talk_ratio_customer":60,"keywords_detected":["Full Value Protection"],"keyword_positions":{{"Full Value Protection":342}},"objections_detected":["Price too high"],"objection_positions":{{"Price too high":891}},"customer_sentiment":"positive","scores":{{"info_sequence":{{"score":0,"note":""}},"price_delivery":{{"score":0,"note":""}},"fvp_pitch":{{"score":0,"note":""}},"closing_attempt":{{"score":0,"note":""}},"call_control":{{"score":0,"note":""}},"professionalism":{{"score":0,"note":""}},"rapport_tone":{{"score":0,"note":""}},"overall":{{"score":0,"note":""}}}},"checklist":{{"got_move_date":false,"got_customer_name":false,"got_phone_number":false,"got_cities":false,"got_home_type":false,"got_stairs_info":false,"did_full_inventory":false,"asked_forgotten_items":false,"asked_about_boxes":false,"gave_price_on_call":false,"pitched_fvp":false,"attempted_to_close":false,"offered_email_estimate":false,"mentioned_confirmations":false,"thanked_customer":false,"asked_name_at_start":false,"led_estimate_process":false,"scheduled_onsite_attempt":false,"offered_alternatives":false,"took_rapport_opportunities":false,"completed_booking_wrapup":false,"captured_lead":false}},"strengths":["s1","s2"],"coaching_points":["c1","c2"]}}"""
    return prompt

# ──────────────────────────────────────────────
# VONAGE FILENAME DATE PARSER
# ──────────────────────────────────────────────

def parse_call_date_from_filename(filename):
    m = re.search(r'(\d{4})_(\d{2})_(\d{2})_(\d{2})_(\d{2})(AM|PM)', filename, re.IGNORECASE)
    if not m:
        return None
    year, month, day, hour, minute, ampm = m.groups()
    h = int(hour)
    if ampm.upper() == 'PM' and h != 12:
        h += 12
    elif ampm.upper() == 'AM' and h == 12:
        h = 0
    try:
        dt = datetime(int(year), int(month), int(day), h, int(minute), tzinfo=timezone.utc)
        return dt.isoformat()
    except Exception:
        return None

# ──────────────────────────────────────────────
# SUPABASE HELPERS
# ──────────────────────────────────────────────

def supa(method, path, body=None, extra_headers=None):
    if not SUPABASE_URL or not SUPABASE_KEY:
        raise Exception("Supabase not configured")
    url = f"{SUPABASE_URL}/rest/v1/{path}"
    data = json.dumps(body).encode() if body else None
    req = urllib.request.Request(url, data=data, method=method)
    req.add_header("apikey", SUPABASE_KEY)
    req.add_header("Authorization", f"Bearer {SUPABASE_KEY}")
    req.add_header("Content-Type", "application/json")
    req.add_header("Prefer", "return=representation")
    if extra_headers:
        for k, v in extra_headers.items():
            req.add_header(k, v)
    with urllib.request.urlopen(req, timeout=30) as r:
        return json.loads(r.read())

def supa_storage_upload(bucket, path, data, content_type="audio/mpeg"):
    url = f"{SUPABASE_URL}/storage/v1/object/{bucket}/{path}"
    req = urllib.request.Request(url, data=data, method="POST")
    req.add_header("apikey", SUPABASE_KEY)
    req.add_header("Authorization", f"Bearer {SUPABASE_KEY}")
    req.add_header("Content-Type", content_type)
    with urllib.request.urlopen(req, timeout=120) as r:
        return json.loads(r.read())

def supa_storage_signed_url(bucket, path, expires=86400):
    url = f"{SUPABASE_URL}/storage/v1/object/sign/{bucket}/{path}"
    body = json.dumps({"expiresIn": expires}).encode()
    req = urllib.request.Request(url, data=body, method="POST")
    req.add_header("apikey", SUPABASE_KEY)
    req.add_header("Authorization", f"Bearer {SUPABASE_KEY}")
    req.add_header("Content-Type", "application/json")
    with urllib.request.urlopen(req, timeout=30) as r:
        result = json.loads(r.read())
    signed = result.get("signedURL", "")
    if signed and not signed.startswith("http"):
        signed = f"{SUPABASE_URL}/storage/v1{signed}"
    return signed

def supa_storage_list(bucket):
    url = f"{SUPABASE_URL}/storage/v1/object/list/{bucket}"
    body = json.dumps({"prefix": "", "limit": 1000}).encode()
    req = urllib.request.Request(url, data=body, method="POST")
    req.add_header("apikey", SUPABASE_KEY)
    req.add_header("Authorization", f"Bearer {SUPABASE_KEY}")
    req.add_header("Content-Type", "application/json")
    with urllib.request.urlopen(req, timeout=30) as r:
        return json.loads(r.read())

def supa_storage_delete(bucket, paths):
    url = f"{SUPABASE_URL}/storage/v1/object/{bucket}"
    body = json.dumps({"prefixes": paths}).encode()
    req = urllib.request.Request(url, data=body, method="DELETE")
    req.add_header("apikey", SUPABASE_KEY)
    req.add_header("Authorization", f"Bearer {SUPABASE_KEY}")
    req.add_header("Content-Type", "application/json")
    with urllib.request.urlopen(req, timeout=30) as r:
        return json.loads(r.read())

def enforce_storage_cap():
    try:
        files = supa_storage_list("call-audio")
        if not files:
            return
        total_bytes = sum(f.get("metadata", {}).get("size", 0) for f in files if isinstance(f, dict))
        if total_bytes <= 900 * 1024 * 1024:
            return
        calls_with_audio = supa("GET", "calls?audio_url=neq.&order=created_at.asc&select=id,audio_url&limit=200")
        for call in (calls_with_audio or []):
            if total_bytes <= 800 * 1024 * 1024:
                break
            audio_url = call.get("audio_url", "")
            if not audio_url:
                continue
            path_match = re.search(r'/call-audio/(.+?)(\?|$)', audio_url)
            if path_match:
                storage_path = path_match.group(1)
                try:
                    file_info = next((f for f in files if isinstance(f, dict) and f.get("name") == storage_path), None)
                    file_size = file_info.get("metadata", {}).get("size", 0) if file_info else 0
                    supa_storage_delete("call-audio", [storage_path])
                    supa("PATCH", f"calls?id=eq.{call['id']}", {"audio_url": ""})
                    total_bytes -= file_size
                except Exception as e:
                    print(f"  Storage cleanup error: {e}")
    except Exception as e:
        print(f"  Storage cap error: {e}")

# ──────────────────────────────────────────────
# CORRECTIONS
# ──────────────────────────────────────────────

def get_recent_corrections(limit=20):
    try:
        return supa("GET", f"corrections?order=created_at.desc&limit={limit}&used_in_prompt=eq.true")
    except Exception:
        return []

# ──────────────────────────────────────────────
# REP NAME FUZZY MATCHING
# ──────────────────────────────────────────────

def fuzzy_match_rep(detected_name, rep_list):
    """
    Simple fuzzy match — returns (matched_name, confidence) or (detected_name, 0).
    No external libraries needed.
    """
    if not detected_name or detected_name == "Unknown" or not rep_list:
        return detected_name, 0.0

    detected_lower = detected_name.lower().strip()

    for rep in rep_list:
        full = rep.get("full_name", "").lower().strip()
        nick = rep.get("nickname", "").lower().strip()
        alts = [a.lower().strip() for a in rep.get("alternate_names", [])]
        location = rep.get("location", "")

        candidates = [full, nick] + alts
        # First name of full name
        if full:
            candidates.append(full.split()[0])

        for candidate in candidates:
            if not candidate:
                continue
            # Exact match
            if detected_lower == candidate:
                return rep.get("full_name"), 1.0
            # Detected is first name of candidate
            if candidate.startswith(detected_lower + " "):
                return rep.get("full_name"), 0.95
            # Candidate starts with detected
            if detected_lower.startswith(candidate):
                return rep.get("full_name"), 0.92

    return detected_name, 0.0

# ──────────────────────────────────────────────
# CONTINUATION GROUP
# ──────────────────────────────────────────────

def find_or_create_continuation_group(rep_name, caller_name):
    try:
        cutoff = (datetime.now(timezone.utc) - timedelta(days=7)).isoformat()
        safe_rep = urllib.parse.quote(rep_name or "")
        safe_caller = urllib.parse.quote(caller_name or "")
        existing = supa("GET", f"calls?rep_name=eq.{safe_rep}&caller_name=eq.{safe_caller}&created_at=gte.{cutoff}&is_continuation=eq.true&order=created_at.desc&limit=1&select=continuation_group_id")
        if existing and existing[0].get("continuation_group_id"):
            return existing[0]["continuation_group_id"]
    except Exception as e:
        print(f"  Continuation lookup error: {e}")
    return str(uuid.uuid4())

def retroactively_link_continuation(rep_name, caller_name, group_id):
    try:
        cutoff = (datetime.now(timezone.utc) - timedelta(days=7)).isoformat()
        safe_rep = urllib.parse.quote(rep_name or "")
        safe_caller = urllib.parse.quote(caller_name or "")
        unlinked = supa("GET", f"calls?rep_name=eq.{safe_rep}&caller_name=eq.{safe_caller}&created_at=gte.{cutoff}&continuation_group_id=eq.&select=id")
        for call in (unlinked or []):
            supa("PATCH", f"calls?id=eq.{call['id']}", {"continuation_group_id": group_id, "is_continuation": True})
    except Exception as e:
        print(f"  Retroactive linking error: {e}")

# ──────────────────────────────────────────────
# HTML LOADER
# ──────────────────────────────────────────────

def read_html():
    for name in ["lgms_dashboard.html", "lgms_analyzer_v3.html"]:
        if os.path.exists(name):
            with open(name, "rb") as f:
                return f.read()
    return b"<h1>Missing lgms_dashboard.html</h1>"

# ──────────────────────────────────────────────
# WHISPER TRANSCRIPTION
# ──────────────────────────────────────────────

def transcribe_audio(audio_bytes, filename):
    if not OPENAI_KEY:
        raise Exception("OPENAI_API_KEY not set")
    ext = filename.rsplit(".", 1)[-1].lower() if "." in filename else "mp3"
    mime_map = {"mp3": "audio/mpeg", "mp4": "audio/mp4", "m4a": "audio/mp4",
                "wav": "audio/wav", "ogg": "audio/ogg", "webm": "audio/webm",
                "mpeg": "audio/mpeg", "mpga": "audio/mpeg"}
    mime = mime_map.get(ext, "audio/mpeg")
    boundary = "----WhisperBoundary"
    body_parts = [
        f"--{boundary}\r\nContent-Disposition: form-data; name=\"model\"\r\n\r\nwhisper-1".encode(),
        f"--{boundary}\r\nContent-Disposition: form-data; name=\"response_format\"\r\n\r\ntext".encode(),
        f"--{boundary}\r\nContent-Disposition: form-data; name=\"file\"; filename=\"{filename}\"\r\nContent-Type: {mime}\r\n\r\n".encode() + audio_bytes,
        f"--{boundary}--".encode()
    ]
    body = b"\r\n".join(body_parts)
    req = urllib.request.Request(
        "https://api.openai.com/v1/audio/transcriptions",
        data=body,
        headers={"Authorization": f"Bearer {OPENAI_KEY}", "Content-Type": f"multipart/form-data; boundary={boundary}"},
        method="POST"
    )
    with urllib.request.urlopen(req, timeout=300) as r:
        return r.read().decode("utf-8")

# ──────────────────────────────────────────────
# CLAUDE ANALYSIS
# ──────────────────────────────────────────────

def run_claude_analysis(transcript, filename, model="claude-sonnet-4-6"):
    corrections = get_recent_corrections()
    prompt = build_prompt(transcript, filename, corrections)
    req_body = json.dumps({
        "model": model,
        "max_tokens": 3000,
        "messages": [{"role": "user", "content": prompt}]
    }).encode()
    req = urllib.request.Request(
        "https://api.anthropic.com/v1/messages", data=req_body,
        headers={"Content-Type": "application/json", "x-api-key": API_KEY, "anthropic-version": "2023-06-01"},
        method="POST"
    )
    with urllib.request.urlopen(req, timeout=120) as r:
        resp = json.loads(r.read())
    tb = next((b for b in resp.get("content", []) if b.get("type") == "text"), None)
    if not tb:
        raise Exception("No response from Claude")
    raw = tb["text"].strip().lstrip("```json").lstrip("```").rstrip("```").strip()
    return json.loads(raw)

# ──────────────────────────────────────────────
# PDF EXPORT
# ──────────────────────────────────────────────

def generate_call_pdf(call):
    try:
        from weasyprint import HTML
    except ImportError:
        raise Exception("WeasyPrint not installed")

    scores = call.get("scores", {})
    checklist = call.get("checklist", {})
    overrides = call.get("score_overrides", {})

    score_keys = ["info_sequence","price_delivery","fvp_pitch","closing_attempt","call_control","professionalism","rapport_tone","overall"]
    score_labels = ["Info Sequence","Price Delivery","FVP Pitch","Closing Attempt","Call Control","Professionalism","Rapport & Tone","Overall"]

    ck_keys = ["got_move_date","got_customer_name","got_phone_number","got_cities","got_home_type","got_stairs_info","did_full_inventory","asked_forgotten_items","asked_about_boxes","gave_price_on_call","pitched_fvp","attempted_to_close","offered_email_estimate","mentioned_confirmations","thanked_customer","asked_name_at_start","led_estimate_process","scheduled_onsite_attempt","offered_alternatives","took_rapport_opportunities","completed_booking_wrapup","captured_lead"]
    ck_labels = ["Move date","Customer name","Phone number","Cities/locations","Home type","Stairs info","Full inventory","Forgotten items","Moving boxes","Gave price on call","FVP pitched","Closing attempt","Email estimate offered","Confirmations mentioned","Thanked customer","Asked name at start","Led estimate process","Scheduled onsite attempt","Offered alternatives","Took rapport opportunities","Completed booking wrap-up","Lead captured"]

    def sc(s):
        if s >= 8: return "#16a34a"
        if s >= 5: return "#d97706"
        return "#dc2626"

    scores_html = ""
    for k, label in zip(score_keys, score_labels):
        s = overrides.get(k) or scores.get(k, {}).get("score", 0)
        note = scores.get(k, {}).get("note", "")
        scores_html += f'<div class="score-item"><div class="sl">{label}</div><div class="sv" style="color:{sc(s)}">{s}/10</div><div class="sb"><div style="width:{s*10}%;background:{sc(s)};height:100%;border-radius:3px"></div></div><div class="sn">{note}</div></div>'

    ck_html = ""
    for k, label in zip(ck_keys, ck_labels):
        ck_html += f'<div class="ck">{"✅" if checklist.get(k) else "❌"} {label}</div>'

    try:
        dt = datetime.fromisoformat(call.get("created_at", datetime.now().isoformat()).replace("Z", "+00:00"))
        formatted_date = dt.strftime("%B %d, %Y at %I:%M %p")
    except Exception:
        formatted_date = call.get("created_at", "")[:10]

    html_content = f"""<!DOCTYPE html><html><head><meta charset="UTF-8"><style>
body{{font-family:Arial,sans-serif;color:#1a1c1a;margin:0;padding:24px;font-size:13px}}
.hdr{{background:#4a7c3f;color:white;padding:20px 24px;border-radius:8px;margin-bottom:20px}}
.hdr h1{{margin:0 0 4px;font-size:20px}}.hdr p{{margin:0;opacity:.85;font-size:12px}}
.sec{{margin-bottom:20px}}.sec h2{{font-size:12px;font-weight:700;color:#4a7c3f;text-transform:uppercase;letter-spacing:.5px;border-bottom:2px solid #e5e7eb;padding-bottom:6px;margin-bottom:12px}}
.mg{{display:grid;grid-template-columns:1fr 1fr;gap:8px}}
.mi{{background:#f6f9f5;border-radius:6px;padding:8px 10px}}.ml{{font-size:9px;font-weight:700;color:#6b7280;text-transform:uppercase;letter-spacing:.4px}}.mv{{font-size:13px;font-weight:600;margin-top:2px}}
.score-item{{margin-bottom:10px}}.sl{{font-size:11px;font-weight:600;color:#374151;margin-bottom:2px}}.sv{{font-size:16px;font-weight:700}}.sb{{height:5px;background:#e5e7eb;border-radius:3px;margin:3px 0}}.sn{{font-size:11px;color:#6b7280}}
.ckg{{display:grid;grid-template-columns:1fr 1fr;gap:4px}}.ck{{font-size:12px;padding:3px 0}}
.sum{{background:#eef6ec;border-left:4px solid #4a7c3f;padding:12px;border-radius:4px;font-size:13px;line-height:1.7}}
.cch{{background:#fff7ed;border-left:4px solid #d97706;padding:12px;border-radius:4px}}.cch li{{font-size:12px;margin-bottom:4px}}
.nt{{background:#f9fafb;border:1px solid #e5e7eb;padding:12px;border-radius:6px;font-size:13px}}
.ftr{{margin-top:30px;text-align:center;font-size:10px;color:#9ca3af;border-top:1px solid #e5e7eb;padding-top:12px}}
</style></head><body>
<div class="hdr"><h1>Call Scorecard — {call.get('rep_name','Unknown Rep')}</h1><p>{formatted_date} · {call.get('caller_name','Unknown Caller')} · {call.get('call_outcome','unknown').replace('_',' ').title()}</p></div>
<div class="sec"><h2>Call Details</h2><div class="mg">
<div class="mi"><div class="ml">Rep</div><div class="mv">{call.get('rep_name','—')}</div></div>
<div class="mi"><div class="ml">Caller</div><div class="mv">{call.get('caller_name','—')}</div></div>
<div class="mi"><div class="ml">Purpose</div><div class="mv">{call.get('call_purpose','—')}</div></div>
<div class="mi"><div class="ml">Move Type</div><div class="mv">{call.get('move_type','—')}</div></div>
<div class="mi"><div class="ml">Outcome</div><div class="mv">{call.get('call_outcome','—').replace('_',' ').title()}</div></div>
<div class="mi"><div class="ml">Sentiment</div><div class="mv">{call.get('customer_sentiment','—').title()}</div></div>
</div></div>
<div class="sec"><h2>Summary</h2><div class="sum">{call.get('call_summary','No summary.')}</div></div>
<div class="sec"><h2>Performance Scores</h2>{scores_html}</div>
<div class="sec"><h2>22-Step Checklist</h2><div class="ckg">{ck_html}</div></div>
<div class="sec"><h2>Coaching Points</h2><div class="cch"><ul>{''.join(f'<li>{p}</li>' for p in call.get('coaching_points',[]) or ['None recorded.'])}</ul></div></div>
{f'<div class="sec"><h2>Manager Notes</h2><div class="nt">{call.get("manager_notes","")}</div></div>' if call.get('manager_notes') else ''}
<div class="ftr">Little Guys Movers — Call Intelligence · Generated {datetime.now().strftime("%B %d, %Y")}</div>
</body></html>"""

    return HTML(string=html_content).write_pdf()

# ──────────────────────────────────────────────
# BACKGROUND RE-ANALYZE
# ──────────────────────────────────────────────

def _reanalyze_worker():
    global _reanalyze_job
    try:
        calls = supa("GET", "calls?order=created_at.desc&limit=5000&select=id,transcript,filename")
        total = len(calls)
        with _reanalyze_lock:
            _reanalyze_job["total"] = total
            _reanalyze_job["processed"] = 0
            _reanalyze_job["errors"] = 0

        for call in calls:
            transcript = call.get("transcript", "")
            filename = call.get("filename", "call.txt")
            call_id = call.get("id")

            with _reanalyze_lock:
                _reanalyze_job["current"] = filename

            if not transcript or not transcript.strip():
                with _reanalyze_lock:
                    _reanalyze_job["processed"] += 1
                continue

            try:
                with _analysis_semaphore:
                    result = run_claude_analysis(transcript, filename)

                call_date = parse_call_date_from_filename(filename)
                update_data = {
                    "availability_decline": result.get("availability_decline", False),
                    "turned_away": result.get("turned_away", False),
                    "onsite_suggested": result.get("onsite_suggested", False),
                    "call_quality": result.get("call_quality", "normal"),
                    "is_continuation": result.get("is_continuation", False),
                    "scores": result.get("scores", {}),
                    "checklist": result.get("checklist", {}),
                    "strengths": result.get("strengths", []),
                    "coaching_points": result.get("coaching_points", []),
                    "keywords_detected": result.get("keywords_detected", []),
                    "keyword_positions": result.get("keyword_positions", {}),
                    "objections_detected": result.get("objections_detected", []),
                    "objection_positions": result.get("objection_positions", {}),
                    "customer_sentiment": result.get("customer_sentiment", "neutral"),
                    "call_summary": result.get("call_summary", ""),
                    "word_count": result.get("word_count", 0),
                    "exclude_from_scoring": result.get("exclude_from_scoring", False),
                    "exclusion_reason": result.get("exclusion_reason", ""),
                    "call_type": result.get("call_type", "sales_estimate"),
                    "rapport_tone": result.get("scores", {}).get("rapport_tone", {}).get("score", 0),
                }
                if call_date:
                    update_data["call_date"] = call_date
                if result.get("call_quality") == "disconnected" and not result.get("exclude_from_scoring"):
                    update_data["exclude_from_scoring"] = True
                    update_data["exclusion_reason"] = "Disconnected/short call — auto excluded"

                supa("PATCH", f"calls?id=eq.{call_id}", update_data)

                with _reanalyze_lock:
                    _reanalyze_job["processed"] += 1

            except Exception as e:
                print(f"  Re-analyze error for {filename}: {e}")
                with _reanalyze_lock:
                    _reanalyze_job["errors"] += 1
                    _reanalyze_job["processed"] += 1

        with _reanalyze_lock:
            _reanalyze_job["status"] = "complete"
            _reanalyze_job["finished_at"] = datetime.now(timezone.utc).isoformat()
            _reanalyze_job["current"] = ""

    except Exception as e:
        with _reanalyze_lock:
            _reanalyze_job["status"] = "error"
            _reanalyze_job["current"] = str(e)
        print(f"  Re-analyze worker error: {e}")

# ──────────────────────────────────────────────
# HTTP HANDLER
# ──────────────────────────────────────────────

class Handler(BaseHTTPRequestHandler):
    def log_message(self, fmt, *args):
        print(f"  {args[0]} {args[1]}")

    def do_GET(self):
        path = self.path.split("?")[0]
        if path == "/calls":
            self._get_calls()
        elif path == "/reps":
            self._get_reps()
        elif path == "/shared_views":
            self._get_shared_views()
        elif path.startswith("/share/"):
            self._get_shared_view_by_token()
        elif path == "/export/csv":
            self._export_csv()
        elif path.startswith("/export/pdf/call/"):
            self._export_pdf_call()
        elif path.startswith("/export/pdf/rep/"):
            self._export_pdf_rep()
        elif path == "/reanalyze/status":
            self._reanalyze_status()
        elif path == "/corrections":
            self._get_corrections()
        elif path.startswith("/audio_url/"):
            self._get_audio_url()
        else:
            html = read_html()
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.end_headers()
            self.wfile.write(html)

    def do_OPTIONS(self):
        self.send_response(200)
        self._cors()
        self.end_headers()

    def do_POST(self):
        n = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(n)
        path = self.path.split("?")[0]

        routes = {
            "/analyze": self._analyze,
            "/transcribe_and_analyze": self._transcribe_and_analyze,
            "/check_duplicate": self._check_dup_endpoint,
            "/extract_zip": self._extract_zip,
            "/save": self._save,
            "/update": self._update,
            "/delete": self._delete,
            "/bulk_delete": self._bulk_delete,
            "/share": self._create_share,
            "/share/delete": self._delete_share,
            "/reps/save": self._save_rep,
            "/reps/update": self._update_rep,
            "/reps/delete": self._delete_rep,
            "/reps/deduplicate": self._dedup_reps,
            "/reps/bulk_rename": self._bulk_rename_rep,
            "/reanalyze/start": self._reanalyze_start,
            "/corrections/save": self._save_correction,
        }
        fn = routes.get(path)
        if fn:
            fn(body)
        else:
            self.send_response(404)
            self.end_headers()

    # ── CALLS ──

    def _get_calls(self):
        try:
            calls = supa("GET", "calls?order=created_at.desc&limit=2000")
            self._ok(calls)
        except Exception as e:
            self._err(500, str(e))

    def _extract_zip(self, body):
        try:
            import base64
            p = json.loads(body)
            zip_bytes = base64.b64decode(p.get("zip", ""))
            audio_exts = {".mp3",".m4a",".wav",".ogg",".mp4",".webm",".mpeg",".mpga"}
            results = []
            with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
                for name in zf.namelist():
                    if name.startswith("__") or name.startswith(".") or name.endswith("/"):
                        continue
                    ext = os.path.splitext(name)[1].lower()
                    if ext not in audio_exts:
                        continue
                    filename = os.path.basename(name)
                    if not filename:
                        continue
                    audio_data = zf.read(name)
                    results.append({"filename": filename, "audio": base64.b64encode(audio_data).decode(), "size": len(audio_data)})
            self._ok({"files": results, "count": len(results)})
        except zipfile.BadZipFile:
            self._err(400, "Invalid zip file")
        except Exception as e:
            self._err(500, f"Zip extraction failed: {str(e)}")

    def _check_dup_endpoint(self, body):
        try:
            p = json.loads(body)
            is_dup = self._check_filename_exists(p.get("filename", ""))
            self._ok({"duplicate": is_dup})
        except Exception:
            self._ok({"duplicate": False})

    def _transcribe_and_analyze(self, body):
        if not OPENAI_KEY:
            self._err(500, "OPENAI_API_KEY not set"); return
        if not API_KEY:
            self._err(500, "ANTHROPIC_API_KEY not set"); return
        if "application/json" not in self.headers.get("Content-Type", ""):
            self._err(400, "Content-Type must be application/json"); return
        try:
            import base64
            p = json.loads(body)
            audio_bytes = base64.b64decode(p.get("audio", ""))
            filename = p.get("filename", "call.mp3")
        except Exception as e:
            self._err(400, f"Bad request: {e}"); return

        try:
            with _analysis_semaphore:
                print(f"  Transcribing {filename} ({len(audio_bytes)} bytes)...")
                transcript = transcribe_audio(audio_bytes, filename)
                if not transcript or not transcript.strip():
                    self._err(400, "Transcription empty — audio may be silent"); return
                print(f"  Transcription done: {len(transcript)} chars")
                result = run_claude_analysis(transcript, filename)

            result["transcript"] = transcript
            result["filename"] = filename

            # Upload audio to storage (best effort)
            try:
                enforce_storage_cap()
                supa_storage_upload("call-audio", filename, audio_bytes, "audio/mpeg")
                result["audio_url"] = supa_storage_signed_url("call-audio", filename)
            except Exception as e:
                print(f"  Audio storage warning: {e}")
                result["audio_url"] = ""

            self._ok(result)
        except urllib.error.HTTPError as e:
            self._err(e.code, f"API error: {e.read().decode()}")
        except Exception as e:
            self._err(500, f"Processing failed: {str(e)}")

    def _analyze(self, body):
        if not API_KEY:
            self._err(500, "ANTHROPIC_API_KEY not set"); return
        try:
            p = json.loads(body)
            transcript = p.get("transcript", "").strip()
            filename = p.get("filename", "call.txt")
            if not transcript:
                self._err(400, "No transcript"); return
            with _analysis_semaphore:
                result = run_claude_analysis(transcript, filename)
            result["filename"] = filename
            self._ok(result)
        except Exception as e:
            self._err(500, str(e))

    def _check_duplicate(self, filename, transcript):
        try:
            clean = filename.replace("'", "''")
            existing = supa("GET", f"calls?filename=ilike.{urllib.parse.quote(clean)}&limit=1")
            if existing:
                return True, f"Duplicate filename: {filename}"
            if transcript and len(transcript) > 100:
                all_calls = supa("GET", "calls?limit=1000&select=transcript,filename")
                tx_start = transcript[:150].strip()
                for c in all_calls:
                    ctx = (c.get("transcript") or "")[:150].strip()
                    if ctx and ctx == tx_start:
                        return True, f"Duplicate content (matches {c.get('filename','unknown')})"
            return False, ""
        except Exception:
            return False, ""

    def _check_filename_exists(self, filename):
        try:
            clean = filename.replace("'", "''")
            existing = supa("GET", f"calls?filename=ilike.{urllib.parse.quote(clean)}&limit=1")
            return bool(existing)
        except Exception:
            return False

    def _save(self, body):
        try:
            p = json.loads(body)

            if p.get("check_duplicate", True):
                is_dup, dup_reason = self._check_duplicate(p.get("filename", ""), p.get("transcript", ""))
                if is_dup:
                    self._ok({"duplicate": True, "reason": dup_reason}); return

            # Parse call date from filename
            call_date = parse_call_date_from_filename(p.get("filename", ""))

            # Rep name matching against reps table
            rep_name_raw = p.get("rep_name") or p.get("rep_name_detected") or "Unknown"
            try:
                rep_list = supa("GET", "reps?active=eq.true")
                matched_name, confidence = fuzzy_match_rep(rep_name_raw, rep_list)
                rep_name = matched_name if confidence >= 0.90 else rep_name_raw
            except Exception:
                rep_name = rep_name_raw

            # Continuation handling
            caller_name = p.get("caller_name", "")
            is_continuation = p.get("is_continuation", False)
            continuation_group_id = ""
            if is_continuation:
                continuation_group_id = find_or_create_continuation_group(rep_name, caller_name)
                retroactively_link_continuation(rep_name, caller_name, continuation_group_id)

            # Auto-exclude disconnected
            call_quality = p.get("call_quality", "normal")
            exclude = p.get("exclude_from_scoring", False)
            exclusion_reason = p.get("exclusion_reason", "")
            if call_quality == "disconnected" and not exclude:
                exclude = True
                exclusion_reason = "Disconnected/short call — auto excluded"

            record = {
                "rep_name": rep_name,
                "filename": p.get("filename", ""),
                "transcript": p.get("transcript", ""),
                "caller_name": caller_name,
                "call_purpose": p.get("call_purpose", ""),
                "call_type": p.get("call_type", "sales_estimate"),
                "move_type": p.get("move_type", ""),
                "call_outcome": p.get("call_outcome", "unknown"),
                "word_count": p.get("word_count", 0),
                "exclude_from_scoring": exclude,
                "exclusion_reason": exclusion_reason,
                "call_summary": p.get("call_summary", ""),
                "key_details": p.get("key_details_captured", ""),
                "talk_ratio_rep": p.get("talk_ratio_rep", 0),
                "talk_ratio_customer": p.get("talk_ratio_customer", 0),
                "keywords_detected": p.get("keywords_detected", []),
                "keyword_positions": p.get("keyword_positions", {}),
                "objections_detected": p.get("objections_detected", []),
                "objection_positions": p.get("objection_positions", {}),
                "customer_sentiment": p.get("customer_sentiment", "neutral"),
                "scores": p.get("scores", {}),
                "checklist": p.get("checklist", {}),
                "strengths": p.get("strengths", []),
                "coaching_points": p.get("coaching_points", []),
                "tags": p.get("tags", []),
                "manager_notes": p.get("manager_notes", ""),
                "score_overrides": p.get("score_overrides", {}),
                "call_date": call_date,
                "audio_url": p.get("audio_url", ""),
                "share_token": p.get("share_token", ""),
                "availability_decline": p.get("availability_decline", False),
                "turned_away": p.get("turned_away", False),
                "onsite_suggested": p.get("onsite_suggested", False),
                "call_quality": call_quality,
                "is_continuation": is_continuation,
                "continuation_group_id": continuation_group_id,
            }
            result = supa("POST", "calls", record)
            self._ok(result)
        except Exception as e:
            self._err(500, str(e))

    def _update(self, body):
        try:
            p = json.loads(body)
            cid = p.pop("id")
            supa("PATCH", f"calls?id=eq.{cid}", p)
            self._ok({"updated": True})
        except Exception as e:
            self._err(500, str(e))

    def _delete(self, body):
        try:
            p = json.loads(body)
            supa("DELETE", f"calls?id=eq.{p['id']}")
            self._ok({"deleted": True})
        except Exception as e:
            self._err(500, str(e))

    def _bulk_delete(self, body):
        try:
            p = json.loads(body)
            ids = p.get("ids", [])
            if not ids:
                self._ok({"deleted": 0}); return
            id_list = ",".join(f'"{i}"' for i in ids)
            supa("DELETE", f"calls?id=in.({id_list})")
            self._ok({"deleted": len(ids)})
        except Exception as e:
            self._err(500, str(e))

    # ── CORRECTIONS ──

    def _get_corrections(self):
        try:
            result = supa("GET", "corrections?order=created_at.desc&limit=200")
            self._ok(result)
        except Exception as e:
            self._err(500, str(e))

    def _get_audio_url(self):
        try:
            call_id = self.path.split("/audio_url/")[1].split("?")[0]
            result = supa("GET", f"calls?id=eq.{call_id}&select=filename&limit=1")
            if not result:
                self._err(404, "Call not found"); return
            filename = result[0].get("filename", "")
            if not filename:
                self._err(404, "No audio filename"); return
            signed_url = supa_storage_signed_url("call-audio", filename, expires=3600)
            if not signed_url:
                self._err(404, "Audio file not found in storage"); return
            self._ok({"url": signed_url})
        except Exception as e:
            self._err(500, str(e))

    def _save_correction(self, body):
        try:
            p = json.loads(body)
            record = {
                "call_id": p.get("call_id"),
                "category": p.get("category"),
                "original_score": p.get("original_score"),
                "corrected_score": p.get("corrected_score"),
                "manager_note": p.get("manager_note", ""),
                "used_in_prompt": p.get("used_in_prompt", True),
            }
            result = supa("POST", "corrections", record)
            self._ok(result)
        except Exception as e:
            self._err(500, str(e))

    # ── SHARE ──

    def _create_share(self, body):
        try:
            p = json.loads(body)
            token = secrets.token_urlsafe(32)
            record = {
                "token": token,
                "label": p.get("label", ""),
                "filters": p.get("filters", {}),
                "view_type": p.get("view_type", "location"),
                "view_level": p.get("view_level", "manager"),
                "rep_ids": p.get("rep_ids", []),
            }
            supa("POST", "shared_views", record)
            share_url = f"{self.headers.get('Origin', 'https://lgms-call-analyzer.onrender.com')}/share/{token}"
            self._ok({"token": token, "url": share_url})
        except Exception as e:
            self._err(500, str(e))

    def _delete_share(self, body):
        try:
            p = json.loads(body)
            supa("DELETE", f"shared_views?id=eq.{p['id']}")
            self._ok({"deleted": True})
        except Exception as e:
            self._err(500, str(e))

    def _get_shared_views(self):
        try:
            result = supa("GET", "shared_views?order=created_at.desc")
            self._ok(result)
        except Exception as e:
            self._err(500, str(e))

    def _get_shared_view_by_token(self):
        try:
            token = self.path.split("/share/")[1].split("?")[0]
            result = supa("GET", f"shared_views?token=eq.{token}&limit=1")
            if not result:
                self._err(404, "Share link not found"); return
            view = result[0]
            html = read_html()
            rep_ids_json = json.dumps(view.get("rep_ids", []))
            inject = f'<script>window.SHARE_TOKEN="{token}";window.SHARE_VIEW_LEVEL="{view.get("view_level","manager")}";window.SHARE_REP_IDS={rep_ids_json};window.SHARE_LABEL="{view.get("label","")}";</script>'
            html = html.replace(b"</head>", inject.encode() + b"</head>", 1)
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.end_headers()
            self.wfile.write(html)
        except Exception as e:
            self._err(500, str(e))

    # ── REPS ──

    def _get_reps(self):
        try:
            result = supa("GET", "reps?order=full_name.asc")
            self._ok(result)
        except Exception as e:
            self._err(500, str(e))

    def _save_rep(self, body):
        try:
            p = json.loads(body)
            record = {
                "full_name": p.get("full_name", ""),
                "nickname": p.get("nickname", ""),
                "location": p.get("location", ""),
                "alternate_names": p.get("alternate_names", []),
                "active": p.get("active", True),
            }
            result = supa("POST", "reps", record)
            self._ok(result)
        except Exception as e:
            self._err(500, str(e))

    def _update_rep(self, body):
        try:
            p = json.loads(body)
            rid = p.pop("id")
            supa("PATCH", f"reps?id=eq.{rid}", p)
            self._ok({"updated": True})
        except Exception as e:
            self._err(500, str(e))

    def _delete_rep(self, body):
        try:
            p = json.loads(body)
            supa("DELETE", f"reps?id=eq.{p['id']}")
            self._ok({"deleted": True})
        except Exception as e:
            self._err(500, str(e))

    def _dedup_reps(self, body):
        try:
            rep_names_raw = supa("GET", "calls?select=rep_name&order=rep_name.asc")
            unique_names = list(set(r["rep_name"] for r in rep_names_raw if r.get("rep_name") and r["rep_name"] != "Unknown"))
            if len(unique_names) < 2:
                self._ok({"suggestions": [], "message": "Not enough rep names"}); return

            dedup_prompt = f"""Analyze this list of sales rep names from a call center.
Identify names that likely refer to the same person (misspellings, nicknames, partial names, abbreviations).
Rep names: {json.dumps(unique_names)}
Return ONLY valid JSON (no markdown):
{{"suggestions":[{{"canonical":"John Smith","variants":["John","Johnny S"],"confidence":0.95,"reason":"Nickname and abbreviation variants"}}],"confidence_overall":0.90}}
If no duplicates found: {{"suggestions":[],"confidence_overall":1.0}}"""

            def call_claude(model):
                req_body = json.dumps({"model": model, "max_tokens": 1000, "messages": [{"role": "user", "content": dedup_prompt}]}).encode()
                req = urllib.request.Request("https://api.anthropic.com/v1/messages", data=req_body,
                    headers={"Content-Type": "application/json", "x-api-key": API_KEY, "anthropic-version": "2023-06-01"}, method="POST")
                with urllib.request.urlopen(req, timeout=60) as r:
                    resp = json.loads(r.read())
                tb = next((b for b in resp.get("content", []) if b.get("type") == "text"), None)
                if not tb: raise Exception("No response")
                raw = tb["text"].strip().lstrip("```json").lstrip("```").rstrip("```").strip()
                return json.loads(raw)

            result = call_claude("claude-sonnet-4-6")
            if result.get("confidence_overall", 1.0) < 0.85:
                print("  Dedup: escalating to Opus")
                result = call_claude("claude-opus-4-6")

            self._ok(result)
        except Exception as e:
            self._err(500, str(e))

    def _bulk_rename_rep(self, body):
        try:
            p = json.loads(body)
            old_name = p.get("old_name", "")
            new_name = p.get("new_name", "")
            if not old_name or not new_name:
                self._err(400, "old_name and new_name required"); return
            result = supa("PATCH", f"calls?rep_name=eq.{urllib.parse.quote(old_name)}", {"rep_name": new_name},
                         extra_headers={"Prefer": "return=representation,count=exact"})
            count = len(result) if isinstance(result, list) else 0
            self._ok({"renamed": count, "old_name": old_name, "new_name": new_name})
        except Exception as e:
            self._err(500, str(e))

    # ── RE-ANALYZE ──

    def _reanalyze_start(self, body):
        global _reanalyze_job
        with _reanalyze_lock:
            if _reanalyze_job["status"] == "running":
                self._ok({"message": "Already running", "status": _reanalyze_job}); return
            _reanalyze_job = {
                "status": "running",
                "total": 0, "processed": 0, "current": "", "errors": 0,
                "started_at": datetime.now(timezone.utc).isoformat(),
                "finished_at": None,
            }
        t = threading.Thread(target=_reanalyze_worker, daemon=True)
        t.start()
        self._ok({"message": "Started", "status": _reanalyze_job})

    def _reanalyze_status(self):
        with _reanalyze_lock:
            status = dict(_reanalyze_job)
        self._ok(status)

    # ── EXPORT ──

    def _export_csv(self):
        try:
            calls = supa("GET", "calls?order=created_at.desc&limit=5000")
            ck_keys = ["got_move_date","got_customer_name","got_phone_number","got_cities","got_home_type","got_stairs_info","did_full_inventory","asked_forgotten_items","asked_about_boxes","gave_price_on_call","pitched_fvp","attempted_to_close","offered_email_estimate","mentioned_confirmations","thanked_customer","asked_name_at_start","led_estimate_process","scheduled_onsite_attempt","offered_alternatives","took_rapport_opportunities","completed_booking_wrapup","captured_lead"]
            hdrs = ["Date","Call Date","Rep","Caller","Purpose","Type","Move","Outcome","Sentiment","Excluded","Overall","Rapport","Info Seq","Price","FVP","Closing","Control","Prof.","Compliance%","Talk Rep%","Talk Cust%","Words","Quality","Declined","Onsite","Continuation","Keywords","Objections","Strengths","Coaching"]
            rows = []
            for c in calls:
                s = c.get("scores", {})
                comp = round(sum(1 for k in ck_keys if c.get("checklist", {}).get(k)) / len(ck_keys) * 100)
                rows.append([
                    (c.get("created_at",""))[:10], (c.get("call_date","") or "")[:10],
                    c.get("rep_name",""), c.get("caller_name",""), c.get("call_purpose",""),
                    c.get("call_type",""), c.get("move_type",""), c.get("call_outcome",""),
                    c.get("customer_sentiment",""), "Yes" if c.get("exclude_from_scoring") else "No",
                    s.get("overall",{}).get("score",""), s.get("rapport_tone",{}).get("score",""),
                    s.get("info_sequence",{}).get("score",""), s.get("price_delivery",{}).get("score",""),
                    s.get("fvp_pitch",{}).get("score",""), s.get("closing_attempt",{}).get("score",""),
                    s.get("call_control",{}).get("score",""), s.get("professionalism",{}).get("score",""),
                    comp, c.get("talk_ratio_rep",""), c.get("talk_ratio_customer",""), c.get("word_count",""),
                    c.get("call_quality","normal"), "Yes" if c.get("availability_decline") else "No",
                    "Yes" if c.get("onsite_suggested") else "No", "Yes" if c.get("is_continuation") else "No",
                    "; ".join(c.get("keywords_detected") or []), "; ".join(c.get("objections_detected") or []),
                    "; ".join(c.get("strengths") or []), "; ".join(c.get("coaching_points") or []),
                ])
            csv_text = "\n".join(",".join(f'"{str(v).replace(chr(34),chr(34)+chr(34))}"' for v in row) for row in [hdrs] + rows)
            self.send_response(200)
            self._cors()
            self.send_header("Content-Type", "text/csv")
            self.send_header("Content-Disposition", f'attachment; filename="lgms_calls_{datetime.now().strftime("%Y-%m-%d")}.csv"')
            self.end_headers()
            self.wfile.write(csv_text.encode("utf-8"))
        except Exception as e:
            self._err(500, str(e))

    def _export_pdf_call(self):
        try:
            call_id = self.path.split("/export/pdf/call/")[1].split("?")[0]
            result = supa("GET", f"calls?id=eq.{call_id}&limit=1")
            if not result:
                self._err(404, "Call not found"); return
            pdf_bytes = generate_call_pdf(result[0])
            self.send_response(200)
            self._cors()
            self.send_header("Content-Type", "application/pdf")
            self.send_header("Content-Disposition", f'attachment; filename="scorecard_{call_id[:8]}.pdf"')
            self.end_headers()
            self.wfile.write(pdf_bytes)
        except Exception as e:
            self._err(500, str(e))

    def _export_pdf_rep(self):
        try:
            rep_name = urllib.parse.unquote(self.path.split("/export/pdf/rep/")[1].split("?")[0])
            calls = supa("GET", f"calls?rep_name=eq.{urllib.parse.quote(rep_name)}&order=created_at.desc&limit=100")
            if not calls:
                self._err(404, "No calls found"); return
            sc_calls = [c for c in calls if not c.get("exclude_from_scoring")]
            avg_overall = round(sum(c.get("scores",{}).get("overall",{}).get("score",0) for c in sc_calls) / max(len(sc_calls),1), 1)
            summary_call = {
                "rep_name": rep_name,
                "caller_name": f"{len(sc_calls)} scored calls",
                "call_outcome": f"Avg overall: {avg_overall}/10",
                "call_summary": f"Rep profile for {rep_name}. Total: {len(calls)} calls. Scored: {len(sc_calls)}. Avg overall: {avg_overall}/10.",
                "scores": {sk: {"score": round(sum(c.get("scores",{}).get(sk,{}).get("score",0) for c in sc_calls)/max(len(sc_calls),1),1), "note": f"Avg across {len(sc_calls)} calls"} for sk in ["info_sequence","price_delivery","fvp_pitch","closing_attempt","call_control","professionalism","rapport_tone","overall"]},
                "checklist": {},
                "coaching_points": list({cp for c in sc_calls for cp in (c.get("coaching_points") or [])}),
                "manager_notes": "",
                "score_overrides": {},
                "created_at": datetime.now().isoformat(),
            }
            pdf_bytes = generate_call_pdf(summary_call)
            safe = re.sub(r'[^a-zA-Z0-9_]', '_', rep_name)
            self.send_response(200)
            self._cors()
            self.send_header("Content-Type", "application/pdf")
            self.send_header("Content-Disposition", f'attachment; filename="rep_{safe}.pdf"')
            self.end_headers()
            self.wfile.write(pdf_bytes)
        except Exception as e:
            self._err(500, str(e))

    # ── HELPERS ──

    def _ok(self, data):
        body = json.dumps(data).encode()
        self.send_response(200)
        self._cors()
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(body)

    def _err(self, code, msg):
        body = json.dumps({"error": msg}).encode()
        self.send_response(code)
        self._cors()
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(body)

    def _cors(self):
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "POST,GET,OPTIONS,DELETE,PATCH")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")


if __name__ == "__main__":
    print("=" * 55)
    print("  Little Guys Movers — Call Analyzer Server v8")
    print("=" * 55)
    missing = [v for v in ["ANTHROPIC_API_KEY","SUPABASE_URL","SUPABASE_KEY","OPENAI_API_KEY"] if not os.environ.get(v)]
    if missing:
        print("\n  WARNING: Missing env vars: " + ", ".join(missing))
    else:
        print("\n  All environment variables loaded")
    print(f"  Running at http://127.0.0.1:{PORT}")
    print("  Press Ctrl+C to stop\n")
    HTTPServer(("0.0.0.0", PORT), Handler).serve_forever()
