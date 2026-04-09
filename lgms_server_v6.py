"""
Little Guys Movers - Call Analyzer Server v7
=============================================
Required environment variables:
  ANTHROPIC_API_KEY  - Anthropic API key
  SUPABASE_URL       - Supabase project URL
  SUPABASE_KEY       - Supabase anon/publishable key
  OPENAI_API_KEY     - OpenAI API key for Whisper transcription
  PORT               - optional, defaults to 8765
"""

import os, json, urllib.request, urllib.error, urllib.parse, tempfile, mimetypes, zipfile, io, secrets, re, uuid, asyncio
from http.server import HTTPServer, BaseHTTPRequestHandler
from datetime import datetime, timezone

API_KEY      = os.environ.get("ANTHROPIC_API_KEY", "")
SUPABASE_URL = os.environ.get("SUPABASE_URL", "")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY", "")
OPENAI_KEY   = os.environ.get("OPENAI_API_KEY", "")
PORT         = int(os.environ.get("PORT", 8765))

# ──────────────────────────────────────────────
# CLAUDE PROMPT
# ──────────────────────────────────────────────

LGMS_PROMPT = """You are a sales call evaluator for Little Guys Movers (LGMS).

FIRST — classify this call:
- Count approximately how many words are in the transcript
- Determine the call type: "sales_estimate", "follow_up", "complaint", "booking_confirmation", "non_sales", "too_short", or "other"
- If the transcript has fewer than 80 words OR is clearly not a sales estimate call, set call_type accordingly and set exclude_from_scoring to true

SECOND — detect the rep name:
- Look for how the rep introduces themselves (e.g. "This is JD", "Hi this is Manning", "Thank you for calling Little Guys this is Sarah")
- Return the rep first name or full name if found, else return "Unknown"

THIRD — detect call quality issues:
- "disconnected": abrupt ending with no conclusion, "Hello? Hello?" patterns, very short (<60 sec of transcript content)
- "poor_audio": heavy [inaudible] density (more than 5 occurrences), one-sided conversation
- "normal": otherwise
- Short disconnected calls: set exclude_from_scoring=true

FOURTH — detect call flags:
- availability_decline: customer said they are NOT available or the move date doesn't work
- onsite_suggested: customer or rep mentioned an onsite visit/estimate would be preferred or needed
- is_continuation: transcript contains phrases like "calling back about", "as we discussed", "following up on", "this is a follow-up", "called earlier", "spoke earlier"

REQUIRED CALL SEQUENCE TO EVALUATE (22 steps):
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
16. Asked customer's name at the START of the call (not mid-call or never)
17. Led the estimate process — rep controlled the flow (Navigator/Pilot), did NOT wait for customer to pull info out
18. Attempted to schedule onsite if customer mentioned preference for onsite visit
19. Offered alternative solutions if customer hesitated on full-service (pack-only, load-only, National Express, etc.) — evaluate as customer-first problem solving, not a hard requirement
20. Took rapport opportunities — when customer volunteered personal info (parents moving, stressful situation, etc.), rep acknowledged and connected
21. Booking wrap-up — reviewed service, confirmed nothing missing, explained confirmation calls + crew call-ahead
22. Lead captured — name, number, notes logged even if no booking

KEY RULES:
- Simple moves (apt, 1-2BR): quote HOURLY with price RANGE
- Always attempt to close
- Give price on first contact
- Be confident, friendly, knowledgeable
- Rep should sound engaged and enthusiastic — not flat or bothered
- Rep should make customer feel they chose the right company

TALK RATIO: Count rep lines vs customer lines, express as percentages. Ideal: rep 40% / customer 60%.

KEYWORDS — check if rep said these:
- "Full Value Protection" or "FVP"
- "confirmation call"
- "hourly" or "per hour"
- "moving boxes"
- "fuel" or "fuel charge"
- "declared value"
- "schedule" or "get you on the calendar"
- "Little Guys" or "Little Guys Movers"

For each keyword in keywords_detected and each objection in objections_detected, also return the CHARACTER POSITION (integer) of its first occurrence in the transcript as:
  keyword_positions: {"Full Value Protection": 342, ...}
  objection_positions: {"Price too high": 891, ...}

OBJECTIONS — identify any customer objections:
Price too high, Need to think about it, Already have another quote, Wrong timing, Need to check with partner, Other

SENTIMENT: positive / neutral / hesitant / negative
OUTCOME: booked / estimate_sent / lost / unknown

Score 1-10. Be honest and critical. 10 = near perfect.
For too_short or non_sales calls, score everything 0.

RAPPORT & TONE scoring (1-10):
- 1-3: Rep sounded flat, bothered, or disengaged; missed obvious rapport opportunities; customer was fishing for trust and didn't find it
- 4-6: Rep was polite but mechanical; some warmth but rapport opportunities missed
- 7-8: Rep was warm and engaged; took most rapport opportunities; customer felt heard
- 9-10: Rep was excellent — enthusiastic, built genuine trust, customer clearly felt this was the right company

Respond ONLY with valid JSON, no markdown:

{"rep_name_detected":"name or Unknown","caller_name":"name or Unknown","call_purpose":"short phrase","call_type":"sales_estimate","move_type":"local/long distance/unknown","call_outcome":"booked/estimate_sent/lost/unknown","word_count":150,"exclude_from_scoring":false,"exclusion_reason":"","call_quality":"normal","availability_decline":false,"onsite_suggested":false,"is_continuation":false,"call_summary":"3-5 sentences","key_details_captured":"details gathered","talk_ratio_rep":40,"talk_ratio_customer":60,"keywords_detected":["Full Value Protection"],"keyword_positions":{"Full Value Protection":342},"objections_detected":["Price too high"],"objection_positions":{"Price too high":891},"customer_sentiment":"positive","scores":{"info_sequence":{"score":0,"note":""},"price_delivery":{"score":0,"note":""},"fvp_pitch":{"score":0,"note":""},"closing_attempt":{"score":0,"note":""},"call_control":{"score":0,"note":""},"professionalism":{"score":0,"note":""},"rapport_tone":{"score":0,"note":""},"overall":{"score":0,"note":""}},"checklist":{"got_move_date":false,"got_customer_name":false,"got_phone_number":false,"got_cities":false,"got_home_type":false,"got_stairs_info":false,"did_full_inventory":false,"asked_forgotten_items":false,"asked_about_boxes":false,"gave_price_on_call":false,"pitched_fvp":false,"attempted_to_close":false,"offered_email_estimate":false,"mentioned_confirmations":false,"thanked_customer":false,"asked_name_at_start":false,"led_estimate_process":false,"scheduled_onsite_attempt":false,"offered_alternatives":false,"took_rapport_opportunities":false,"completed_booking_wrapup":false,"captured_lead":false},"strengths":["s1","s2"],"coaching_points":["c1","c2"]}"""

# ──────────────────────────────────────────────
# VONAGE FILENAME DATE PARSER
# ──────────────────────────────────────────────

def parse_call_date_from_filename(filename):
    """
    Vonage pattern: 2026_04_01_01_54PM_3011_1214....mp3
    Returns ISO 8601 string or None
    """
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

def supa(method, path, body=None):
    if not SUPABASE_URL or not SUPABASE_KEY:
        raise Exception("Supabase not configured")
    url = f"{SUPABASE_URL}/rest/v1/{path}"
    data = json.dumps(body).encode() if body else None
    req = urllib.request.Request(url, data=data, method=method)
    req.add_header("apikey", SUPABASE_KEY)
    req.add_header("Authorization", f"Bearer {SUPABASE_KEY}")
    req.add_header("Content-Type", "application/json")
    req.add_header("Prefer", "return=representation")
    with urllib.request.urlopen(req, timeout=30) as r:
        return json.loads(r.read())

def supa_storage_upload(bucket, path, data, content_type="audio/mpeg"):
    """Upload a file to Supabase Storage"""
    url = f"{SUPABASE_URL}/storage/v1/object/{bucket}/{path}"
    req = urllib.request.Request(url, data=data, method="POST")
    req.add_header("apikey", SUPABASE_KEY)
    req.add_header("Authorization", f"Bearer {SUPABASE_KEY}")
    req.add_header("Content-Type", content_type)
    with urllib.request.urlopen(req, timeout=120) as r:
        return json.loads(r.read())

def supa_storage_signed_url(bucket, path, expires=86400):
    """Get a signed URL for a stored file"""
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
    """List all files in a storage bucket"""
    url = f"{SUPABASE_URL}/storage/v1/object/list/{bucket}"
    body = json.dumps({"prefix": "", "limit": 1000}).encode()
    req = urllib.request.Request(url, data=body, method="POST")
    req.add_header("apikey", SUPABASE_KEY)
    req.add_header("Authorization", f"Bearer {SUPABASE_KEY}")
    req.add_header("Content-Type", "application/json")
    with urllib.request.urlopen(req, timeout=30) as r:
        return json.loads(r.read())

def supa_storage_delete(bucket, paths):
    """Delete files from storage"""
    url = f"{SUPABASE_URL}/storage/v1/object/{bucket}"
    body = json.dumps({"prefixes": paths}).encode()
    req = urllib.request.Request(url, data=body, method="DELETE")
    req.add_header("apikey", SUPABASE_KEY)
    req.add_header("Authorization", f"Bearer {SUPABASE_KEY}")
    req.add_header("Content-Type", "application/json")
    with urllib.request.urlopen(req, timeout=30) as r:
        return json.loads(r.read())

def enforce_storage_cap():
    """
    Check total bucket size. If over 900MB, delete oldest files until under 800MB.
    Uses calls table created_at to find oldest audio_url entries.
    """
    try:
        files = supa_storage_list("call-audio")
        if not files:
            return
        total_bytes = sum(f.get("metadata", {}).get("size", 0) for f in files if isinstance(f, dict))
        cap_bytes = 900 * 1024 * 1024  # 900MB
        target_bytes = 800 * 1024 * 1024  # 800MB
        if total_bytes <= cap_bytes:
            return
        # Get calls ordered by oldest, that have audio_url set
        calls_with_audio = supa("GET", "calls?audio_url=neq.&order=created_at.asc&select=id,audio_url,created_at&limit=200")
        for call in calls_with_audio:
            if total_bytes <= target_bytes:
                break
            audio_url = call.get("audio_url", "")
            if not audio_url:
                continue
            # Extract storage path from URL
            path_match = re.search(r'/call-audio/(.+?)(\?|$)', audio_url)
            if path_match:
                storage_path = path_match.group(1)
                try:
                    # Find file size
                    file_info = next((f for f in files if isinstance(f, dict) and f.get("name") == storage_path), None)
                    file_size = file_info.get("metadata", {}).get("size", 0) if file_info else 0
                    supa_storage_delete("call-audio", [storage_path])
                    supa("PATCH", f"calls?id=eq.{call['id']}", {"audio_url": ""})
                    total_bytes -= file_size
                except Exception as e:
                    print(f"  Storage cleanup error for {storage_path}: {e}")
    except Exception as e:
        print(f"  Storage cap enforcement error: {e}")

# ──────────────────────────────────────────────
# CONTINUATION GROUP LINKING
# ──────────────────────────────────────────────

def find_or_create_continuation_group(rep_name, caller_name):
    """
    Look for existing calls with same rep + caller within last 7 days.
    Returns existing group_id or new uuid.
    """
    try:
        from datetime import timedelta
        cutoff = (datetime.now(timezone.utc) - timedelta(days=7)).isoformat()
        safe_rep = urllib.parse.quote(rep_name or "")
        safe_caller = urllib.parse.quote(caller_name or "")
        existing = supa("GET", f"calls?rep_name=eq.{safe_rep}&caller_name=eq.{safe_caller}&created_at=gte.{cutoff}&is_continuation=eq.true&order=created_at.desc&limit=1&select=continuation_group_id")
        if existing and existing[0].get("continuation_group_id"):
            return existing[0]["continuation_group_id"]
    except Exception as e:
        print(f"  Continuation group lookup error: {e}")
    return str(uuid.uuid4())

def retroactively_link_continuation(rep_name, caller_name, group_id):
    """
    When a new continuation is detected, check if there are recent calls from the same
    rep+caller that don't yet have a group_id, and link them.
    """
    try:
        from datetime import timedelta
        cutoff = (datetime.now(timezone.utc) - timedelta(days=7)).isoformat()
        safe_rep = urllib.parse.quote(rep_name or "")
        safe_caller = urllib.parse.quote(caller_name or "")
        unlinked = supa("GET", f"calls?rep_name=eq.{safe_rep}&caller_name=eq.{safe_caller}&created_at=gte.{cutoff}&continuation_group_id=eq.&select=id")
        for call in (unlinked or []):
            supa("PATCH", f"calls?id=eq.{call['id']}", {"continuation_group_id": group_id, "is_continuation": True})
    except Exception as e:
        print(f"  Retroactive linking error: {e}")

# ──────────────────────────────────────────────
# HTML DASHBOARD LOADER
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
    prompt = f"Filename: {filename}\n\nTranscript:\n{transcript}\n\n{LGMS_PROMPT}"
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
# PDF EXPORT (WeasyPrint)
# ──────────────────────────────────────────────

def generate_call_pdf(call):
    try:
        from weasyprint import HTML
    except ImportError:
        raise Exception("WeasyPrint not installed. Add 'weasyprint' to requirements.txt")

    scores = call.get("scores", {})
    checklist = call.get("checklist", {})
    overrides = call.get("score_overrides", {})

    score_keys = ["info_sequence", "price_delivery", "fvp_pitch", "closing_attempt",
                  "call_control", "professionalism", "rapport_tone", "overall"]
    score_labels = ["Info Sequence", "Price Delivery", "FVP Pitch", "Closing Attempt",
                    "Call Control", "Professionalism", "Rapport & Tone", "Overall"]

    checklist_keys = ["got_move_date", "got_customer_name", "got_phone_number", "got_cities",
                      "got_home_type", "got_stairs_info", "did_full_inventory", "asked_forgotten_items",
                      "asked_about_boxes", "gave_price_on_call", "pitched_fvp", "attempted_to_close",
                      "offered_email_estimate", "mentioned_confirmations", "thanked_customer",
                      "asked_name_at_start", "led_estimate_process", "scheduled_onsite_attempt",
                      "offered_alternatives", "took_rapport_opportunities", "completed_booking_wrapup",
                      "captured_lead"]
    checklist_labels = ["Move date", "Customer name", "Phone number", "Cities/locations",
                        "Home type (load & unload)", "Stairs info", "Full inventory",
                        "Forgotten items", "Moving boxes", "Gave price on call", "FVP pitched",
                        "Closing attempt", "Email estimate offered", "Confirmations mentioned",
                        "Thanked customer", "Asked name at start", "Led estimate process",
                        "Scheduled onsite attempt", "Offered alternatives", "Took rapport opportunities",
                        "Completed booking wrap-up", "Lead captured"]

    def score_color(s):
        if s >= 8: return "#16a34a"
        if s >= 5: return "#d97706"
        return "#dc2626"

    scores_html = ""
    for k, label in zip(score_keys, score_labels):
        s = overrides.get(k) or scores.get(k, {}).get("score", 0)
        note = scores.get(k, {}).get("note", "")
        color = score_color(s)
        scores_html += f"""
        <div class="score-item">
          <div class="score-label">{label}</div>
          <div class="score-val" style="color:{color}">{s}/10</div>
          <div class="score-bar"><div style="width:{s*10}%;background:{color};height:100%;border-radius:3px"></div></div>
          <div class="score-note">{note}</div>
        </div>"""

    checklist_html = ""
    for k, label in zip(checklist_keys, checklist_labels):
        checked = checklist.get(k, False)
        icon = "✅" if checked else "❌"
        checklist_html += f'<div class="ck-item">{icon} {label}</div>'

    dt = datetime.fromisoformat(call.get("created_at", datetime.now().isoformat()).replace("Z", "+00:00"))
    formatted_date = dt.strftime("%B %d, %Y at %I:%M %p")

    html_content = f"""
    <!DOCTYPE html><html><head><meta charset="UTF-8">
    <style>
      body {{ font-family: Arial, sans-serif; color: #231f20; margin: 0; padding: 24px; font-size: 13px; }}
      .header {{ background: #4a7c3f; color: white; padding: 20px 24px; border-radius: 8px; margin-bottom: 20px; }}
      .header h1 {{ margin: 0 0 4px; font-size: 20px; }}
      .header p {{ margin: 0; opacity: .85; font-size: 12px; }}
      .section {{ margin-bottom: 20px; }}
      .section h2 {{ font-size: 13px; font-weight: 700; color: #4a7c3f; text-transform: uppercase;
                     letter-spacing: .5px; border-bottom: 2px solid #e5e7eb; padding-bottom: 6px; margin-bottom: 12px; }}
      .meta-grid {{ display: grid; grid-template-columns: 1fr 1fr; gap: 8px; }}
      .meta-item {{ background: #f3f4f6; border-radius: 6px; padding: 8px 10px; }}
      .meta-label {{ font-size: 9px; font-weight: 700; color: #6b7280; text-transform: uppercase; letter-spacing: .4px; }}
      .meta-val {{ font-size: 13px; font-weight: 600; margin-top: 2px; }}
      .score-item {{ margin-bottom: 10px; }}
      .score-label {{ font-size: 11px; font-weight: 600; color: #374151; margin-bottom: 2px; }}
      .score-val {{ font-size: 16px; font-weight: 700; }}
      .score-bar {{ height: 5px; background: #e5e7eb; border-radius: 3px; margin: 3px 0; }}
      .score-note {{ font-size: 11px; color: #6b7280; }}
      .ck-grid {{ display: grid; grid-template-columns: 1fr 1fr; gap: 4px; }}
      .ck-item {{ font-size: 12px; padding: 3px 0; }}
      .summary {{ background: #e8f4e6; border-left: 4px solid #4a7c3f; padding: 12px; border-radius: 4px; font-size: 13px; line-height: 1.7; }}
      .coaching {{ background: #fff7ed; border-left: 4px solid #d97706; padding: 12px; border-radius: 4px; }}
      .coaching li {{ font-size: 12px; margin-bottom: 4px; }}
      .notes {{ background: #f9fafb; border: 1px solid #e5e7eb; padding: 12px; border-radius: 6px; font-size: 13px; }}
      .footer {{ margin-top: 30px; text-align: center; font-size: 10px; color: #9ca3af; border-top: 1px solid #e5e7eb; padding-top: 12px; }}
    </style></head><body>
    <div class="header">
      <h1>Call Scorecard — {call.get('rep_name', 'Unknown Rep')}</h1>
      <p>{formatted_date} · {call.get('caller_name', 'Unknown Caller')} · {call.get('call_outcome', 'unknown').replace('_', ' ').title()}</p>
    </div>

    <div class="section">
      <h2>Call Details</h2>
      <div class="meta-grid">
        <div class="meta-item"><div class="meta-label">Rep</div><div class="meta-val">{call.get('rep_name', '—')}</div></div>
        <div class="meta-item"><div class="meta-label">Caller</div><div class="meta-val">{call.get('caller_name', '—')}</div></div>
        <div class="meta-item"><div class="meta-label">Purpose</div><div class="meta-val">{call.get('call_purpose', '—')}</div></div>
        <div class="meta-item"><div class="meta-label">Move Type</div><div class="meta-val">{call.get('move_type', '—')}</div></div>
        <div class="meta-item"><div class="meta-label">Outcome</div><div class="meta-val">{call.get('call_outcome', '—').replace('_', ' ').title()}</div></div>
        <div class="meta-item"><div class="meta-label">Sentiment</div><div class="meta-val">{call.get('customer_sentiment', '—').title()}</div></div>
      </div>
    </div>

    <div class="section">
      <h2>Summary</h2>
      <div class="summary">{call.get('call_summary', 'No summary available.')}</div>
    </div>

    <div class="section">
      <h2>Performance Scores</h2>
      {scores_html}
    </div>

    <div class="section">
      <h2>22-Step Checklist</h2>
      <div class="ck-grid">{checklist_html}</div>
    </div>

    <div class="section">
      <h2>Coaching Points</h2>
      <div class="coaching"><ul>{''.join(f'<li>{p}</li>' for p in call.get('coaching_points', []) or ['No coaching points recorded.'])}</ul></div>
    </div>

    {"<div class='section'><h2>Manager Notes</h2><div class='notes'>" + call.get('manager_notes', '') + "</div></div>" if call.get('manager_notes') else ""}

    <div class="footer">Little Guys Movers — Call Intelligence Platform · Generated {datetime.now().strftime('%B %d, %Y')}</div>
    </body></html>"""

    pdf_bytes = HTML(string=html_content).write_pdf()
    return pdf_bytes

# ──────────────────────────────────────────────
# HTTP HANDLER
# ──────────────────────────────────────────────

class Handler(BaseHTTPRequestHandler):
    def log_message(self, fmt, *args):
        print(f"  {args[0]} {args[1]}")

    def do_GET(self):
        if self.path.startswith("/calls"):
            self._get_calls()
        elif self.path.startswith("/reps"):
            self._get_reps()
        elif self.path.startswith("/shared_views"):
            self._get_shared_views()
        elif self.path.startswith("/share/"):
            self._get_shared_view_by_token()
        elif self.path.startswith("/export/csv"):
            self._export_csv()
        elif self.path.startswith("/reanalyze/stream"):
            self._reanalyze_stream()
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
            "/reps/save": self._save_rep,
            "/reps/update": self._update_rep,
            "/reps/delete": self._delete_rep,
            "/reps/deduplicate": self._dedup_reps,
            "/reps/bulk_rename": self._bulk_rename_rep,
        }
        # Handle dynamic export routes
        if self.path.startswith("/export/pdf/call/"):
            self._export_pdf_call(body)
            return
        if self.path.startswith("/export/pdf/rep/"):
            self._export_pdf_rep(body)
            return
        fn = routes.get(self.path)
        if fn:
            fn(body)
        else:
            self.send_response(404)
            self.end_headers()

    # ── CALLS ──

    def _get_calls(self):
        try:
            calls = supa("GET", "calls?order=created_at.desc&limit=1000")
            self._ok(calls)
        except Exception as e:
            self._err(500, str(e))

    def _extract_zip(self, body):
        try:
            import base64
            p = json.loads(body)
            zip_b64 = p.get("zip", "")
            zip_bytes = base64.b64decode(zip_b64)
            audio_exts = {".mp3", ".m4a", ".wav", ".ogg", ".mp4", ".webm", ".mpeg", ".mpga"}
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
                    audio_b64 = base64.b64encode(audio_data).decode()
                    results.append({"filename": filename, "audio": audio_b64, "size": len(audio_data)})
            self._ok({"files": results, "count": len(results)})
        except zipfile.BadZipFile:
            self._err(400, "Invalid zip file")
        except Exception as e:
            self._err(500, f"Zip extraction failed: {str(e)}")

    def _check_dup_endpoint(self, body):
        try:
            p = json.loads(body)
            filename = p.get("filename", "")
            is_dup = self._check_filename_exists(filename)
            self._ok({"duplicate": is_dup, "filename": filename})
        except Exception:
            self._ok({"duplicate": False})

    def _transcribe_and_analyze(self, body):
        if not OPENAI_KEY:
            self._err(500, "OPENAI_API_KEY not set")
            return
        if not API_KEY:
            self._err(500, "ANTHROPIC_API_KEY not set")
            return
        content_type = self.headers.get("Content-Type", "")
        if "application/json" not in content_type:
            self._err(400, "Content-Type must be application/json with base64 audio")
            return
        try:
            import base64
            p = json.loads(body)
            audio_b64 = p.get("audio", "")
            filename = p.get("filename", "call.mp3")
            audio_bytes = base64.b64decode(audio_b64)
        except Exception as e:
            self._err(400, f"Bad request: {e}")
            return

        # Step 1: Transcribe
        try:
            print(f"  Transcribing {filename} ({len(audio_bytes)} bytes)...")
            transcript = transcribe_audio(audio_bytes, filename)
            print(f"  Transcription done: {len(transcript)} chars")
            if not transcript or not transcript.strip():
                self._err(400, "Transcription returned empty — audio may be too short or silent")
                return
        except urllib.error.HTTPError as e:
            err_body = e.read().decode()
            self._err(e.code, f"Whisper API error: {err_body}")
            return
        except Exception as e:
            self._err(500, f"Transcription failed: {str(e)}")
            return

        # Step 2: Analyze with Claude
        try:
            result = run_claude_analysis(transcript, filename)
            result["transcript"] = transcript
            result["filename"] = filename

            # Step 3: Upload audio to Supabase Storage (best effort)
            try:
                enforce_storage_cap()
                storage_path = f"{filename}"
                supa_storage_upload("call-audio", storage_path, audio_bytes, "audio/mpeg")
                signed_url = supa_storage_signed_url("call-audio", storage_path)
                result["audio_url"] = signed_url
            except Exception as e:
                print(f"  Audio storage warning: {e}")
                result["audio_url"] = ""

            self._ok(result)
        except urllib.error.HTTPError as e:
            err_body = e.read().decode()
            self._err(e.code, f"Claude API error: {err_body}")
        except json.JSONDecodeError as e:
            self._err(500, f"Could not parse Claude response as JSON: {str(e)}")
        except Exception as e:
            self._err(500, f"Analysis failed: {str(e)}")

    def _analyze(self, body):
        if not API_KEY:
            self._err(500, "ANTHROPIC_API_KEY not set")
            return
        try:
            p = json.loads(body)
            transcript = p.get("transcript", "").strip()
            filename = p.get("filename", "call.txt")
        except Exception as e:
            self._err(400, str(e))
            return
        if not transcript:
            self._err(400, "No transcript")
            return
        try:
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
                        return True, f"Duplicate content (matches {c.get('filename', 'unknown')})"
            return False, ""
        except Exception as e:
            print(f"  Duplicate check error: {e}")
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

            # Duplicate check
            if p.get("check_duplicate", True):
                is_dup, dup_reason = self._check_duplicate(p.get("filename", ""), p.get("transcript", ""))
                if is_dup:
                    self._ok({"duplicate": True, "reason": dup_reason})
                    return

            # Parse call date from filename
            call_date = parse_call_date_from_filename(p.get("filename", ""))

            # Handle continuation group
            continuation_group_id = ""
            is_continuation = p.get("is_continuation", False)
            rep_name = p.get("rep_name") or p.get("rep_name_detected") or "Unknown"
            caller_name = p.get("caller_name", "")

            if is_continuation:
                continuation_group_id = find_or_create_continuation_group(rep_name, caller_name)
                retroactively_link_continuation(rep_name, caller_name, continuation_group_id)

            # Auto-exclude disconnected short calls
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
                "objections_detected": p.get("objections_detected", []),
                "customer_sentiment": p.get("customer_sentiment", "neutral"),
                "scores": p.get("scores", {}),
                "checklist": p.get("checklist", {}),
                "strengths": p.get("strengths", []),
                "coaching_points": p.get("coaching_points", []),
                "tags": p.get("tags", []),
                "manager_notes": p.get("manager_notes", ""),
                "score_overrides": p.get("score_overrides", {}),
                # New fields
                "call_date": call_date,
                "audio_url": p.get("audio_url", ""),
                "share_token": p.get("share_token", ""),
                "availability_decline": p.get("availability_decline", False),
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
            result = supa("PATCH", f"calls?id=eq.{cid}", p)
            self._ok(result)
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
                self._ok({"deleted": 0})
                return
            id_list = ",".join(f'"{i}"' for i in ids)
            supa("DELETE", f"calls?id=in.({id_list})")
            self._ok({"deleted": len(ids)})
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
                "view_type": p.get("view_type", "calls"),
                "view_level": p.get("view_level", "manager"),
                "rep_ids": p.get("rep_ids", []),
            }
            result = supa("POST", "shared_views", record)
            share_url = f"{SUPABASE_URL.replace('https://riovfkogzmcttwfuievn.supabase.co', 'https://lgms-call-analyzer.onrender.com')}/share/{token}"
            self._ok({"token": token, "url": share_url, "record": result})
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
                self._err(404, "Share link not found")
                return
            view = result[0]
            # Return the dashboard HTML — the frontend handles rep/manager view logic
            html = read_html()
            # Inject the token and view_level into the page
            inject = f'<script>window.SHARE_TOKEN="{token}";window.SHARE_VIEW_LEVEL="{view.get("view_level","manager")}";</script>'
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
            result = supa("PATCH", f"reps?id=eq.{rid}", p)
            self._ok(result)
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
        """
        AI-powered rep deduplication.
        Uses Sonnet first; escalates to Opus only if confidence < 0.85.
        """
        try:
            rep_names = supa("GET", "calls?select=rep_name&order=rep_name.asc")
            unique_names = list(set(r["rep_name"] for r in rep_names if r.get("rep_name") and r["rep_name"] != "Unknown"))

            if len(unique_names) < 2:
                self._ok({"suggestions": [], "message": "Not enough rep names to deduplicate"})
                return

            dedup_prompt = f"""You are analyzing a list of sales rep names from a call center. 
Identify any names that likely refer to the same person (misspellings, nicknames, partial names, etc).

Rep names: {json.dumps(unique_names)}

Return ONLY valid JSON:
{{"suggestions": [{{"canonical": "John Smith", "variants": ["John", "Johnny S", "J Smith"], "confidence": 0.95, "reason": "Same person — nickname and abbreviation variants"}}], "confidence_overall": 0.90}}

If no duplicates found, return {{"suggestions": [], "confidence_overall": 1.0}}"""

            # Try Sonnet first
            result_text = None
            try:
                req_body = json.dumps({
                    "model": "claude-sonnet-4-6",
                    "max_tokens": 1000,
                    "messages": [{"role": "user", "content": dedup_prompt}]
                }).encode()
                req = urllib.request.Request(
                    "https://api.anthropic.com/v1/messages", data=req_body,
                    headers={"Content-Type": "application/json", "x-api-key": API_KEY, "anthropic-version": "2023-06-01"},
                    method="POST"
                )
                with urllib.request.urlopen(req, timeout=60) as r:
                    resp = json.loads(r.read())
                tb = next((b for b in resp.get("content", []) if b.get("type") == "text"), None)
                if tb:
                    raw = tb["text"].strip().lstrip("```json").lstrip("```").rstrip("```").strip()
                    parsed = json.loads(raw)
                    overall_conf = parsed.get("confidence_overall", 1.0)

                    # Escalate to Opus if low confidence
                    if overall_conf < 0.85:
                        print("  Dedup: Low confidence from Sonnet, escalating to Opus...")
                        req_body2 = json.dumps({
                            "model": "claude-opus-4-6",
                            "max_tokens": 1000,
                            "messages": [{"role": "user", "content": dedup_prompt}]
                        }).encode()
                        req2 = urllib.request.Request(
                            "https://api.anthropic.com/v1/messages", data=req_body2,
                            headers={"Content-Type": "application/json", "x-api-key": API_KEY, "anthropic-version": "2023-06-01"},
                            method="POST"
                        )
                        with urllib.request.urlopen(req2, timeout=60) as r2:
                            resp2 = json.loads(r2.read())
                        tb2 = next((b for b in resp2.get("content", []) if b.get("type") == "text"), None)
                        if tb2:
                            raw2 = tb2["text"].strip().lstrip("```json").lstrip("```").rstrip("```").strip()
                            parsed = json.loads(raw2)

                    self._ok(parsed)
                    return
            except Exception as e:
                print(f"  Dedup analysis error: {e}")

            self._ok({"suggestions": [], "error": "Analysis failed"})
        except Exception as e:
            self._err(500, str(e))

    def _bulk_rename_rep(self, body):
        """Rename a rep across all historical calls"""
        try:
            p = json.loads(body)
            old_name = p.get("old_name", "")
            new_name = p.get("new_name", "")
            if not old_name or not new_name:
                self._err(400, "old_name and new_name required")
                return
            safe_old = urllib.parse.quote(old_name)
            result = supa("PATCH", f"calls?rep_name=eq.{safe_old}", {"rep_name": new_name})
            count = len(result) if isinstance(result, list) else 0
            self._ok({"renamed": count, "old_name": old_name, "new_name": new_name})
        except Exception as e:
            self._err(500, str(e))

    # ── EXPORT ──

    def _export_csv(self):
        try:
            calls = supa("GET", "calls?order=created_at.desc&limit=5000")
            # Parse query string filters if provided (basic support)
            hdrs = ["Date","Call Date","Rep","Caller","Purpose","Call Type","Move Type","Outcome","Sentiment",
                    "Excluded","Overall","Info Seq","Price","FVP","Closing","Control","Prof.","Rapport",
                    "Compliance%","Talk Rep%","Talk Cust%","Word Count","Call Quality",
                    "Availability Decline","Onsite Suggested","Is Continuation",
                    "Keywords","Objections","Strengths","Coaching Points"]
            ck_keys = ["got_move_date","got_customer_name","got_phone_number","got_cities","got_home_type",
                       "got_stairs_info","did_full_inventory","asked_forgotten_items","asked_about_boxes",
                       "gave_price_on_call","pitched_fvp","attempted_to_close","offered_email_estimate",
                       "mentioned_confirmations","thanked_customer","asked_name_at_start","led_estimate_process",
                       "scheduled_onsite_attempt","offered_alternatives","took_rapport_opportunities",
                       "completed_booking_wrapup","captured_lead"]
            rows = []
            for c in calls:
                s = c.get("scores", {})
                comp = round(sum(1 for k in ck_keys if c.get("checklist", {}).get(k)) / len(ck_keys) * 100)
                rows.append([
                    c.get("created_at", "")[:10],
                    (c.get("call_date") or "")[:10],
                    c.get("rep_name", ""), c.get("caller_name", ""),
                    c.get("call_purpose", ""), c.get("call_type", ""), c.get("move_type", ""),
                    c.get("call_outcome", ""), c.get("customer_sentiment", ""),
                    "Yes" if c.get("exclude_from_scoring") else "No",
                    s.get("overall", {}).get("score", ""), s.get("info_sequence", {}).get("score", ""),
                    s.get("price_delivery", {}).get("score", ""), s.get("fvp_pitch", {}).get("score", ""),
                    s.get("closing_attempt", {}).get("score", ""), s.get("call_control", {}).get("score", ""),
                    s.get("professionalism", {}).get("score", ""), s.get("rapport_tone", {}).get("score", ""),
                    comp, c.get("talk_ratio_rep", ""), c.get("talk_ratio_customer", ""), c.get("word_count", ""),
                    c.get("call_quality", "normal"),
                    "Yes" if c.get("availability_decline") else "No",
                    "Yes" if c.get("onsite_suggested") else "No",
                    "Yes" if c.get("is_continuation") else "No",
                    "; ".join(c.get("keywords_detected") or []),
                    "; ".join(c.get("objections_detected") or []),
                    "; ".join(c.get("strengths") or []),
                    "; ".join(c.get("coaching_points") or []),
                ])
            csv_rows = [hdrs] + rows
            csv_text = "\n".join(",".join(f'"{str(v).replace(chr(34), chr(34)+chr(34))}"' for v in row) for row in csv_rows)
            csv_bytes = csv_text.encode("utf-8")
            self.send_response(200)
            self._cors()
            self.send_header("Content-Type", "text/csv")
            self.send_header("Content-Disposition", f'attachment; filename="lgms_calls_{datetime.now().strftime("%Y-%m-%d")}.csv"')
            self.end_headers()
            self.wfile.write(csv_bytes)
        except Exception as e:
            self._err(500, str(e))

    def _export_pdf_call(self, body):
        try:
            call_id = self.path.split("/export/pdf/call/")[1]
            result = supa("GET", f"calls?id=eq.{call_id}&limit=1")
            if not result:
                self._err(404, "Call not found")
                return
            pdf_bytes = generate_call_pdf(result[0])
            filename = f"scorecard_{call_id[:8]}.pdf"
            self.send_response(200)
            self._cors()
            self.send_header("Content-Type", "application/pdf")
            self.send_header("Content-Disposition", f'attachment; filename="{filename}"')
            self.end_headers()
            self.wfile.write(pdf_bytes)
        except Exception as e:
            self._err(500, str(e))

    def _export_pdf_rep(self, body):
        try:
            rep_name = urllib.parse.unquote(self.path.split("/export/pdf/rep/")[1])
            calls = supa("GET", f"calls?rep_name=eq.{urllib.parse.quote(rep_name)}&order=created_at.desc&limit=100")
            if not calls:
                self._err(404, "No calls found for this rep")
                return
            # Build a rep summary PDF
            sc_calls = [c for c in calls if not c.get("exclude_from_scoring")]
            avg_score = round(sum(c.get("scores", {}).get("overall", {}).get("score", 0) for c in sc_calls) / max(len(sc_calls), 1), 1)
            rep_summary_call = {
                "rep_name": rep_name,
                "caller_name": f"{len(sc_calls)} scored calls",
                "call_outcome": f"Avg overall: {avg_score}/10",
                "call_summary": f"Rep profile for {rep_name}. Total calls: {len(calls)}. Scored calls: {len(sc_calls)}. Average overall score: {avg_score}/10.",
                "scores": {},
                "checklist": {},
                "coaching_points": list(set(cp for c in sc_calls for cp in (c.get("coaching_points") or []))),
                "manager_notes": "",
                "score_overrides": {},
                "created_at": datetime.now().isoformat(),
            }
            # Aggregate scores
            for sk in ["info_sequence", "price_delivery", "fvp_pitch", "closing_attempt", "call_control", "professionalism", "rapport_tone", "overall"]:
                avg = round(sum(c.get("scores", {}).get(sk, {}).get("score", 0) for c in sc_calls) / max(len(sc_calls), 1), 1)
                rep_summary_call["scores"][sk] = {"score": avg, "note": f"Average across {len(sc_calls)} calls"}
            pdf_bytes = generate_call_pdf(rep_summary_call)
            safe_name = re.sub(r'[^a-zA-Z0-9_]', '_', rep_name)
            self.send_response(200)
            self._cors()
            self.send_header("Content-Type", "application/pdf")
            self.send_header("Content-Disposition", f'attachment; filename="rep_profile_{safe_name}.pdf"')
            self.end_headers()
            self.wfile.write(pdf_bytes)
        except Exception as e:
            self._err(500, str(e))

    # ── BULK RE-ANALYZE (SSE) ──

    def _reanalyze_stream(self):
        """
        SSE endpoint: re-runs all stored transcripts through updated Claude prompt.
        Streams progress updates to frontend.
        """
        self.send_response(200)
        self._cors()
        self.send_header("Content-Type", "text/event-stream")
        self.send_header("Cache-Control", "no-cache")
        self.send_header("Connection", "keep-alive")
        self.end_headers()

        def send_event(data):
            msg = f"data: {json.dumps(data)}\n\n"
            try:
                self.wfile.write(msg.encode())
                self.wfile.flush()
            except Exception:
                pass

        try:
            calls = supa("GET", "calls?order=created_at.desc&limit=5000&select=id,transcript,filename")
            total = len(calls)
            processed = 0
            errors = 0

            send_event({"status": "starting", "total": total, "processed": 0})

            for i, call in enumerate(calls):
                transcript = call.get("transcript", "")
                filename = call.get("filename", "call.txt")
                call_id = call.get("id")

                if not transcript or not transcript.strip():
                    send_event({"processed": i + 1, "total": total, "current": filename, "skipped": True})
                    continue

                send_event({"processed": i + 1, "total": total, "current": filename})

                try:
                    result = run_claude_analysis(transcript, filename)
                    call_date = parse_call_date_from_filename(filename)

                    update_data = {
                        "availability_decline": result.get("availability_decline", False),
                        "onsite_suggested": result.get("onsite_suggested", False),
                        "call_quality": result.get("call_quality", "normal"),
                        "is_continuation": result.get("is_continuation", False),
                        "scores": result.get("scores", {}),
                        "checklist": result.get("checklist", {}),
                        "strengths": result.get("strengths", []),
                        "coaching_points": result.get("coaching_points", []),
                        "keywords_detected": result.get("keywords_detected", []),
                        "objections_detected": result.get("objections_detected", []),
                        "customer_sentiment": result.get("customer_sentiment", "neutral"),
                        "call_summary": result.get("call_summary", ""),
                        "word_count": result.get("word_count", 0),
                        "exclude_from_scoring": result.get("exclude_from_scoring", False),
                        "exclusion_reason": result.get("exclusion_reason", ""),
                        "call_type": result.get("call_type", "sales_estimate"),
                    }
                    if call_date:
                        update_data["call_date"] = call_date

                    # Handle auto-exclusion for disconnected calls
                    if result.get("call_quality") == "disconnected" and not result.get("exclude_from_scoring"):
                        update_data["exclude_from_scoring"] = True
                        update_data["exclusion_reason"] = "Disconnected/short call — auto excluded"

                    supa("PATCH", f"calls?id=eq.{call_id}", update_data)
                    processed += 1
                except Exception as e:
                    errors += 1
                    print(f"  Re-analyze error for {filename}: {e}")

            send_event({"status": "complete", "total": total, "processed": processed, "errors": errors})
        except Exception as e:
            send_event({"status": "error", "message": str(e)})

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


# ──────────────────────────────────────────────
# ENTRY POINT
# ──────────────────────────────────────────────

if __name__ == "__main__":
    print("=" * 55)
    print("  Little Guys Movers — Call Analyzer Server v7")
    print("=" * 55)
    missing = [v for v in ["ANTHROPIC_API_KEY", "SUPABASE_URL", "SUPABASE_KEY", "OPENAI_API_KEY"] if not os.environ.get(v)]
    if missing:
        print("\n  WARNING: Missing env vars: " + ", ".join(missing))
    else:
        print("\n  All environment variables loaded")
    print(f"  Running at http://127.0.0.1:{PORT}")
    print("  Press Ctrl+C to stop\n")
    HTTPServer(("0.0.0.0", PORT), Handler).serve_forever()
