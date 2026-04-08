"""
Little Guys Movers - Call Analyzer Server v6
=============================================
Required environment variables:
  ANTHROPIC_API_KEY  - Anthropic API key
  SUPABASE_URL       - Supabase project URL
  SUPABASE_KEY       - Supabase anon/publishable key
  OPENAI_API_KEY     - OpenAI API key for Whisper transcription
  PORT               - optional, defaults to 8765
"""

import os, json, urllib.request, urllib.error, urllib.parse, tempfile, mimetypes
from http.server import HTTPServer, BaseHTTPRequestHandler

API_KEY      = os.environ.get("ANTHROPIC_API_KEY", "")
SUPABASE_URL = os.environ.get("SUPABASE_URL", "")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY", "")
OPENAI_KEY   = os.environ.get("OPENAI_API_KEY", "")
PORT         = int(os.environ.get("PORT", 8765))

LGMS_PROMPT = """You are a sales call evaluator for Little Guys Movers (LGMS).

FIRST — classify this call:
- Count approximately how many words are in the transcript
- Determine the call type: "sales_estimate", "follow_up", "complaint", "booking_confirmation", "non_sales", "too_short", or "other"
- If the transcript has fewer than 80 words OR is clearly not a sales estimate call, set call_type accordingly and set exclude_from_scoring to true

SECOND — detect the rep name:
- Look for how the rep introduces themselves (e.g. "This is JD", "Hi this is Manning", "Thank you for calling Little Guys this is Sarah")
- Return the rep first name or full name if found, else return "Unknown"

REQUIRED CALL SEQUENCE TO EVALUATE:
1. Move date 2. Customer full name 3. Phone number 4. Cities/locations 5. House or apt (load AND unload) 6. Stairs/long walk both locations 7. Room-by-room inventory (rep controls pace) 8. Forgotten items: appliances, outdoor/garage, rugs/pictures/lamps/TVs 9. Boxes ("moving boxes") 10. Give price IMMEDIATELY — #1 priority 11. FVP pitch: declared value, $7 per $1000, handle objections 12. Attempt to close 13. If booking: time slot, email, mention 2 confirmation calls 14. If not booking: offer email estimate 15. Thank customer

KEY RULES:
- Simple moves (apt, 1-2BR): quote HOURLY with price RANGE
- Always attempt to close
- Give price on first contact
- Be confident, friendly, knowledgeable

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

OBJECTIONS — identify any customer objections:
Price too high, Need to think about it, Already have another quote, Wrong timing, Need to check with partner, Other

SENTIMENT: positive / neutral / hesitant / negative
OUTCOME: booked / estimate_sent / lost / unknown

Score 1-10. Be honest and critical. 10 = near perfect.
For too_short or non_sales calls, score everything 0.

Respond ONLY with valid JSON, no markdown:

{"rep_name_detected":"name or Unknown","caller_name":"name or Unknown","call_purpose":"short phrase","call_type":"sales_estimate","move_type":"local/long distance/unknown","call_outcome":"booked/estimate_sent/lost/unknown","word_count":150,"exclude_from_scoring":false,"exclusion_reason":"","call_summary":"3-5 sentences","key_details_captured":"details gathered","talk_ratio_rep":40,"talk_ratio_customer":60,"keywords_detected":["Full Value Protection"],"objections_detected":["Price too high"],"customer_sentiment":"positive","scores":{"info_sequence":{"score":0,"note":""},"price_delivery":{"score":0,"note":""},"fvp_pitch":{"score":0,"note":""},"closing_attempt":{"score":0,"note":""},"call_control":{"score":0,"note":""},"professionalism":{"score":0,"note":""},"overall":{"score":0,"note":""}},"checklist":{"got_move_date":false,"got_customer_name":false,"got_phone_number":false,"got_cities":false,"got_home_type":false,"got_stairs_info":false,"did_full_inventory":false,"asked_forgotten_items":false,"asked_about_boxes":false,"gave_price_on_call":false,"pitched_fvp":false,"attempted_to_close":false,"offered_email_estimate":false,"mentioned_confirmations":false,"thanked_customer":false},"strengths":["s1","s2"],"coaching_points":["c1","c2"]}"""

def read_html():
    for name in ["lgms_dashboard.html", "lgms_analyzer_v3.html"]:
        if os.path.exists(name):
            with open(name, "rb") as f:
                return f.read()
    return b"<h1>Missing lgms_dashboard.html</h1>"

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

def transcribe_audio(audio_bytes, filename):
    """Transcribe audio using OpenAI Whisper API"""
    if not OPENAI_KEY:
        raise Exception("OPENAI_API_KEY not set")

    # Determine mime type
    ext = filename.rsplit(".", 1)[-1].lower() if "." in filename else "mp3"
    mime_map = {"mp3":"audio/mpeg","mp4":"audio/mp4","m4a":"audio/mp4",
                "wav":"audio/wav","ogg":"audio/ogg","webm":"audio/webm",
                "mpeg":"audio/mpeg","mpga":"audio/mpeg"}
    mime = mime_map.get(ext, "audio/mpeg")

    # Build multipart form data
    boundary = "----WhisperBoundary"
    body_parts = []
    body_parts.append(f"--{boundary}\r\nContent-Disposition: form-data; name=\"model\"\r\n\r\nwhisper-1".encode())
    body_parts.append(f"--{boundary}\r\nContent-Disposition: form-data; name=\"response_format\"\r\n\r\ntext".encode())
    body_parts.append(f"--{boundary}\r\nContent-Disposition: form-data; name=\"file\"; filename=\"{filename}\"\r\nContent-Type: {mime}\r\n\r\n".encode() + audio_bytes)
    body_parts.append(f"--{boundary}--".encode())

    body = b"\r\n".join(body_parts)

    req = urllib.request.Request(
        "https://api.openai.com/v1/audio/transcriptions",
        data=body,
        headers={
            "Authorization": f"Bearer {OPENAI_KEY}",
            "Content-Type": f"multipart/form-data; boundary={boundary}"
        },
        method="POST"
    )

    with urllib.request.urlopen(req, timeout=300) as r:
        return r.read().decode("utf-8")

class Handler(BaseHTTPRequestHandler):
    def log_message(self, fmt, *args):
        print(f"  {args[0]} {args[1]}")

    def do_GET(self):
        if self.path.startswith("/calls"):
            self._get_calls()
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
            "/save": self._save,
            "/update": self._update,
            "/delete": self._delete,
            "/bulk_delete": self._bulk_delete
        }
        fn = routes.get(self.path)
        if fn:
            fn(body)
        else:
            self.send_response(404)
            self.end_headers()

    def _get_calls(self):
        try:
            calls = supa("GET", "calls?order=created_at.desc&limit=1000")
            self._ok(calls)
        except Exception as e:
            self._err(500, str(e))

    def _transcribe_and_analyze(self, body):
        """Accept raw audio bytes, transcribe with Whisper, then analyze with Claude"""
        if not OPENAI_KEY:
            self._err(500, "OPENAI_API_KEY not set")
            return
        if not API_KEY:
            self._err(500, "ANTHROPIC_API_KEY not set")
            return

        # Parse multipart or JSON with base64
        content_type = self.headers.get("Content-Type", "")

        if "application/json" in content_type:
            try:
                import base64
                p = json.loads(body)
                audio_b64 = p.get("audio", "")
                filename = p.get("filename", "call.mp3")
                audio_bytes = base64.b64decode(audio_b64)
            except Exception as e:
                self._err(400, f"Bad request: {e}")
                return
        else:
            self._err(400, "Content-Type must be application/json with base64 audio")
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
            result = self._run_claude_analysis(transcript, filename)
            result["transcript"] = transcript
            result["filename"] = filename
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
            result = self._run_claude_analysis(transcript, filename)
            result["filename"] = filename
            self._ok(result)
        except Exception as e:
            self._err(500, str(e))

    def _run_claude_analysis(self, transcript, filename):
        prompt = f"Filename: {filename}\n\nTranscript:\n{transcript}\n\n{LGMS_PROMPT}"
        req_body = json.dumps({
            "model": "claude-sonnet-4-20250514",
            "max_tokens": 2500,
            "messages": [{"role": "user", "content": prompt}]
        }).encode()
        req = urllib.request.Request(
            "https://api.anthropic.com/v1/messages", data=req_body,
            headers={"Content-Type":"application/json","x-api-key":API_KEY,"anthropic-version":"2023-06-01"},
            method="POST")
        with urllib.request.urlopen(req, timeout=120) as r:
            resp = json.loads(r.read())
        tb = next((b for b in resp.get("content",[]) if b.get("type")=="text"), None)
        if not tb:
            raise Exception("No response from Claude")
        raw = tb["text"].strip().lstrip("```json").lstrip("```").rstrip("```").strip()
        return json.loads(raw)

    def _check_duplicate(self, filename, transcript):
        """Check if a call already exists with same filename or similar transcript"""
        try:
            # Check by filename first (fast)
            existing = supa("GET", f"calls?filename=eq.{urllib.parse.quote(filename)}&limit=1")
            if existing:
                return True, f"Duplicate filename: {filename}"
            # Check by transcript similarity (first 200 chars)
            if transcript and len(transcript) > 50:
                snippet = transcript[:200].replace("'", "''")
                # Check if transcript starts the same way
                all_calls = supa("GET", "calls?limit=500&select=transcript,filename")
                for c in all_calls:
                    if c.get("transcript") and c["transcript"][:200] == transcript[:200]:
                        return True, f"Duplicate content (matches {c.get('filename','unknown')})"
            return False, ""
        except Exception:
            return False, ""

    def _save(self, body):
        try:
            p = json.loads(body)

            # Duplicate check
            skip_dup = p.get("check_duplicate", True)
            if skip_dup:
                is_dup, dup_reason = self._check_duplicate(
                    p.get("filename",""), p.get("transcript",""))
                if is_dup:
                    self._ok({"duplicate": True, "reason": dup_reason})
                    return

            record = {
                "rep_name": p.get("rep_name") or p.get("rep_name_detected") or "Unknown",
                "filename": p.get("filename",""),
                "transcript": p.get("transcript",""),
                "caller_name": p.get("caller_name",""),
                "call_purpose": p.get("call_purpose",""),
                "call_type": p.get("call_type","sales_estimate"),
                "move_type": p.get("move_type",""),
                "call_outcome": p.get("call_outcome","unknown"),
                "word_count": p.get("word_count",0),
                "exclude_from_scoring": p.get("exclude_from_scoring",False),
                "exclusion_reason": p.get("exclusion_reason",""),
                "call_summary": p.get("call_summary",""),
                "key_details": p.get("key_details_captured",""),
                "talk_ratio_rep": p.get("talk_ratio_rep",0),
                "talk_ratio_customer": p.get("talk_ratio_customer",0),
                "keywords_detected": p.get("keywords_detected",[]),
                "objections_detected": p.get("objections_detected",[]),
                "customer_sentiment": p.get("customer_sentiment","neutral"),
                "scores": p.get("scores",{}),
                "checklist": p.get("checklist",{}),
                "strengths": p.get("strengths",[]),
                "coaching_points": p.get("coaching_points",[]),
                "tags": p.get("tags",[]),
                "manager_notes": p.get("manager_notes",""),
                "score_overrides": p.get("score_overrides",{})
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

    def _ok(self, data):
        body = json.dumps(data).encode()
        self.send_response(200)
        self._cors()
        self.send_header("Content-Type","application/json")
        self.end_headers()
        self.wfile.write(body)

    def _err(self, code, msg):
        body = json.dumps({"error": msg}).encode()
        self.send_response(code)
        self._cors()
        self.send_header("Content-Type","application/json")
        self.end_headers()
        self.wfile.write(body)

    def _cors(self):
        self.send_header("Access-Control-Allow-Origin","*")
        self.send_header("Access-Control-Allow-Methods","POST,GET,OPTIONS,DELETE,PATCH")
        self.send_header("Access-Control-Allow-Headers","Content-Type")

if __name__ == "__main__":
    print("="*55)
    print("  Little Guys Movers — Call Analyzer Server v6")
    print("="*55)
    missing = [v for v in ["ANTHROPIC_API_KEY","SUPABASE_URL","SUPABASE_KEY","OPENAI_API_KEY"] if not os.environ.get(v)]
    if missing:
        print("\n  WARNING: Missing env vars: " + ", ".join(missing))
    else:
        print("\n  All environment variables loaded")
    print(f"  Running at http://127.0.0.1:{PORT}")
    print("  Press Ctrl+C to stop\n")
    HTTPServer(("0.0.0.0", PORT), Handler).serve_forever()
