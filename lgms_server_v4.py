"""
LGMS Call Analyzer - Server v4
================================
Setup:
1. Set environment variable ANTHROPIC_API_KEY to your key
2. Run: python lgms_server_v4.py
3. Open: http://127.0.0.1:8765
"""

import os
import json
import urllib.request
import urllib.error
from http.server import HTTPServer, BaseHTTPRequestHandler

API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
PORT = int(os.environ.get("PORT", 8765))

LGMS_PROMPT = """You are a sales call evaluator for LGMS, a moving company. Analyze this call transcript against the LGMS sales process.

REQUIRED CALL SEQUENCE: 1) Move date 2) Customer full name 3) Phone number 4) Cities/locations 5) House or apartment (load AND unload) 6) Stairs or long walk at both locations 7) Room-by-room inventory (control the pace) 8) Forgotten items: appliances (fridge, washer/dryer), outdoor/garage, rugs/pictures/lamps/TVs 9) Boxes ("moving boxes") 10) Give price IMMEDIATELY after inventory — #1 priority 11) FVP pitch: declared value, ask if it covers everything, $7 per $1,000, handle objections 12) Attempt to schedule/close 13) If booking: confirm time slot, get email, mention 2 confirmation calls 14) If not booking: offer estimate via email 15) Thank customer, invite questions.

KEY RULES:
- Simple moves (apartments, 1-2 BR): quote HOURLY, give a price RANGE on the call
- Larger moves: still give a ballpark price on first contact
- ALWAYS attempt to close before ending the call
- If price objection: ask about budget or competitor quote
- Be confident, friendly, knowledgeable at ALL TIMES
- Control the pace of the call

Score each category 1-10. Be honest and critical — 10 = near perfect.

Respond ONLY with valid JSON, no markdown, no extra text:

{"caller_name":"Unknown","call_purpose":"short phrase","move_type":"local/long distance/unknown","call_summary":"3-5 sentences","key_details_captured":"details gathered on the call","scores":{"info_sequence":{"score":0,"note":"what was collected in order and what was skipped"},"price_delivery":{"score":0,"note":"did they give price on call? promptly? range for simple move?"},"fvp_pitch":{"score":0,"note":"FVP pitch quality — declared value, cost, objections handled?"},"closing_attempt":{"score":0,"note":"did they try to book? handle objections? offer email?"},"call_control":{"score":0,"note":"did rep control pace and stay organized?"},"professionalism":{"score":0,"note":"tone, confidence, friendliness, clarity"},"overall":{"score":0,"note":"overall assessment"}},"checklist":{"got_move_date":false,"got_customer_name":false,"got_phone_number":false,"got_cities":false,"got_home_type":false,"got_stairs_info":false,"did_full_inventory":false,"asked_forgotten_items":false,"asked_about_boxes":false,"gave_price_on_call":false,"pitched_fvp":false,"attempted_to_close":false,"offered_email_estimate":false,"mentioned_confirmations":false,"thanked_customer":false},"strengths":["s1","s2","s3"],"coaching_points":["c1","c2","c3"]}"""

HTML = open("lgms_analyzer_v3.html", "rb").read() if os.path.exists("lgms_analyzer_v3.html") else b"<h1>Missing lgms_analyzer_v3.html</h1>"

class Handler(BaseHTTPRequestHandler):
    def log_message(self, format, *args):
        print(f"  {args[0]} {args[1]}")

    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-Type", "text/html")
        self.end_headers()
        self.wfile.write(HTML)

    def do_OPTIONS(self):
        self.send_response(200)
        self._cors()
        self.end_headers()

    def do_POST(self):
        if self.path != "/analyze":
            self.send_response(404)
            self.end_headers()
            return

        if not API_KEY:
            self._json_error(500, "ANTHROPIC_API_KEY environment variable not set.")
            return

        length = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(length)

        try:
            payload = json.loads(body)
            transcript = payload.get("transcript", "").strip()
            filename = payload.get("filename", "call.txt")
        except Exception as e:
            self._json_error(400, f"Bad request: {e}")
            return

        if not transcript:
            self._json_error(400, "No transcript provided")
            return

        full_prompt = f"Filename: {filename}\n\nTranscript:\n{transcript}\n\n{LGMS_PROMPT}"

        request_body = json.dumps({
            "model": "claude-sonnet-4-20250514",
            "max_tokens": 2000,
            "messages": [{"role": "user", "content": full_prompt}]
        }).encode("utf-8")

        req = urllib.request.Request(
            "https://api.anthropic.com/v1/messages",
            data=request_body,
            headers={
                "Content-Type": "application/json",
                "x-api-key": API_KEY,
                "anthropic-version": "2023-06-01"
            },
            method="POST"
        )

        try:
            with urllib.request.urlopen(req, timeout=120) as resp:
                api_response = json.loads(resp.read())
        except urllib.error.HTTPError as e:
            self._json_error(e.code, f"API error: {e.read().decode()}")
            return
        except Exception as e:
            self._json_error(500, f"Request failed: {e}")
            return

        text_block = next((b for b in api_response.get("content", []) if b.get("type") == "text"), None)
        if not text_block:
            self._json_error(500, "No text in API response")
            return

        raw = text_block["text"].strip().lstrip("```json").lstrip("```").rstrip("```").strip()
        try:
            result = json.loads(raw)
            result["filename"] = filename
        except Exception as e:
            self._json_error(500, f"JSON parse error: {e} | Raw: {raw[:300]}")
            return

        self.send_response(200)
        self._cors()
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(json.dumps(result).encode())

    def _cors(self):
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "POST, GET, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")

    def _json_error(self, code, message):
        body = json.dumps({"error": message}).encode()
        self.send_response(code)
        self._cors()
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(body)

if __name__ == "__main__":
    print("=" * 50)
    print("  LGMS Call Analyzer Server v4")
    print("=" * 50)
    if not API_KEY:
        print("\n  WARNING: ANTHROPIC_API_KEY not set!")
        print("  Set it as an environment variable before running.\n")
    else:
        print(f"\n  API key loaded")
    print(f"  Server running at http://127.0.0.1:{PORT}")
    print(f"  Open that URL in your browser")
    print(f"\n  Press Ctrl+C to stop\n")
    server = HTTPServer(("0.0.0.0", PORT), Handler)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\n  Server stopped.")
