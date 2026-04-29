"""
Little Guys Movers - Call Analyzer Server v12
=============================================
Required environment variables:
  ANTHROPIC_API_KEY  - Anthropic API key
  SUPABASE_URL       - Supabase project URL
  SUPABASE_KEY       - Supabase anon/publishable key
  DEEPGRAM_API_KEY   - Deepgram API key for transcription
  OPENAI_API_KEY     - optional fallback if Deepgram unavailable
  PORT               - optional, defaults to 8765
"""

import os, json, urllib.request, urllib.error, urllib.parse, tempfile, mimetypes
import zipfile, io, secrets, re, uuid, threading, time, sys
from http.server import HTTPServer, BaseHTTPRequestHandler, ThreadingHTTPServer
from datetime import datetime, timezone, timedelta

# Force stdout flush immediately so Render logs show in real time
sys.stdout.reconfigure(line_buffering=True)

def log(msg):
    print(msg, flush=True)

API_KEY      = os.environ.get("ANTHROPIC_API_KEY", "")
SUPABASE_URL = os.environ.get("SUPABASE_URL", "")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY", "")
DEEPGRAM_KEY = os.environ.get("DEEPGRAM_API_KEY", "")
OPENAI_KEY   = os.environ.get("OPENAI_API_KEY", "")  # fallback
PORT         = int(os.environ.get("PORT", 8765))

# Global semaphore — max 1 concurrent reanalysis call (prevents memory exhaustion on Render standard plan)
# Single file uploads and batch uploads share this semaphore but are unaffected since they're user-initiated
_analysis_semaphore = threading.Semaphore(1)

# Keyterm cache — rebuilt every 30 minutes
_keyterm_cache = {"terms": [], "built_at": 0}
_keyterm_lock = threading.Lock()

# Background re-analyze job state
_reanalyze_job = {
    "status": "idle",
    "total": 0, "processed": 0, "current": "", "errors": 0,
    "started_at": None, "finished_at": None,
    "stop_requested": False,
}
_reanalyze_lock = threading.Lock()

# Background batch upload job state
_batch_job = {
    "status": "idle",
    "total": 0, "processed": 0, "skipped": 0, "errors": 0,
    "current": "", "started_at": None, "finished_at": None,
    "error_list": [],
}
_batch_lock = threading.Lock()

# ──────────────────────────────────────────────
# VONAGE VBC INTEGRATION
# ──────────────────────────────────────────────
# Env vars (all optional — integration is dormant if any are missing):
#   VONAGE_CLIENT_ID       — OAuth client ID from VBC API dashboard
#   VONAGE_CLIENT_SECRET   — OAuth client secret
#   VONAGE_ACCOUNT_ID      — VBC account identifier (used in API URL paths)
#   VONAGE_API_BASE        — Base URL for VBC API
#                            (default https://api.vonage.com — confirm in dashboard)
#   VONAGE_TOKEN_URL       — OAuth token endpoint
#                            (default https://api.vonage.com/oauth2/token — confirm)
#   VONAGE_POLL_INTERVAL   — Seconds between polls (default 1800 = 30 min)
#   VONAGE_AUTOSTART       — "1" to auto-start polling at boot (default "1")

VONAGE_CLIENT_ID     = os.environ.get("VONAGE_CLIENT_ID", "")
VONAGE_CLIENT_SECRET = os.environ.get("VONAGE_CLIENT_SECRET", "")
VONAGE_ACCOUNT_ID    = os.environ.get("VONAGE_ACCOUNT_ID", "")
VONAGE_API_BASE      = os.environ.get("VONAGE_API_BASE", "https://api.vonage.com").rstrip("/")
VONAGE_TOKEN_URL     = os.environ.get("VONAGE_TOKEN_URL", "https://api.vonage.com/oauth2/token")
VONAGE_POLL_INTERVAL = int(os.environ.get("VONAGE_POLL_INTERVAL", 1800))  # 30 min default
VONAGE_AUTOSTART     = os.environ.get("VONAGE_AUTOSTART", "1") == "1"

# OAuth token cache. Refreshed automatically when expired or on 401.
_vonage_token_cache = {"token": None, "expires_at": 0}
_vonage_token_lock = threading.Lock()

# Polling worker state. Mirrors the pattern used by _reanalyze_job and _batch_job.
_vonage_job = {
    "status": "idle",        # idle | running | paused | error | disabled
    "started_at": None,
    "last_poll_at": None,
    "last_poll_succeeded": None,
    "last_error": None,
    "ingested_total": 0,     # since worker started
    "ingested_today": 0,
    "skipped_total": 0,
    "errors_total": 0,
    "stop_requested": False,
    "pause_requested": False,
}
_vonage_lock = threading.Lock()
_vonage_thread = None  # Holds the background thread reference; check if alive before spawning

# Whether VBC env is configured at all
def _vonage_configured():
    return bool(VONAGE_CLIENT_ID and VONAGE_CLIENT_SECRET and VONAGE_ACCOUNT_ID)


# ──────────────────────────────────────────────
# CLAUDE PROMPT
# ──────────────────────────────────────────────

def build_prompt(transcript, filename, corrections=None, is_diarized=False):
    """Build Claude prompt v12 — new scoring formula, hourly/pointed, timeline-weighted closing, improved rep detection."""

    corrections_block = ""
    if corrections:
        examples = []
        for c in corrections[:50]:
            note = c.get("manager_note", "")
            examples.append(
                f"- {c['category']}: scored {c['original_score']} but correct score is {c['corrected_score']}"
                + (f" — reason: {note}" if note else "")
            )
        if examples:
            corrections_block = "\n\nRECENT SCORING CORRECTIONS (calibrate your scoring using these):\n" + "\n".join(examples) + "\n"

    diarization_note = ""
    if is_diarized:
        diarization_note = """
TRANSCRIPT FORMAT: This transcript has speaker labels (Speaker 0, Speaker 1, etc.).
- Speaker 0 is typically the rep (answers the phone, introduces themselves first)
- Speaker 1 is typically the customer
- Use these labels to accurately determine talk ratio and who said what
- Rep name detection: look for Speaker 0's introduction context only
"""

    prompt = f"""You are a sales call evaluator for Little Guys Movers (LGMS).
{corrections_block}{diarization_note}
CALIBRATION EXAMPLES — anchor your scoring to these LGMS standards:

EXCELLENT CALL (9/10 overall) — Alex, storage-to-apartment move:
- Assumed the close: "I can get started first thing tomorrow morning at 8 o'clock" — moved straight to scheduling
- Rapport: immediately acknowledged customer's stress about last-minute situation, warm and confident — 9/10
- Closing: 10/10 — never asked "do you want to book?", just booked it (assumed close = minimum 7 base)
- Salesmanship: mentioned "clothes in the dresser" naturally, background checked movers — 8/10
- Price: delivered confidently with pointed flat-rate framing — 8/10
Key lesson: assume the close, move with confidence, acknowledge the customer's situation

GOOD CALL (8/10 overall) — Dylan, large house move:
- Thorough room-by-room inventory, all right detail questions
- Closing: "I could have movers there as early as 8 a.m. next Thursday, if you wanted to get that set up" — offered but waited for confirmation — 8/10
- Salesmanship: 5/10 — missed obvious opportunities (clothes in dresser, disassembly, background checks)
- Excellent expectation setting: explained email, attachments, day-of 7:45 call — 9/10 professionalism
Key lesson: thorough and professional but missed salesmanship

AVERAGE CALL (6/10 overall) — JD, studio apartment move:
- Good inventory detail questions
- Closing: "Do you want to go ahead and get this scheduled?" — question not assumption — 6/10
- Salesmanship: mentioned clothes in dresser but only as afterthought at end — 4/10
- Missed customer's anxiety about unknown floor situation — no reassurance given
Key lesson: closing as a question instead of assumption costs points; missed rapport hurts salesmanship

POOR CALL (4/10 overall) — James, small marketplace pickup:
- Customer said "I'm going to try and find another option" — James said "completely understand" TWICE and let him walk — 1/10 closing
- MISSED RAPPORT: customer said "the guy that was supposed to help me decided to leave town" — James gave no acknowledgment
- Salesmanship: nothing — 2/10
- Price delivered without any value framing first
Key lesson: never let a customer walk without attempting to overcome the objection. Always acknowledge emotional moments.

WHAT A 10/10 CALL LOOKS LIKE:
- Rep introduces themselves and gets customer name within first 30 seconds
- Acknowledges any stress or difficulty the customer mentions immediately and warmly
- Controls the call with confidence while feeling natural and unhurried
- Gets all required information for the move type
- Delivers price confidently with value framing first
- Uses multiple salesmanship phrases naturally throughout
- Assumes the close — moves to scheduling without asking permission
- Overcomes any objection with specific counters (FCFS, no deposit, availability, value)
- Sets clear expectations for confirmation and day-of communication
- Customer feels like they made the right choice

---

STEP 1 — CLASSIFY CALL:
- move_category: "standard" | "specialty" | "unload_only" | "in_house" | "commercial" | "storage" | "non_move"
- call_type: "sales_estimate" | "follow_up" | "complaint" | "booking_confirmation" | "non_sales" | "too_short" | "other"
- word_count: approximate
- exclude_from_scoring: true if fewer than 80 words OR clearly not a sales call OR disconnected
IMPORTANT: Even if excluded, still detect turned_away.

STEP 2 — DETECT REP NAME:
The rep ANSWERS the call — they do not initiate it.
- The rep typically says "Thank you for calling Little Guys, this is [name]" or similar greeting
- If diarized, Speaker 0 is almost always the rep — look for their greeting/introduction
- Rep name is a FIRST NAME spoken in an introduction/greeting context
- If a name appears later but NOT in an introduction, it is likely the customer
- Cross-reference against context: if the detected name sounds like it came from the customer side, return "Unknown"
- PREFER returning "Unknown" over a wrong guess — a wrong name creates phantom reps
Return FIRST NAME ONLY. Return "Unknown" if not confident.

STEP 3 — CALL QUALITY:
- "disconnected": abrupt ending, "Hello? Hello?" patterns, very short
- "poor_audio": heavy [inaudible] density (5+ occurrences), one-sided
- "normal": otherwise

STEP 4 — FLAGS:
- turned_away: LGMS could NOT accommodate — "we're booked", "no availability", "can't do that date"
- onsite_suggested: onsite visit/estimate mentioned
- is_continuation: "calling back about", "as we discussed", "following up"

STEP 5 — OUTCOME & PIPELINE:
- call_outcome: "booked" | "estimate_sent" | "soft_pipeline" | "lost" | "unknown"
- soft_pipeline: customer interested but not ready — needs partner, no exact date, needs to think
- loss_reason (if lost): "price_too_high" | "went_with_competitor" | "wrong_timing" | "no_availability" | "just_shopping" | "other" | ""
- soft_pipeline_reason (if soft_pipeline): "needs_partner" | "no_exact_date" | "needs_to_think" | "will_call_back" | "other" | ""

STEP 6 — MOVE TIMELINE:
Detect when customer wants to move. Classify move_timeline:
- "exact_date": customer provided a specific date → assumed close expected regardless of how far out
- "this_week": within 7 days → must attempt to book on call, overcome objections
- "two_to_four_weeks": 2-4 weeks → strong close + objection handling expected
- "one_to_three_months": 1-3 months → close attempt expected, lighter pressure
- "three_plus_months": 3+ months → attempt appreciated, soft pipeline acceptable
- "unknown": no clear timeline — look for clues (seasons, life events, vague references)

STEP 7 — PRICING MODEL DETECTION (CRITICAL — read carefully):

Output one of: "hourly", "pointed", or "unknown"

SCAN THE ENTIRE TRANSCRIPT for pricing language before deciding.
Pricing might be discussed for only 30 seconds in a long call.

═══════════════════════════════════════════════════════════════════
HOURLY INDICATORS (any one of these = hourly):
═══════════════════════════════════════════════════════════════════

Strong signals:
- Rep says "hourly rate", "hourly", "per hour", "an hour",
  "$X an hour", "$X per hour"
- Rep mentions an "X hour minimum" (2-hour, 1.5-hour, etc.)
- Rep mentions "rated for the quarter hour" or proration
- Rep says "if it takes less than X hours you'll pay less"
- Rep mentions a mileage fee separate from the rate
- Rep gives an estimated number of hours the job will take
  ("about 2 hours", "should take 2 to 3 hours", "predicting 4 hours")
- Rep gives a per-guy rate ("$1.30 an hour for two guys")

Combined signal pattern (both must be present):
- Quote includes a time component (e.g. "for 2 hours, that's $X")
  even if no explicit per-hour rate is stated, AND
- Cash/card price split given for that time-bounded quote

═══════════════════════════════════════════════════════════════════
POINTED / FLAT-RATE INDICATORS (any one of these = pointed):
═══════════════════════════════════════════════════════════════════

Strong signals:
- Rep says "flat fee", "flat rate", "fixed price", "exact price",
  "exact cost"
- Rep says "based on inventory", "based on what we discussed",
  "based on what we move"
- Rep says "time doesn't affect that", "regardless of how long",
  "whether it takes 2 hours or 5"
- Single dollar figure quoted for the entire job with NO time
  component mentioned at all
- Multiple price options that differ only by per-item add-on
  (e.g., "$1,210 without boxes, $1,398 with 50 boxes" where the
  difference is a fixed per-box amount and NO time impact discussed)

Default rules:
- Long-distance / interstate moves are POINTED unless the rep
  explicitly quotes hourly mechanics (federal regs essentially
  require flat-rate for interstate)
- Multi-day moves are POINTED unless explicitly hourly

═══════════════════════════════════════════════════════════════════
DISAMBIGUATION (when both kinds of language appear):
═══════════════════════════════════════════════════════════════════

Look at HOW THE FINAL QUOTE WAS DELIVERED. Tiebreaker priority:

1. Mileage fee mentioned → HOURLY (only exists on hourly jobs)
2. Quarter-hour proration mentioned → HOURLY
3. "Time doesn't affect" or "regardless of how long" → POINTED
4. "Flat" or "exact" used as adjective for the price → POINTED
5. Multi-quote with per-item differential → POINTED
6. Quote tied to specific number of hours → HOURLY
7. Quote stated as fixed total with no time framing → POINTED

═══════════════════════════════════════════════════════════════════
UNKNOWN — use ONLY in these cases:
═══════════════════════════════════════════════════════════════════

- No price was given on the call at all
- Price was discussed only in the abstract ("we'd need to do an
  estimate") with no numbers
- Rep gave a price but FAILED to anchor it as either model AND none
  of the above patterns apply (this case requires a coaching point —
  see scoring section)
- Audio quality too poor to determine

DO NOT use "unknown" just because the rep didn't say a magic word.
If a price was given, work through the indicators above to decide.
If after working through them the model is still genuinely unclear,
unknown is correct AND a coaching point should be added noting that
the rep needs to clearly anchor the pricing model.

STEP 8 — CLOSING ANALYSIS:
COUNT close_attempts — how many times did rep explicitly try to book/schedule?
ASSUMED CLOSE: if rep moved directly to scheduling without asking permission (e.g. "Let me get you on for Saturday" not "Do you want to book?"), closing score starts at minimum 7.

CLOSING LANGUAGE (strong signals):
"I have a spot available", "let me get you on the board/schedule/calendar",
"first come first serve", "no cancellation fee", "no deposit to hold a spot",
"save/hold that for you", "get you taken care of", "let me go ahead and book"

OBJECTION HANDLING:
- overcome: rep countered with specific response and advanced conversation
- abandoned: rep accepted without counter ("ok call us back", "completely understand", "no problem")

Ideal counters:
- Need to think → "No deposit required — would it make sense to hold the spot while you decide?"
- Need to check with partner → "No deposit to hold — want me to lock that in while you check?"
- Price too high → "What were you expecting? Let me see what I can do"
- Already have a quote → "What are they quoting? We'd love to earn your business today"

FOLLOW-UP LANGUAGE (important for soft pipeline):
Good reps mention: follow-up call timeframe, first come first serve, no deposit to hold.
Missing these on soft pipeline = coaching point.

pipeline_recovery_quality (soft_pipeline only, 1-10):
- 1-3: "ok call us back" — no next step
- 4-6: sent email estimate, some info
- 7-8: specific callback time + FCFS + no deposit mentioned
- 9-10: held spot + callback + email + FCFS + no deposit — clear path to book

STEP 9 — RAPPORT OPPORTUNITIES:
Scan for moments where customer shared something personal, stressful, or emotionally significant.
Missed opportunities = specific coaching points: "Customer said [X], rep gave no acknowledgment"
Examples:
- Someone bailed on them → "We won't do that to you"
- Life event (divorce, new job) → empathy
- Stressed/anxious → reassurance
- Bad experience with movers → "That won't happen with us"

STEP 10 — SALESMANSHIP:
Score on CONCEPT not exact words.

ALWAYS-RELEVANT (most calls):
- Background checked / screened movers
- No day labor / professional employees
- We show up / reliable / don't miss appointments
- Proper equipment / trucks / tools
- Customer reassurance from concern — when customer expresses worry, rep responds with specific LGMS quality

SITUATIONAL (when applicable):
- "Clothes in the dresser" — bedroom furniture present
- "Disassemble and reassemble" — furniture assembly needed
- "Can rearrange things" — in-house or layout mentioned
- "Careful with belongings" — customer expresses concern

POINTED VALUE PROPS (when pricing_model = pointed):
- No clock pressure, take time to do it right
- Disassembly/reassembly included
- Rearranging included
- Exact cost upfront, no surprise bill
- Detailed inventory ensures accuracy

HOURLY VALUE PROPS (when pricing_model = hourly):
- Only pay for time used
- Rounded to nearest quarter hour
- Clock starts on arrival not drive time
- Flexibility for extra tasks
- MUST provide estimated hours — if not given, coaching point

CUSTOMER NAME USAGE:
- Never used: no contribution
- Used at least once naturally: small bonus
- Used naturally multiple times: meaningful bonus
- Forced/awkward: no bonus

Scoring:
- 1-3: No value props, no name use
- 4-5: One always-relevant phrase
- 6-7: Two+ always-relevant, or one + name use
- 8-9: Always-relevant + situational + name naturally + pricing model props
- 10: Full suite woven naturally, name used, reassurance addressed, pricing props used

STEP 11 — CHECKLIST (22 steps):

STANDARD MOVE — all 22:
1. got_move_date
2. got_customer_name
3. got_phone_number — TRUE only if rep asked OR confirmed ("I have this number, is that right?"). Passive caller ID alone = false.
4. got_cities
5. got_home_type
6. got_stairs_info
7. did_full_inventory
8. asked_forgotten_items
9. asked_about_boxes (more important on pointed moves)
10. gave_price_on_call
11. attempted_to_close
12. offered_email_estimate
13. mentioned_confirmations
14. thanked_customer
15. asked_name_at_start — TRUE if rep asked, OR if customer introduced themselves first
16. led_estimate_process
17. got_email — TRUE if rep asked for OR confirmed an existing email
18. scheduled_onsite_attempt (na unless triggered)
19. offered_alternatives (na unless triggered)
20. took_rapport_opportunities
21. completed_booking_wrapup
22. captured_lead

SPECIALTY: Apply: move_date, name, phone, cities, stairs, price, close, confirmations, thanks, name_at_start, email, lead. Rest = "na".
UNLOAD ONLY: Apply: move_date, name, phone, cities, stairs, price, close, confirmations, thanks, name_at_start, email, lead. Rest = "na".
IN-HOUSE: Apply: move_date, name, phone, inventory, price, close, thanks, name_at_start, email, lead. Rest = "na".
COMMERCIAL: Apply all standard steps. onsite/alternatives = "na" unless triggered.
STORAGE: Apply: move_date, name, phone, cities, stairs, price, close, confirmations, thanks, name_at_start, email, lead. Rest = "na".

CHECKLIST VALUES: true | false | "na"

STEP 12 — SCORING (1-10 each):

OVERALL — server recalculates. Sanity check:
overall = round(closing*0.25 + price*0.15 + rapport*0.15 + salesmanship*0.20 + info_control*0.15 + professionalism*0.10)

CLOSING ATTEMPT (25% weight):
- 1-2: Never attempted
- 3-4: Weak, accepted first objection without counter
- 5-6: One attempt, gave up too easily
- 7: MINIMUM if rep assumed the close
- 7-8: Clear attempt, handled at least one objection
- 9: Strong, used FCFS/no-deposit/urgency, overcame objections
- 10: Multiple attempts, assumed close, overcame all objections
Timeline adjustment: exact date or this week = not booking after real attempt is NOT penalized, not attempting IS penalized

PRICE DELIVERY (15% weight):

═══ POINTED quotes ═══
- 1-3: No price given on the call
- 4-6: Price given, but with no inventory/value framing — just a number
- 7-8: Confident delivery with inventory context (price is based on
       what was discussed)
- 9-10: Confident, value-framed, AND price objections handled effectively

═══ HOURLY quotes — four mechanics expected ═══
The rep should explain ALL FOUR pieces of how hourly works:
  1. Per-hour rate stated explicitly ($X an hour for X guys)
  2. Quarter-hour rounding / proration mentioned
  3. Mileage fee mentioned
  4. Clock starts when work starts (not drive time, not arrival)

Scoring:
- 1-3: No rate given, or job hours estimate completely missing
- 4-6: Rate given but missing 2+ of the four mechanics. Auto-cap at 6
       if estimated hours not given (unfair to customer).
- 7-8: Rate, hours estimate, and 2-3 of the four mechanics explained
       confidently
- 9-10: All four mechanics explained AND value framing (only paying
        for time used, flexibility, etc.)

For each missing mechanic on an hourly call, add a coaching point
naming the specific mechanic ("did not mention the mileage fee",
"did not explain quarter-hour rounding", etc.)

═══ MODEL ANCHORING (applies to both) ═══

Whether pointed or hourly, the rep should explicitly state which
pricing model is being used. Phrases like "this is a flat-rate quote,
time doesn't affect it" or "we charge hourly with a 2-hour minimum"
should appear when pricing is delivered.

If the rep gives a price WITHOUT anchoring it to a model, this is a
coaching point — UNLESS:
- The customer has used LGMS before (mentioned in call, or system
  found them), OR
- The customer themselves explicitly used pricing model language
  first ("I'm looking for an hourly rate"), making the model already
  clear

When the anchoring coaching point applies, include in coaching_points
phrasing like: "Did not explicitly anchor the quote as [hourly/pointed].
Should clarify pricing model when delivering price for new customers."

RAPPORT & TONE (15% weight):
- 1-3: Flat, missed emotional moments
- 4-6: Polite but mechanical
- 7-8: Warm, acknowledged situation
- 9-10: Genuine connection, used situation to reinforce LGMS trust

SALESMANSHIP (20% weight): see Step 10 rubric

INFORMATION & CONTROL (15% weight — combined):
Score on what was gathered AND how well rep steered the call.
Penalize only if required info never obtained. Natural conversation scores well. Do not penalize N/A items.

PROFESSIONALISM (10% weight):
Expectation setting, email explanation, day-of call mention, clean wrap-up, handling complications smoothly.

CONFIDENCE SCORE (1-10):
10: clear transcript. 7-9: minor issues. 4-6: some audio problems. 1-3: poor audio, manager should review.

TALK RATIO: speaker labels if diarized, otherwise estimate.

KEYWORDS — detect when the rep used the CONCEPT (not exact words).

CRITICAL: Return the canonical key from the list below, NOT the rep's
actual phrasing. The dashboard groups by canonical key. If you return
"background checks" instead of "background checked", the call won't
appear on the chart. ALWAYS use the canonical form on the left.

CLOSING / URGENCY:
- "spot available" — rep mentioned having a spot, opening, slot
- "get you on the board" — rep offered to put on schedule/calendar/board
- "first come first serve" — rep mentioned FCFS booking
- "no cancellation fee" — rep noted they don't charge cancellation fees
  (use singular form even if rep said "fees")
- "save that for you" — rep offered to hold/save the spot
- "go ahead and book" — assumptive close language
- "no deposit" — rep noted no deposit needed (use singular "deposit"
  even if rep said "deposits")

VALUE PROPS:
- "background checked" — rep mentioned crew is background-checked,
  background-screened, "guys pass background checks", or similar
- "no day labor" — rep distinguished from day labor / temps
- "we show up" — rep emphasized reliability, "don't miss appointments"
- "clothes in the dresser" — rep mentioned this specific service
- "disassemble" — rep mentioned disassembly/reassembly of furniture
  (use canonical "disassemble" even if rep said "take apart" or
  "disassembling")

PROCESS:
- "confirmation call" — rep mentioned the day-of or week-of
  confirmation call

Return CHARACTER POSITION of first occurrence in the transcript.
ONLY include keywords actually said by the REP. Don't include things
the customer said unless the rep agreed and repeated them.

OBJECTIONS: Price too high, Need to think about it, Already have another quote, Wrong timing, Need to check with partner, Other

SENTIMENT: positive / neutral / hesitant / negative

Filename: {filename}

Transcript:
{transcript}

Respond ONLY with valid JSON, no markdown:

{{"rep_name_detected":"name or Unknown","caller_name":"name or Unknown","call_purpose":"short phrase","call_type":"sales_estimate","move_category":"standard","move_type":"local/long distance/unknown","pricing_model":"unknown","move_timeline":"unknown","call_outcome":"booked","loss_reason":"","soft_pipeline_reason":"","word_count":150,"exclude_from_scoring":false,"exclusion_reason":"","call_quality":"normal","turned_away":false,"onsite_suggested":false,"is_continuation":false,"evaluation_confidence":8,"close_attempts":1,"objections_overcome":[],"objections_abandoned":[],"pipeline_recovery_quality":0,"missed_rapport_opportunities":[],"value_props_used":[],"salesmanship_score":0,"call_summary":"3-5 sentences","key_details_captured":"details gathered","talk_ratio_rep":40,"talk_ratio_customer":60,"keywords_detected":["confirmation call"],"keyword_positions":{{"confirmation call":342}},"objections_detected":["Price too high"],"objection_positions":{{"Price too high":891}},"customer_sentiment":"positive","scores":{{"information_control":{{"score":0,"note":""}},"price_delivery":{{"score":0,"note":""}},"closing_attempt":{{"score":0,"note":""}},"professionalism":{{"score":0,"note":""}},"rapport_tone":{{"score":0,"note":""}},"salesmanship":{{"score":0,"note":""}},"overall":{{"score":0,"note":""}}}},"checklist":{{"got_move_date":false,"got_customer_name":false,"got_phone_number":false,"got_cities":false,"got_home_type":false,"got_stairs_info":false,"did_full_inventory":false,"asked_forgotten_items":false,"asked_about_boxes":false,"gave_price_on_call":false,"attempted_to_close":false,"offered_email_estimate":false,"mentioned_confirmations":false,"thanked_customer":false,"asked_name_at_start":false,"led_estimate_process":false,"got_email":false,"scheduled_onsite_attempt":"na","offered_alternatives":"na","took_rapport_opportunities":false,"completed_booking_wrapup":false,"captured_lead":false}},"strengths":["s1","s2"],"coaching_points":["c1","c2"]}}"""
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

def _supa_auth_header(key):
    """Return the correct Authorization header value for the given Supabase key.
    Legacy JWT keys (eyJ...) require 'Bearer <token>'.
    New keys (sb_secret_... or sb_publishable_...) must NOT have Bearer prefix —
    per Supabase docs, the Authorization value must exactly match the apikey value."""
    if key and key.startswith("sb_"):
        return key  # New format — match apikey exactly
    return f"Bearer {key}"  # Legacy JWT format

def supa(method, path, body=None, extra_headers=None, prefer_minimal=False):
    if not SUPABASE_URL or not SUPABASE_KEY:
        raise Exception("Supabase not configured")
    url = f"{SUPABASE_URL}/rest/v1/{path}"
    data = json.dumps(body).encode() if body else None
    req = urllib.request.Request(url, data=data, method=method)
    req.add_header("apikey", SUPABASE_KEY)
    req.add_header("Authorization", _supa_auth_header(SUPABASE_KEY))
    req.add_header("Content-Type", "application/json")
    req.add_header("Prefer", "return=minimal" if prefer_minimal else "return=representation")
    if extra_headers:
        for k, v in extra_headers.items():
            req.add_header(k, v)
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            body_bytes = r.read()
            if not body_bytes or body_bytes == b'':
                return {}
            return json.loads(body_bytes)
    except urllib.error.HTTPError as e:
        error_body = e.read().decode('utf-8', errors='replace')
        raise Exception(f"Supabase {method} {path[:60]} HTTP {e.code}: {error_body[:300]}")

def supa_storage_upload(bucket, path, data, content_type="audio/mpeg"):
    url = f"{SUPABASE_URL}/storage/v1/object/{bucket}/{path}"
    req = urllib.request.Request(url, data=data, method="POST")
    req.add_header("apikey", SUPABASE_KEY)
    req.add_header("Authorization", _supa_auth_header(SUPABASE_KEY))
    req.add_header("Content-Type", content_type)
    try:
        with urllib.request.urlopen(req, timeout=120) as r:
            return json.loads(r.read())
    except urllib.error.HTTPError as e:
        error_body = e.read().decode('utf-8', errors='replace')
        raise Exception(f"Storage upload {bucket}/{path[:50]} HTTP {e.code}: {error_body[:300]}")

def supa_storage_signed_url(bucket, path, expires=86400):
    url = f"{SUPABASE_URL}/storage/v1/object/sign/{bucket}/{path}"
    body = json.dumps({"expiresIn": expires}).encode()
    req = urllib.request.Request(url, data=body, method="POST")
    req.add_header("apikey", SUPABASE_KEY)
    req.add_header("Authorization", _supa_auth_header(SUPABASE_KEY))
    req.add_header("Content-Type", "application/json")
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            result = json.loads(r.read())
        signed = result.get("signedURL", "")
        if not signed:
            return ""
        # Supabase returns a relative path like "/object/sign/{bucket}/{path}?token=..."
        # We need to prepend "{SUPABASE_URL}/storage/v1" to make it a full URL.
        if signed.startswith("http"):
            return signed  # Already absolute (some Supabase versions return full URL)
        if signed.startswith("/storage/v1"):
            return f"{SUPABASE_URL}{signed}"  # Has prefix already
        # Otherwise, prepend the storage API path
        if not signed.startswith("/"):
            signed = "/" + signed
        return f"{SUPABASE_URL}/storage/v1{signed}"
    except urllib.error.HTTPError as e:
        error_body = e.read().decode('utf-8', errors='replace')
        log(f"  Signed URL error for {bucket}/{path[:50]}: HTTP {e.code} {error_body[:200]}")
        return ""

def supa_storage_list(bucket):
    url = f"{SUPABASE_URL}/storage/v1/object/list/{bucket}"
    body = json.dumps({"prefix": "", "limit": 1000}).encode()
    req = urllib.request.Request(url, data=body, method="POST")
    req.add_header("apikey", SUPABASE_KEY)
    req.add_header("Authorization", _supa_auth_header(SUPABASE_KEY))
    req.add_header("Content-Type", "application/json")
    with urllib.request.urlopen(req, timeout=30) as r:
        return json.loads(r.read())

def supa_storage_delete(bucket, paths):
    url = f"{SUPABASE_URL}/storage/v1/object/{bucket}"
    body = json.dumps({"prefixes": paths}).encode()
    req = urllib.request.Request(url, data=body, method="DELETE")
    req.add_header("apikey", SUPABASE_KEY)
    req.add_header("Authorization", _supa_auth_header(SUPABASE_KEY))
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
                    supa("PATCH", f"calls?id=eq.{call['id']}", {"audio_url": "", "storage_filename": ""})
                    total_bytes -= file_size
                except Exception as e:
                    log(f"  Storage cleanup error: {e}")
    except Exception as e:
        log(f"  Storage cap error: {e}")

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
        log(f"  Continuation lookup error: {e}")
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
        log(f"  Retroactive linking error: {e}")

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
# TRANSCRIPT CORRECTIONS
# ──────────────────────────────────────────────

def get_transcript_corrections():
    """Fetch all saved find/replace corrections from Supabase."""
    try:
        return supa("GET", "transcript_corrections?order=created_at.asc&limit=500")
    except Exception:
        return []

def apply_transcript_corrections(transcript, corrections):
    """Apply find/replace corrections to transcript before Claude analysis."""
    if not corrections:
        return transcript
    for c in corrections:
        find = c.get("find_text", "")
        replace = c.get("replace_text", "")
        if find:
            transcript = re.sub(re.escape(find), replace, transcript, flags=re.IGNORECASE)
    return transcript

def build_keyterms(rep_names=None, corrections=None):
    """Build keyterm list for Deepgram from rep names + corrections + known LGMS vocabulary."""
    terms = set([
        "Little Guys Movers", "Little Guys", "Full Value Protection", "FVP",
        "confirmation call", "fuel charge", "declared value", "National Express",
        "Rivermont", "moving boxes", "no cancellation fee", "first come first serve",
        "background checked", "no day labor",
    ])
    if rep_names:
        for name in rep_names:
            if name and name != "Unknown":
                terms.add(name)
    if corrections:
        for c in corrections:
            replace = c.get("replace_text", "")
            if replace and len(replace) > 2:
                terms.add(replace)
    return list(terms)[:100]

def get_cached_keyterms():
    """Return cached keyterms, rebuilding if older than 30 minutes."""
    global _keyterm_cache
    with _keyterm_lock:
        age = time.time() - _keyterm_cache["built_at"]
        if age < 1800 and _keyterm_cache["terms"]:
            return _keyterm_cache["terms"]
    try:
        rep_list = supa("GET", "reps?active=eq.true&select=full_name,nickname")
        rep_names = []
        for r in (rep_list or []):
            if r.get("full_name"): rep_names.append(r["full_name"])
            if r.get("nickname"): rep_names.append(r["nickname"])
        tx_corrections = get_transcript_corrections()
        terms = build_keyterms(rep_names=rep_names, corrections=tx_corrections)
    except Exception:
        terms = build_keyterms()
    with _keyterm_lock:
        _keyterm_cache = {"terms": terms, "built_at": time.time()}
    log(f"  Keyterm cache rebuilt: {len(terms)} terms")
    return terms

# ──────────────────────────────────────────────
# DEEPGRAM TRANSCRIPTION
# ──────────────────────────────────────────────

def transcribe_audio_deepgram(audio_bytes, filename, keyterms=None):
    """Transcribe audio using Deepgram Nova-3 with diarization and keyterm prompting."""
    if not DEEPGRAM_KEY:
        raise Exception("DEEPGRAM_API_KEY not set")

    # Build query params
    params = {
        "model": "nova-3",
        "diarize": "true",
        "punctuate": "true",
        "smart_format": "true",
        "numerals": "true",
        "utterances": "true",
    }
    # Add keyterms (each as separate param)
    keyterm_str = ""
    if keyterms:
        keyterm_str = "&" + "&".join(f"keyterm={urllib.parse.quote(k)}" for k in keyterms[:100])

    query = urllib.parse.urlencode(params) + keyterm_str
    url = f"https://api.deepgram.com/v1/listen?{query}"

    ext = filename.rsplit(".", 1)[-1].lower() if "." in filename else "mp3"
    mime_map = {"mp3": "audio/mpeg", "mp4": "audio/mp4", "m4a": "audio/mp4",
                "wav": "audio/wav", "ogg": "audio/ogg", "webm": "audio/webm",
                "mpeg": "audio/mpeg", "mpga": "audio/mpeg"}
    mime = mime_map.get(ext, "audio/mpeg")

    req = urllib.request.Request(url, data=audio_bytes, method="POST")
    req.add_header("Authorization", f"Token {DEEPGRAM_KEY}")
    req.add_header("Content-Type", mime)

    with urllib.request.urlopen(req, timeout=300) as r:
        result = json.loads(r.read())

    # Log Deepgram response structure for debugging
    has_utterances = bool(result.get("results", {}).get("utterances"))
    has_channels = bool(result.get("results", {}).get("channels"))
    log(f"  Deepgram response: utterances={has_utterances}, channels={has_channels}")

    # Try utterances first (best diarization)
    utterances = result.get("results", {}).get("utterances", [])
    if utterances:
        lines = []
        words_flat = []  # Flat list of {w, s, e, sp} for click-to-seek transcript
        for u in utterances:
            speaker = u.get("speaker", 0)
            text = u.get("transcript", "").strip()
            if text:
                lines.append(f"Speaker {speaker}: {text}")
            for w in u.get("words", []):
                words_flat.append({
                    "w": w.get("punctuated_word", w.get("word", "")),
                    "s": round(float(w.get("start", 0)), 3),
                    "e": round(float(w.get("end", 0)), 3),
                    "sp": w.get("speaker", speaker),
                })
        transcript = "\n".join(lines)
        log(f"  Diarized via utterances: {len(utterances)} turns, {len(words_flat)} words timestamped")
        return transcript, True, words_flat

    # Fallback: try word-level diarization from channels
    channels = result.get("results", {}).get("channels", [])
    if channels:
        words = channels[0].get("alternatives", [{}])[0].get("words", [])
        plain = channels[0].get("alternatives", [{}])[0].get("transcript", "")

        words_flat = [{
            "w": w.get("punctuated_word", w.get("word", "")),
            "s": round(float(w.get("start", 0)), 3),
            "e": round(float(w.get("end", 0)), 3),
            "sp": w.get("speaker", 0),
        } for w in words]

        if words and any("speaker" in w for w in words):
            # Build diarized transcript from word-level speaker tags
            lines = []
            current_speaker = None
            current_words = []
            for w in words:
                spk = w.get("speaker", 0)
                word = w.get("punctuated_word", w.get("word", ""))
                if spk != current_speaker:
                    if current_words:
                        lines.append(f"Speaker {current_speaker}: {' '.join(current_words)}")
                    current_speaker = spk
                    current_words = [word]
                else:
                    current_words.append(word)
            if current_words:
                lines.append(f"Speaker {current_speaker}: {' '.join(current_words)}")
            transcript = "\n".join(lines)
            log(f"  Diarized via word-level: {len(lines)} turns, {len(words_flat)} words timestamped")
            return transcript, True, words_flat
        else:
            log(f"  No diarization data, using plain transcript ({len(plain)} chars), {len(words_flat)} words timestamped")
            return plain, False, words_flat

    log("  Deepgram returned no usable transcript")
    return "", False, []

# Fallback Whisper transcription if Deepgram unavailable
def transcribe_audio_whisper(audio_bytes, filename):
    if not OPENAI_KEY:
        raise Exception("Neither DEEPGRAM_API_KEY nor OPENAI_API_KEY is set")
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
        "https://api.openai.com/v1/audio/transcriptions", data=body,
        headers={"Authorization": f"Bearer {OPENAI_KEY}", "Content-Type": f"multipart/form-data; boundary={boundary}"},
        method="POST"
    )
    with urllib.request.urlopen(req, timeout=300) as r:
        return r.read().decode("utf-8"), False, []

def transcribe_audio(audio_bytes, filename, keyterms=None):
    """Transcribe using Deepgram, fall back to Whisper if needed."""
    if DEEPGRAM_KEY:
        return transcribe_audio_deepgram(audio_bytes, filename, keyterms)
    elif OPENAI_KEY:
        log("  WARNING: Deepgram not configured, falling back to Whisper")
        return transcribe_audio_whisper(audio_bytes, filename)
    else:
        raise Exception("No transcription API configured. Set DEEPGRAM_API_KEY.")

# ──────────────────────────────────────────────
# CLAUDE ANALYSIS
# ──────────────────────────────────────────────

def calculate_weighted_overall(scores):
    """Calculate weighted overall score server-side — guaranteed accuracy."""
    w = {
        "closing_attempt":      0.25,
        "price_delivery":       0.15,
        "rapport_tone":         0.15,
        "salesmanship":         0.20,
        "information_control":  0.15,
        "professionalism":      0.10,
    }
    total = 0.0
    weight_used = 0.0
    for key, weight in w.items():
        s = scores.get(key, {})
        score = s.get("score", 0) if isinstance(s, dict) else 0
        if score > 0:
            total += score * weight
            weight_used += weight
    if weight_used < 0.3:
        return 0
    normalized = total / weight_used if weight_used > 0 else 0
    return min(10, max(1, round(normalized)))

# Vonage ingestion settings
# Max call duration in seconds to process (default 15 min = 900s). Calls longer than this are skipped.
MAX_CALL_DURATION_SECONDS = int(os.environ.get("MAX_CALL_DURATION_SECONDS", 900))

def should_skip_by_duration(duration_seconds):
    """Return (skip, reason) for calls that exceed the max duration threshold."""
    if duration_seconds and duration_seconds > MAX_CALL_DURATION_SECONDS:
        mins = duration_seconds // 60
        return True, f"Call duration {mins}m exceeds {MAX_CALL_DURATION_SECONDS//60}m limit"
    return False, ""

def normalize_objection(raw):
    """Normalize objection labels to canonical display names regardless of how Claude returned them."""
    if not raw:
        return raw
    s = str(raw).lower().strip().replace('_', ' ').replace('-', ' ')
    # Remove trailing punctuation
    s = s.rstrip('.,!?')
    if any(x in s for x in ['price', 'too high', 'expensive', 'cost', 'quote', 'cheaper']):
        if any(x in s for x in ['another quote', 'other quote', 'competitor', 'someone else', 'already have', 'have a quote', 'have another']):
            return "Already have another quote"
        return "Price too high"
    if any(x in s for x in ['think', 'consider', 'decide', 'not sure', 'unsure', 'need time', 'think about']):
        return "Need to think about it"
    if any(x in s for x in ['partner', 'spouse', 'husband', 'wife', 'check with', 'talk to', 'significant other', 'roommate']):
        return "Need to check with partner"
    if any(x in s for x in ['timing', 'not ready', 'wrong time', 'bad time', 'too early', 'too late', 'not the right']):
        return "Wrong timing"
    if any(x in s for x in ['another quote', 'other quote', 'have a quote', 'already have', 'someone else', 'other company', 'other mover']):
        return "Already have another quote"
    return raw.strip()

def run_claude_analysis(transcript, filename, model="claude-sonnet-4-20250514", is_diarized=False):
    corrections = get_recent_corrections()
    # Truncate very long transcripts to prevent exceeding Claude's token limit
    # Prompt template is ~16k chars; Claude's limit is ~200k tokens but request body has practical limits
    # 14,000 chars of transcript ≈ 3,500 tokens — combined with prompt stays well within limits
    MAX_TRANSCRIPT_CHARS = 14000
    if len(transcript) > MAX_TRANSCRIPT_CHARS:
        log(f"  Truncating long transcript: {len(transcript)} chars → {MAX_TRANSCRIPT_CHARS} chars ({filename})")
        # Try to truncate at a sentence boundary
        truncated = transcript[:MAX_TRANSCRIPT_CHARS]
        last_newline = truncated.rfind('\n')
        if last_newline > MAX_TRANSCRIPT_CHARS * 0.9:
            truncated = truncated[:last_newline]
        transcript = truncated + "\n[Transcript truncated — call was unusually long]"
    prompt = build_prompt(transcript, filename, corrections, is_diarized=is_diarized)
    req_body = json.dumps({
        "model": model,
        "max_tokens": 4096,
        "messages": [{"role": "user", "content": prompt}]
    }).encode()
    req = urllib.request.Request(
        "https://api.anthropic.com/v1/messages", data=req_body,
        headers={"Content-Type": "application/json", "x-api-key": API_KEY, "anthropic-version": "2023-06-01"},
        method="POST"
    )
    try:
        with urllib.request.urlopen(req, timeout=120) as r:
            resp = json.loads(r.read())
    except urllib.error.HTTPError as e:
        error_body = e.read().decode('utf-8', errors='replace')
        raise Exception(f"Anthropic API HTTP {e.code}: {error_body[:500]}")
    tb = next((b for b in resp.get("content", []) if b.get("type") == "text"), None)
    if not tb:
        raise Exception("No response from Claude")
    raw = tb["text"].strip()
    if raw.startswith("```"):
        raw = re.sub(r'^```[a-z]*\n?', '', raw)
        raw = re.sub(r'\n?```$', '', raw)
        raw = raw.strip()
    start = raw.find('{')
    end = raw.rfind('}')
    if start >= 0 and end > start:
        raw = raw[start:end+1]
    try:
        result = json.loads(raw)
    except json.JSONDecodeError as e:
        log(f"  JSON parse error: {e} — attempting repair")
        raw = re.sub(r',\s*}', '}', raw)
        raw = re.sub(r',\s*]', ']', raw)
        raw = re.sub(r'[\x00-\x08\x0b\x0c\x0e-\x1f]', '', raw)
        result = json.loads(raw)

    # Calculate weighted overall server-side
    scores = result.get("scores", {})
    # Add salesmanship score to scores dict if returned separately
    if "salesmanship_score" in result and "salesmanship" not in scores:
        scores["salesmanship"] = {"score": result["salesmanship_score"], "note": ""}
    weighted_overall = calculate_weighted_overall(scores)
    if weighted_overall > 0:
        scores["overall"] = {"score": weighted_overall, "note": "Weighted: closing 25%, price 15%, rapport 15%, salesmanship 20%, info&control 15%, prof 10%"}
        result["scores"] = scores

    # Normalize objection labels — Claude sometimes returns snake_case or slight variants
    result["objections_detected"] = [normalize_objection(o) for o in result.get("objections_detected", [])]
    # Re-key objection_positions with normalized labels
    raw_pos = result.get("objection_positions", {})
    result["objection_positions"] = {normalize_objection(k): v for k, v in raw_pos.items()}

    # Save token counts for cost tracking
    usage = resp.get("usage", {})
    result["input_tokens"] = usage.get("input_tokens", 0)
    result["output_tokens"] = usage.get("output_tokens", 0)

    return result

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

    score_keys = ["rapport_tone","information_control","price_delivery","closing_attempt","salesmanship","professionalism","overall"]
    score_labels = ["Rapport & Tone","Information & Control","Price Delivery","Closing Attempt","Salesmanship","Professionalism","Overall"]

    ck_keys = ["got_move_date","got_customer_name","got_phone_number","got_cities","got_home_type","got_stairs_info","did_full_inventory","asked_forgotten_items","asked_about_boxes","gave_price_on_call","attempted_to_close","offered_email_estimate","mentioned_confirmations","thanked_customer","asked_name_at_start","led_estimate_process","got_email","scheduled_onsite_attempt","offered_alternatives","took_rapport_opportunities","completed_booking_wrapup","captured_lead"]
    ck_labels = ["Move date","Customer name","Phone number","Cities/locations","Home type","Stairs info","Full inventory","Forgotten items","Moving boxes","Gave price on call","Closing attempt","Email estimate offered","Confirmations mentioned","Thanked customer","Asked name at start","Led estimate process","Got email","Scheduled onsite attempt","Offered alternatives","Took rapport opportunities","Completed booking wrap-up","Lead captured"]

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
        # Wait for any active batch upload to finish first
        waited = 0
        while _batch_job.get("status") == "running" and waited < 300:
            log("Reanalyze waiting for batch upload to finish...")
            time.sleep(10)
            waited += 10
        calls = supa("GET", "calls?order=created_at.desc&limit=5000&select=id,transcript,filename")
        total = len(calls)
        # Fetch transcript corrections ONCE before the loop, not on every iteration
        try:
            tx_corrections_cached = get_transcript_corrections()
        except Exception:
            tx_corrections_cached = []
        with _reanalyze_lock:
            _reanalyze_job["total"] = total
            _reanalyze_job["processed"] = 0
            _reanalyze_job["errors"] = 0
            _reanalyze_job["failed_calls"] = []
            _reanalyze_job["skipped"] = 0

        for call in calls:
            # Check if stop was requested
            with _reanalyze_lock:
                if _reanalyze_job.get("stop_requested"):
                    _reanalyze_job["status"] = "stopped"
                    _reanalyze_job["current"] = ""
                    _reanalyze_job["finished_at"] = datetime.now(timezone.utc).isoformat()
                    log("  Re-analyze stopped by user request")
                    return

            transcript = call.get("transcript", "")
            filename = call.get("filename", "call.txt")
            call_id = call.get("id")

            with _reanalyze_lock:
                _reanalyze_job["current"] = filename

            if not transcript or not transcript.strip():
                with _reanalyze_lock:
                    _reanalyze_job["processed"] += 1
                    _reanalyze_job["skipped"] += 1
                continue

            try:
                with _analysis_semaphore:
                    # Apply corrections before analysis (using corrections cached at job start)
                    clean_transcript = apply_transcript_corrections(transcript, tx_corrections_cached)
                    result = run_claude_analysis(clean_transcript, filename)

                # Fetch existing record's manager-controlled fields so we don't overwrite them
                # (managers may have manually toggled turned_away, exclusions, etc.)
                try:
                    existing = supa("GET", f"calls?id=eq.{call_id}&select=availability_decline,turned_away,onsite_suggested,exclude_from_scoring,exclusion_reason,manager_notes,tags,score_overrides,is_continuation&limit=1")
                    existing_call = existing[0] if existing else {}
                except Exception:
                    existing_call = {}

                call_date = parse_call_date_from_filename(filename)
                update_data = {
                    # Claude-derived fields — overwrite freely
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
                    "call_type": result.get("call_type", "sales_estimate"),
                    "move_category": result.get("move_category", "standard"),
                    "loss_reason": result.get("loss_reason", ""),
                    "soft_pipeline_reason": result.get("soft_pipeline_reason", ""),
                    "evaluation_confidence": result.get("evaluation_confidence", 8),
                    "close_attempts": result.get("close_attempts", 0),
                    "objections_overcome": result.get("objections_overcome", []),
                    "objections_abandoned": result.get("objections_abandoned", []),
                    "pipeline_recovery_quality": result.get("pipeline_recovery_quality", 0),
                    "salesmanship_score": result.get("scores", {}).get("salesmanship", {}).get("score", 0) if isinstance(result.get("scores", {}).get("salesmanship"), dict) else 0,
                    "value_props_used": result.get("value_props_used", []),
                    "missed_rapport_opportunities": result.get("missed_rapport_opportunities", []),
                    "input_tokens": result.get("input_tokens", 0),
                    "output_tokens": result.get("output_tokens", 0),
                    "pricing_model": result.get("pricing_model", "unknown"),
                    "move_timeline": result.get("move_timeline", "unknown"),
                    "call_quality": result.get("call_quality", "normal"),
                    # Manager-controlled fields — preserve existing values if previously set
                    # If existing has a True value, keep True (manager-set). Otherwise use Claude's detection.
                    "availability_decline": existing_call.get("availability_decline") or result.get("availability_decline", False),
                    "turned_away": existing_call.get("turned_away") or result.get("turned_away", False),
                    "onsite_suggested": existing_call.get("onsite_suggested") or result.get("onsite_suggested", False),
                    "is_continuation": existing_call.get("is_continuation") or result.get("is_continuation", False),
                }
                # Manager exclusion handling: preserve manual exclusions but allow auto-exclude on disconnect
                existing_exclude = existing_call.get("exclude_from_scoring", False)
                existing_reason = existing_call.get("exclusion_reason", "")
                if existing_exclude and not existing_reason.startswith("Disconnected"):
                    # Manager manually excluded — preserve their decision and reason
                    update_data["exclude_from_scoring"] = True
                    update_data["exclusion_reason"] = existing_reason
                elif result.get("call_quality") == "disconnected":
                    # Auto-exclude disconnected
                    update_data["exclude_from_scoring"] = True
                    update_data["exclusion_reason"] = "Disconnected/short call — auto excluded"
                else:
                    update_data["exclude_from_scoring"] = result.get("exclude_from_scoring", False)
                    update_data["exclusion_reason"] = result.get("exclusion_reason", "")

                if call_date:
                    update_data["call_date"] = call_date

                supa("PATCH", f"calls?id=eq.{call_id}", update_data, prefer_minimal=True)

                with _reanalyze_lock:
                    _reanalyze_job["processed"] += 1

                time.sleep(0.5)  # Brief pause between calls — keeps server responsive to other requests

            except Exception as e:
                import traceback
                full_error = traceback.format_exc()
                log(f"  Re-analyze error for {filename}: {e}")
                log(f"  Full traceback: {full_error[-500:]}")
                with _reanalyze_lock:
                    _reanalyze_job["errors"] += 1
                    _reanalyze_job["processed"] += 1
                    _reanalyze_job["failed_calls"].append({"filename": filename, "id": call_id, "error": str(e)[:300]})

        with _reanalyze_lock:
            _reanalyze_job["status"] = "complete"
            _reanalyze_job["finished_at"] = datetime.now(timezone.utc).isoformat()
            _reanalyze_job["current"] = ""
            _reanalyze_job["summary"] = {
                "total": total,
                "succeeded": total - _reanalyze_job["errors"] - _reanalyze_job["skipped"],
                "skipped": _reanalyze_job["skipped"],
                "errors": _reanalyze_job["errors"],
                "finished_at": _reanalyze_job["finished_at"],
            }

    except Exception as e:
        with _reanalyze_lock:
            _reanalyze_job["status"] = "error"
            _reanalyze_job["current"] = str(e)
        log(f"  Re-analyze worker error: {e}")

# ──────────────────────────────────────────────
# BATCH UPLOAD WORKER
# ──────────────────────────────────────────────

def _process_single_file(audio_bytes, filename, keyterms, tx_corrections):
    """Process one audio file — transcribe, analyze, save. Returns (saved_record, skip_reason)."""
    # Check duplicate
    try:
        safe = filename.replace("'", "''")
        existing = supa("GET", f"calls?filename=ilike.{urllib.parse.quote(safe)}&limit=1")
        if existing:
            return None, "duplicate"
    except Exception:
        pass

    # Transcribe
    transcript, is_diarized, word_timestamps = transcribe_audio(audio_bytes, filename, keyterms=keyterms)
    if not transcript or not transcript.strip():
        return None, "empty transcript"

    # Apply corrections
    clean_transcript = apply_transcript_corrections(transcript, tx_corrections)

    # Analyze
    result = run_claude_analysis(clean_transcript, filename, is_diarized=is_diarized)
    result["transcript"] = clean_transcript
    result["filename"] = filename
    result["is_diarized"] = is_diarized
    if word_timestamps:
        result["word_timestamps"] = word_timestamps

    # Upload audio to storage
    try:
        enforce_storage_cap()
        safe_filename = re.sub(r'[^\w\-_\.]', '_', filename)
        supa_storage_upload("call-audio", safe_filename, audio_bytes, "audio/mpeg")
        result["audio_url"] = supa_storage_signed_url("call-audio", safe_filename)
        result["storage_filename"] = safe_filename
    except Exception as e:
        log(f"  Audio storage warning for {filename}: {e}")
        result["audio_url"] = ""
        result["storage_filename"] = ""

    # Save to database
    call_date = parse_call_date_from_filename(filename)
    rep_name_raw = result.get("rep_name_detected") or "Unknown"
    try:
        rep_list = supa("GET", "reps?active=eq.true")
        matched_name, confidence = fuzzy_match_rep(rep_name_raw, rep_list)
        rep_name = matched_name if confidence >= 0.90 else rep_name_raw
    except Exception:
        rep_name = rep_name_raw

    call_quality = result.get("call_quality", "normal")
    exclude = result.get("exclude_from_scoring", False)
    exclusion_reason = result.get("exclusion_reason", "")
    if call_quality == "disconnected" and not exclude:
        exclude = True
        exclusion_reason = "Disconnected/short call — auto excluded"

    scores = result.get("scores", {})
    record = {
        "rep_name": rep_name,
        "filename": filename,
        "storage_filename": result.get("storage_filename", ""),
        "transcript": clean_transcript,
        "caller_name": result.get("caller_name", ""),
        "call_purpose": result.get("call_purpose", ""),
        "call_type": result.get("call_type", "sales_estimate"),
        "move_type": result.get("move_type", ""),
        "move_category": result.get("move_category", "standard"),
        "call_outcome": result.get("call_outcome", "unknown"),
        "loss_reason": result.get("loss_reason", ""),
        "soft_pipeline_reason": result.get("soft_pipeline_reason", ""),
        "word_count": result.get("word_count", 0),
        "exclude_from_scoring": exclude,
        "exclusion_reason": exclusion_reason,
        "call_summary": result.get("call_summary", ""),
        "key_details": result.get("key_details_captured", ""),
        "talk_ratio_rep": result.get("talk_ratio_rep", 0),
        "talk_ratio_customer": result.get("talk_ratio_customer", 0),
        "keywords_detected": result.get("keywords_detected", []),
        "keyword_positions": result.get("keyword_positions", {}),
        "objections_detected": result.get("objections_detected", []),
        "objection_positions": result.get("objection_positions", {}),
        "customer_sentiment": result.get("customer_sentiment", "neutral"),
        "scores": scores,
        "checklist": result.get("checklist", {}),
        "strengths": result.get("strengths", []),
        "coaching_points": result.get("coaching_points", []),
        "tags": [],
        "manager_notes": "",
        "score_overrides": {},
        "call_date": call_date,
        "audio_url": result.get("audio_url", ""),
        "availability_decline": result.get("availability_decline", False),
        "turned_away": result.get("turned_away", False),
        "onsite_suggested": result.get("onsite_suggested", False),
        "call_quality": call_quality,
        "is_continuation": result.get("is_continuation", False),
        "continuation_group_id": "",
        "evaluation_confidence": result.get("evaluation_confidence", 8),
        "is_diarized": is_diarized,
        "close_attempts": result.get("close_attempts", 0),
        "objections_overcome": result.get("objections_overcome", []),
        "objections_abandoned": result.get("objections_abandoned", []),
        "pipeline_recovery_quality": result.get("pipeline_recovery_quality", 0),
        "salesmanship_score": scores.get("salesmanship", {}).get("score", 0) if isinstance(scores.get("salesmanship"), dict) else 0,
        "value_props_used": result.get("value_props_used", []),
        "missed_rapport_opportunities": result.get("missed_rapport_opportunities", []),
        "input_tokens": result.get("input_tokens", 0),
        "output_tokens": result.get("output_tokens", 0),
        "pricing_model": result.get("pricing_model", "unknown"),
        "move_timeline": result.get("move_timeline", "unknown"),
        "word_timestamps": result.get("word_timestamps") or None,
    }

    # Save to Supabase
    try:
        saved = supa("POST", "calls", record)
        return saved, None
    except Exception as e:
        log(f"  Save error for {filename}: {e}")
        raise

def _batch_upload_worker(zip_bytes):
    """Background worker — extract ZIP, process each audio file, save to DB."""
    global _batch_job
    try:
        # Get cached keyterms and corrections once for the whole batch
        keyterms = get_cached_keyterms()
        tx_corrections = get_transcript_corrections()

        # Extract audio files from ZIP
        audio_exts = {".mp3", ".m4a", ".wav", ".ogg", ".mp4", ".webm", ".mpeg", ".mpga"}
        audio_files = []
        try:
            with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
                for name in zf.namelist():
                    if name.startswith("__") or name.startswith(".") or name.endswith("/"):
                        continue
                    ext = os.path.splitext(name)[1].lower()
                    if ext not in audio_exts:
                        continue
                    basename = os.path.basename(name)
                    if not basename:
                        continue
                    audio_data = zf.read(name)
                    if len(audio_data) > 25 * 1024 * 1024:
                        with _batch_lock:
                            _batch_job["skipped"] += 1
                            _batch_job["error_list"].append(f"{basename} — too large (>25MB)")
                        continue
                    audio_files.append((basename, audio_data))
        except zipfile.BadZipFile as e:
            with _batch_lock:
                _batch_job["status"] = "error"
                _batch_job["current"] = f"Invalid ZIP: {e}"
            return

        total = len(audio_files)
        with _batch_lock:
            _batch_job["total"] = total

        log(f"  Batch upload: {total} files extracted from ZIP")

        # Process in batches of 3
        for i in range(0, total, 3):
            # Check for stop request before each group
            with _batch_lock:
                if _batch_job.get("stop_requested"):
                    _batch_job["status"] = "stopped"
                    _batch_job["finished_at"] = datetime.now(timezone.utc).isoformat()
                    _batch_job["current"] = ""
                    log(f"  Batch upload stopped by user: {_batch_job['processed']} processed before halt")
                    return
            batch = audio_files[i:i+3]
            threads = []
            results = [None] * len(batch)

            def process_file(idx, fname, fdata):
                with _batch_lock:
                    _batch_job["current"] = fname
                try:
                    with _analysis_semaphore:
                        saved, skip_reason = _process_single_file(fdata, fname, keyterms, tx_corrections)
                    if skip_reason:
                        with _batch_lock:
                            _batch_job["skipped"] += 1
                            if skip_reason != "duplicate":
                                _batch_job["error_list"].append(f"{fname} — {skip_reason}")
                    else:
                        with _batch_lock:
                            _batch_job["processed"] += 1
                except Exception as e:
                    log(f"  Batch error for {fname}: {e}")
                    with _batch_lock:
                        _batch_job["errors"] += 1
                        _batch_job["error_list"].append(f"{fname} — {str(e)[:80]}")
                finally:
                    with _batch_lock:
                        if _batch_job["current"] == fname:
                            _batch_job["current"] = ""

            for j, (fname, fdata) in enumerate(batch):
                t = threading.Thread(target=process_file, args=(j, fname, fdata), daemon=True)
                threads.append(t)
                t.start()
            for t in threads:
                t.join()

        with _batch_lock:
            _batch_job["status"] = "complete"
            _batch_job["finished_at"] = datetime.now(timezone.utc).isoformat()
            _batch_job["current"] = ""
        log(f"  Batch upload complete: {_batch_job['processed']} processed, {_batch_job['skipped']} skipped, {_batch_job['errors']} errors")

    except Exception as e:
        with _batch_lock:
            _batch_job["status"] = "error"
            _batch_job["current"] = str(e)
        log(f"  Batch upload worker error: {e}")

# ──────────────────────────────────────────────
# VONAGE VBC — OAUTH, API CLIENT, POLLING WORKER
# ──────────────────────────────────────────────
# Implementation notes:
#
# This is "scaffolded" code. The structure (token caching, polling loop,
# idempotency, error handling) is solid and won't change. But the exact
# endpoint URLs, OAuth grant type, response field names, and pagination
# style need to be confirmed against the actual VBC API docs (visible
# from the user's developer dashboard at vbcdeveloper.vonage.com).
#
# Every place that depends on a specific endpoint detail is marked with
# a # TODO(vbc) comment. Find them all by grepping for "TODO(vbc)" before
# going live.

def _vonage_get_token(force_refresh=False):
    """Get a valid OAuth bearer token, refreshing if needed.

    Caches the token in _vonage_token_cache. Refreshes 60 seconds before
    actual expiry to avoid edge-case 401s. Thread-safe.
    """
    if not _vonage_configured():
        raise RuntimeError("Vonage not configured (missing client ID, secret, or account ID)")

    with _vonage_token_lock:
        now = time.time()
        cached = _vonage_token_cache
        if not force_refresh and cached.get("token") and cached.get("expires_at", 0) > now + 60:
            return cached["token"]

        # TODO(vbc): Confirm the OAuth grant type. Most likely "client_credentials"
        # for server-to-server VBC integrations, but VBC docs may specify otherwise.
        # If they require Basic auth in the header instead of body, swap to that.
        body = urllib.parse.urlencode({
            "grant_type": "client_credentials",
            "client_id": VONAGE_CLIENT_ID,
            "client_secret": VONAGE_CLIENT_SECRET,
        }).encode()
        req = urllib.request.Request(
            VONAGE_TOKEN_URL,
            data=body,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            method="POST",
        )
        try:
            with urllib.request.urlopen(req, timeout=30) as r:
                resp = json.loads(r.read().decode("utf-8"))
        except urllib.error.HTTPError as e:
            err = e.read().decode("utf-8", errors="replace")[:500]
            raise RuntimeError(f"OAuth token request failed: HTTP {e.code} — {err}")

        token = resp.get("access_token")
        expires_in = int(resp.get("expires_in", 3600))
        if not token:
            raise RuntimeError(f"OAuth token response missing access_token: {resp}")

        cached["token"] = token
        cached["expires_at"] = now + expires_in
        return token


def _vonage_api(method, path, params=None, retry_on_401=True):
    """Call a VBC API endpoint and return the parsed JSON response.

    `path` should start with "/" and be relative to VONAGE_API_BASE.
    Automatically refreshes token on 401 and retries once.
    """
    token = _vonage_get_token()
    qs = ""
    if params:
        qs = "?" + urllib.parse.urlencode(params)
    url = f"{VONAGE_API_BASE}{path}{qs}"
    req = urllib.request.Request(
        url,
        headers={"Authorization": f"Bearer {token}", "Accept": "application/json"},
        method=method,
    )
    try:
        with urllib.request.urlopen(req, timeout=60) as r:
            data = r.read()
            if not data:
                return None
            return json.loads(data.decode("utf-8"))
    except urllib.error.HTTPError as e:
        if e.code == 401 and retry_on_401:
            log("  Vonage API 401 — refreshing token and retrying")
            _vonage_get_token(force_refresh=True)
            return _vonage_api(method, path, params=params, retry_on_401=False)
        err_body = e.read().decode("utf-8", errors="replace")[:500] if e.fp else ""
        raise RuntimeError(f"Vonage API {method} {path} failed: HTTP {e.code} — {err_body}")


def _vonage_list_recordings(since_iso):
    """List recordings created since the given ISO timestamp.

    Returns a list of dicts. Each dict represents one recording.
    Pagination is handled internally (follows next-page links until
    exhausted).
    """
    # TODO(vbc): Confirm the actual endpoint path. Educated guess:
    #   /vbc/v1/accounts/{account_id}/call_recordings
    #   /api/v1/accounts/{account_id}/recordings
    # The path will include the account ID. Filter param for time-bounded
    # queries is typically called "start" or "from" — adjust to match docs.
    endpoint = f"/vbc/v1/accounts/{VONAGE_ACCOUNT_ID}/call_recordings"

    all_items = []
    next_page = None
    page_count = 0
    while True:
        page_count += 1
        if page_count > 50:  # Safety brake — never paginate forever
            log("  Vonage list paginated past 50 pages, stopping for safety")
            break
        params = {"start": since_iso, "page_size": 100}
        if next_page:
            params["page"] = next_page
        try:
            resp = _vonage_api("GET", endpoint, params=params)
        except Exception as e:
            log(f"  Vonage list error on page {page_count}: {e}")
            raise

        # TODO(vbc): The response shape is unknown until we see the real one.
        # Most VBC APIs return either:
        #   {"items": [...], "_links": {"next": {"href": "..."}}}
        # or
        #   {"data": [...], "next_cursor": "..."}
        # Adjust these two lines once the real shape is known:
        items = resp.get("items") or resp.get("data") or []
        all_items.extend(items)

        # Pagination cursor extraction — also needs confirmation:
        next_link = (resp.get("_links") or {}).get("next") or {}
        next_page = next_link.get("href") or resp.get("next_cursor")
        if not next_page:
            break

    return all_items


def _vonage_download_recording(recording_id):
    """Download the audio bytes for a single recording.

    Returns (audio_bytes, mime_type). Raises on error.
    """
    # TODO(vbc): Confirm download endpoint. Could be:
    #   /vbc/v1/accounts/{account_id}/call_recordings/{recording_id}/download
    #   /vbc/v1/recordings/{recording_id}/audio
    # And it may require a separate signed URL fetch first. Adjust as needed.
    endpoint = f"/vbc/v1/accounts/{VONAGE_ACCOUNT_ID}/call_recordings/{recording_id}/download"
    token = _vonage_get_token()
    url = f"{VONAGE_API_BASE}{endpoint}"
    req = urllib.request.Request(
        url,
        headers={"Authorization": f"Bearer {token}", "Accept": "audio/mpeg, audio/*"},
        method="GET",
    )
    try:
        with urllib.request.urlopen(req, timeout=300) as r:
            audio = r.read()
            mime = r.headers.get("Content-Type", "audio/mpeg")
            return audio, mime
    except urllib.error.HTTPError as e:
        if e.code == 401:
            _vonage_get_token(force_refresh=True)
            return _vonage_download_recording(recording_id)
        err = e.read().decode("utf-8", errors="replace")[:300] if e.fp else ""
        raise RuntimeError(f"Recording download failed: HTTP {e.code} — {err}")


def _vonage_recording_seen(recording_id):
    """Check the idempotency table — have we already processed this recording?"""
    try:
        rows = supa("GET", f"vonage_recordings?recording_id=eq.{urllib.parse.quote(recording_id)}&limit=1")
        return bool(rows)
    except Exception:
        return False  # On lookup error, treat as unseen (we'll dedupe again at insert via PK constraint)


def _vonage_record_status(recording_id, status, **fields):
    """Insert or update the idempotency row for a recording.

    Uses an explicit check-then-insert/update pattern rather than PostgREST's
    upsert (which requires conflicting Prefer headers vs. the rest of the
    codebase). The recording_id is the PK so this is race-safe enough for
    a single-worker setup.
    """
    record = {
        "status": status,
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }
    record.update({k: v for k, v in fields.items() if v is not None})
    try:
        existing = supa("GET", f"vonage_recordings?recording_id=eq.{urllib.parse.quote(recording_id)}&limit=1")
        if existing:
            supa("PATCH", f"vonage_recordings?recording_id=eq.{urllib.parse.quote(recording_id)}", record, prefer_minimal=True)
        else:
            record["recording_id"] = recording_id
            supa("POST", "vonage_recordings", record, prefer_minimal=True)
    except Exception as e:
        log(f"  Failed to update vonage_recordings row for {recording_id}: {e}")


def _vonage_get_extension_map():
    """Fetch the full extension → rep_name map. Returns dict."""
    try:
        rows = supa("GET", "vonage_extension_map?select=extension_id,rep_name")
        return {r["extension_id"]: r["rep_name"] for r in rows}
    except Exception as e:
        log(f"  Failed to load extension map: {e}")
        return {}


def _vonage_process_one(recording, ext_map, keyterms, tx_corrections):
    """Process a single recording: dedupe, download, hand off to existing
    pipeline, attribute to rep based on extension mapping. Returns one of
    'ingested', 'skipped', 'duplicate', 'failed'.
    """
    # TODO(vbc): These field names are guesses. Confirm against real API response.
    recording_id    = recording.get("id") or recording.get("recording_id")
    extension_id    = recording.get("extension_id") or recording.get("extension") or ""
    started_at      = recording.get("start_time") or recording.get("call_started_at") or recording.get("created_at")
    duration_secs   = recording.get("duration") or recording.get("duration_seconds") or 0
    filename_hint   = recording.get("filename") or f"vonage_{recording_id}.mp3"

    if not recording_id:
        log(f"  Vonage: recording missing ID, skipping: {recording}")
        return "skipped"

    # Idempotency check
    if _vonage_recording_seen(recording_id):
        return "duplicate"

    # Mark as pending so we have a record even if download fails
    _vonage_record_status(
        recording_id, "pending",
        extension_id=str(extension_id) if extension_id else None,
        call_started_at=started_at,
        duration_seconds=int(duration_secs) if duration_secs else None,
    )

    # Skip overly-long calls early to avoid wasting download bandwidth
    skip, reason = should_skip_by_duration(int(duration_secs) if duration_secs else 0)
    if skip:
        _vonage_record_status(recording_id, "skipped", error_message=reason)
        return "skipped"

    # Download audio
    try:
        audio_bytes, mime = _vonage_download_recording(recording_id)
    except Exception as e:
        _vonage_record_status(recording_id, "failed", error_message=str(e)[:500])
        log(f"  Vonage download failed for {recording_id}: {e}")
        return "failed"

    # Bytes-level skip if file is too big for downstream transcription
    if len(audio_bytes) > 25 * 1024 * 1024:
        _vonage_record_status(
            recording_id, "skipped",
            error_message=f"Audio file too large ({len(audio_bytes)//(1024*1024)}MB > 25MB cap)",
        )
        return "skipped"

    _vonage_record_status(recording_id, "downloaded")

    # Build a sensible filename — Vonage-style date format that the existing
    # parse_call_date_from_filename() helper already understands. Falls back
    # to the API-provided filename hint if we can't construct one.
    filename = filename_hint
    if started_at:
        try:
            dt = datetime.fromisoformat(started_at.replace("Z", "+00:00"))
            ts = dt.strftime("%Y_%m_%d_%I_%M%p")
            ext_suffix = f"_ext{extension_id}" if extension_id else ""
            filename = f"{ts}{ext_suffix}_vonage.mp3"
        except Exception:
            pass

    # Hand off to the existing transcription/analysis pipeline
    try:
        with _analysis_semaphore:
            saved, skip_reason = _process_single_file(audio_bytes, filename, keyterms, tx_corrections)
    except Exception as e:
        _vonage_record_status(recording_id, "failed", error_message=f"Process error: {str(e)[:400]}")
        log(f"  Vonage processing failed for {recording_id}: {e}")
        return "failed"

    if skip_reason:
        _vonage_record_status(recording_id, "skipped", error_message=skip_reason)
        return "skipped"

    # `saved` from supa POST is typically a list with one record
    call_id = None
    if saved:
        if isinstance(saved, list) and saved:
            call_id = saved[0].get("id")
        elif isinstance(saved, dict):
            call_id = saved.get("id")

    # Override rep_name from extension mapping. Extension ID is more reliable
    # than transcript inference because it's tied to the Vonage user account.
    if call_id and extension_id:
        ext_str = str(extension_id)
        mapped_name = ext_map.get(ext_str)
        if mapped_name:
            try:
                supa("PATCH", f"calls?id=eq.{call_id}", {"rep_name": mapped_name})
            except Exception as e:
                log(f"  Vonage: failed to apply rep mapping for ext {ext_str}: {e}")
        else:
            # Auto-create "Unknown" rep_name including the extension so it shows
            # up in the Unmatched Reps panel in Management for easy mapping.
            unknown_name = f"Unknown — ext {ext_str}"
            try:
                supa("PATCH", f"calls?id=eq.{call_id}", {"rep_name": unknown_name})
            except Exception as e:
                log(f"  Vonage: failed to set unknown rep name: {e}")

    _vonage_record_status(recording_id, "done", call_id=call_id)
    return "ingested"


def _vonage_poll_once():
    """Run a single polling cycle. Returns (ingested, skipped, errors)."""
    # Compute "since" timestamp — the most recent call_started_at we've seen,
    # or 1 hour ago for the very first poll. We deliberately *don't* use
    # ingested_at because Vonage may publish recordings out of order or with
    # delays.
    try:
        latest = supa("GET", "vonage_recordings?order=call_started_at.desc.nullslast&limit=1&select=call_started_at")
        if latest and latest[0].get("call_started_at"):
            since_iso = latest[0]["call_started_at"]
        else:
            since_iso = (datetime.now(timezone.utc) - timedelta(hours=1)).isoformat()
    except Exception:
        since_iso = (datetime.now(timezone.utc) - timedelta(hours=1)).isoformat()

    log(f"  Vonage poll: listing recordings since {since_iso}")
    try:
        recordings = _vonage_list_recordings(since_iso)
    except Exception as e:
        log(f"  Vonage poll list failed: {e}")
        raise

    if not recordings:
        log("  Vonage poll: no new recordings")
        return 0, 0, 0

    log(f"  Vonage poll: found {len(recordings)} recording(s) to evaluate")

    # Build context once per cycle
    ext_map = _vonage_get_extension_map()
    try:
        keyterms = get_cached_keyterms()
    except Exception:
        keyterms = []
    try:
        tx_corrections = get_transcript_corrections()
    except Exception:
        tx_corrections = []

    ingested = skipped = errors = 0
    for rec in recordings:
        # Respect pause/stop between recordings
        with _vonage_lock:
            if _vonage_job.get("stop_requested") or _vonage_job.get("pause_requested"):
                break
        try:
            outcome = _vonage_process_one(rec, ext_map, keyterms, tx_corrections)
        except Exception as e:
            outcome = "failed"
            log(f"  Vonage process loop error: {e}")
        if outcome == "ingested":
            ingested += 1
        elif outcome in ("skipped", "duplicate"):
            skipped += 1
        elif outcome == "failed":
            errors += 1

    return ingested, skipped, errors


def _vonage_poll_worker():
    """Background loop that polls Vonage on a regular interval."""
    log(f"  Vonage poll worker starting (interval: {VONAGE_POLL_INTERVAL}s)")
    with _vonage_lock:
        _vonage_job["status"] = "running"
        _vonage_job["started_at"] = datetime.now(timezone.utc).isoformat()
        _vonage_job["stop_requested"] = False
        _vonage_job["pause_requested"] = False

    today = datetime.now(timezone.utc).date()

    while True:
        # Check stop flag at top of loop
        with _vonage_lock:
            if _vonage_job.get("stop_requested"):
                _vonage_job["status"] = "idle"
                log("  Vonage poll worker stopped by request")
                return
            paused = _vonage_job.get("pause_requested", False)
            if paused:
                _vonage_job["status"] = "paused"

        # When paused, sleep in short increments and re-check rather than
        # blocking on the full interval — so resume reacts within ~5s.
        if paused:
            time.sleep(5)
            continue

        # Reset today counter on day rollover
        cur_day = datetime.now(timezone.utc).date()
        if cur_day != today:
            today = cur_day
            with _vonage_lock:
                _vonage_job["ingested_today"] = 0

        # Run one poll cycle
        try:
            ingested, skipped, errors = _vonage_poll_once()
            with _vonage_lock:
                _vonage_job["last_poll_at"] = datetime.now(timezone.utc).isoformat()
                _vonage_job["last_poll_succeeded"] = True
                _vonage_job["ingested_total"] += ingested
                _vonage_job["ingested_today"] += ingested
                _vonage_job["skipped_total"] += skipped
                _vonage_job["errors_total"] += errors
                _vonage_job["status"] = "running"
                if ingested or errors:
                    log(f"  Vonage poll cycle: +{ingested} ingested, {skipped} skipped, {errors} errors")
        except Exception as e:
            with _vonage_lock:
                _vonage_job["last_poll_at"] = datetime.now(timezone.utc).isoformat()
                _vonage_job["last_poll_succeeded"] = False
                _vonage_job["last_error"] = str(e)[:500]
                _vonage_job["status"] = "error"
            log(f"  Vonage poll cycle error: {e}")
            # Don't kill the worker on a single failed cycle — just back off
            # and try again next interval.

        # Sleep in 5-second chunks so stop/pause requests are honored quickly
        slept = 0
        while slept < VONAGE_POLL_INTERVAL:
            with _vonage_lock:
                if _vonage_job.get("stop_requested") or _vonage_job.get("pause_requested"):
                    break
            time.sleep(5)
            slept += 5


def _vonage_start_worker():
    """Start the background polling thread if not already running.
    Returns True if started, False if already running or not configured.
    """
    global _vonage_thread
    if not _vonage_configured():
        log("  Vonage worker not started: integration not configured (missing env vars)")
        with _vonage_lock:
            _vonage_job["status"] = "disabled"
            _vonage_job["last_error"] = "Vonage env vars not set"
        return False
    with _vonage_lock:
        if _vonage_thread and _vonage_thread.is_alive():
            log("  Vonage worker already running, ignoring start request")
            return False
        _vonage_job["stop_requested"] = False
        _vonage_job["pause_requested"] = False
    _vonage_thread = threading.Thread(target=_vonage_poll_worker, daemon=True, name="vonage-poll")
    _vonage_thread.start()
    return True


def _vonage_stop_worker():
    """Signal the worker to stop. Returns True if a stop was signaled."""
    with _vonage_lock:
        if _vonage_job.get("status") not in ("running", "paused", "error"):
            return False
        _vonage_job["stop_requested"] = True
    return True


def _vonage_pause_worker():
    """Pause the worker (does not kill the thread; resumes on demand)."""
    with _vonage_lock:
        _vonage_job["pause_requested"] = True
    return True


def _vonage_resume_worker():
    """Resume a paused worker."""
    with _vonage_lock:
        _vonage_job["pause_requested"] = False
    return True


# ──────────────────────────────────────────────
# HTTP HANDLER
# ──────────────────────────────────────────────

class Handler(BaseHTTPRequestHandler):
    def log_message(self, fmt, *args):
        log(f"  {args[0]} {args[1]}")

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
        elif path == "/reanalyze/unscored":
            self._reanalyze_unscored()
        elif path == "/reanalyze/stop":
            self._reanalyze_stop()
        elif path == "/batch_upload/status":
            self._batch_upload_status()
        elif path == "/corrections":
            self._get_corrections()
        elif path == "/transcript_corrections":
            self._get_transcript_corrections_endpoint()
        elif path.startswith("/audio_url/"):
            self._get_audio_url()
        elif path == "/vonage/status":
            self._vonage_status()
        elif path == "/vonage/extension_map":
            self._vonage_get_extension_map_endpoint()
        elif path == "/vonage/recent":
            self._vonage_get_recent()
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
            "/reanalyze/stop": self._reanalyze_stop,
            "/corrections/save": self._save_correction,
            "/transcript_corrections/save": self._save_transcript_correction,
            "/transcript_corrections/delete": self._delete_transcript_correction,
            "/transcript_corrections/reapply": self._reapply_corrections,
            "/batch_upload/start": self._batch_upload_start,
            "/batch_upload/stop": self._batch_upload_stop,
            "/vonage/start": self._vonage_start_endpoint,
            "/vonage/stop": self._vonage_stop_endpoint,
            "/vonage/pause": self._vonage_pause_endpoint,
            "/vonage/resume": self._vonage_resume_endpoint,
            "/vonage/poll_now": self._vonage_poll_now_endpoint,
            "/vonage/extension_map/save": self._vonage_save_extension,
            "/vonage/extension_map/delete": self._vonage_delete_extension,
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
            # Optional ?slim=1 strips transcript text from response (for list views)
            qs = urllib.parse.parse_qs(self.path.split("?", 1)[1]) if "?" in self.path else {}
            slim = qs.get("slim", ["0"])[0] == "1"
            if slim:
                # Select all columns except transcript
                calls = supa("GET", "calls?order=created_at.desc&limit=2000&select=id,filename,storage_filename,rep_name,rep_name_detected,caller_name,call_purpose,call_type,move_type,move_category,call_outcome,loss_reason,soft_pipeline_reason,word_count,exclude_from_scoring,exclusion_reason,call_summary,key_details,talk_ratio_rep,talk_ratio_customer,keywords_detected,keyword_positions,objections_detected,objection_positions,customer_sentiment,scores,checklist,strengths,coaching_points,tags,manager_notes,score_overrides,call_date,audio_url,share_token,availability_decline,turned_away,onsite_suggested,call_quality,is_continuation,continuation_group_id,evaluation_confidence,is_diarized,close_attempts,objections_overcome,objections_abandoned,pipeline_recovery_quality,salesmanship_score,value_props_used,missed_rapport_opportunities,input_tokens,output_tokens,pricing_model,move_timeline,created_at")
            else:
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
        if not (DEEPGRAM_KEY or OPENAI_KEY):
            self._err(500, "No transcription API configured — set DEEPGRAM_API_KEY"); return
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
                # Use cached keyterms — no Supabase query needed on every call
                try:
                    keyterms = get_cached_keyterms()
                    tx_corrections = get_transcript_corrections()
                except Exception:
                    keyterms = build_keyterms()
                    tx_corrections = []

                log(f"  Transcribing {filename} ({len(audio_bytes)} bytes) via {'Deepgram' if DEEPGRAM_KEY else 'Whisper'}...")
                transcript, is_diarized, word_timestamps = transcribe_audio(audio_bytes, filename, keyterms=keyterms)
                if not transcript or not transcript.strip():
                    self._err(400, "Transcription empty — audio may be silent"); return
                log(f"  Transcription done: {len(transcript)} chars, diarized={is_diarized}, timestamps={len(word_timestamps)}")

                # Apply corrections
                clean_transcript = apply_transcript_corrections(transcript, tx_corrections)

                result = run_claude_analysis(clean_transcript, filename, is_diarized=is_diarized)

            result["transcript"] = clean_transcript
            result["filename"] = filename
            result["is_diarized"] = is_diarized
            if word_timestamps:
                result["word_timestamps"] = word_timestamps

            # Upload audio to storage (best effort)
            try:
                enforce_storage_cap()
                # Sanitize filename for storage — remove spaces and special chars
                safe_filename = re.sub(r'[^\w\-_\.]', '_', filename)
                supa_storage_upload("call-audio", safe_filename, audio_bytes, "audio/mpeg")
                result["audio_url"] = supa_storage_signed_url("call-audio", safe_filename)
                result["storage_filename"] = safe_filename
            except Exception as e:
                log(f"  Audio storage warning: {e}")
                result["audio_url"] = ""
                result["storage_filename"] = ""

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
                "move_category": p.get("move_category", "standard"),
                "loss_reason": p.get("loss_reason", ""),
                "soft_pipeline_reason": p.get("soft_pipeline_reason", ""),
                "evaluation_confidence": p.get("evaluation_confidence", 8),
                "is_diarized": p.get("is_diarized", False),
                "close_attempts": p.get("close_attempts", 0),
                "objections_overcome": p.get("objections_overcome", []),
                "objections_abandoned": p.get("objections_abandoned", []),
                "pipeline_recovery_quality": p.get("pipeline_recovery_quality", 0),
                "salesmanship_score": p.get("salesmanship_score", 0),
                "value_props_used": p.get("value_props_used", []),
                "missed_rapport_opportunities": p.get("missed_rapport_opportunities", []),
                "storage_filename": p.get("storage_filename", ""),
                "input_tokens": p.get("input_tokens", 0),
                "output_tokens": p.get("output_tokens", 0),
                "pricing_model": p.get("pricing_model", "unknown"),
                "move_timeline": p.get("move_timeline", "unknown"),
                "word_timestamps": p.get("word_timestamps") or None,
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
            result = supa("GET", f"calls?id=eq.{call_id}&select=filename,storage_filename&limit=1")
            if not result:
                self._err(404, "Call not found"); return
            # Use storage_filename if saved, otherwise sanitize the original filename
            storage_filename = result[0].get("storage_filename") or re.sub(r'[^\w\-_\.]', '_', result[0].get("filename", ""))
            if not storage_filename:
                self._err(404, "No audio filename"); return
            signed_url = supa_storage_signed_url("call-audio", storage_filename, expires=3600)
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

    # ── TRANSCRIPT CORRECTIONS ──

    def _get_transcript_corrections_endpoint(self):
        try:
            result = supa("GET", "transcript_corrections?order=created_at.asc&limit=500")
            self._ok(result)
        except Exception as e:
            self._err(500, str(e))

    def _save_transcript_correction(self, body):
        try:
            p = json.loads(body)
            find_text = p.get("find_text", "").strip()
            replace_text = p.get("replace_text", "").strip()
            call_id = p.get("call_id")
            new_transcript = p.get("new_transcript")

            # If neither a rule nor a transcript edit is provided, that's an error
            if not find_text and not (call_id and new_transcript):
                self._err(400, "Either find_text or (call_id + new_transcript) is required"); return

            response = {"saved": True}

            # If find_text was provided, save/update the rule
            if find_text:
                existing = supa("GET", f"transcript_corrections?find_text=ilike.{urllib.parse.quote(find_text)}&limit=1")
                if existing:
                    supa("PATCH", f"transcript_corrections?id=eq.{existing[0]['id']}", {"replace_text": replace_text})
                    response["rule"] = {"updated": True, "id": existing[0]["id"]}
                else:
                    rule_result = supa("POST", "transcript_corrections", {"find_text": find_text, "replace_text": replace_text})
                    response["rule"] = {"created": True, "data": rule_result}

            # If call transcript was edited, save it before responding (so we don't _ok twice)
            if call_id and new_transcript:
                supa("PATCH", f"calls?id=eq.{call_id}", {"transcript": new_transcript})
                response["transcript_updated"] = True

            self._ok(response)
        except Exception as e:
            self._err(500, str(e))

    def _delete_transcript_correction(self, body):
        try:
            p = json.loads(body)
            supa("DELETE", f"transcript_corrections?id=eq.{p['id']}")
            self._ok({"deleted": True})
        except Exception as e:
            self._err(500, str(e))

    def _reapply_corrections(self, body):
        """Re-apply transcript corrections to a single call and re-analyze."""
        try:
            p = json.loads(body)
            call_id = p.get("call_id")
            if not call_id:
                self._err(400, "call_id required"); return
            result = supa("GET", f"calls?id=eq.{call_id}&limit=1")
            if not result:
                self._err(404, "Call not found"); return
            call = result[0]
            transcript = call.get("transcript", "")
            filename = call.get("filename", "call.txt")
            if not transcript:
                self._err(400, "No transcript"); return
            tx_corrections = get_transcript_corrections()
            clean_transcript = apply_transcript_corrections(transcript, tx_corrections)
            analysis = run_claude_analysis(clean_transcript, filename)
            update_data = {
                "transcript": clean_transcript,
                "scores": analysis.get("scores", {}),
                "checklist": analysis.get("checklist", {}),
                "strengths": analysis.get("strengths", []),
                "coaching_points": analysis.get("coaching_points", []),
                "call_summary": analysis.get("call_summary", ""),
                "keywords_detected": analysis.get("keywords_detected", []),
                "keyword_positions": analysis.get("keyword_positions", {}),
                "objections_detected": analysis.get("objections_detected", []),
                "objection_positions": analysis.get("objection_positions", {}),
                "move_category": analysis.get("move_category", "standard"),
                "evaluation_confidence": analysis.get("evaluation_confidence", 8),
                "loss_reason": analysis.get("loss_reason", ""),
                "soft_pipeline_reason": analysis.get("soft_pipeline_reason", ""),
            }
            supa("PATCH", f"calls?id=eq.{call_id}", update_data)
            self._ok({"reanalyzed": True, "call_id": call_id})
        except Exception as e:
            self._err(500, str(e))

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
            # Validate token format (only allow URL-safe base64 characters that secrets.token_urlsafe produces)
            if not re.match(r'^[A-Za-z0-9_-]{1,128}$', token):
                self._err(404, "Share link not found"); return
            result = supa("GET", f"shared_views?token=eq.{token}&limit=1")
            if not result:
                self._err(404, "Share link not found"); return
            view = result[0]
            html = read_html()
            # Use json.dumps for ALL injected values — this safely escapes quotes, backslashes, </script>, etc.
            inject_payload = {
                "SHARE_TOKEN": token,
                "SHARE_VIEW_LEVEL": view.get("view_level", "manager"),
                "SHARE_REP_IDS": view.get("rep_ids", []),
                "SHARE_LABEL": view.get("label", ""),
            }
            # Build script with each window assignment using json.dumps for safe escaping
            assignments = ";".join(f"window.{k}={json.dumps(v)}" for k, v in inject_payload.items())
            inject = f'<script>{assignments};</script>'
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
                log("  Dedup: escalating to Opus")
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
        if _batch_job.get("status") == "running":
            self._ok({"message": "Batch upload in progress — wait for it to finish before re-analyzing", "blocked": True}); return
        with _reanalyze_lock:
            _reanalyze_job = {
                "status": "running",
                "total": 0, "processed": 0, "current": "", "errors": 0,
                "started_at": datetime.now(timezone.utc).isoformat(),
                "finished_at": None,
                "stop_requested": False,
                "skipped": 0,
                "failed_calls": [],
            }
        t = threading.Thread(target=_reanalyze_worker, daemon=True)
        t.start()
        self._ok({"message": "Started", "status": _reanalyze_job})

    def _reanalyze_status(self):
        with _reanalyze_lock:
            status = dict(_reanalyze_job)
        self._ok(status)

    def _reanalyze_stop(self, body=None):
        with _reanalyze_lock:
            if _reanalyze_job["status"] != "running":
                self._ok({"message": "No job running", "status": _reanalyze_job["status"]}); return
            _reanalyze_job["stop_requested"] = True
        self._ok({"message": "Stop requested — will halt after current call completes"})

    def _reanalyze_unscored(self):
        """Return count and list of calls with no scores (never analyzed or failed)."""
        try:
            calls = supa("GET", "calls?select=id,filename,created_at,rep_name&order=created_at.desc&limit=5000")
            unscored = []
            for c in calls:
                # A call is considered unscored if scores is missing/empty or overall score is 0
                scores = c.get("scores") or {}
                overall = scores.get("overall", {})
                score_val = overall.get("score", 0) if isinstance(overall, dict) else 0
                if not scores or score_val == 0:
                    unscored.append({
                        "id": c.get("id"),
                        "filename": c.get("filename"),
                        "created_at": c.get("created_at"),
                        "rep_name": c.get("rep_name"),
                    })
            self._ok({"total": len(calls), "unscored_count": len(unscored), "unscored": unscored})
        except Exception as e:
            self._err(500, str(e))

    def _batch_upload_start(self, body):
        """Accept raw ZIP bytes via multipart or base64 JSON, start background processing."""
        global _batch_job
        with _batch_lock:
            if _batch_job["status"] == "running":
                self._ok({"message": "Already running", "status": _batch_job}); return
        # Reciprocal block: don't start batch upload while reanalyze is running
        if _reanalyze_job.get("status") == "running":
            self._ok({"message": "Bulk re-analyze in progress — wait for it to finish before uploading new calls", "blocked": True}); return

        content_type = self.headers.get("Content-Type", "")
        try:
            if "multipart/form-data" in content_type:
                # Raw multipart upload — parse boundary
                boundary = re.search(r'boundary=([^\s;]+)', content_type)
                if not boundary:
                    self._err(400, "Missing boundary in multipart"); return
                bound = boundary.group(1).encode()
                parts = body.split(b"--" + bound)
                zip_bytes = None
                for part in parts:
                    if b'filename=' in part and b'.zip' in part.lower():
                        # Find end of headers
                        header_end = part.find(b"\r\n\r\n")
                        if header_end >= 0:
                            zip_bytes = part[header_end+4:].rstrip(b"\r\n--")
                            break
                if not zip_bytes:
                    self._err(400, "No ZIP file found in upload"); return
            else:
                # JSON with base64
                import base64
                p = json.loads(body)
                zip_bytes = base64.b64decode(p.get("zip", ""))

            if len(zip_bytes) < 100:
                self._err(400, "ZIP file too small or empty"); return

        except Exception as e:
            self._err(400, f"Failed to parse upload: {e}"); return

        with _batch_lock:
            _batch_job = {
                "status": "running",
                "total": 0, "processed": 0, "skipped": 0, "errors": 0,
                "current": "Extracting ZIP...",
                "started_at": datetime.now(timezone.utc).isoformat(),
                "finished_at": None,
                "error_list": [],
                "stop_requested": False,
            }

        t = threading.Thread(target=_batch_upload_worker, args=(zip_bytes,), daemon=True)
        t.start()
        log(f"  Batch upload started: {len(zip_bytes):,} bytes")
        self._ok({"message": "Batch upload started", "status": _batch_job})

    def _batch_upload_status(self):
        with _batch_lock:
            status = dict(_batch_job)
        self._ok(status)

    def _batch_upload_stop(self, body=None):
        with _batch_lock:
            if _batch_job.get("status") != "running":
                self._ok({"message": "No batch upload running", "status": _batch_job.get("status")}); return
            _batch_job["stop_requested"] = True
        self._ok({"message": "Stop requested — will halt after current files finish"})

    # ── EXPORT ──

    def _export_csv(self):
        try:
            calls = supa("GET", "calls?order=created_at.desc&limit=5000")
            # v12 checklist keys (no pitched_fvp; added v12 items like got_email)
            ck_keys = ["got_move_date","got_customer_name","got_phone_number","got_cities","got_home_type","got_stairs_info","did_full_inventory","asked_forgotten_items","asked_about_boxes","gave_price_on_call","attempted_to_close","offered_email_estimate","mentioned_confirmations","thanked_customer","asked_name_at_start","led_estimate_process","got_email","scheduled_onsite_attempt","offered_alternatives","took_rapport_opportunities","completed_booking_wrapup","captured_lead"]
            # v12 score categories: overall, rapport_tone, information_control (merged), price_delivery, closing_attempt, salesmanship, professionalism
            hdrs = ["Date","Call Date","Rep","Caller","Purpose","Type","Move","Outcome","Sentiment","Excluded","Overall","Rapport","Info & Control","Price","Closing","Salesmanship","Prof.","Compliance%","Talk Rep%","Talk Cust%","Words","Quality","Declined","Turned Away","Onsite","Continuation","Keywords","Objections","Strengths","Coaching"]
            rows = []
            for c in calls:
                s = c.get("scores", {}) or {}
                ck = c.get("checklist", {}) or {}
                comp = round(sum(1 for k in ck_keys if ck.get(k)) / len(ck_keys) * 100)
                rows.append([
                    (c.get("created_at",""))[:10], (c.get("call_date","") or "")[:10],
                    c.get("rep_name",""), c.get("caller_name",""), c.get("call_purpose",""),
                    c.get("call_type",""), c.get("move_type",""), c.get("call_outcome",""),
                    c.get("customer_sentiment",""), "Yes" if c.get("exclude_from_scoring") else "No",
                    (s.get("overall") or {}).get("score",""), (s.get("rapport_tone") or {}).get("score",""),
                    (s.get("information_control") or {}).get("score",""), (s.get("price_delivery") or {}).get("score",""),
                    (s.get("closing_attempt") or {}).get("score",""), (s.get("salesmanship") or {}).get("score",""),
                    (s.get("professionalism") or {}).get("score",""),
                    comp, c.get("talk_ratio_rep",""), c.get("talk_ratio_customer",""), c.get("word_count",""),
                    c.get("call_quality","normal"), "Yes" if c.get("availability_decline") else "No",
                    "Yes" if c.get("turned_away") else "No",
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
                "scores": {sk: {"score": round(sum((c.get("scores",{}) or {}).get(sk,{}).get("score",0) for c in sc_calls)/max(len(sc_calls),1),1), "note": f"Avg across {len(sc_calls)} calls"} for sk in ["rapport_tone","information_control","price_delivery","closing_attempt","salesmanship","professionalism","overall"]},
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

    # ── VONAGE ──

    def _vonage_status(self):
        """Return current polling worker state plus a recent-activity summary."""
        try:
            with _vonage_lock:
                state = dict(_vonage_job)
            state["configured"] = _vonage_configured()
            state["poll_interval_seconds"] = VONAGE_POLL_INTERVAL
            state["autostart_enabled"] = VONAGE_AUTOSTART
            # Add a count of recent ingestions for at-a-glance UI
            try:
                cutoff = (datetime.now(timezone.utc) - timedelta(hours=24)).isoformat()
                rows = supa("GET", f"vonage_recordings?ingested_at=gte.{cutoff}&select=status")
                state["last_24h"] = {
                    "done":      sum(1 for r in rows if r.get("status") == "done"),
                    "skipped":   sum(1 for r in rows if r.get("status") == "skipped"),
                    "failed":    sum(1 for r in rows if r.get("status") == "failed"),
                    "pending":   sum(1 for r in rows if r.get("status") in ("pending", "downloaded")),
                }
            except Exception:
                state["last_24h"] = None
            self._ok(state)
        except Exception as e:
            self._err(500, str(e))

    def _vonage_get_extension_map_endpoint(self):
        try:
            rows = supa("GET", "vonage_extension_map?order=extension_id.asc")
            self._ok(rows)
        except Exception as e:
            self._err(500, str(e))

    def _vonage_get_recent(self):
        """Recent ingestions joined with call info for the dashboard table."""
        try:
            rows = supa("GET", "vonage_recordings?order=ingested_at.desc&limit=50")
            # Optionally enrich with rep_name from the linked call
            call_ids = [r["call_id"] for r in rows if r.get("call_id")]
            calls_by_id = {}
            if call_ids:
                ids_csv = ",".join(call_ids)
                try:
                    cs = supa("GET", f"calls?id=in.({ids_csv})&select=id,rep_name,filename")
                    calls_by_id = {c["id"]: c for c in cs}
                except Exception:
                    pass
            for r in rows:
                cid = r.get("call_id")
                if cid and cid in calls_by_id:
                    r["call_rep_name"] = calls_by_id[cid].get("rep_name")
                    r["call_filename"] = calls_by_id[cid].get("filename")
            self._ok(rows)
        except Exception as e:
            self._err(500, str(e))

    def _vonage_start_endpoint(self, body):
        try:
            started = _vonage_start_worker()
            self._ok({"started": started})
        except Exception as e:
            self._err(500, str(e))

    def _vonage_stop_endpoint(self, body):
        try:
            stopped = _vonage_stop_worker()
            self._ok({"stopped": stopped})
        except Exception as e:
            self._err(500, str(e))

    def _vonage_pause_endpoint(self, body):
        try:
            _vonage_pause_worker()
            self._ok({"paused": True})
        except Exception as e:
            self._err(500, str(e))

    def _vonage_resume_endpoint(self, body):
        try:
            _vonage_resume_worker()
            self._ok({"resumed": True})
        except Exception as e:
            self._err(500, str(e))

    def _vonage_poll_now_endpoint(self, body):
        """Trigger a single poll cycle on demand. Useful for testing."""
        if not _vonage_configured():
            self._err(400, "Vonage not configured")
            return
        try:
            ingested, skipped, errors = _vonage_poll_once()
            self._ok({"ingested": ingested, "skipped": skipped, "errors": errors})
        except Exception as e:
            self._err(500, str(e))

    def _vonage_save_extension(self, body):
        try:
            p = json.loads(body)
            extension_id = str(p.get("extension_id", "")).strip()
            rep_name = str(p.get("rep_name", "")).strip()
            notes = p.get("notes", "")
            if not extension_id or not rep_name:
                self._err(400, "extension_id and rep_name required")
                return
            record = {
                "rep_name": rep_name,
                "notes": notes,
                "updated_at": datetime.now(timezone.utc).isoformat(),
            }
            existing = supa("GET", f"vonage_extension_map?extension_id=eq.{urllib.parse.quote(extension_id)}&limit=1")
            if existing:
                supa("PATCH", f"vonage_extension_map?extension_id=eq.{urllib.parse.quote(extension_id)}", record, prefer_minimal=True)
            else:
                record["extension_id"] = extension_id
                supa("POST", "vonage_extension_map", record, prefer_minimal=True)
            self._ok({"saved": True})
        except Exception as e:
            self._err(500, str(e))

    def _vonage_delete_extension(self, body):
        try:
            p = json.loads(body)
            extension_id = str(p.get("extension_id", "")).strip()
            if not extension_id:
                self._err(400, "extension_id required")
                return
            supa("DELETE", f"vonage_extension_map?extension_id=eq.{urllib.parse.quote(extension_id)}")
            self._ok({"deleted": True})
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
    log("=" * 55)
    log("  Little Guys Movers — Call Analyzer Server v12")
    log("=" * 55)
    missing = [v for v in ["ANTHROPIC_API_KEY","SUPABASE_URL","SUPABASE_KEY","DEEPGRAM_API_KEY"] if not os.environ.get(v)]
    if missing:
        log("\n  WARNING: Missing env vars: " + ", ".join(missing))
    else:
        log("\n  All environment variables loaded")

    # Vonage VBC integration — auto-start worker if configured
    if _vonage_configured():
        if VONAGE_AUTOSTART:
            log("  Vonage VBC: configured, auto-starting polling worker")
            _vonage_start_worker()
        else:
            log("  Vonage VBC: configured but autostart disabled — start manually from dashboard")
    else:
        log("  Vonage VBC: not configured (env vars missing) — integration dormant")

    log(f"  Running at http://127.0.0.1:{PORT}")
    log("  Press Ctrl+C to stop\n")
    ThreadingHTTPServer(("0.0.0.0", PORT), Handler).serve_forever()
