"""
Little Guys Movers - Call Analyzer Server v10
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
from http.server import HTTPServer, BaseHTTPRequestHandler
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

# Global semaphore — max 3 concurrent transcription/Claude calls
_analysis_semaphore = threading.Semaphore(3)

# Keyterm cache — rebuilt every 30 minutes
_keyterm_cache = {"terms": [], "built_at": 0}
_keyterm_lock = threading.Lock()

# Background re-analyze job state
_reanalyze_job = {
    "status": "idle",
    "total": 0, "processed": 0, "current": "", "errors": 0,
    "started_at": None, "finished_at": None,
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
# CLAUDE PROMPT
# ──────────────────────────────────────────────

def build_prompt(transcript, filename, corrections=None, is_diarized=False):
    """Build Claude prompt v10 — salesmanship, calibration examples, rapport opportunities, weighted scoring."""

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
- Rep name detection: look for Speaker 0's introduction
"""

    prompt = f"""You are a sales call evaluator for Little Guys Movers (LGMS).
{corrections_block}{diarization_note}
CALIBRATION EXAMPLES — use these to anchor your scoring to LGMS standards:

EXCELLENT CALL (8/10 overall) — Alex, storage-to-apartment move:
- Assumed the close: said "I can get started first thing tomorrow morning at 8 o'clock" and moved straight to scheduling without asking permission
- FVP: included naturally in price breakdown with cost stated — score 9/10
- Rapport: immediately acknowledged customer's stress about last-minute situation, warm and confident throughout — score 9/10
- Closing: 10/10 — never asked "do you want to book?", just booked it
- Salesmanship: mentioned "clothes in the dresser" tip naturally — score 7/10
Key lesson: assume the close, move with confidence, acknowledge the customer's situation

GOOD CALL (8/10 overall) — Dylan, large house move:
- Thorough room-by-room inventory, asked all the right detail questions
- FVP: mentioned $19,000 coverage and asked if adequate but never explained what it is — score 7/10
- Closing: "I could have movers there as early as 8 a.m. next Thursday, if you wanted to get that set up" — offered but waited for confirmation rather than assuming — score 9/10
- Salesmanship: 5/10 — missed obvious opportunities on large house move (clothes in dresser, disassembly, background checks)
- Excellent expectation setting: explained email, attachments, day-of 7:45 call — score 9/10 professionalism
Key lesson: thorough and professional but missed salesmanship and FVP explanation

AVERAGE CALL (7/10 overall) — JD, studio apartment move:
- Good inventory detail questions (headboard, platform type, mirror)
- FVP: completely absent — never mentioned — score 1/10 (MAJOR miss)
- Closing: "Do you want to go ahead and get this scheduled?" — question not assumption — score 6/10
- Name came late: "I kind of dove right into it and didn't get your name"
- Mentioned clothes in dresser but only at the very end as afterthought — score 4/10 salesmanship
- Missed customer's anxiety about unknown floor situation — no reassurance given
Key lesson: FVP miss drops overall significantly regardless of other strengths

POOR CALL (4/10 overall) — James, small marketplace pickup:
- Customer said "I'm going to try and find another option" after price
- James said "completely understand" TWICE and let him walk — zero close attempt — score 1/10 closing
- MISSED RAPPORT OPPORTUNITY: customer said "the guy that was supposed to help me decided to leave town unbeknownst to me" — James gave no acknowledgment. Should have said "oh that's rough, I'm glad you found us — we won't do that to you"
- FVP: never mentioned — score 0/10
- Salesmanship: nothing — score 2/10
- Price delivered without any value framing first
Key lesson: never let a customer walk without attempting to overcome the objection. Always acknowledge emotional moments.

WHAT A 10/10 CALL LOOKS LIKE:
- Rep introduces themselves and gets customer name within first 30 seconds
- Acknowledges any stress or difficulty the customer mentions immediately and warmly
- Controls the call with confidence while feeling natural and unhurried
- Gets all required information for the move type
- Delivers price confidently without being asked
- Pitches FVP naturally, explains what it covers, states the cost
- Uses multiple salesmanship phrases naturally woven into conversation
- Assumes the close — moves to scheduling without asking permission
- Overcomes any objection with specific counters (no cancellation fee, availability, value)
- Sets clear expectations for confirmation and day-of communication
- Customer feels like they made the right choice

---

STEP 1 — CLASSIFY CALL TYPE:
- move_category: "standard" | "specialty" | "unload_only" | "in_house" | "commercial" | "storage" | "non_move"
- call_type: "sales_estimate" | "follow_up" | "complaint" | "booking_confirmation" | "non_sales" | "too_short" | "other"
- word_count: approximate
- exclude_from_scoring: true if fewer than 80 words OR clearly not a sales call OR disconnected

IMPORTANT: Even if exclude_from_scoring is true, still detect turned_away. A call where LGMS says "we don't have availability" is turned_away even if only 10 words long.

STEP 2 — DETECT REP NAME:
- Look for rep introduction or Speaker 0's first lines if diarized
- Return FIRST NAME ONLY. Return "Unknown" if not found.

STEP 3 — CALL QUALITY:
- "disconnected": abrupt ending, "Hello? Hello?" patterns, very short
- "poor_audio": heavy [inaudible] density (5+ occurrences), one-sided
- "normal": otherwise

STEP 4 — FLAGS:
- turned_away: LGMS could NOT accommodate — "we're booked", "no availability", "can't do that date"
- onsite_suggested: onsite visit/estimate mentioned
- is_continuation: "calling back about", "as we discussed", "following up", "spoke earlier"

STEP 5 — OUTCOME & PIPELINE:
- call_outcome: "booked" | "estimate_sent" | "soft_pipeline" | "lost" | "unknown"
- soft_pipeline: customer interested but not ready — needs partner, no exact date, needs to think, will call back
- loss_reason (if lost): "price_too_high" | "went_with_competitor" | "wrong_timing" | "no_availability" | "just_shopping" | "other" | ""
- soft_pipeline_reason (if soft_pipeline): "needs_partner" | "no_exact_date" | "needs_to_think" | "will_call_back" | "other" | ""

STEP 6 — RAPPORT OPPORTUNITY ANALYSIS:
Scan the transcript for moments where the customer shared something personal, stressful, or emotionally significant.
For each such moment, evaluate:
- Did the rep acknowledge it warmly? (good)
- Did the rep use it to reinforce trust in LGMS? (better — e.g. "we won't do that to you", "we handle this all the time")
- Did the rep completely ignore it and barrel through the script? (coaching point)

Examples of rapport opportunities:
- Customer mentions someone bailed on them or let them down → "I'm sorry to hear that, we won't do that to you"
- Customer mentions a life event (divorce, new job, family change) → acknowledgment and empathy
- Customer is clearly stressed or anxious → reassurance
- Customer mentions bad experience with movers → "that won't happen with us, here's why"
- Customer shares something personal (moving mom, first apartment, etc.) → genuine acknowledgment

List missed_rapport_opportunities as specific moments: "Customer said [X], rep gave no acknowledgment"

STEP 7 — CLOSING ANALYSIS:
close_attempts: count how many times rep explicitly tried to book/schedule
- Assuming the close counts: "Let me get you on the calendar for Saturday" = 1 attempt
- Asking permission is weaker: "Do you want to go ahead and book?" = 0.5 but still counts
- Never attempting = 0

CLOSING LANGUAGE (strong signals):
"I have a spot available for you", "let me get you on the board/schedule/calendar",
"first come first serve", "no cancellation fee", "save that for you", "hold that for you",
"get you taken care of", "can I go ahead and", "let me go ahead and book",
"shall I go ahead and get that set up"

OBJECTION HANDLING — for each objection:
- overcome: rep countered with specific response and advanced conversation
- abandoned: rep accepted without counter ("ok, call us back", "completely understand", "no problem")

Ideal counters:
- Price too high → "What were you expecting? Let me see what I can do" + work through it
- Need to think → "I understand — no cancellation fee, so would it make sense to hold the spot while you decide?"
- Need to check with partner → "Of course — no cancellation fee, want me to hold that while you check?"
- Already have a quote → "What are they quoting you? We'd love to earn your business today"

pipeline_recovery_quality (soft_pipeline only, 1-10):
- 1-3: "ok call us back" — no next step, lead will go cold
- 4-6: sent email estimate, gave some info
- 7-8: set specific callback time or confirmed next step
- 9-10: held tentative spot + specific callback + email — clear path to book

STEP 8 — SALESMANSHIP:
Detect whether rep used value proposition language. Score the CONCEPT not exact words.

ALWAYS-RELEVANT (should say on most calls):
- Background checked / screened movers
- No day labor / professional employees
- We show up / we don't miss appointments / we're reliable
- We have the proper equipment / trucks / tools

SITUATIONAL (only when applicable):
- "You can leave your clothes in the dresser" (when bedroom furniture present)
- "We disassemble and reassemble" (when furniture assembly needed)
- "We can rearrange things" (in-house or customer mentions layout)
- "We're careful with your belongings" / takes care with items

Scoring:
- 1-3: None of the above mentioned
- 4-5: One always-relevant phrase used
- 6-7: Two or more always-relevant phrases
- 8-9: Always-relevant + appropriate situational phrases, woven naturally
- 10: Multiple value props woven throughout conversation naturally

Track value_props_used as list of concepts detected.

STEP 9 — CHECKLIST (apply based on move_category):

STANDARD MOVE — all 22 steps:
1. got_move_date, 2. got_customer_name, 3. got_phone_number, 4. got_cities,
5. got_home_type, 6. got_stairs_info, 7. did_full_inventory, 8. asked_forgotten_items,
9. asked_about_boxes, 10. gave_price_on_call, 11. pitched_fvp,
12. attempted_to_close, 13. offered_email_estimate, 14. mentioned_confirmations,
15. thanked_customer, 16. asked_name_at_start, 17. led_estimate_process,
18. scheduled_onsite_attempt (na unless triggered), 19. offered_alternatives (na unless triggered),
20. took_rapport_opportunities, 21. completed_booking_wrapup, 22. captured_lead

SPECIALTY: Apply: move_date, name, phone, cities, stairs, price, fvp, close, confirmations, thanks, name_at_start, lead. Set rest to "na".
UNLOAD ONLY: Apply: move_date, name, phone, cities, stairs, price, close, confirmations, thanks, name_at_start, lead. Set pitched_fvp and rest to "na".
IN-HOUSE: Apply: move_date, name, phone, inventory, price, fvp, close, thanks, name_at_start, lead. Set cities/stairs/boxes and rest to "na".
COMMERCIAL: Apply all standard steps. Set onsite/alternatives to "na" unless triggered.
STORAGE: Apply: move_date, name, phone, cities, stairs, price, fvp, close, confirmations, thanks, name_at_start, lead. Set inventory/boxes and rest to "na".

CHECKLIST VALUES: true | false | "na"

STEP 10 — SCORING (1-10):

OVERALL — calculate server-side using weighted formula. Set your overall score using this formula:
overall = round(closing*0.25 + price*0.15 + fvp*0.15 + rapport*0.13 + salesmanship*0.10 + info*0.09 + control*0.09 + professionalism*0.08 + 0.5)
Note: server will recalculate — your overall score is a sanity check only.

CLOSING ATTEMPT (25% weight):
- 1-2: Never attempted — let customer go without asking
- 3-4: Weak attempt, accepted first objection without any counter
- 5-6: One attempt, gave up too easily
- 7-8: Clear attempt, handled at least one objection with counter
- 9: Strong attempt, overcame objections, used urgency/no-cancel-fee language
- 10: Multiple attempts, assumed close, overcame all objections confidently

INFORMATION COMPLETENESS (9% weight):
- Score on WHAT was gathered, NOT the order
- Penalize only if required info was never obtained
- Natural conversation that circles back scores well
- Do NOT penalize for N/A checklist items

CALL CONTROL (9% weight):
- Score on whether rep ultimately steered to all required info
- Do NOT penalize for natural conversation or customer tangents
- Penalize only if key info was never recovered

FVP PITCH (15% weight):
- 0-2: Never mentioned (when required)
- 3-5: Mentioned briefly, no explanation
- 6: Mentioned, said can be removed or dismissed it
- 7: Mentioned, included cost, moved on — standard
- 8: Explained briefly what it covers, stated cost
- 9-10: Explained thoroughly, adjusted coverage, handled objection
- N/A for unload_only and storage moves

RAPPORT & TONE (13% weight):
- 1-3: Flat, bothered, missed obvious rapport opportunities, customer uncomfortable
- 4-6: Polite but mechanical, some warmth but missed moments
- 7-8: Warm, acknowledged customer's situation, customer felt heard
- 9-10: Excellent — genuine connection, used customer's situation to build trust

SALESMANSHIP (10% weight): see rubric above

PROFESSIONALISM (8% weight):
- Includes expectation setting, email explanation, day-of call mention, smooth handling of complications

CONFIDENCE SCORE (1-10):
- 10: Clear complete transcript
- 7-9: Good, minor ambiguities
- 4-6: Some audio issues
- 1-3: Poor audio or very ambiguous

TALK RATIO: use speaker labels if diarized, otherwise estimate.

KEYWORDS — detect if rep said (concept not exact words):
- "Full Value Protection" or "FVP"
- "confirmation call"
- "hourly" or "per hour"
- "moving boxes"
- "fuel" or "fuel charge"
- "declared value"
- "Little Guys" or "Little Guys Movers"
- "spot available" or "have a spot"
- "get you on the board/schedule/calendar"
- "first come first serve"
- "no cancellation fee"
- "save/hold that for you"
- "get you taken care of"
- "go ahead and book/get you"
- "background checked" or "background screened"
- "no day labor"
- "we show up" or "we don't miss"
- "clothes in the dresser"
- "disassemble" or "reassemble"

Return CHARACTER POSITION of first occurrence for each keyword and objection.

OBJECTIONS: Price too high, Need to think about it, Already have another quote, Wrong timing, Need to check with partner, Other

SENTIMENT: positive / neutral / hesitant / negative

Filename: {filename}

Transcript:
{transcript}

Respond ONLY with valid JSON, no markdown:

{{"rep_name_detected":"name or Unknown","caller_name":"name or Unknown","call_purpose":"short phrase","call_type":"sales_estimate","move_category":"standard","move_type":"local/long distance/unknown","call_outcome":"booked","loss_reason":"","soft_pipeline_reason":"","word_count":150,"exclude_from_scoring":false,"exclusion_reason":"","call_quality":"normal","turned_away":false,"onsite_suggested":false,"is_continuation":false,"evaluation_confidence":8,"close_attempts":1,"objections_overcome":[],"objections_abandoned":[],"pipeline_recovery_quality":0,"missed_rapport_opportunities":[],"value_props_used":[],"salesmanship_score":0,"audio_duration_estimate":0,"call_summary":"3-5 sentences","key_details_captured":"details gathered","talk_ratio_rep":40,"talk_ratio_customer":60,"keywords_detected":["Full Value Protection"],"keyword_positions":{{"Full Value Protection":342}},"objections_detected":["Price too high"],"objection_positions":{{"Price too high":891}},"customer_sentiment":"positive","scores":{{"info_sequence":{{"score":0,"note":""}},"price_delivery":{{"score":0,"note":""}},"fvp_pitch":{{"score":0,"note":""}},"closing_attempt":{{"score":0,"note":""}},"call_control":{{"score":0,"note":""}},"professionalism":{{"score":0,"note":""}},"rapport_tone":{{"score":0,"note":""}},"salesmanship":{{"score":0,"note":""}},"overall":{{"score":0,"note":""}}}},"checklist":{{"got_move_date":false,"got_customer_name":false,"got_phone_number":false,"got_cities":false,"got_home_type":false,"got_stairs_info":false,"did_full_inventory":false,"asked_forgotten_items":false,"asked_about_boxes":false,"gave_price_on_call":false,"pitched_fvp":false,"attempted_to_close":false,"offered_email_estimate":false,"mentioned_confirmations":false,"thanked_customer":false,"asked_name_at_start":false,"led_estimate_process":false,"scheduled_onsite_attempt":"na","offered_alternatives":"na","took_rapport_opportunities":false,"completed_booking_wrapup":false,"captured_lead":false}},"strengths":["s1","s2"],"coaching_points":["c1","c2"]}}"""
    return prompt

    prompt = f"""You are a sales call evaluator for Little Guys Movers (LGMS).
{corrections_block}{diarization_note}
STEP 1 — CLASSIFY CALL TYPE:
Determine the move_category (this drives which checklist steps apply):
- "standard": typical household move, load and unload
- "specialty": single heavy item — piano, safe, hot tub, pool table, single appliance
- "unload_only": truck already packed, just unloading
- "in_house": moving items within the same location
- "commercial": office or business move
- "storage": moving out of or into storage
- "non_move": not a moving estimate — complaint, inquiry, wrong number, etc.

Also determine:
- call_type: "sales_estimate" | "follow_up" | "complaint" | "booking_confirmation" | "non_sales" | "too_short" | "other"
- word_count: approximate word count
- exclude_from_scoring: true if fewer than 80 words OR clearly not a sales call OR disconnected

IMPORTANT: Even if exclude_from_scoring is true, still detect turned_away. A call where LGMS
says "we don't have availability on that date" is a Turned Away call even if it's only 10 words.
These short turned-away calls must still be captured.

STEP 2 — DETECT REP NAME:
- Look for rep introduction (e.g. "This is Britt", "Thank you for calling Little Guys this is Sarah")
- If diarized transcript, look at Speaker 0's first lines
- Return FIRST NAME ONLY. Return "Unknown" if not found.

STEP 3 — CALL QUALITY:
- "disconnected": abrupt ending, "Hello? Hello?" patterns, very short
- "poor_audio": heavy [inaudible] density (5+ occurrences), one-sided
- "normal": otherwise

STEP 4 — FLAGS:
- turned_away: LGMS could NOT accommodate — rep said "we're booked", "we don't have availability", "we can't do that date", "we're full that day", "we're not available then". Capture even on very short calls.
- onsite_suggested: onsite visit/estimate mentioned
- is_continuation: "calling back about", "as we discussed", "following up", "spoke earlier"

STEP 5 — OUTCOME & PIPELINE:
- call_outcome: "booked" | "estimate_sent" | "soft_pipeline" | "lost" | "unknown"
- Use "soft_pipeline" when customer is interested but not ready: needs partner approval, no exact date, needs to think, will call back
- loss_reason (only if lost): "price_too_high" | "went_with_competitor" | "wrong_timing" | "no_availability" | "just_shopping" | "other" | ""
- soft_pipeline_reason (only if soft_pipeline): "needs_partner" | "no_exact_date" | "needs_to_think" | "will_call_back" | "other" | ""

STEP 6 — CLOSING ANALYSIS (most important part of evaluation):

LGMS reps are trained to actively close on every call. The goal is to book the move on the first call,
not let the customer "call back." Every call should end with a clear close attempt or a clear next step.

COUNT close_attempts: how many times did the rep explicitly try to book/schedule the move?
- Each distinct ask counts: "Can I get you on the calendar?", "Let me go ahead and get you booked", "Shall I save that spot for you?" = 3 attempts
- Repeating after an objection = additional attempt (good)
- Never asking at all = 0 (very bad)

CLOSING LANGUAGE to look for (these are strong signals of a good close):
- "I have a spot available for you"
- "let me get you on the board"
- "let me get you on the schedule"
- "first come first serve" (creates urgency)
- "no cancellation fee" (removes objection barrier)
- "save that for you" / "hold that for you"
- "get you taken care of"
- "get you on the calendar"
- "can I go ahead and" / "shall I go ahead and"
- "let me go ahead and book"

OBJECTION HANDLING — for each objection detected, evaluate:
- Did rep acknowledge the objection? (good)
- Did rep provide a counter/solution? (better)
- Did rep attempt to close again after the objection? (best)
- Or did rep just say "ok, call us back"? (failure)

Classify each objection as either overcome or abandoned:
- overcome: rep countered and either booked or meaningfully advanced the conversation
- abandoned: rep accepted the objection without any counter and let customer go

IDEAL RESPONSES by objection type:
- "Need to think about it" → good rep: "I totally understand — we do have availability on that date and there's no cancellation fee, so would it make sense to go ahead and hold that spot while you think it over?"
- "Need to check with partner" → good rep: "Of course — while I have you, would it help to hold that spot? No cancellation fee, so there's no risk, and availability goes fast"
- "Price too high" → good rep: "I hear you — what were you expecting? Let me see what I can do" then works through it
- "Already have another quote" → good rep: "That's great — what are they quoting you? We're very competitive and I'd love to earn your business today"

PIPELINE RECOVERY (soft_pipeline calls only):
pipeline_recovery_quality 1-10:
- 1-3: Rep just said "ok call us back" with no next step — lead will likely go cold
- 4-6: Rep offered to send email estimate or gave some follow-up info
- 7-8: Rep set a specific callback time or sent confirmation of next step
- 9-10: Rep held a tentative spot, set specific callback, sent email — customer has a clear path to book

STEP 7 — CHECKLIST (apply based on move_category):

STANDARD MOVE — all 22 steps apply:
1. got_move_date
2. got_customer_name
3. got_phone_number
4. got_cities
5. got_home_type
6. got_stairs_info
7. did_full_inventory
8. asked_forgotten_items
9. asked_about_boxes
10. gave_price_on_call
11. pitched_fvp (Full Value Protection — ALWAYS required for standard)
12. attempted_to_close
13. offered_email_estimate (if not booked) / mentioned_confirmations (if booked)
14. mentioned_confirmations
15. thanked_customer
16. asked_name_at_start
17. led_estimate_process
18. scheduled_onsite_attempt — set to "na" unless customer specifically asked for onsite
19. offered_alternatives — set to "na" unless customer hesitated or move was difficult
20. took_rapport_opportunities
21. completed_booking_wrapup (if booked)
22. captured_lead

SPECIALTY (piano, safe, hot tub, etc.) — reduced checklist:
- Apply: got_move_date, got_customer_name, got_phone_number, got_cities, got_stairs_info, gave_price_on_call, pitched_fvp, attempted_to_close, mentioned_confirmations, thanked_customer, asked_name_at_start, captured_lead
- Set to "na": got_home_type, did_full_inventory, asked_forgotten_items, asked_about_boxes, scheduled_onsite_attempt, offered_alternatives, led_estimate_process, completed_booking_wrapup, offered_email_estimate, took_rapport_opportunities

UNLOAD ONLY — reduced checklist:
- Apply: got_move_date, got_customer_name, got_phone_number, got_cities, got_stairs_info, gave_price_on_call, attempted_to_close, mentioned_confirmations, thanked_customer, asked_name_at_start, captured_lead
- Set to "na": got_home_type, did_full_inventory, asked_forgotten_items, asked_about_boxes, pitched_fvp, scheduled_onsite_attempt, offered_alternatives, led_estimate_process, completed_booking_wrapup, offered_email_estimate, took_rapport_opportunities

IN-HOUSE SHUFFLE — reduced checklist:
- Apply: got_move_date, got_customer_name, got_phone_number, did_full_inventory, gave_price_on_call, pitched_fvp, attempted_to_close, thanked_customer, asked_name_at_start, captured_lead
- Set to "na": got_cities, got_home_type, got_stairs_info, asked_forgotten_items, asked_about_boxes, scheduled_onsite_attempt, offered_alternatives, mentioned_confirmations, completed_booking_wrapup, offered_email_estimate, took_rapport_opportunities, led_estimate_process

COMMERCIAL — apply most steps, treat inventory as equipment/furniture list:
- Apply all standard steps
- Set to "na": scheduled_onsite_attempt, offered_alternatives (unless triggered)

STORAGE — reduced checklist:
- Apply: got_move_date, got_customer_name, got_phone_number, got_cities, got_stairs_info, gave_price_on_call, pitched_fvp, attempted_to_close, mentioned_confirmations, thanked_customer, asked_name_at_start, captured_lead
- Set to "na": got_home_type, did_full_inventory, asked_forgotten_items, asked_about_boxes, scheduled_onsite_attempt, offered_alternatives, led_estimate_process, completed_booking_wrapup, offered_email_estimate, took_rapport_opportunities

CHECKLIST VALUES: true | false | "na"

STEP 8 — SCORING (1-10, be honest and critical):

OVERALL SCORE — calculated weighted average (do not override with gut feel):
- closing_attempt: 25% weight (most important)
- price_delivery: 15% weight
- fvp_pitch: 15% weight
- rapport_tone: 15% weight
- info_sequence: 10% weight
- call_control: 10% weight
- professionalism: 10% weight
Calculate: overall = (closing*0.25 + price*0.15 + fvp*0.15 + rapport*0.15 + info*0.10 + control*0.10 + prof*0.10)
Round to nearest integer.

CLOSING ATTEMPT (1-10) — most heavily weighted:
- 1-2: Never attempted to close at all — just let customer hang up
- 3-4: Weak close, no follow-through — accepted first objection without any counter
- 5-6: Made one attempt but gave up too easily, or close was vague/passive
- 7-8: Made clear close attempt, handled at least one objection with a counter
- 9-10: Multiple close attempts, overcame objections with confidence and urgency, used no-cancellation-fee and availability language

INFORMATION COMPLETENESS:
- Score based on WHAT information was gathered, NOT the order
- Penalize only if required information was NEVER obtained
- Natural conversation that circles back to get info scores well

CALL CONTROL:
- Score on whether rep ULTIMATELY steered to all required info
- Do NOT penalize for natural conversation or customer tangents
- Penalize only if key info was never recovered

FVP PITCH:
- Standard, specialty, commercial, in_house: required
- Unload only, storage: mark note as "not required for this move type"

RAPPORT & TONE (1-10):
- 1-3: Flat, bothered, disengaged; missed obvious rapport opportunities
- 4-6: Polite but mechanical; some warmth but opportunities missed
- 7-8: Warm and engaged; acknowledged customer's situation; customer felt heard
- 9-10: Excellent — enthusiastic, genuine connection

CONFIDENCE SCORE:
- 10: Clear complete transcript, straightforward call
- 7-9: Good transcript, minor ambiguities
- 4-6: Some audio issues or ambiguous moments
- 1-3: Poor audio, very short, or highly ambiguous — manager should review

TALK RATIO: Use speaker labels if diarized, otherwise estimate from line counts.

KEYWORDS — check if rep said any of these:
- "Full Value Protection" or "FVP"
- "confirmation call"
- "hourly" or "per hour"
- "moving boxes"
- "fuel" or "fuel charge"
- "declared value"
- "Little Guys" or "Little Guys Movers"
- "spot available" or "have a spot"
- "get you on the board" or "get you on the schedule" or "get you on the calendar"
- "first come first serve"
- "no cancellation fee"
- "save that for you" or "hold that for you"
- "get you taken care of"
- "go ahead and book" or "go ahead and get you"

Return CHARACTER POSITION of first occurrence for each keyword and objection found.

OBJECTIONS: Price too high, Need to think about it, Already have another quote, Wrong timing, Need to check with partner, Other

SENTIMENT: positive / neutral / hesitant / negative

Filename: {filename}

Transcript:
{transcript}

Respond ONLY with valid JSON, no markdown:

{{"rep_name_detected":"name or Unknown","caller_name":"name or Unknown","call_purpose":"short phrase","call_type":"sales_estimate","move_category":"standard","move_type":"local/long distance/unknown","call_outcome":"booked","loss_reason":"","soft_pipeline_reason":"","word_count":150,"exclude_from_scoring":false,"exclusion_reason":"","call_quality":"normal","turned_away":false,"onsite_suggested":false,"is_continuation":false,"evaluation_confidence":8,"close_attempts":0,"objections_overcome":[],"objections_abandoned":[],"pipeline_recovery_quality":0,"call_summary":"3-5 sentences","key_details_captured":"details gathered","talk_ratio_rep":40,"talk_ratio_customer":60,"keywords_detected":["Full Value Protection"],"keyword_positions":{{"Full Value Protection":342}},"objections_detected":["Price too high"],"objection_positions":{{"Price too high":891}},"customer_sentiment":"positive","scores":{{"info_sequence":{{"score":0,"note":""}},"price_delivery":{{"score":0,"note":""}},"fvp_pitch":{{"score":0,"note":""}},"closing_attempt":{{"score":0,"note":""}},"call_control":{{"score":0,"note":""}},"professionalism":{{"score":0,"note":""}},"rapport_tone":{{"score":0,"note":""}},"overall":{{"score":0,"note":""}}}},"checklist":{{"got_move_date":false,"got_customer_name":false,"got_phone_number":false,"got_cities":false,"got_home_type":false,"got_stairs_info":false,"did_full_inventory":false,"asked_forgotten_items":false,"asked_about_boxes":false,"gave_price_on_call":false,"pitched_fvp":false,"attempted_to_close":false,"offered_email_estimate":false,"mentioned_confirmations":false,"thanked_customer":false,"asked_name_at_start":false,"led_estimate_process":false,"scheduled_onsite_attempt":"na","offered_alternatives":"na","took_rapport_opportunities":false,"completed_booking_wrapup":false,"captured_lead":false}},"strengths":["s1","s2"],"coaching_points":["c1","c2"]}}"""
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
        for u in utterances:
            speaker = u.get("speaker", 0)
            text = u.get("transcript", "").strip()
            if text:
                lines.append(f"Speaker {speaker}: {text}")
        transcript = "\n".join(lines)
        log(f"  Diarized via utterances: {len(utterances)} turns")
        return transcript, True

    # Fallback: try word-level diarization from channels
    channels = result.get("results", {}).get("channels", [])
    if channels:
        words = channels[0].get("alternatives", [{}])[0].get("words", [])
        plain = channels[0].get("alternatives", [{}])[0].get("transcript", "")

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
            log(f"  Diarized via word-level: {len(lines)} turns")
            return transcript, True
        else:
            log(f"  No diarization data, using plain transcript ({len(plain)} chars)")
            return plain, False

    log("  Deepgram returned no usable transcript")
    return "", False

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
        return r.read().decode("utf-8"), False

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
        "closing_attempt": 0.25,
        "price_delivery": 0.15,
        "fvp_pitch": 0.15,
        "rapport_tone": 0.13,
        "salesmanship": 0.10,
        "info_sequence": 0.09,
        "call_control": 0.09,
        "professionalism": 0.08,
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
    # Normalize if some scores were 0/missing
    normalized = total / weight_used if weight_used > 0 else 0
    return min(10, max(1, round(normalized)))

def run_claude_analysis(transcript, filename, model="claude-sonnet-4-6", is_diarized=False):
    corrections = get_recent_corrections()
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
    with urllib.request.urlopen(req, timeout=120) as r:
        resp = json.loads(r.read())
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
        scores["overall"] = {"score": weighted_overall, "note": "Weighted: closing 25%, price 15%, fvp 15%, rapport 13%, salesmanship 10%, info 9%, control 9%, prof 8%"}
        result["scores"] = scores

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
                    # Apply corrections before analysis
                    tx_corrections = get_transcript_corrections()
                    clean_transcript = apply_transcript_corrections(transcript, tx_corrections)
                    result = run_claude_analysis(clean_transcript, filename)

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
                log(f"  Re-analyze error for {filename}: {e}")
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
    transcript, is_diarized = transcribe_audio(audio_bytes, filename, keyterms=keyterms)
    if not transcript or not transcript.strip():
        return None, "empty transcript"

    # Apply corrections
    clean_transcript = apply_transcript_corrections(transcript, tx_corrections)

    # Analyze
    result = run_claude_analysis(clean_transcript, filename, is_diarized=is_diarized)
    result["transcript"] = clean_transcript
    result["filename"] = filename
    result["is_diarized"] = is_diarized

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
    }
    saved = supa("POST", "calls", record)
    return saved, None

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
        elif path == "/batch_upload/status":
            self._batch_upload_status()
        elif path == "/corrections":
            self._get_corrections()
        elif path == "/transcript_corrections":
            self._get_transcript_corrections_endpoint()
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
            "/transcript_corrections/save": self._save_transcript_correction,
            "/transcript_corrections/delete": self._delete_transcript_correction,
            "/transcript_corrections/reapply": self._reapply_corrections,
            "/batch_upload/start": self._batch_upload_start,
            "/transcript_corrections/delete": self._delete_transcript_correction,
            "/transcript_corrections/reapply": self._reapply_corrections,
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
                transcript, is_diarized = transcribe_audio(audio_bytes, filename, keyterms=keyterms)
                if not transcript or not transcript.strip():
                    self._err(400, "Transcription empty — audio may be silent"); return
                log(f"  Transcription done: {len(transcript)} chars, diarized={is_diarized}")

                # Apply corrections
                clean_transcript = apply_transcript_corrections(transcript, tx_corrections)

                result = run_claude_analysis(clean_transcript, filename, is_diarized=is_diarized)

            result["transcript"] = clean_transcript
            result["filename"] = filename
            result["is_diarized"] = is_diarized

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
            if not find_text:
                self._err(400, "find_text required"); return
            # Update existing rule if same find_text exists
            existing = supa("GET", f"transcript_corrections?find_text=ilike.{urllib.parse.quote(find_text)}&limit=1")
            if existing:
                supa("PATCH", f"transcript_corrections?id=eq.{existing[0]['id']}", {"replace_text": replace_text})
                self._ok({"updated": True, "id": existing[0]["id"]})
            else:
                result = supa("POST", "transcript_corrections", {"find_text": find_text, "replace_text": replace_text})
                self._ok(result)
            # Also save updated transcript on the call if provided
            call_id = p.get("call_id")
            new_transcript = p.get("new_transcript")
            if call_id and new_transcript:
                supa("PATCH", f"calls?id=eq.{call_id}", {"transcript": new_transcript})
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

    def _batch_upload_start(self, body):
        """Accept raw ZIP bytes via multipart or base64 JSON, start background processing."""
        global _batch_job
        with _batch_lock:
            if _batch_job["status"] == "running":
                self._ok({"message": "Already running", "status": _batch_job}); return

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
            }

        t = threading.Thread(target=_batch_upload_worker, args=(zip_bytes,), daemon=True)
        t.start()
        log(f"  Batch upload started: {len(zip_bytes):,} bytes")
        self._ok({"message": "Batch upload started", "status": _batch_job})

    def _batch_upload_status(self):
        with _batch_lock:
            status = dict(_batch_job)
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
    log("=" * 55)
    log("  Little Guys Movers — Call Analyzer Server v9")
    log("=" * 55)
    missing = [v for v in ["ANTHROPIC_API_KEY","SUPABASE_URL","SUPABASE_KEY","DEEPGRAM_API_KEY"] if not os.environ.get(v)]
    if missing:
        log("\n  WARNING: Missing env vars: " + ", ".join(missing))
    else:
        log("\n  All environment variables loaded")
    log(f"  Running at http://127.0.0.1:{PORT}")
    log("  Press Ctrl+C to stop\n")
    HTTPServer(("0.0.0.0", PORT), Handler).serve_forever()
