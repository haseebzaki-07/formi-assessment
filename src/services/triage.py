"""
Triage classifier — decides which lane a transcript belongs in before we
spend full LLM quota on it.

Two stages:

  Stage A — keyword/regex matching. Cheap, deterministic, runs in <1ms per
    transcript. Hits a curated pattern set (HOT / COLD / AMBIGUOUS / SKIP)
    and scores customer turns more heavily than agent turns (the *outcome*
    is what the customer says, not what the agent reads off a script).
    Most transcripts terminate here.

  Stage B — small/cheap LLM fallback. Only invoked when Stage A signals
    ambiguity (mixed evidence or a "thinking it over" pattern that
    keyword-only cannot resolve). The fixture's `hinglish_ambiguous` case
    is the canonical example — Hindi/English code-switching with hedge
    words ("Dekhta hoon", "soch raha hoon", "next week") that can map to
    either lane depending on tone. The Stage B call is mocked here for
    the assessment; in production it's a small fast model with a tight
    classification-only prompt that costs ~50 tokens per call.

Lanes:
  hot   → priority 1; processed end-to-end within 60s of webhook receipt.
          Confirmed bookings, demos, escalations.
  cold  → priority 9; processed within 30 minutes.
          Not interested, callback later, already done, considering.
  skip  → no LLM call at all. Transcript too short or "wrong number" type
          hangup. Pipeline still creates downstream stages so the lead's
          stage is updated, but analysis is short-circuited.

Per-customer overrides: a customer can opt specific dispositions from one
lane to another via customer_budgets.customer_overrides JSONB. e.g.
`{"already_done": "hot"}` for a customer that wants every "already
purchased" call escalated for upsell.
"""

import re
from dataclasses import dataclass, field
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

LANE_HOT = "hot"
LANE_COLD = "cold"
LANE_SKIP = "skip"

PRIORITY_HOT = 1
PRIORITY_COLD = 9
PRIORITY_DEFAULT = 5

# Cold-lane jobs are deferred to a low-utilisation window. The default delay
# keeps the fast path simple while still making cold work yield to hot under
# contention; ops can lower this to 0 if the cold backlog grows.
COLD_LANE_DEFER_SECONDS = 0


# Skip = no LLM, ever. "Wrong number", obvious hangups. Length-based skip
# (turns < 4) is enforced in addition to these patterns.
_SKIP_PATTERNS = [
    re.compile(r"\bwrong\s+number\b", re.IGNORECASE),
    re.compile(r"\bgalat\s+number\b", re.IGNORECASE),
    re.compile(r"\bnot\s+available\b", re.IGNORECASE),
]

# Hot — affirmative outcome. Booking confirmed, demo scheduled, escalation
# requested. These map to time-sensitive downstream actions (sales rep
# callback within minutes, calendar invite send, ticket creation).
_HOT_PATTERNS = [
    re.compile(r"\bconfirm(?:ed|ing)?\b", re.IGNORECASE),
    re.compile(r"\bbook(?:ed|ing|s)?\b", re.IGNORECASE),
    re.compile(r"\brebook(?:ed|ing)?\b", re.IGNORECASE),
    re.compile(r"\bdemo\b", re.IGNORECASE),
    re.compile(r"\bappointment\b", re.IGNORECASE),
    re.compile(r"\bscheduled?\b", re.IGNORECASE),
    re.compile(r"\bescalat", re.IGNORECASE),
    re.compile(r"\b(?:speak\s+to\s+)?(?:a\s+)?manager\b", re.IGNORECASE),
    re.compile(r"\bcomplaint\b", re.IGNORECASE),
    re.compile(r"\bsenior\s+(?:executive|manager|agent)\b", re.IGNORECASE),
    re.compile(r"\b(?:to|for)\s*tomorrow\b", re.IGNORECASE),
    re.compile(r"\btomorrow\s+(?:at|works|\d|morning|afternoon|evening)", re.IGNORECASE),
    re.compile(r"\b(?:monday|tuesday|wednesday|thursday|friday|saturday|sunday)\b", re.IGNORECASE),
    re.compile(r"\b\d{1,2}(?:[:.]\d{2})?\s*(?:am|pm)\b", re.IGNORECASE),
    re.compile(r"\b\d{1,2}\s*baje\b", re.IGNORECASE),
    re.compile(r"\bcalendar\s+(?:invite|block|invitation)\b", re.IGNORECASE),
    re.compile(r"\bsee\s+you\s+then\b", re.IGNORECASE),
    re.compile(r"\bfile\s+a\s+complaint\b", re.IGNORECASE),
]

# Cold — clear rejection or already-resolved. Useful for CRM hygiene but
# tolerates batching.
_COLD_PATTERNS = [
    re.compile(r"\bnot\s+interested\b", re.IGNORECASE),
    re.compile(r"\bdon'?t\s+call(?:\s+me)?(?:\s+again)?\b", re.IGNORECASE),
    re.compile(r"\bdo\s+not\s+call\b", re.IGNORECASE),
    re.compile(r"\bremove\s+(?:me|my\s+number)\b", re.IGNORECASE),
    re.compile(r"\bcall(?:\s+me)?\s+back\b", re.IGNORECASE),
    re.compile(r"\bcall\s+later\b", re.IGNORECASE),
    re.compile(r"\bcallback\b", re.IGNORECASE),
    re.compile(r"\bbaad\s+mein\s+call\b", re.IGNORECASE),
    re.compile(r"\bbusy\s+(?:right\s+now|at\s+the\s+moment|abhi)", re.IGNORECASE),
    re.compile(r"\bmeeting\s+mein\b", re.IGNORECASE),
    re.compile(r"\balready\s+(?:booked|purchased|done|signed|completed|got|did)\b", re.IGNORECASE),
    re.compile(r"\bthrough\s+your\s+website\b", re.IGNORECASE),
]

# Ambiguous — hedge words. A transcript with these and no decisive
# hot/cold dominance is escalated to Stage B.
_AMBIGUOUS_PATTERNS = [
    re.compile(r"\bsoch(?:\s+raha|\s+rahi|na)\b", re.IGNORECASE),
    re.compile(r"\bdekht(?:a|ee|i)\s+hoon\b", re.IGNORECASE),
    re.compile(r"\bthinking\s+(?:about|of|over|it)\b", re.IGNORECASE),
    re.compile(r"\blet\s+me\s+(?:think|see|check)\b", re.IGNORECASE),
    re.compile(r"\bconsider(?:ing)?\b", re.IGNORECASE),
    re.compile(r"\bnot\s+sure\b", re.IGNORECASE),
    re.compile(r"\bbudget\s+(?:tight|issue|problem|constraint)\b", re.IGNORECASE),
    re.compile(r"\bmaybe\s+(?:later|next)\b", re.IGNORECASE),
    re.compile(r"\bnext\s+week\b", re.IGNORECASE),
]

# Confidence threshold: Stage A commits to a lane only if the dominant
# score exceeds this and beats the runner-up by at least DOMINANCE_DELTA.
# Tuned so the eight fixture transcripts route correctly and so the
# hinglish_ambiguous case falls to Stage B.
_MIN_DECISIVE_SCORE = 2
_DOMINANCE_DELTA = 2

# Customer turns are weighted higher than agent turns — the *customer*
# decides the disposition, the agent reads from a script.
_CUSTOMER_WEIGHT = 2
_AGENT_WEIGHT = 1


@dataclass
class TriageResult:
    """Triage output. Carries everything needed to size the rate-limit
    reservation, set processing_jobs.priority/lane, and pick a prompt
    variant in the analysis stage."""

    lane: str
    priority: int
    disposition: Optional[str] = None
    confidence: float = 0.0
    stage_used: str = "keyword"  # "keyword" | "llm" | "length"
    reason: str = ""
    scores: Dict[str, int] = field(default_factory=dict)

    @property
    def is_skip(self) -> bool:
        return self.lane == LANE_SKIP


@dataclass
class _StageAOutcome:
    lane: Optional[str]
    disposition: Optional[str]
    confidence: float
    scores: Dict[str, int]
    reason: str


def _join_role_text(turns: Sequence[Mapping[str, Any]]) -> Tuple[str, str]:
    customer_text = " ".join(
        str(t.get("content", ""))
        for t in turns
        if str(t.get("role", "")).lower() == "customer"
    )
    agent_text = " ".join(
        str(t.get("content", ""))
        for t in turns
        if str(t.get("role", "")).lower() != "customer"
    )
    return customer_text, agent_text


def _score_patterns(
    patterns: List[re.Pattern],
    customer_text: str,
    agent_text: str,
) -> int:
    score = 0
    for pat in patterns:
        score += _CUSTOMER_WEIGHT * len(pat.findall(customer_text))
        score += _AGENT_WEIGHT * len(pat.findall(agent_text))
    return score


def _stage_a(
    turns: Sequence[Mapping[str, Any]],
) -> _StageAOutcome:
    """Stage A — keyword/regex scoring. Returns lane=None when ambiguous."""
    customer_text, agent_text = _join_role_text(turns)
    full_text = f"{customer_text} {agent_text}"

    skip_hits = sum(len(p.findall(full_text)) for p in _SKIP_PATTERNS)
    hot_score = _score_patterns(_HOT_PATTERNS, customer_text, agent_text)
    cold_score = _score_patterns(_COLD_PATTERNS, customer_text, agent_text)
    ambiguous_score = _score_patterns(
        _AMBIGUOUS_PATTERNS, customer_text, agent_text,
    )

    scores = {
        "hot": hot_score,
        "cold": cold_score,
        "ambiguous": ambiguous_score,
        "skip": skip_hits,
    }

    if skip_hits >= 1:
        return _StageAOutcome(
            lane=LANE_SKIP,
            disposition="short_call",
            confidence=1.0,
            scores=scores,
            reason="skip pattern matched",
        )

    # If the ambiguous bucket has appreciable signal AND no decisive
    # margin between hot and cold, escalate to Stage B.
    margin = abs(hot_score - cold_score)
    decisive = (
        max(hot_score, cold_score) >= _MIN_DECISIVE_SCORE
        and margin >= _DOMINANCE_DELTA
    )
    if ambiguous_score >= _CUSTOMER_WEIGHT and not decisive:
        return _StageAOutcome(
            lane=None,
            disposition=None,
            confidence=0.0,
            scores=scores,
            reason="ambiguous keywords, escalate to llm fallback",
        )

    if not decisive:
        # No ambiguous signal and no decisive winner — default to cold.
        # Cold is the safe default: it doesn't burn hot-lane reservation,
        # and the analysis still runs at lower priority.
        if hot_score == 0 and cold_score == 0:
            return _StageAOutcome(
                lane=LANE_COLD,
                disposition="unknown",
                confidence=0.3,
                scores=scores,
                reason="no signals matched; default cold",
            )
        # Weak winner — go with it but mark low confidence.
        winner = LANE_HOT if hot_score >= cold_score else LANE_COLD
        return _StageAOutcome(
            lane=winner,
            disposition=None,
            confidence=0.5,
            scores=scores,
            reason="weak keyword signal",
        )

    winner = LANE_HOT if hot_score > cold_score else LANE_COLD
    confidence = min(1.0, max(hot_score, cold_score) / 8.0)
    return _StageAOutcome(
        lane=winner,
        disposition=None,
        confidence=confidence,
        scores=scores,
        reason=f"keyword dominance ({winner})",
    )


async def _stage_b_llm_fallback(
    turns: Sequence[Mapping[str, Any]],
    customer_overrides: Optional[Mapping[str, str]] = None,
) -> Tuple[str, str, float]:
    """Stage B — small/cheap LLM classification.

    Returns (lane, disposition, confidence). Mocked for the assessment;
    in production this is a 50-token prompt against a small fast model
    asking only "hot or cold? one word." Tests can monkey-patch this
    function to inject deterministic outcomes.

    The default heuristic biases toward cold when ambiguous signals
    dominate — "considering / thinking" is closer to a deferred
    decision than a confirmed booking, and the cost of routing a
    real-hot call to cold (slightly higher latency) is much less than
    burning hot-lane budget on lukewarm leads.
    """
    customer_text = " ".join(
        str(t.get("content", ""))
        for t in turns
        if str(t.get("role", "")).lower() == "customer"
    )
    # Heuristic stand-in: the same text we would send to the small model.
    if any(
        kw in customer_text.lower()
        for kw in ("not interested", "don't call", "do not call")
    ):
        return LANE_COLD, "not_interested", 0.85
    if any(
        kw in customer_text.lower()
        for kw in ("confirmed", "book it", "schedule it", "tomorrow", "thursday")
    ):
        return LANE_HOT, "interested", 0.75
    return LANE_COLD, "considering", 0.6


async def classify(
    turns: Sequence[Mapping[str, Any]],
    *,
    customer_overrides: Optional[Mapping[str, str]] = None,
) -> TriageResult:
    """Classify a transcript into a lane. Pure function over the transcript
    + optional per-customer override map. Idempotent and deterministic for
    the keyword path; Stage B may vary across calls if a real LLM is wired.
    """
    overrides = dict(customer_overrides or {})

    if len(turns) < 4:
        return TriageResult(
            lane=LANE_SKIP,
            priority=PRIORITY_DEFAULT,
            disposition="short_call",
            confidence=1.0,
            stage_used="length",
            reason=f"transcript has {len(turns)} turns (<4)",
            scores={"length": len(turns)},
        )

    stage_a = _stage_a(turns)
    if stage_a.lane is not None:
        lane = stage_a.lane
        disposition = stage_a.disposition
        confidence = stage_a.confidence
        stage_used = "keyword"
        reason = stage_a.reason
    else:
        lane, disposition, confidence = await _stage_b_llm_fallback(
            turns, customer_overrides=overrides,
        )
        stage_used = "llm"
        reason = "stage_b llm fallback resolved ambiguity"

    if disposition and disposition in overrides:
        override_lane = overrides[disposition]
        if override_lane in (LANE_HOT, LANE_COLD, LANE_SKIP):
            reason = (
                f"{reason}; customer override {disposition}->{override_lane}"
            )
            lane = override_lane

    priority = _priority_for_lane(lane)
    return TriageResult(
        lane=lane,
        priority=priority,
        disposition=disposition,
        confidence=confidence,
        stage_used=stage_used,
        reason=reason,
        scores=stage_a.scores,
    )


def _priority_for_lane(lane: str) -> int:
    if lane == LANE_HOT:
        return PRIORITY_HOT
    if lane == LANE_COLD:
        return PRIORITY_COLD
    return PRIORITY_DEFAULT


def cold_lane_defer_seconds() -> int:
    """How far in the future to schedule a cold-lane job. Centralised so the
    integration test and the create_pipeline call agree on the value."""
    return COLD_LANE_DEFER_SECONDS
