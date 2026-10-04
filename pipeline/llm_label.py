"""BONUS — an LLM inside the pipeline (slide "LLM là một bước transform").

The support team wants an LLM pre-triage label on every live ticket
(gold_ticket_labels), to compare with the human `category` and to triage new
tickets faster. An LLM step is a transform like any other — except it is
expensive, slow and NOT deterministic, so the slide's four rules apply:

  1. key = hash(input) + model + prompt version  -> a re-run makes 0 LLM calls;
     changing the prompt re-labels everything ON PURPOSE
  2. force a structured output, validate it; invalid -> quarantine, never Gold
  3. estimate the cost BEFORE running (rows x tokens x price)
  4. LLM labels are versioned data (model + prompt_version stored on every row)

The shipped `label_tickets` is the NAIVE version: it calls the model for every
ticket on every run and writes whatever comes back. Your bonus task is to make
`python -m scripts.bonus_llm` print BONUS PASS. Zero-key: `FakeLLM` stands in for a
real model (swap in any provider via .env if you like — the pipeline is the same).
"""
from __future__ import annotations

import hashlib
import json
import re

import duckdb

MODEL = "fake-llm-2026-09"
PROMPT_VERSION = "triage-v1"
ALLOWED_LABELS = ("bug", "billing", "other")
PRICE_PER_1K_TOKENS_USD = 0.002          # pretend price, for the cost estimate


PROMPT_TEMPLATE = """You triage customer-support tickets.
Answer ONLY with JSON: {{"label": "bug" | "billing" | "other"}}.
Ticket: {text}"""


class FakeLLM:
    """Deterministic stand-in for a chat model. Counts calls and tokens."""

    def __init__(self, model: str = MODEL) -> None:
        self.model = model
        self.calls = 0
        self.tokens = 0

    def complete(self, prompt: str) -> str:
        self.calls += 1
        self.tokens += len(prompt.split()) + 8
        text = prompt.lower()
        if "xuất" in text:
            return 'Sure! Here is the label: {"label": "export"}'   # off-schema answer
        if re.search(r"crash|lỗi|sso|đăng nhập|chatbot", text):
            return '{"label": "bug"}'
        if re.search(r"tiền|hoá đơn|thanh toán|gói|vat", text):
            return '{"label": "billing"}'
        return '{"label": "other"}'


def estimate_tokens(texts: list[str]) -> int:
    return sum(len(PROMPT_TEMPLATE.format(text=t).split()) + 8 for t in texts)


def parse_label(raw: str) -> str | None:
    """Pull {"label": ...} out of the model's answer; None if it is not valid."""
    m = re.search(r"\{.*\}", raw, flags=re.S)
    if not m:
        return None
    try:
        label = json.loads(m.group(0)).get("label")
    except json.JSONDecodeError:
        return None
    return label if label in ALLOWED_LABELS else None


def live_tickets(con: duckdb.DuckDBPyConnection) -> list[tuple[str, str]]:
    return con.execute("""
        SELECT ticket_id, subject || '. ' || body AS text
        FROM silver_tickets
        WHERE NOT is_deleted
        ORDER BY ticket_id
    """).fetchall()


def cache_key(prompt: str) -> str:
    """hash(input) + model + prompt version: change any of the three -> a new key."""
    return hashlib.sha256(f"{MODEL}|{PROMPT_VERSION}|{prompt}".encode("utf-8")).hexdigest()


def label_tickets(con: duckdb.DuckDBPyConnection, llm: FakeLLM) -> dict:
    """Cached + validated LLM step.

    1. look every ticket up in llm_label_cache by cache_key(prompt)
    2. estimate the cost of the misses BEFORE calling the model
    3. call the model only for misses; cache the raw answer (valid or not),
       so a re-run with the same model + prompt makes 0 calls
    4. validate: allowed label -> gold_ticket_labels, anything else -> llm_label_quarantine
    """
    con.execute("""CREATE TABLE IF NOT EXISTS llm_label_cache (
        cache_key VARCHAR, model VARCHAR, prompt_version VARCHAR, raw_answer VARCHAR)""")
    todo = []
    for ticket_id, text in live_tickets(con):
        prompt = PROMPT_TEMPLATE.format(text=text)
        todo.append((ticket_id, text, prompt, cache_key(prompt)))
    cached = dict(con.execute(
        "SELECT cache_key, raw_answer FROM llm_label_cache WHERE model = ? AND prompt_version = ?",
        [MODEL, PROMPT_VERSION]).fetchall())
    misses = [t for t in todo if t[3] not in cached]
    est_tokens = estimate_tokens([text for _, text, _, _ in misses])

    calls_before = llm.calls
    for _, _, prompt, key in misses:
        if key in cached:                  # two tickets with the same text: one call
            continue
        raw = llm.complete(prompt)
        con.execute("INSERT INTO llm_label_cache VALUES (?, ?, ?, ?)",
                    [key, MODEL, PROMPT_VERSION, raw])
        cached[key] = raw

    good, bad = [], []
    for ticket_id, _, _, key in todo:
        raw = cached[key]
        label = parse_label(raw)
        if label is None:
            bad.append((ticket_id, raw, MODEL, PROMPT_VERSION))
        else:
            good.append((ticket_id, label, MODEL, PROMPT_VERSION))
    con.execute("""CREATE OR REPLACE TABLE gold_ticket_labels (
        ticket_id VARCHAR, label VARCHAR, model VARCHAR, prompt_version VARCHAR)""")
    con.execute("""CREATE OR REPLACE TABLE llm_label_quarantine (
        ticket_id VARCHAR, raw_answer VARCHAR, model VARCHAR, prompt_version VARCHAR)""")
    if good:
        con.executemany("INSERT INTO gold_ticket_labels VALUES (?, ?, ?, ?)", good)
    if bad:
        con.executemany("INSERT INTO llm_label_quarantine VALUES (?, ?, ?, ?)", bad)
    return {"labeled": len(good), "quarantined": len(bad), "calls": llm.calls - calls_before,
            "estimated_tokens": est_tokens,
            "estimated_cost_usd": est_tokens / 1000 * PRICE_PER_1K_TOKENS_USD}
