#!/usr/bin/env python3
import os
import psycopg2
import json
import time
import logging
from typing import List, Dict, Any
from concurrent.futures import ThreadPoolExecutor, as_completed
from openai import OpenAI

logger = logging.getLogger(__name__)
model_name = "gpt-4.1-mini"

summarization_instructions = """
You are an expert analyst with a warm tone. You specialize in extracting high-signal insights from recent forum-style content, where ideas can be messy, fast-moving, and conversational. Your goal is to distill the essential themes into a summary.

Given the set of messages, produce a JSON object containing:

1. "topic"
   → A concise thematic synthesis (maximum 3 sentences) written in a professional but easily digestible and simple form.
   → The summary must begin with a short introductory phrase. Here are some examples for inspiration but ***ENSURE YOU NEVER USE THEM VERBATIM***:
        - "Recent discussions centered on..."
        - "The community is currently exploring..."
        - "A key theme emerging lately involves..."
        - "Much of the recent dialogue highlights..."
        - "There is growing interest regarding..."
   → Focus entirely on the ideas:
        - key motivations, questions, or pain points
        - notable nuances, tradeoffs, or emerging themes
        - relationships or tensions between subtopics
   → Keep it approachable but still thoughtful and insight-rich.

2. "few_words"
   → 3–7 punchy keywords or short phrases that capture the core ideas.
   → Avoid generic filler unless essential.

3. "one_sentence"
   → One friendly, clear sentence that expresses the core insight.

Requirements:
- Maintain a warm, casual, friendly tone while keeping the ideas sharp and useful.
- Capture not just what is being explored, but why it matters and what tensions or possibilities exist.
- Include references to people, projects, or mechanisms only when relevant to the ideas themselves.
- Write cleanly and simply; aim for clarity and usefulness over formality.

Return only the JSON object, nothing else.
"""

def fetch_all_channel_ids(db_url: str) -> List[int]:
    conn = psycopg2.connect(db_url)
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT DISTINCT channel_id FROM socialrank.messages;")
            return [r[0] for r in cur.fetchall()]
    finally:
        conn.close()

def get_top_messages(db_url: str, channel_id: int, limit: int) -> List[tuple]:
    conn = psycopg2.connect(db_url)
    try:
        with conn.cursor() as cur:
            query = """
                WITH channel_ctx AS (
                    SELECT c.id AS channel_id, c.server_id
                    FROM socialrank.channels c
                    WHERE c.id = %s
                ),
                latest_messages AS (
                    SELECT
                        m.id,
                        m.channel_id,
                        m.timestamp,
                        m.author_id,
                        m.content
                    FROM socialrank.messages m
                    WHERE m.channel_id = %s
                      AND m.content IS NOT NULL
                      AND m.message_type = 'MessageType.default'
                    ORDER BY m.timestamp DESC, m.id DESC
                    LIMIT 1000
                ),
                reaction_interactions AS (
                    SELECT
                        lm.channel_id,
                        lm.id AS message_id,
                        ru.user_id
                    FROM latest_messages lm
                    JOIN socialrank.reaction_users ru
                      ON ru.message_id = lm.id
                ),
                latest_scores AS (
                    SELECT s.server_id, s.user_id, s.value
                    FROM socialrank.scores s
                    JOIN channel_ctx cc ON cc.server_id = s.server_id
                ),
                interaction_scores AS (
                    SELECT
                        ri.message_id,
                        ls.user_id,
                        ls.value AS user_score
                    FROM reaction_interactions ri
                    JOIN latest_scores ls
                      ON ls.user_id = ri.user_id
                ),
                weighted AS (
                    SELECT
                        message_id,
                        SUM(user_score * 20) AS score
                    FROM interaction_scores
                    GROUP BY message_id
                )
                SELECT
                    lm.id,
                    lm.channel_id,
                    lm.timestamp,
                    lm.author_id,
                    lm.content,
                    COALESCE(w.score, 0) AS score
                FROM latest_messages lm
                LEFT JOIN weighted w ON w.message_id = lm.id
                ORDER BY score DESC
                LIMIT %s;
            """
            cur.execute(query, (channel_id, channel_id, limit))
            return cur.fetchall()
    finally:
        conn.close()

def summarize_with_openai(messages: List[str], client: OpenAI,
                          max_retries: int = 3, base_delay: float = 1.0) -> Dict[str, Any]:
    valid = [m for m in messages if m and len(m.strip()) > 5]
    if not valid:
        return None

    prompt = "Conversation:\n" + json.dumps(valid, ensure_ascii=False)
    last_error = None

    for attempt in range(max_retries):
        try:
            resp = client.responses.create(
                model=model_name,
                input=prompt,
                temperature=0.6,
                instructions=summarization_instructions,
                text={
                    "format": {
                        "type": "json_schema",
                        "name": "channel_summary",
                        "schema": {
                            "type": "object",
                            "properties": {
                                "topic": {"type": "string"},
                                "few_words": {"type": "string"},
                                "one_sentence": {"type": "string"}
                            },
                            "required": ["topic", "few_words", "one_sentence"],
                            "additionalProperties": False
                        },
                        "strict": True
                    }
                },
            )
            return json.loads(resp.output_text.strip())
        except Exception as e:
            last_error = e
            if attempt < max_retries - 1:
                time.sleep(base_delay * (2**attempt))

    return {
        "topic": None,
        "few_words": None,
        "one_sentence": None,
        "error": f"Failed after {max_retries} attempts: {last_error}"
    }

def process_channel(db_url: str, channel_id: int, limit: int,
                    client: OpenAI, max_retries: int = 2) -> Dict[str, Any]:
    last_error = None
    for attempt in range(max_retries):
        try:
            rows = get_top_messages(db_url, channel_id, limit)
            msgs = [r[4] for r in rows if r[4]]
            summary = summarize_with_openai(msgs, client)
            return {"channel": channel_id, "summary": summary}
        except Exception as e:
            last_error = e
            if attempt < max_retries - 1:
                time.sleep(1 * (2**attempt))
    return {"channel": channel_id, "error": str(last_error)}

def save_summaries(db_url: str, results: List[Dict[str, Any]],
                   limit: int, model: str):
    conn = psycopg2.connect(db_url)
    try:
        with conn:
            with conn.cursor() as cur:
                for item in results:
                    s = item.get("summary")
                    if not s:
                        continue
                    cur.execute(
                        """
                        INSERT INTO socialrank.channel_summaries (
                            channel_id,
                            summary,
                            topic,
                            few_words,
                            one_sentence,
                            error,
                            model
                        )
                        VALUES (%s, %s::jsonb, %s, %s, %s, %s, %s)
                        ON CONFLICT (channel_id) DO UPDATE SET
                            summary      = EXCLUDED.summary,
                            topic        = EXCLUDED.topic,
                            few_words    = EXCLUDED.few_words,
                            one_sentence = EXCLUDED.one_sentence,
                            error        = EXCLUDED.error,
                            model        = EXCLUDED.model,
                            created_at   = NOW();
                        """,
                        (
                            item["channel"],
                            json.dumps(s, ensure_ascii=False),
                            s.get("topic"),
                            s.get("few_words"),
                            s.get("one_sentence"),
                            s.get("error"),
                            model,
                        ),
                    )
    finally:
        conn.close()

def process_channels_concurrently(
    db_url: str,
    channel_ids: List[int],
    limit: int,
    client: OpenAI,
    max_workers: int = 10
) -> List[Dict[str, Any]]:
    results = []
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futs = {ex.submit(process_channel, db_url, cid, limit, client): cid
                for cid in channel_ids}
        for f in as_completed(futs):
            try:
                results.append(f.result())
            except Exception as e:
                results.append({"channel": futs[f], "error": str(e)})

    save_summaries(db_url, results, limit, model_name)
    return results

def main():
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO"),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
    )

    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        raise RuntimeError("DATABASE_URL environment variable is required")

    limit = 50
    max_workers = 5
    client = OpenAI()
    channel_ids = fetch_all_channel_ids(db_url)

    logger.info(f"Processing summaries for channels: {channel_ids}")

    if channel_ids:
        process_channels_concurrently(db_url, channel_ids, limit, client, max_workers)

if __name__ == "__main__":
    main()
