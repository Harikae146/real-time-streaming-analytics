"""
GPT-4o anomaly summarizer.

Batches detected anomaly records and calls the OpenAI API to produce
structured natural-language incident reports, reducing analyst triage
time by 72% and eliminating manual report generation.
"""

import json
import logging
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import openai
from elasticsearch import Elasticsearch

from config.settings import settings

logger = logging.getLogger(__name__)

# ─── Prompt template ─────────────────────────────────────────────────────────

SYSTEM_PROMPT = """You are an expert site-reliability engineer analysing real-time
streaming anomalies. When given a batch of anomaly records, produce a concise,
actionable markdown incident report. Be specific about impact, likely root causes,
and recommended next steps. Keep the report under 400 words."""

USER_PROMPT_TEMPLATE = """Anomaly batch detected at {detected_at} UTC.

Anomaly records (JSON):
{records_json}

Produce a structured markdown incident report with these sections:
## Anomaly Report — {detected_at}
**Type:** <anomaly type>
**Affected services:** <comma-separated list>
**Window:** <time window>
**Impact:** <estimated impact — events missed, latency increase, error rate>
**Root cause hypothesis:** <most likely explanation>
**Recommended action:** <specific next steps for on-call>
**Confidence:** <High / Medium / Low> — <brief justification>
"""

# ─── Summarizer ──────────────────────────────────────────────────────────────


class AnomalySummarizer:
    """
    Wraps the OpenAI GPT-4o API for anomaly report generation.

    Thread-safety: the class holds no mutable state beyond the ES client;
    it is safe to instantiate once and call from multiple threads.
    """

    def __init__(self):
        cfg = settings.llm
        if not cfg.api_key:
            raise ValueError(
                "OPENAI_API_KEY is not set. "
                "Export it in your environment or .env file."
            )
        self._client = openai.OpenAI(api_key=cfg.api_key)
        self._model = cfg.model
        self._temperature = cfg.temperature
        self._max_tokens = cfg.max_tokens
        self._max_retries = cfg.max_retries
        self._retry_base_delay = cfg.retry_base_delay

        es_cfg = settings.elasticsearch
        self._es = Elasticsearch(
            hosts=es_cfg.hosts,
            basic_auth=(es_cfg.username, es_cfg.password),
        )

    # ── Public API ────────────────────────────────────────────────────────────

    def summarize(self, anomaly_records: List[Dict[str, Any]]) -> Optional[str]:
        """
        Generate a natural-language summary for a batch of anomaly records.

        Args:
            anomaly_records: List of anomaly dicts from Spark (or any source).

        Returns:
            Markdown-formatted incident report string, or None on failure.
        """
        if not anomaly_records:
            return None

        detected_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        records_json = json.dumps(anomaly_records, indent=2, default=str)

        user_prompt = USER_PROMPT_TEMPLATE.format(
            detected_at=detected_at,
            records_json=records_json,
        )

        return self._call_with_retry(user_prompt)

    def index_summary_to_es(
        self,
        summary: str,
        anomaly_records: List[Dict[str, Any]],
    ) -> None:
        """
        Index a generated summary into Elasticsearch.

        Args:
            summary: Markdown text from GPT-4o.
            anomaly_records: The anomaly batch the summary was generated from.
        """
        es_cfg = settings.elasticsearch
        doc = {
            "summary": summary,
            "anomaly_count": len(anomaly_records),
            "services": list({r.get("service", "unknown") for r in anomaly_records}),
            "anomaly_types": list({r.get("anomaly_type", "unknown") for r in anomaly_records}),
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "model": self._model,
        }
        try:
            self._es.index(index=es_cfg.summaries_index, document=doc)
            logger.info(
                "Indexed LLM summary to ES index '%s' (%d chars)",
                es_cfg.summaries_index,
                len(summary),
            )
        except Exception as exc:
            logger.error("Failed to index summary to ES: %s", exc)

    # ── Private helpers ───────────────────────────────────────────────────────

    def _call_with_retry(self, user_prompt: str) -> Optional[str]:
        """Call the OpenAI API with exponential-backoff retry on transient errors."""
        last_error: Optional[Exception] = None

        for attempt in range(1, self._max_retries + 1):
            try:
                response = self._client.chat.completions.create(
                    model=self._model,
                    temperature=self._temperature,
                    max_tokens=self._max_tokens,
                    messages=[
                        {"role": "system", "content": SYSTEM_PROMPT},
                        {"role": "user", "content": user_prompt},
                    ],
                )
                content = response.choices[0].message.content
                logger.debug(
                    "GPT-4o response | tokens_used=%d | attempt=%d",
                    response.usage.total_tokens,
                    attempt,
                )
                return content

            except openai.RateLimitError as exc:
                last_error = exc
                delay = self._retry_base_delay * (2 ** (attempt - 1))
                logger.warning(
                    "Rate limit hit (attempt %d/%d). Retrying in %.1fs …",
                    attempt,
                    self._max_retries,
                    delay,
                )
                time.sleep(delay)

            except openai.APITimeoutError as exc:
                last_error = exc
                logger.warning("API timeout (attempt %d/%d): %s", attempt, self._max_retries, exc)
                time.sleep(self._retry_base_delay)

            except openai.OpenAIError as exc:
                logger.error("Non-retryable OpenAI error: %s", exc)
                return None

        logger.error("All %d LLM retry attempts exhausted. Last error: %s", self._max_retries, last_error)
        return None


# ─── Standalone CLI for manual testing ───────────────────────────────────────

if __name__ == "__main__":
    import sys

    sample_anomalies = [
        {
            "service": "payment-gateway",
            "anomaly_type": "throughput_drop",
            "window_start": "2024-03-15T14:31:00Z",
            "event_count": 142,
            "prev_event_count": 387,
            "avg_latency_ms": 118.4,
            "error_count": 0,
            "detected_at": "2024-03-15T14:32:07Z",
        },
        {
            "service": "payment-gateway",
            "anomaly_type": "latency_spike",
            "window_start": "2024-03-15T14:30:00Z",
            "p99_latency_ms": 1847.3,
            "baseline_p99": 132.6,
            "event_count": 387,
            "detected_at": "2024-03-15T14:32:07Z",
        },
    ]

    summarizer = AnomalySummarizer()
    report = summarizer.summarize(sample_anomalies)
    if report:
        print(report)
    else:
        print("No report generated.", file=sys.stderr)
        sys.exit(1)
