"""
Prometheus metrics + CloudWatch integration.

Exposes a /metrics HTTP endpoint (port 8000 by default) for Prometheus scraping
and mirrors critical metrics to CloudWatch for AWS-native alerting.
"""

import logging
import time
from typing import Optional

import boto3
from botocore.exceptions import ClientError
from prometheus_client import Counter, Gauge, Histogram, start_http_server

from config.settings import settings

logger = logging.getLogger(__name__)

# ─── Prometheus metrics ───────────────────────────────────────────────────

EVENTS_PROCESSED = Counter(
    "streaming_events_processed_total",
    "Total number of events processed by the pipeline",
    ["service", "status"],
)

ANOMALIES_DETECTED = Counter(
    "streaming_anomalies_detected_total",
    "Total anomalies detected by Spark Streaming",
    ["service", "anomaly_type"],
)

LLM_SUMMARIES_GENERATED = Counter(
    "streaming_llm_summaries_total",
    "Total LLM-generated anomaly summary reports",
    ["model"],
)

LATENCY_HISTOGRAM = Histogram(
    "streaming_latency_ms",
    "Event latency in milliseconds",
    ["service"],
    buckets=[1, 5, 10, 50, 100, 250, 500, 1000, 2500, 5000],
)

KAFKA_CONSUMER_LAG = Gauge(
    "streaming_kafka_consumer_lag",
    "Kafka consumer lag (messages behind latest offset)",
    ["topic", "partition"],
)

KINESIS_SHARD_LAG = Gauge(
    "streaming_kinesis_shard_lag_seconds",
    "Kinesis shard lag in seconds behind the latest record",
    ["stream", "shard_id"],
)

ES_INDEX_LATENCY = Histogram(
    "streaming_es_index_latency_ms",
    "Elasticsearch indexing latency in milliseconds",
    ["index_pattern"],
    buckets=[1, 5, 10, 50, 100, 500, 1000],
)

# ─── CloudWatch bridge ───────────────────────────────────────────────────────


def push_to_cloudwatch(
    namespace: str,
    metric_name: str,
    value: float,
    unit: str = "Count",
    dimensions: dict = None,
) -> None:
    """Push a single metric datapoint to AWS CloudWatch."""
    if not settings.aws.enable_cloudwatch:
        return
    dimensions = dimensions or {}
    cw = boto3.client("cloudwatch", region_name=settings.aws.region)
    try:
        cw.put_metric_data(
            Namespace=namespace,
            MetricData=[
                {
                    "MetricName": metric_name,
                    "Value": value,
                    "Unit": unit,
                    "Dimensions": [
                        {"Name": k, "Value": v} for k, v in dimensions.items()
                    ],
                }
            ],
        )
    except ClientError as exc:
        logger.warning("CloudWatch put_metric_data failed: %s", exc)


def record_event(service: str, status: str, latency_ms: float) -> None:
    """Record a single event into Prometheus and CloudWatch."""
    EVENTS_PROCESSED.labels(service=service, status=status).inc()
    LATENCY_HISTOGRAM.labels(service=service).observe(latency_ms)
    push_to_cloudwatch(
        namespace="StreamingAnalytics",
        metric_name="EventLatencyMs",
        value=latency_ms,
        unit="Milliseconds",
        dimensions={"service": service},
    )


def record_anomaly(service: str, anomaly_type: str) -> None:
    """Record an anomaly detection event."""
    ANOMALIES_DETECTED.labels(service=service, anomaly_type=anomaly_type).inc()
    push_to_cloudwatch(
        namespace="StreamingAnalytics",
        metric_name="AnomaliesDetected",
        value=1.0,
        dimensions={"service": service, "anomaly_type": anomaly_type},
    )


def record_llm_summary(model: str = "gpt-4o") -> None:
    """Record a successful LLM summary generation."""
    LLM_SUMMARIES_GENERATED.labels(model=model).inc()
    push_to_cloudwatch(
        namespace="StreamingAnalytics",
        metric_name="LLMSummariesGenerated",
        value=1.0,
        dimensions={"model": model},
    )


def update_kafka_lag(topic: str, partition: int, lag: int) -> None:
    """Update the Kafka consumer lag gauge."""
    KAFKA_CONSUMER_LAG.labels(topic=topic, partition=str(partition)).set(lag)


def update_kinesis_lag(stream: str, shard_id: str, lag_sec: float) -> None:
    """Update the Kinesis shard lag gauge."""
    KINESIS_SHARG_LAG.labels(stream=stream, shard_id=shard_id).set(lag_sec)


def record_es_index_latency(index_pattern: str, latency_ms: float) -> None:
    """Record an Elasticsearch indexing latency observation."""
    ES_INDEX_LATECYHISTOGRAM.labels(index_pattern=index_pattern).observe(latency_ms)


# ─── Startup ──────────────────────────────────────────────────────────────


def start_metrics_server(port: int = 8000) -> None:
    """Start the Prometheus HTTP scrape endpoint."""
    start_http_server(port)
    logger.info("Prometheus metrics server started on port %d", port)
