"""
High-throughput Kafka producer.

Sends analytics events to the configured Kafka topic at a target rate of
25 events/second (≈ 2.16M events/day), with snappy compression and
asynchronous delivery for minimal overhead.

Usage:
    python -m producer.kafka_producer --topic events --rate 25
"""

import argparse
import json
import logging
import signal
import sys
import time
from typing import Optional

from confluent_kafka import Producer, KafkaError
from confluent_kafka.admin import AdminClient, NewTopic

from config.settings import settings
from producer.event_generator import event_stream

logging.basicConfig(
    level=settings.log_level,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
logger = logging.getLogger(__name__)

# ─── Metrics ────────────────────────────────────────────────────────────────

_total_sent = 0
_total_errors = 0
_start_time: float = 0.0

# ─── Producer helpers ────────────────────────────────────────────────────────


def _delivery_report(err: Optional[KafkaError], msg) -> None:
    """Callback fired by librdkafka for each delivered (or failed) message."""
    global _total_sent, _total_errors
    if err:
        _total_errors += 1
        logger.warning("Delivery failed for key=%s: %s", msg.key(), err)
    else:
        _total_sent += 1


def _build_producer_config(kafka_cfg=None) -> dict:
    cfg = kafka_cfg or settings.kafka
    return {
        "bootstrap.servers": cfg.bootstrap_servers,
        "batch.size": cfg.batch_size,
        "linger.ms": cfg.linger_ms,
        "compression.type": cfg.compression_type,
        "acks": cfg.acks,
        "retries": cfg.retries,
        "max.in.flight.requests.per.connection": cfg.max_in_flight,
        # Idempotent delivery
        "enable.idempotence": True,
        "statistics.interval.ms": 5000,
    }


def ensure_topic_exists(topic: str, num_partitions: int = 6, replication_factor: int = 1) -> None:
    """Create the Kafka topic if it does not already exist."""
    admin = AdminClient({"bootstrap.servers": settings.kafka.bootstrap_servers})
    existing = admin.list_topics(timeout=10).topics
    if topic not in existing:
        new_topic = NewTopic(topic, num_partitions=num_partitions, replication_factor=replication_factor)
        futures = admin.create_topics([new_topic])
        for t, future in futures.items():
            try:
                future.result()
                logger.info("Created Kafka topic: %s", t)
            except Exception as exc:
                logger.warning("Topic creation warning for %s: %s", t, exc)


# ─── Main producer loop ──────────────────────────────────────────────────────


def run_producer(
    topic: str,
    rate_per_second: int = 25,
    anomaly_rate: float = 0.005,
    duration_seconds: Optional[int] = None,
) -> None:
    """
    Produce events to Kafka at the specified rate.

    Args:
        topic: Kafka topic name.
        rate_per_second: Target throughput. 25 ≈ 2.16M events/day.
        anomaly_rate: Fraction of events that are synthetic anomalies.
        duration_seconds: Run time cap. None = run until interrupted.
    """
    global _start_time
    _start_time = time.monotonic()

    ensure_topic_exists(topic)
    producer = Producer(_build_producer_config())

    logger.info(
        "Starting producer | topic=%s | rate=%d/s | anomaly_rate=%.1f%%",
        topic,
        rate_per_second,
        anomaly_rate * 100,
    )

    # Graceful shutdown on SIGINT / SIGTERM
    running = True

    def _shutdown(signum, frame):
        nonlocal running
        logger.info("Shutdown signal received — flushing remaining messages …")
        running = False

    signal.signal(signal.SIGINT, _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    last_log_time = time.monotonic()
    last_log_count = 0

    for event in event_stream(
        rate_per_second=rate_per_second,
        anomaly_rate=anomaly_rate,
        duration_seconds=duration_seconds,
    ):
        if not running:
            break

        payload = json.dumps(event).encode("utf-8")
        partition_key = event["service"].encode("utf-8")

        # Non-blocking produce; callback fires on poll()
        producer.produce(
            topic=topic,
            key=partition_key,
            value=payload,
            on_delivery=_delivery_report,
        )
        # Poll to trigger delivery callbacks without blocking the send loop
        producer.poll(0)

        # Log throughput every 10 seconds
        now = time.monotonic()
        if now - last_log_time >= 10:
            elapsed = now - last_log_time
            rate = (_total_sent - last_log_count) / elapsed
            logger.info(
                "Throughput: %.1f events/s | total_sent=%d | errors=%d",
                rate,
                _total_sent,
                _total_errors,
            )
            last_log_time = now
            last_log_count = _total_sent

    # Flush remaining messages (wait up to 30 s)
    logger.info("Flushing producer buffer …")
    remaining = producer.flush(timeout=30)
    if remaining:
        logger.warning("%d messages still in queue after flush timeout", remaining)

    elapsed_total = time.monotonic() - _start_time
    avg_rate = _total_sent / elapsed_total if elapsed_total > 0 else 0
    logger.info(
        "Producer finished | total_sent=%d | errors=%d | avg_rate=%.1f/s | elapsed=%.1fs",
        _total_sent,
        _total_errors,
        avg_rate,
        elapsed_total,
    )


# ─── CLI entrypoint ──────────────────────────────────────────────────────────


def _parse_args():
    parser = argparse.ArgumentParser(description="Kafka analytics event producer")
    parser.add_argument("--topic", default=settings.kafka.topic, help="Target Kafka topic")
    parser.add_argument("--rate", type=int, default=settings.events_per_second, help="Events per second")
    parser.add_argument("--anomaly-rate", type=float, default=0.005, help="Fraction of anomalous events")
    parser.add_argument("--duration", type=int, default=None, help="Run for N seconds then exit")
    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    run_producer(
        topic=args.topic,
        rate_per_second=args.rate,
        anomaly_rate=args.anomaly_rate,
        duration_seconds=args.duration,
    )
