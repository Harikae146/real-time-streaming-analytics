"""
Synthetic event data factory.

Generates realistic analytics events with controlled anomaly injection
for testing the streaming pipeline end-to-end.
"""

import random
import time
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, Optional


# Services that generate events
SERVICES = [
    "payment-gateway",
    "user-auth",
    "product-catalog",
    "order-management",
    "inventory-sync",
    "recommendation-engine",
    "search-service",
    "notification-hub",
]

EVENT_TYPES = ["page_view", "api_call", "checkout", "search", "login", "error", "purchase"]

REGIONS = ["us-east-1", "us-west-2", "eu-west-1", "ap-southeast-1"]


def _normal_latency(service: str) -> float:
    """Return normally-distributed latency (ms) representative of each service."""
    baselines = {
        "payment-gateway": (120, 30),
        "user-auth": (40, 10),
        "product-catalog": (25, 8),
        "order-management": (90, 20),
        "inventory-sync": (60, 15),
        "recommendation-engine": (200, 50),
        "search-service": (45, 12),
        "notification-hub": (30, 8),
    }
    mu, sigma = baselines.get(service, (50, 15))
    return max(1.0, random.gauss(mu, sigma))


def generate_event(
    inject_anomaly: bool = False,
    anomaly_type: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Generate a single analytics event dict.

    Args:
        inject_anomaly: If True, artificially spike latency or inject an error.
        anomaly_type: One of 'latency_spike' | 'error_burst' | 'throughput_drop'.
                      When None and inject_anomaly is True, type is chosen at random.

    Returns:
        Event dict ready to be JSON-serialised and sent to Kafka / Kinesis.
    """
    service = random.choice(SERVICES)
    event_type = random.choice(EVENT_TYPES)
    region = random.choice(REGIONS)
    latency_ms = _normal_latency(service)
    status_code = 200

    if inject_anomaly:
        if anomaly_type is None:
            anomaly_type = random.choice(["latency_spike", "error_burst"])

        if anomaly_type == "latency_spike":
            latency_ms *= random.uniform(5.0, 12.0)  # 5-12× normal
        elif anomaly_type == "error_burst":
            status_code = random.choice([500, 502, 503, 504])
            event_type = "error"

    return {
        "event_id": str(uuid.uuid4()),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "epoch_ms": int(time.time() * 1000),
        "service": service,
        "event_type": event_type,
        "region": region,
        "user_id": f"user_{random.randint(1, 500_000)}",
        "session_id": f"sess_{random.randint(1, 100_000)}",
        "latency_ms": round(latency_ms, 2),
        "status_code": status_code,
        "is_anomaly": inject_anomaly,
        "anomaly_type": anomaly_type if inject_anomaly else None,
        "payload_bytes": random.randint(200, 8192),
        "metadata": {
            "client": random.choice(["web", "mobile-ios", "mobile-android", "api"]),
            "version": f"v{random.randint(1, 5)}.{random.randint(0, 9)}.{random.randint(0, 9)}",
        },
    }


def event_stream(
    rate_per_second: int = 25,
    anomaly_rate: float = 0.005,
    duration_seconds: Optional[int] = None,
):
    """
    Infinite (or time-bounded) generator of analytics events.

    Args:
        rate_per_second: Target events/second (~2.16M/day at default 25).
        anomaly_rate: Fraction of events that are injected anomalies.
        duration_seconds: Stop after this many seconds. None = run forever.

    Yields:
        Event dicts.
    """
    interval = 1.0 / rate_per_second
    start = time.monotonic()
    count = 0

    while True:
        inject = random.random() < anomaly_rate
        yield generate_event(inject_anomaly=inject)
        count += 1

        if duration_seconds and (time.monotonic() - start) >= duration_seconds:
            break

        # Pace output to the target rate
        target_time = start + count * interval
        sleep_for = target_time - time.monotonic()
        if sleep_for > 0:
            time.sleep(sleep_for)
