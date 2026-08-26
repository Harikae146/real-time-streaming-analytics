"""Unit tests for the event generator."""
import pytest
from producer.event_generator import generate_event, event_stream


def test_generate_event_structure():
    event = generate_event()
    required_keys = [
        "event_id", "timestamp", "epoch_ms", "service", "event_type",
        "region", "user_id", "session_id", "latency_ms", "status_code",
        "is_anomaly", "payload_bytes", "metadata",
    ]
    for key in required_keys:
        assert key in event, f"Missing key: {key}"


def test_generate_normal_event():
    event = generate_event(inject_anomaly=False)
    assert event["status_code"] == 200
    assert event["is_anomaly"] is False
    assert event["anomaly_type"] is None


def test_generate_latency_spike_anomaly():
    event = generate_event(inject_anomaly=True, anomaly_type="latency_spike")
    assert event["is_anomaly"] is True
    assert event["anomaly_type"] == "latency_spike"
    # Latency spike multiplies by 5-12x — should be well above 1ms
    assert event["latency_ms"] > 10


def test_generate_error_burst_anomaly():
    event = generate_event(inject_anomaly=True, anomaly_type="error_burst")
    assert event["is_anomaly"] is True
    assert event["status_code"] in (500, 502, 503, 504)
    assert event["event_type"] == "error"


def test_event_stream_count():
    events = list(event_stream(rate_per_second=100, duration_seconds=1))
    # Should produce approximately 100 events in 1 second (allow ±20%)
    assert 80 <= len(events) <= 120


def test_event_stream_anomaly_rate():
    events = list(event_stream(rate_per_second=1000, anomaly_rate=0.1, duration_seconds=2))
    anomaly_count = sum(1 for e in events if e["is_anomaly"])
    # Anomaly rate should be roughly 10% (allow wide tolerance for randomness)
    assert 0.04 < anomaly_count / len(events) < 0.20
