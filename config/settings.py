"""
Centralized configuration for the Real-Time Streaming Analytics pipeline.
All settings are loaded from environment variables with sensible defaults
for local development (docker-compose).
"""

import os
from dataclasses import dataclass, field
from typing import List


# ---------------------------------------------------------------------------
# Kafka
# ---------------------------------------------------------------------------

@dataclass
class KafkaConfig:
    bootstrap_servers: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    topic: str = os.getenv("KAFKA_TOPIC", "analytics-events")
    anomaly_topic: str = os.getenv("KAFKA_ANOMALY_TOPIC", "anomaly-alerts")
    group_id: str = os.getenv("KAFKA_CONSUMER_GROUP", "streaming-analytics-cg")
    # Producer settings
    batch_size: int = int(os.getenv("KAFKA_BATCH_SIZE", "65536"))        # 64 KB
    linger_ms: int = int(os.getenv("KAFKA_LINGER_MS", "5"))
    compression_type: str = os.getenv("KAFKA_COMPRESSION", "snappy")
    acks: str = os.getenv("KAFKA_ACKS", "1")
    retries: int = int(os.getenv("KAFKA_RETRIES", "5"))
    max_in_flight: int = int(os.getenv("KAFKA_MAX_IN_FLIGHT", "5"))


# ---------------------------------------------------------------------------
# AWS Kinesis
# ---------------------------------------------------------------------------

@dataclass
class KinesisConfig:
    stream_name: str = os.getenv("KINESIS_STREAM_NAME", "analytics-events")
    region: str = os.getenv("AWS_REGION", "us-east-1")
    shard_count: int = int(os.getenv("KINESIS_SHARD_COUNT", "6"))
    consumer_count: int = int(os.getenv("KINESIS_CONSUMER_COUNT", "4"))
    # Iterator type: LATEST | TRIM_HORIZON | AT_TIMESTAMP
    iterator_type: str = os.getenv("KINESIS_ITERATOR_TYPE", "LATEST")
    checkpoint_table: str = os.getenv("KINESIS_CHECKPOINT_TABLE", "streaming-checkpoints")
    # Auto-scaling thresholds
    scale_up_utilization: float = float(os.getenv("KINESIS_SCALE_UP_PCT", "0.70"))
    scale_down_utilization: float = float(os.getenv("KINESIS_SCALE_DOWN_PCT", "0.25"))


# ---------------------------------------------------------------------------
# Spark
# ---------------------------------------------------------------------------

@dataclass
class SparkConfig:
    app_name: str = "RealTimeStreamingAnalytics"
    master: str = os.getenv("SPARK_MASTER", "local[*]")
    # Tumbling window for throughput anomaly detection
    tumbling_window_duration: str = "1 minute"
    # Sliding window for latency spike detection
    sliding_window_duration: str = "5 minutes"
    sliding_window_slide: str = "30 seconds"
    # Watermark for late data tolerance
    watermark_delay: str = "10 seconds"
    # Trigger interval — target 500ms end-to-end latency
    trigger_interval: str = "1 second"
    checkpoint_location: str = os.getenv("SPARK_CHECKPOINT", "/tmp/spark-checkpoints")
    # Anomaly thresholds
    throughput_drop_threshold: float = float(os.getenv("ANOMALY_THROUGHPUT_DROP", "0.40"))
    latency_spike_multiplier: float = float(os.getenv("ANOMALY_LATENCY_MULT", "2.0"))
    session_anomaly_score_threshold: float = float(os.getenv("ANOMALY_SESSION_SCORE", "0.85"))


# ---------------------------------------------------------------------------
# Elasticsearch
# ---------------------------------------------------------------------------

@dataclass
class ElasticsearchConfig:
    hosts: List[str] = field(
        default_factory=lambda: os.getenv("ES_HOSTS", "http://localhost:9200").split(",")
    )
    events_index: str = os.getenv("ES_EVENTS_INDEX", "analytics-events")
    anomalies_index: str = os.getenv("ES_ANOMALIES_INDEX", "anomaly-reports")
    summaries_index: str = os.getenv("ES_SUMMARIES_INDEX", "llm-summaries")
    username: str = os.getenv("ES_USERNAME", "elastic")
    password: str = os.getenv("ES_PASSWORD", "changeme")
    # Refresh interval — 10-second data freshness target
    refresh_interval: str = os.getenv("ES_REFRESH_INTERVAL", "1s")
    bulk_size: int = int(os.getenv("ES_BULK_SIZE", "500"))


# ---------------------------------------------------------------------------
# OpenAI / LLM
# ---------------------------------------------------------------------------

@dataclass
class LLMConfig:
    api_key: str = os.getenv("OPENAI_API_KEY", "")
    model: str = os.getenv("LLM_MODEL", "gpt-4o")
    temperature: float = float(os.getenv("LLM_TEMPERATURE", "0.2"))
    max_tokens: int = int(os.getenv("LLM_MAX_TOKENS", "800"))
    # Batch anomalies before calling LLM (reduces API cost)
    batch_size: int = int(os.getenv("LLM_BATCH_SIZE", "10"))
    batch_timeout_seconds: int = int(os.getenv("LLM_BATCH_TIMEOUT", "30"))
    # Retry / rate-limit settings
    max_retries: int = int(os.getenv("LLM_MAX_RETRIES", "3"))
    retry_base_delay: float = float(os.getenv("LLM_RETRY_DELAY", "1.0"))


# ---------------------------------------------------------------------------
# Monitoring
# ---------------------------------------------------------------------------

@dataclass
class MonitoringConfig:
    prometheus_port: int = int(os.getenv("PROMETHEUS_PORT", "8000"))
    cloudwatch_namespace: str = os.getenv("CW_NAMESPACE", "StreamingAnalytics")
    pagerduty_routing_key: str = os.getenv("PAGERDUTY_ROUTING_KEY", "")
    # Alert thresholds
    consumer_lag_alert_threshold: int = int(os.getenv("ALERT_LAG_THRESHOLD", "50000"))
    anomaly_rate_alert_pct: float = float(os.getenv("ALERT_ANOMALY_RATE", "0.05"))  # 5%


# ---------------------------------------------------------------------------
# Aggregate settings object
# ---------------------------------------------------------------------------

@dataclass
class Settings:
    kafka: KafkaConfig = field(default_factory=KafkaConfig)
    kinesis: KinesisConfig = field(default_factory=KinesisConfig)
    spark: SparkConfig = field(default_factory=SparkConfig)
    elasticsearch: ElasticsearchConfig = field(default_factory=ElasticsearchConfig)
    llm: LLMConfig = field(default_factory=LLMConfig)
    monitoring: MonitoringConfig = field(default_factory=MonitoringConfig)
    # Event producer target rate
    events_per_second: int = int(os.getenv("PRODUCER_RATE", "25"))  # ≈ 2.16M/day
    log_level: str = os.getenv("LOG_LEVEL", "INFO")


settings = Settings()
