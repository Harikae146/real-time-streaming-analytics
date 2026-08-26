"""
Spark Structured Streaming consumer with windowed anomaly detection.

Reads events from Kafka (or AWS Kinesis via the Spark-Kinesis connector),
applies three complementary windowing strategies to detect anomalies within
500ms of occurrence, and writes results to Elasticsearch for Kibana dashboards.

Usage:
    python -m consumer.spark_streaming --source kafka --topic analytics-events
"""

import argparse
import json
import logging
from datetime import datetime, timezone

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
    BooleanType,
)

from config.settings import settings
from llm.anomaly_summarizer import AnomalySummarizer

logging.basicConfig(
    level=settings.log_level,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
logger = logging.getLogger(__name__)

# ─── Schema ──────────────────────────────────────────────────────────────────

EVENT_SCHEMA = StructType([
    StructField("event_id", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("epoch_ms", IntegerType(), True),
    StructField("service", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("region", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("session_id", StringType(), True),
    StructField("latency_ms", DoubleType(), True),
    StructField("status_code", IntegerType(), True),
    StructField("is_anomaly", BooleanType(), True),
    StructField("anomaly_type", StringType(), True),
    StructField("payload_bytes", IntegerType(), True),
])

# ─── Spark session builder ────────────────────────────────────────────────────


def build_spark_session(source: str) -> SparkSession:
    cfg = settings.spark
    builder = (
        SparkSession.builder
        .appName(cfg.app_name)
        .master(cfg.master)
        .config("spark.sql.streaming.checkpointLocation", cfg.checkpoint_location)
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.streaming.stopGracefullyOnShutdown", "true")
        # Enable Adaptive Query Execution
        .config("spark.sql.adaptive.enabled", "true")
    )

    if source == "kafka":
        builder = builder.config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
            "org.elasticsearch:elasticsearch-spark-streaming-30_2.12:8.11.0",
        )
    elif source == "kinesis":
        builder = builder.config(
            "spark.jars.packages",
            "org.apache.spark:spark-streaming-kinesis-asl_2.12:3.5.0,"
            "org.elasticsearch:elasticsearch-spark-30_2.12:8.11.0",
        )

    return builder.getOrCreate()


# ─── Source readers ───────────────────────────────────────────────────────────


def read_kafka(spark: SparkSession, topic: str) -> DataFrame:
    """Return a streaming DataFrame backed by the Kafka source."""
    raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", settings.kafka.bootstrap_servers)
        .option("subscribe", topic)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .option("maxOffsetsPerTrigger", 50_000)   # back-pressure ceiling
        .load()
    )
    return _parse_events(raw)


def read_kinesis(spark: SparkSession, stream_name: str) -> DataFrame:
    """Return a streaming DataFrame backed by the Kinesis source."""
    raw = (
        spark.readStream
        .format("kinesis")
        .option("streamName", stream_name)
        .option("regionName", settings.kinesis.region)
        .option("initialPosition", settings.kinesis.iterator_type)
        .option("kinesis.client.describeShardInterval", "1min")
        .load()
    )
    return _parse_events(raw, data_col="data")


def _parse_events(raw: DataFrame, data_col: str = "value") -> DataFrame:
    """Deserialise JSON payload and cast to the event schema."""
    return (
        raw
        .select(F.from_json(F.col(data_col).cast("string"), EVENT_SCHEMA"�ae�as �)"�      elect("e.*")
        .withColumn("event_ts", F.to_timestamp("timestamp"))
        .withWatermark("event_ts", settings.spark.watermark_delay)
    )


# ─── Anomaly detection windows ─────────────────────────────────────────────────


def detect_throughput_anomalies(events: DataFrame) -> DataFrame:
    """
    Tumbling 1-minute window: flag services whose event rate dropped >40%
    compared to the previous window.
    """
    window_counts = (
        events
        .groupBy(
            F.window("event_ts", settings.spark.tumbling_window_duration),
            "service",
        )
        .agg(
            F.count("*").alias("event_count"),
            F.sum(F.when(F.col("status_code") >= 500, 1).otherwise(0)).alias("error_count"),
            F.avg("latency_ms").alias("avg_latency_ms"),
        )
        .select(
            "service",
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "event_count",
            "error_count",
            "avg_latency_ms",
        )
    )

    # Lag over the previous window per service using a self-join on offset windows
    prev = window_counts.alias("prev")
    curr = window_counts.alias("curr")

    joined = curr.join(
        prev,
        (F.col("curr.service") == F.col("prev.service"))
        & (F.col("curr.window_start") == F.col("prev.window_end")),
        "left",
    ).select(
        F.col("curr.service").alias("service"),
        F.col("curr.window_start").alias("window_start"),
        F.col("curr.event_count").alias("event_count"),
        F.col("prev.event_count").alias("prev_event_count"),
        F.col("curr.avg_latency_ms").alias("avg_latency_ms"),
        F.col("curr.error_count").alias("error_count"),
    )

    threshold = settings.spark.throughput_drop_threshold
    return joined.filter(
        (F.col("prev_event_count").isNotNull())
        & (
            (F.col("event_count") / F.col("prev_event_count")) < (1.0 - threshold)
        )
    ).withColumn("anomaly_type", F.lit("throughput_drop")).withColumn(
        "detected_at", F.current_timestamp()
    )


def detect_latency_anomalies(events: DataFrame) -> DataFrame:
    """
    Sliding 5-minute / 30-second window: flag services whose p99 latency
    exceeded 2× the rolling baseline.
    """
    cfg = settings.spark
    windowed = (
        events
        .groupBy(
            F.window("event_ts", cfg.sliding_window_duration, cfg.sliding_window_slide),
            "service",
        )
        .agg(
            F.percentile_approx("latency_ms", 0.99).alias("p99_latency_ms"),
            F.avg("latency_ms").alias("avg_latency_ms"),
            F.count("*").alias("event_count"),
        )
        .select(
            "service",
            F.col("window.start").alias("window_start"),
            "p99_latency_ms",
            "avg_latency_ms",
            "event_count",
        )
    )

    # Compute rolling mean p99 as baseline (average over recent windows)
    baseline = (
        windowed.groupBy("service")
        .agg(F.avg("p99_latency_ms").alias("baseline_p99"))
    )

    multiplier = cfg.latency_spike_multiplier
    return (
        windowed.join(baseline, on="service")
        .filter(F.col("p99_latency_ms") > F.col("baseline_p99") * multiplier)
        .withColumn("anomaly_type", F.lit("latency_spike"))
        .withColumn("detected_at", F.current_timestamp())
    )


# ─── Elasticsearch writer ─────────────────────────────────────────────────────


def write_to_elasticsearch(df: DataFrame, index: str, checkpoint_suffix: str) -> None:
    """Write a streaming DataFrame to Elasticsearch using the ES-Hadoop connector."""
    es_cfg = settings.elasticsearch
    (
        df.writeStream
        .format("org.elasticsearch.spark.sql")
        .option("es.nodes", ",".join(hreplace("http://", "") for h in es_cfg.hosts))
        .option("es.resource", index)
        .option("es.mapping.id", "event_id")
        .option("es.batch.size.entries", str(es_cfg.bulk_size))
        .option("es.net.http.auth.user", es_cfg.username)
        .option("es.net.http.auth.pass", es_cfg.password)
        .option("checkpointLocation", f"{settings.spark.checkpoint_location}/{checkpoint_suffix}")
        .outputMode("append")
        .trigger(processingTime=settings.spark.trigger_interval)
        .start()
    )


# ─── Anomaly writer with LLM hook ────────────────────────────────────────────


def write_anomalies_with_llm(anomalies: DataFrame, checkpoint_suffix: str):
    """
    For evach micro-batch of anomalies:
    1. Persist raw anomaly records to Elasticsearch.
    2. Forward batched anomalies to GPT-4o for NL summarization.
    3. Index the generated summary back to Elasticsearch.
    """
    summarizer = AnomalySummarizer()

    def _process_batch(batch_df: DataFrame, batch_id: int):
        if batch_df.isEmpty():
            return

        anomaly_records = [row.asDict() for row in batch_df.collect()]
        logger.info("Batch %d: processing %d anomalies", batch_id, len(anomaly_records))

        # 1 – Raw anomaly records → ES
        es_cfg = settings.elasticsearch
        (
            batch_df.write
            .format("org.elasticsearch.spark.sql")
            .option("es.nodes", ",".join(hreplace("http://", "") for h in es_cfg.hosts))
            .option("es.resource", es_cfg.anomalies_index)
            .option("es.net.http.auth.user", es_cfg.username)
            .option("es.net.http.auth.pass", es_cfg.password)
            .mode("append")
            .save()
        )

        # 2 – LLM summarization (batched to reduce API calls)
        try:
            summary = summarizer.summarize(anomaly_records)
            if summary:
                logger.info("LLM summary generated (%d chars)", len(summary))
                # 3 – Write summary back to ES (via REST for simplicity in foreachBatch)
                summarizer.index_summary_to_es(summary, anomaly_records)
        except Exception as exc:
            logger.error("LLM summarization failed for batch %d: %s", batch_id, exc)

    anomalies.writeStream.foreachBatch(_process_batch).option(
        "checkpointLocation",
        f"{settings.spark.checkpoint_location}/{checkpoint_suffix}",
    ).trigger(processingTime=settings.spark.trigger_interval).start()


# ─── Main ────────────────────────────────────────────────────────────────────


def run(source: str = "kafka", topic_or_stream: str = None):
    topic_or_stream = topic_or_stream or (
        settings.kafka.topic if source == "kafka" else settings.kinesis.stream_name
    )

    spark = build_spark_session(source)
    spark.sparkContext.setLogLevel("WARN")

    logger.info("Starting Spark Structured Streaming | source=%s | stream=%s", source, topic_or_stream)

    # Read events
    events = read_kafka(spark, topic_or_stream) if source == "kafka" else read_kinesis(spark, topic_or_stream)

    # 1 – Write all events to ES (raw)
    write_to_elasticsearch(events, settings.elasticsearch.events_index, "events-raw")

    # 2 – Throughput anomalies
    throughput_anomalies = detect_throughput_anomalies(events)
    write_anomalies_with_llm(throughput_anomalies, "anomalies-throughput")

    # 3 – Latency anomalies
    latency_anomalies = detect_latency_anomalies(events)
    write_anomalies_with_llm(latency_anomalies, "anomalies-latency")

    logger.info("All streaming queries started. Awaiting termination …")
    spark.streams.awaitAnyTermination()


# ─── CLI ─────────────────────────────────────────────────────────────────────


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Spark Structured Streaming consumer")
    parser.add_argument("--source", choices=["kafka", "kinesis"], default="kafka")
    parser.add_argument("--topic", default=None, help="Kafka topic or Kinesis stream name")
    args = parser.parse_args()
    run(source=args.source, topic_or_stream=args.topic)
