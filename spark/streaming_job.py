"""
Spark Structured Streaming Job — adsb.lol Aircraft States
-----------------------------------------------------------
Source  : Kafka topic  (aircraft-states)
Process : Parse JSON aircraft events
Sinks   : InfluxDB v2
            - aircraft_states_raw  : one point per aircraft per batch
                                      (lat/lon/altitude/speed -> Grafana Geomap)
            - aircraft_states_agg  : windowed counts & stats per aircraft type
"""

import os
import logging
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, window, count, avg, max as spark_max,
    current_timestamp
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    BooleanType, LongType
)

from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS

# ── Config from environment ──────────────────────────────────
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
KAFKA_TOPIC     = os.getenv("KAFKA_TOPIC", "aircraft-states")
INFLUX_URL      = os.getenv("INFLUX_URL", "http://influxdb:8086")
INFLUX_TOKEN    = os.getenv("INFLUX_TOKEN", "my-super-secret-token")
INFLUX_ORG      = os.getenv("INFLUX_ORG", "pipeline-org")
INFLUX_BUCKET   = os.getenv("INFLUX_BUCKET", "aircraft-states")

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("SparkStreaming")

# ── Schema for incoming JSON events (adsb.lol) ──────────────
state_schema = StructType([
    StructField("hex",           StringType(),  True),
    StructField("flight",        StringType(),  True),
    StructField("registration",  StringType(),  True),
    StructField("aircraft_type", StringType(),  True),
    StructField("category",      StringType(),  True),
    StructField("latitude",      DoubleType(),  True),
    StructField("longitude",     DoubleType(),  True),
    StructField("alt_baro",      DoubleType(),  True),
    StructField("alt_geom",      DoubleType(),  True),
    StructField("ground_speed",  DoubleType(),  True),
    StructField("track",         DoubleType(),  True),
    StructField("baro_rate",     DoubleType(),  True),
    StructField("geom_rate",     DoubleType(),  True),
    StructField("squawk",        StringType(),  True),
    StructField("emergency",     StringType(),  True),
    StructField("on_ground",     BooleanType(), True),
    StructField("spi",           BooleanType(), True),
    StructField("messages",      LongType(),    True),
    StructField("seen",          DoubleType(),  True),
    StructField("seen_pos",      DoubleType(),  True),
    StructField("rssi",          DoubleType(),  True),
    StructField("ingested_at",   StringType(),  True),
])


# ── InfluxDB writer: raw aircraft positions (for Geomap) ─────
def write_raw_to_influx(batch_df, batch_id):
    rows = batch_df.collect()
    if not rows:
        return

    client = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)
    write_api = client.write_api(write_options=SYNCHRONOUS)

    points = []
    for row in rows:
        if row.latitude is None or row.longitude is None:
            continue

        p = (
            Point("aircraft_states_raw")
            .tag("hex", row.hex or "unknown")
            .tag("aircraft_type", row.aircraft_type or "unknown")
            .tag("on_ground", str(bool(row.on_ground)))
            .field("flight", row.flight or "")
            .field("registration", row.registration or "")
            .field("latitude", float(row.latitude))
            .field("longitude", float(row.longitude))
            .field("alt_baro", float(row.alt_baro) if row.alt_baro is not None else 0.0)
            .field("alt_geom", float(row.alt_geom) if row.alt_geom is not None else 0.0)
            .field("ground_speed", float(row.ground_speed) if row.ground_speed is not None else 0.0)
            .field("track", float(row.track) if row.track is not None else 0.0)
            .field("baro_rate", float(row.baro_rate) if row.baro_rate is not None else 0.0)
            .time(datetime.utcnow(), WritePrecision.NS)
        )
        points.append(p)

    if points:
        write_api.write(bucket=INFLUX_BUCKET, org=INFLUX_ORG, record=points)
    client.close()
    log.info(f"Batch {batch_id}: wrote {len(points)} raw aircraft positions to InfluxDB")


# ── InfluxDB writer: windowed per-aircraft-type aggregates ───
def write_agg_to_influx(batch_df, batch_id):
    rows = batch_df.collect()
    if not rows:
        return

    client = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)
    write_api = client.write_api(write_options=SYNCHRONOUS)

    points = []
    for row in rows:
        aircraft_type = row.aircraft_type or "unknown"

        p = (
            Point("aircraft_states_agg")
            .tag("aircraft_type", aircraft_type)
            .tag("batch_id", str(batch_id))
            .field("aircraft_count", int(row.aircraft_count))
            .field("avg_ground_speed", float(row.avg_ground_speed) if row.avg_ground_speed is not None else 0.0)
            .field("avg_alt_baro", float(row.avg_alt_baro) if row.avg_alt_baro is not None else 0.0)
            .field("max_ground_speed", float(row.max_ground_speed) if row.max_ground_speed is not None else 0.0)
            .time(datetime.utcnow(), WritePrecision.NS)
        )
        points.append(p)

    if points:
        write_api.write(bucket=INFLUX_BUCKET, org=INFLUX_ORG, record=points)
    client.close()
    log.info(f"Batch {batch_id}: wrote {len(points)} aircraft-type aggregates to InfluxDB")


def main():
    spark = (
        SparkSession.builder
        .appName("AircraftStateStreaming")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.streaming.stopGracefullyOnShutdown", "true")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # ── Read from Kafka ──────────────────────────────────────
    raw_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )

    # ── Parse JSON ───────────────────────────────────────────
    parsed_df = (
        raw_df
        .select(from_json(col("value").cast("string"), state_schema).alias("data"))
        .select("data.*")
        .withColumn("event_time", current_timestamp())
    )

    # ── Stream 1: Raw aircraft positions → InfluxDB (Geomap) ─
    raw_query = (
        parsed_df.writeStream
        .foreachBatch(write_raw_to_influx)
        .outputMode("append")
        .option("checkpointLocation", "/tmp/spark-checkpoint/raw")
        .trigger(processingTime="10 seconds")
        .start()
    )

    # ── Stream 2: Windowed aggregation by aircraft type ──────
    agg_df = (
        parsed_df
        .withWatermark("event_time", "1 minute")
        .groupBy(
            window(col("event_time"), "1 minute", "30 seconds"),
            col("aircraft_type")
        )
        .agg(
            count("*").alias("aircraft_count"),
            avg("ground_speed").alias("avg_ground_speed"),
            avg("alt_baro").alias("avg_alt_baro"),
            spark_max("ground_speed").alias("max_ground_speed"),
        )
    )

    agg_query = (
        agg_df.writeStream
        .foreachBatch(write_agg_to_influx)
        .outputMode("update")
        .option("checkpointLocation", "/tmp/spark-checkpoint/agg")
        .trigger(processingTime="30 seconds")
        .start()
    )

    log.info("Spark Streaming started. Waiting for termination...")
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
