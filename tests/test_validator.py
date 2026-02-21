from __future__ import annotations

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Import from src/ (works when running pytest from repo root)
from src.validator import (
    drop_null_sensor_axes,
    drop_out_of_range_axes,
    drop_duplicate_sensor_events,
    enforce_consent_by_time,
)


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    """
    Provide a Spark session for tests.

    Session scope keeps tests fast by reusing the Spark JVM.
    """
    spark = (
        SparkSession.builder
        .appName("sensor-integrity-pipeline-tests")
        .master("local[2]")
        .getOrCreate()
    )
    yield spark
    spark.stop()


def test_drop_null_sensor_axes(spark: SparkSession) -> None:
    """
    Rows with any null in x/y/z should be removed.
    """
    df = spark.createDataFrame(
        [
            ("A", 1, 0.1, 0.2, 0.3),
            ("A", 2, None, 0.2, 0.3),
            ("A", 3, 0.1, None, 0.3),
            ("A", 4, 0.1, 0.2, None),
        ],
        ["serialNumber", "timestamp", "x", "y", "z"],
    ).withColumn("event_ts", F.to_timestamp(F.from_unixtime(F.col("timestamp"))))

    filtered, metrics = drop_null_sensor_axes(df)

    assert metrics["null_axis_rows"] == 3
    assert filtered.count() == 1


def test_drop_out_of_range_axes(spark: SparkSession) -> None:
    """
    Rows with values outside [-5, 5] on any axis should be removed.
    """
    df = spark.createDataFrame(
        [
            ("A", 1, 0.1, 0.2, 0.3),
            ("A", 2, 6.0, 0.2, 0.3),   # out-of-range x
            ("A", 3, 0.1, -6.0, 0.3),  # out-of-range y
            ("A", 4, 0.1, 0.2, 9.5),   # out-of-range z
        ],
        ["serialNumber", "timestamp", "x", "y", "z"],
    ).withColumn("event_ts", F.to_timestamp(F.from_unixtime(F.col("timestamp"))))

    filtered, metrics = drop_out_of_range_axes(df, min_g=-5.0, max_g=5.0)

    assert metrics["out_of_range_rows"] == 3
    assert filtered.count() == 1


def test_drop_duplicate_sensor_events(spark: SparkSession) -> None:
    """
    Duplicate events (same serialNumber + event_ts) should be dropped.
    """
    df = spark.createDataFrame(
        [
            ("A", 10, 0.1, 0.2, 0.3),
            ("A", 10, 0.1, 0.2, 0.3),  # duplicate
            ("A", 11, 0.1, 0.2, 0.3),
        ],
        ["serialNumber", "timestamp", "x", "y", "z"],
    ).withColumn("event_ts", F.to_timestamp(F.from_unixtime(F.col("timestamp"))))

    deduped, metrics = drop_duplicate_sensor_events(df, key_cols=("serialNumber", "event_ts"))

    assert metrics["duplicate_rows"] == 1
    assert deduped.count() == 2


def test_enforce_consent_by_time(spark: SparkSession) -> None:
    """
    Keep only events where:
      - consent_ts exists
      - event_ts >= consent_ts
    """
    # Customers: A consents at t=100, B has no consent
    customers = spark.createDataFrame(
        [
            ("A", 100),
            ("B", None),
        ],
        ["serialNumber", "consent_epoch"],
    ).withColumn(
        "consent_ts",
        F.when(F.col("consent_epoch").isNotNull(),
               F.to_timestamp(F.from_unixtime(F.col("consent_epoch"))))
         .otherwise(F.lit(None).cast("timestamp"))
    ).select("serialNumber", "consent_ts")

    # Events: A has one pre-consent (90) and one post-consent (110); B has an event but no consent
    accel = spark.createDataFrame(
        [
            ("A", 90, 0.1, 0.2, 0.3),
            ("A", 110, 0.1, 0.2, 0.3),
            ("B", 110, 0.1, 0.2, 0.3),
        ],
        ["serialNumber", "timestamp", "x", "y", "z"],
    ).withColumn("event_ts", F.to_timestamp(F.from_unixtime(F.col("timestamp"))))

    filtered, metrics = enforce_consent_by_time(accel, customers)

    assert metrics["no_consent_rows"] == 1
    assert metrics["pre_consent_rows"] == 1
    assert filtered.count() == 1

    row = filtered.select("serialNumber", "timestamp").collect()[0]
    assert row["serialNumber"] == "A"
    assert row["timestamp"] == 110