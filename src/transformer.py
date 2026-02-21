from __future__ import annotations

from typing import Tuple

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def parse_customers(customers_df: DataFrame) -> DataFrame:
    """
    Parse/normalize customer fields into strongly-typed columns.

    Adds:
        - birthDay_date: date
        - registration_ts: timestamp (from epoch seconds)
        - last_update_ts: timestamp (from epoch seconds)
        - consent_ts: timestamp (from shareWithResearchAsOfDate epoch seconds)

    Args:
        customers_df: Raw customers DataFrame (schema already applied in loader).

    Returns:
        Normalized customers DataFrame.
    """
    return (
        customers_df
        .withColumn("birthDay_date", F.to_date(F.col("birthDay")))
        .withColumn("registration_ts", F.to_timestamp(F.from_unixtime(F.col("registrationDate"))))
        .withColumn("last_update_ts", F.to_timestamp(F.from_unixtime(F.col("lastUpdateDate"))))
        .withColumn(
            "consent_ts",
            F.when(F.col("shareWithResearchAsOfDate").isNotNull(),
                   F.to_timestamp(F.from_unixtime(F.col("shareWithResearchAsOfDate"))))
             .otherwise(F.lit(None).cast("timestamp"))
        )
    )


def parse_accelerometer(accel_df: DataFrame) -> DataFrame:
    """
    Parse accelerometer timestamps into a timestamp column used for joins/validation.

    Adds:
        - event_ts: timestamp (from epoch seconds)

    Args:
        accel_df: Raw accelerometer DataFrame.

    Returns:
        Parsed accelerometer DataFrame with event_ts.
    """
    return accel_df.withColumn("event_ts", F.to_timestamp(F.from_unixtime(F.col("timestamp"))))


def parse_step_trainer(step_df: DataFrame) -> DataFrame:
    """
    Parse step trainer sensorReadingTime into a proper timestamp.

    Notes:
        In your synthetic generator, sensorReadingTime is written as ISO-8601 string.
        This function converts it to Spark timestamp.

    Adds:
        - sensor_ts: timestamp

    Args:
        step_df: Raw step trainer DataFrame.

    Returns:
        Parsed step trainer DataFrame with sensor_ts.
    """
    return step_df.withColumn("sensor_ts", F.to_timestamp(F.col("sensorReadingTime")))


def build_curated_dataset(
    validated_accel_df: DataFrame,
    step_df: DataFrame,
) -> DataFrame:
    """
    Build a curated dataset by joining validated accelerometer events with step trainer readings.

    Join strategy:
        - Inner join on (serialNumber, event_ts == sensor_ts)

    Rationale:
        This keeps only events that have both accelerometer readings and step trainer readings
        aligned in time. In a real system, you might use window joins (tolerance) instead.

    Args:
        validated_accel_df: Accelerometer events that passed validation and consent checks.
        step_df: Parsed step trainer readings (contains sensor_ts).

    Returns:
        Curated DataFrame suitable for downstream analytics/ML feature engineering.
    """
    joined = validated_accel_df.join(
        step_df.select("serialNumber", "sensor_ts", "distanceFromObject"),
        (validated_accel_df["serialNumber"] == step_df["serialNumber"]) &
        (validated_accel_df["event_ts"] == step_df["sensor_ts"]),
        how="inner",
    )

    return (
        joined
        .drop(step_df["serialNumber"])
        .drop(step_df["sensor_ts"])
        .select(
            "serialNumber",
            "event_ts",
            "x", "y", "z",
            "distanceFromObject",
        )
        .orderBy("serialNumber", "event_ts")
    )


def prepare_all(
    customers_df: DataFrame,
    accel_df: DataFrame,
    step_df: DataFrame,
) -> Tuple[DataFrame, DataFrame, DataFrame]:
    """
    Convenience function to parse/normalize all three input datasets.

    Args:
        customers_df: Raw customers DataFrame.
        accel_df: Raw accelerometer DataFrame.
        step_df: Raw step trainer DataFrame.

    Returns:
        (customers_parsed, accel_parsed, step_parsed)
    """
    return (
        parse_customers(customers_df),
        parse_accelerometer(accel_df),
        parse_step_trainer(step_df),
    )