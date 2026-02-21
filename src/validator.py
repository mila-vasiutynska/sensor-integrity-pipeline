# src/validator.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Tuple

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


@dataclass(frozen=True)
class ValidationSummary:
    """
    Captures validation metrics for a dataset run.

    Attributes:
        total_rows: Total number of rows inspected.
        dropped_rows: Total number of rows removed by validation filters.
        checks: Per-check counts/metrics that help explain why rows were dropped.
    """
    total_rows: int
    dropped_rows: int
    checks: Dict[str, int]


def count_rows(df: DataFrame) -> int:
    """
    Count rows in a DataFrame. Use sparingly on large datasets (triggers action).

    Args:
        df: Input DataFrame.

    Returns:
        Row count (int).
    """
    return df.count()


def drop_null_sensor_axes(accel_df: DataFrame) -> Tuple[DataFrame, Dict[str, int]]:
    """
    Drop accelerometer rows where any axis (x, y, z) is null.

    Args:
        accel_df: Accelerometer DataFrame with columns x, y, z.

    Returns:
        (filtered_df, metrics)
        metrics includes: null_axis_rows
    """
    null_axis_rows = accel_df.filter(F.col("x").isNull() | F.col("y").isNull() | F.col("z").isNull()).count()
    filtered = accel_df.filter(F.col("x").isNotNull() & F.col("y").isNotNull() & F.col("z").isNotNull())
    return filtered, {"null_axis_rows": null_axis_rows}


def drop_out_of_range_axes(
    accel_df: DataFrame,
    min_g: float = -5.0,
    max_g: float = 5.0,
) -> Tuple[DataFrame, Dict[str, int]]:
    """
    Drop accelerometer rows where any axis falls outside a realistic range.

    Args:
        accel_df: Accelerometer DataFrame with columns x, y, z.
        min_g: Minimum allowed value.
        max_g: Maximum allowed value.

    Returns:
        (filtered_df, metrics)
        metrics includes: out_of_range_rows
    """
    out_of_range_cond = (
        (F.col("x") < F.lit(min_g)) | (F.col("x") > F.lit(max_g)) |
        (F.col("y") < F.lit(min_g)) | (F.col("y") > F.lit(max_g)) |
        (F.col("z") < F.lit(min_g)) | (F.col("z") > F.lit(max_g))
    )
    out_of_range_rows = accel_df.filter(out_of_range_cond).count()
    filtered = accel_df.filter(~out_of_range_cond)
    return filtered, {"out_of_range_rows": out_of_range_rows}


def drop_duplicate_sensor_events(
    accel_df: DataFrame,
    key_cols: Tuple[str, ...] = ("serialNumber", "event_ts"),
) -> Tuple[DataFrame, Dict[str, int]]:
    """
    Drop duplicate sensor events using key columns.

    Notes:
        This assumes `event_ts` already exists (a timestamp column created during parsing).
        If you prefer epoch seconds, change key_cols to ("serialNumber", "timestamp").

    Args:
        accel_df: Accelerometer DataFrame.
        key_cols: Columns defining event uniqueness.

    Returns:
        (deduped_df, metrics)
        metrics includes: duplicate_rows
    """
    total = accel_df.count()
    deduped = accel_df.dropDuplicates(list(key_cols))
    after = deduped.count()
    return deduped, {"duplicate_rows": total - after}


def enforce_consent_by_time(
    accel_df: DataFrame,
    customers_df: DataFrame,
) -> Tuple[DataFrame, Dict[str, int]]:
    """
    Enforce time-based research consent:

    Keep accelerometer events only when:
    - customer has shareWithResearchAsOfDate (not null)
    - event_ts >= consent_ts

    Assumptions:
        - customers_df contains `serialNumber` and `consent_ts` (timestamp)
        - accel_df contains `serialNumber` and `event_ts` (timestamp)

    Args:
        accel_df: Parsed accelerometer events.
        customers_df: Parsed customers with consent timestamp.

    Returns:
        (filtered_df, metrics)
        metrics includes: no_consent_rows, pre_consent_rows
    """
    joined = accel_df.join(
        customers_df.select("serialNumber", "consent_ts"),
        on="serialNumber",
        how="left",
    )

    no_consent_rows = joined.filter(F.col("consent_ts").isNull()).count()
    pre_consent_rows = joined.filter(F.col("consent_ts").isNotNull() & (F.col("event_ts") < F.col("consent_ts"))).count()

    filtered = joined.filter(F.col("consent_ts").isNotNull() & (F.col("event_ts") >= F.col("consent_ts"))) \
                     .drop("consent_ts")
    return filtered, {"no_consent_rows": no_consent_rows, "pre_consent_rows": pre_consent_rows}


def validate_accelerometer_dataset(
    accel_df: DataFrame,
    customers_df: DataFrame,
) -> Tuple[DataFrame, ValidationSummary]:
    """
    Apply a set of validations to the accelerometer dataset and return a summary.

    Pipeline:
        1) Drop null axis values
        2) Drop out-of-range axis values
        3) Drop duplicates (serialNumber + event_ts)
        4) Enforce consent-by-time

    Args:
        accel_df: Parsed accelerometer DataFrame containing event_ts.
        customers_df: Parsed customers DataFrame containing consent_ts.

    Returns:
        (validated_df, ValidationSummary)
    """
    total_before = accel_df.count()
    checks: Dict[str, int] = {}

    df, m = drop_null_sensor_axes(accel_df)
    checks.update(m)

    df, m = drop_out_of_range_axes(df, min_g=-5.0, max_g=5.0)
    checks.update(m)

    df, m = drop_duplicate_sensor_events(df, key_cols=("serialNumber", "event_ts"))
    checks.update(m)

    df, m = enforce_consent_by_time(df, customers_df)
    checks.update(m)

    total_after = df.count()
    summary = ValidationSummary(
        total_rows=total_before,
        dropped_rows=total_before - total_after,
        checks=checks,
    )
    return df, summary