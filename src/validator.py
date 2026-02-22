from dataclasses import dataclass
from typing import Dict, Tuple

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.config import load_config
from src.const import (
    X, Y, Z,
    COL_EVENT_TS,
    COL_CONSENT_TS,
    COL_USER,          # "user" in accelerometer
    COL_EMAIL,         # "email" in customers
)


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


def drop_null_sensor_axes(accel_df: DataFrame) -> Tuple[DataFrame, Dict[str, int]]:
    """
    Drop accelerometer rows where any axis (x, y, z) is null.
    """
    null_axis_rows = accel_df.filter(
        F.col(X).isNull() | F.col(Y).isNull() | F.col(Z).isNull()
    ).count()

    filtered = accel_df.filter(
        F.col(X).isNotNull() & F.col(Y).isNotNull() & F.col(Z).isNotNull()
    )
    return filtered, {"null_axis_rows": null_axis_rows}


def drop_out_of_range_axes(
    accel_df: DataFrame,
    min_g: float = -5.0,
    max_g: float = 5.0,
) -> Tuple[DataFrame, Dict[str, int]]:
    """
    Drop accelerometer rows where any axis falls outside a realistic range.
    """
    out_of_range_cond = (
        (F.col(X) < F.lit(min_g)) | (F.col(X) > F.lit(max_g)) |
        (F.col(Y) < F.lit(min_g)) | (F.col(Y) > F.lit(max_g)) |
        (F.col(Z) < F.lit(min_g)) | (F.col(Z) > F.lit(max_g))
    )
    out_of_range_rows = accel_df.filter(out_of_range_cond).count()
    filtered = accel_df.filter(~out_of_range_cond)
    return filtered, {"out_of_range_rows": out_of_range_rows}


def drop_duplicate_sensor_events(
    accel_df: DataFrame,
    key_cols: Tuple[str, ...] = (COL_USER, COL_EVENT_TS),
) -> Tuple[DataFrame, Dict[str, int]]:
    """
    Drop duplicate sensor events.

    NOTE:
      Accelerometer events now belong to the phone/user (email), so the natural uniqueness key is:
        (user, event_ts)

      Previously (before refactor) this used (serialNumber, event_ts).
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
    Enforce time-based research consent.

    Keep accelerometer events only when:
      - customer has consent timestamp (consent_ts is not null)
      - event_ts >= consent_ts

    Joins:
      - accel_df.user == customers_df.email

    Assumptions:
      - customers_df contains: email, consent_ts
      - accel_df contains: user, event_ts
    """
    joined = accel_df.join(
        customers_df.select(COL_EMAIL, COL_CONSENT_TS),
        accel_df[COL_USER] == customers_df[COL_EMAIL],
        how="left",
    )

    no_consent_rows = joined.filter(F.col(COL_CONSENT_TS).isNull()).count()

    pre_consent_rows = joined.filter(
        F.col(COL_CONSENT_TS).isNotNull() & (F.col(COL_EVENT_TS) < F.col(COL_CONSENT_TS))
    ).count()

    filtered = (
        joined
        .filter(F.col(COL_CONSENT_TS).isNotNull() & (F.col(COL_EVENT_TS) >= F.col(COL_CONSENT_TS)))
        .drop(COL_CONSENT_TS)
        .drop(customers_df[COL_EMAIL])  # keep accel_df.user as the identity
    )

    return filtered, {"no_consent_rows": no_consent_rows, "pre_consent_rows": pre_consent_rows}


def validate_accelerometer_dataset(
    accel_df: DataFrame,
    customers_df: DataFrame,
    axis_min_g: float = -5.0,
    axis_max_g: float = 5.0,
) -> Tuple[DataFrame, ValidationSummary]:
    """
    Apply validations to the accelerometer dataset and return a summary.

    Pipeline:
      1) Drop null axis values
      2) Drop out-of-range axis values
      3) Drop duplicates (user + event_ts)
      4) Enforce consent-by-time (join user -> customers.email)

    Returns:
      (validated_df, ValidationSummary)
    """
    cfg = load_config()

    total_before = accel_df.count()
    checks: Dict[str, int] = {}

    df, m = drop_null_sensor_axes(accel_df)
    checks.update(m)

    df, m = drop_out_of_range_axes(df, min_g=axis_min_g, max_g=axis_max_g)
    checks.update(m)

    # config.yaml can now set dedupe_keys to ["user", "event_ts"]
    df, m = drop_duplicate_sensor_events(df, key_cols=tuple(cfg.validation.dedupe_keys))
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