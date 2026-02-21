# src/pipeline.py
from __future__ import annotations

import os
from typing import Dict

from pyspark.sql import DataFrame

# Works whether you run as a package or from src/
try:
    from .loader import get_spark, load_all
    from .transformer import prepare_all, build_curated_dataset
    from .validator import validate_accelerometer_dataset
except ImportError:
    from loader import get_spark, load_all
    from transformer import prepare_all, build_curated_dataset
    from validator import validate_accelerometer_dataset


def ensure_dir(path: str) -> None:
    """
    Ensure a directory exists.

    Args:
        path: Directory path.
    """
    os.makedirs(path, exist_ok=True)


def write_parquet(df: DataFrame, path: str, mode: str = "overwrite") -> None:
    """
    Write a DataFrame to Parquet.

    Args:
        df: DataFrame to write.
        path: Output path.
        mode: Spark write mode (default: overwrite).
    """
    df.write.mode(mode).parquet(path)


def run_pipeline(
    input_base: str = "data/sample",
    output_base: str = "out",
) -> Dict[str, int]:
    """
    Run the end-to-end local Spark pipeline:

        1) Load JSONL inputs using strict schemas
        2) Parse/normalize timestamps and dates
        3) Validate accelerometer dataset (nulls, range, dedupe, consent-by-time)
        4) Join validated accelerometer with step trainer readings
        5) Write outputs to Parquet for inspection

    Args:
        input_base: Base folder containing customers.json, accelerometer.json, step_trainer.json.
        output_base: Base output folder.

    Returns:
        A small dict of headline metrics (counts) to print or assert in CI.
    """
    spark = get_spark(app_name="sensor-integrity-pipeline")

    customers_raw, accel_raw, step_raw = load_all(spark, base_path=input_base)
    customers, accel, step = prepare_all(customers_raw, accel_raw, step_raw)

    accel_validated, summary = validate_accelerometer_dataset(accel, customers)
    curated = build_curated_dataset(accel_validated, step)

    ensure_dir(output_base)
    write_parquet(customers, f"{output_base}/customers_parsed")
    write_parquet(accel_validated, f"{output_base}/accelerometer_validated")
    write_parquet(curated, f"{output_base}/curated")

    metrics = {
        "customers": customers.count(),
        "accelerometer_raw": accel.count(),
        "accelerometer_validated": accel_validated.count(),
        "curated": curated.count(),
        "accelerometer_dropped": summary.dropped_rows,
    }

    # Print a readable validation summary (useful for reviewers)
    print("\n=== Validation Summary (Accelerometer) ===")
    print(f"Total rows:   {summary.total_rows}")
    print(f"Dropped rows: {summary.dropped_rows}")
    for k, v in summary.checks.items():
        print(f"- {k}: {v}")

    print("\n=== Headline Metrics ===")
    for k, v in metrics.items():
        print(f"{k}: {v}")

    spark.stop()
    return metrics


if __name__ == "__main__":
    run_pipeline()