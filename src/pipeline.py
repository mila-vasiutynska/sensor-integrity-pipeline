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

from src.config import load_config


def ensure_dir(path: str) -> None:
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
    input_base: str | None = None,
    output_base: str | None = None,
) -> Dict[str, int]:
    """
    Run the end-to-end local Spark pipeline.

    Steps:
      1) Load JSONL inputs using strict schemas
      2) Parse/normalize timestamps and dates
      3) Validate accelerometer dataset (nulls, range, dedupe, consent-by-time)
      4) Build curated dataset (map phone->customer->device, then join to step trainer)
      5) Write outputs to Parquet for inspection
    """
    cfg = load_config()

    # Allow CLI/other callers to override config, but default to config.yaml
    input_base = input_base or cfg.paths.input_base
    output_base = output_base or cfg.paths.output_base

    spark = get_spark(app_name=cfg.name)

    customers_raw, accel_raw, step_raw = load_all(spark, base_path=input_base)
    customers, accel, step = prepare_all(customers_raw, accel_raw, step_raw)

    accel_validated, summary = validate_accelerometer_dataset(
        accel_df=accel,
        customers_df=customers,
        axis_min_g=cfg.validation.axis_min_g,
        axis_max_g=cfg.validation.axis_max_g,
    )

    # IMPORTANT: curated now needs customers_df because accel is keyed by user(email),
    # and we map to step trainer device serialNumber via customers.
    curated = build_curated_dataset(
        validated_accel_df=accel_validated,
        customers_df=customers,
        step_df=step,
    )

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