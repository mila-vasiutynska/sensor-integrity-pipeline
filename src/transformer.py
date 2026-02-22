from __future__ import annotations

from typing import Tuple

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.const import (
    COL_USER,
    COL_EMAIL,
    COL_SERIAL_NUMBER,
    COL_EVENT_TS,
    COL_SENSOR_TS,
    COL_CONSENT_TS,
    COL_ACCEL_TS_RAW,
    COL_STEP_TIME_RAW,
    COL_DISTANCE,
    X, Y, Z,
)


def parse_customers(customers_df: DataFrame) -> DataFrame:
    """
    Normalize customer fields and derive consent_ts as Spark timestamp.
    """
    return (
        customers_df
        .withColumn("birthDay_date", F.to_date(F.col("birthDay")))
        .withColumn("registration_ts", F.to_timestamp(F.from_unixtime(F.col("registrationDate"))))
        .withColumn("last_update_ts", F.to_timestamp(F.from_unixtime(F.col("lastUpdateDate"))))
        .withColumn(
            COL_CONSENT_TS,
            F.when(
                F.col("shareWithResearchAsOfDate").isNotNull(),
                F.to_timestamp(F.from_unixtime(F.col("shareWithResearchAsOfDate")))
            ).otherwise(F.lit(None).cast("timestamp"))
        )
    )


def parse_accelerometer(accel_df: DataFrame) -> DataFrame:
    """
    Adds event_ts as Spark timestamp from epoch seconds.
    """
    return accel_df.withColumn(
        COL_EVENT_TS,
        F.to_timestamp(F.from_unixtime(F.col(COL_ACCEL_TS_RAW)))
    )


def parse_step_trainer(step_df: DataFrame) -> DataFrame:
    """
    Adds sensor_ts as Spark timestamp from ISO string.
    """
    return step_df.withColumn(COL_SENSOR_TS, F.to_timestamp(F.col(COL_STEP_TIME_RAW)))


def build_curated_dataset(
    validated_accel_df: DataFrame,
    customers_df: DataFrame,
    step_df: DataFrame,
) -> DataFrame:
    """
    Build curated dataset.

    New (realistic) join path:
      1) Map accelerometer (phone) events to customer using user(email):
           accel.user == customers.email
         to get the step trainer device serialNumber.
      2) Join to step trainer readings on:
           serialNumber AND event_ts == sensor_ts
    """

    # 1) Map phone accelerometer events -> device serialNumber via customer
    accel_with_customer = validated_accel_df.join(
    customers_df.select(
        COL_EMAIL,
        COL_SERIAL_NUMBER,
        "customerName",
        "birthDay_date",
    ),
    validated_accel_df[COL_USER] == customers_df[COL_EMAIL],
    how="inner",
    )

    # 2) Join to step trainer on device serial + aligned time
    joined = accel_with_customer.join(
        step_df.select(COL_SERIAL_NUMBER, COL_SENSOR_TS, COL_DISTANCE),
        (accel_with_customer[COL_SERIAL_NUMBER] == step_df[COL_SERIAL_NUMBER]) &
        (accel_with_customer[COL_EVENT_TS] == step_df[COL_SENSOR_TS]),
        how="inner",
    )

    return (
        joined
        .drop(step_df[COL_SERIAL_NUMBER])
        .drop(step_df[COL_SENSOR_TS])
        .select(
            COL_EMAIL,
            "customerName",
            "birthDay_date",
            COL_SERIAL_NUMBER,
            COL_EVENT_TS,
            X, Y, Z,
            COL_DISTANCE,
        )
        .orderBy(COL_SERIAL_NUMBER, COL_EVENT_TS)
    )


def prepare_all(
    customers_df: DataFrame,
    accel_df: DataFrame,
    step_df: DataFrame,
) -> Tuple[DataFrame, DataFrame, DataFrame]:
    """
    Function to parse/normalize all three input datasets.
    """
    return (
        parse_customers(customers_df),
        parse_accelerometer(accel_df),
        parse_step_trainer(step_df),
    )