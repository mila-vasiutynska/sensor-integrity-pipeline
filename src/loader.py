from pyspark.sql import SparkSession

try:
    from .schemas import CUSTOMERS_SCHEMA, ACCEL_SCHEMA, STEP_TRAINER_SCHEMA
except ImportError:
    from schemas import CUSTOMERS_SCHEMA, ACCEL_SCHEMA, STEP_TRAINER_SCHEMA

def get_spark(app_name="sensor-integrity-pipeline"):
    return (SparkSession.builder
            .appName(app_name)
            .master("local[*]")
            .getOrCreate())

def load_jsonl(spark, path, schema):
    return spark.read.schema(schema).json(path)

def load_all(spark, base_path="data/sample"):
    customers = load_jsonl(spark, f"{base_path}/customers.json", CUSTOMERS_SCHEMA)
    accel = load_jsonl(spark, f"{base_path}/accelerometer.json", ACCEL_SCHEMA)
    step = load_jsonl(spark, f"{base_path}/step_trainer.json", STEP_TRAINER_SCHEMA)
    return customers, accel, step
