from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, DoubleType, IntegerType
)

CUSTOMERS_SCHEMA = StructType([
    StructField("customerName", StringType(), False),
    StructField("email", StringType(), False),
    StructField("phone", StringType(), True),
    StructField("birthDay", StringType(), True),  # parsed later to date
    StructField("serialNumber", StringType(), False),  # Step Trainer device ID
    StructField("registrationDate", LongType(), False),
    StructField("lastUpdateDate", LongType(), False),
    StructField("shareWithResearchAsOfDate", LongType(), True),
    StructField("shareWithPublicAsOfDate", LongType(), True),
    StructField("shareWithFriendsAsOfDate", LongType(), True),
])


ACCEL_SCHEMA = StructType([
    StructField("user", StringType(), False),  # email of the phone owner
    StructField("timestamp", LongType(), False),  # epoch seconds
    StructField("x", DoubleType(), True),
    StructField("y", DoubleType(), True),
    StructField("z", DoubleType(), True),
])

STEP_TRAINER_SCHEMA = StructType([
    StructField("sensorReadingTime", StringType(), False),  # ISO string -> timestamp later
    StructField("serialNumber", StringType(), False),  # device ID
    StructField("distanceFromObject", IntegerType(), True),
])