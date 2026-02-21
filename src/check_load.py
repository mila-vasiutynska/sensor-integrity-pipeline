from loader import get_spark, load_all

spark = get_spark()
customers, accel, step = load_all(spark)
customers.show(3, truncate=False)
accel.show(3, truncate=False)
step.show(3, truncate=False)