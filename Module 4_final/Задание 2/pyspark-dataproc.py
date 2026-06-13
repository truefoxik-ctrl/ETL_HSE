from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import col, when, lit, to_timestamp, date_format

spark = SparkSession.builder \
    .appName("NYC_Taxi_Proc") \
    .enableHiveSupport() \
    .getOrCreate()

schema = StructType([
    StructField('id', StringType(), True),
    StructField('vendor_id', IntegerType(), True),
    StructField('pickup_datetime', StringType(), True),
    StructField('dropoff_datetime', StringType(), True),
    StructField('passenger_count', IntegerType(), True),
    StructField('pickup_longitude', StringType(), True),
    StructField('pickup_latitude', StringType(), True),
    StructField('dropoff_longitude', StringType(), True),
    StructField('dropoff_latitude', StringType(), True),
    StructField('store_and_fwd_flag', StringType(), True),
    StructField('trip_duration', IntegerType(), True)])

input_path = "s3a://for-dataproc-tat/in/NYC.csv"
output_path = "s3a://for-dataproc-tat/out/taxi"

df = spark.read \
    .option("header", "true") \
    .option("delimiter", ",") \
    .option("inferSchema", "false") \
    .schema(schema) \
    .csv(input_path)

df = df.withColumn("pickup_dt", to_timestamp(col("pickup_datetime"), "yyyy-MM-dd HH:mm:ss"))
df = df.withColumn("dropoff_dt", to_timestamp(col("dropoff_datetime"), "yyyy-MM-dd HH:mm:ss"))

df = df.withColumn(
    "store_and_fwd_bool",
    when(col("store_and_fwd_flag") == "Y", lit(True))
     .when(col("store_and_fwd_flag") == "N", lit(False))
     .otherwise(lit(None))
)

df_clean = df.filter(col("id").isNotNull()) \
             .filter(col("trip_duration") > 30) \
             .filter(col("passenger_count") > 0)

df_clean = df_clean.withColumn("trip_duration_min", col("trip_duration") / 60.0)

df_clean = df_clean.withColumn("pickup_day_of_week", date_format(col("pickup_dt"), "EEEE"))

df_clean.write \
    .mode("overwrite") \
    .parquet(output_path)

spark.stop()
