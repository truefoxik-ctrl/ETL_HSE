#!/usr/bin/env python3

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType, MapType
from pyspark.sql.functions import from_json, col, explode

def main():
    spark = SparkSession.builder.appName("dataproc-kafka-read-batch-app").getOrCreate()

    schema = StructType([
        StructField("application_id", StringType()),
        StructField("customer", StructType([
            StructField("customer_id", StringType()),
            StructField("region", StringType())
        ])),
        StructField("loan", StructType([
            StructField("amount", IntegerType()),
            StructField("term_months", IntegerType())
        ])),
        StructField("scoring", StructType([
            StructField("score", IntegerType()),
            StructField("risk_level", StringType())
        ])),
        StructField("documents", ArrayType(StructType([
            StructField("type", StringType()),
            StructField("status", StringType())
        ]))),
        StructField("decision_status", StringType()),
        StructField("submitted_at", StringType())
    ])

    df_raw = spark.read.format("kafka") \
        .option("kafka.bootstrap.servers", "rc1b-tbmlun786dms0qsg.mdb.yandexcloud.net:9091") \
        .option("subscribe", "dataproc-kafka-topic") \
        .option("kafka.security.protocol", "SASL_SSL") \
        .option("kafka.sasl.mechanism", "SCRAM-SHA-512") \
        .option("kafka.sasl.jaas.config",
                "org.apache.kafka.common.security.scram.ScramLoginModule required "
                "username=user1 "
                "password=password1 "
                ";") \
        .option("startingOffsets", "earliest") \
        .load()

    df_parsed = df_raw.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING) as json_value") \
        .withColumn("data", from_json(col("json_value"), schema)) \
        .select("data.*")

    df_flat = df_parsed.select(
        col("application_id"),
        col("customer.customer_id").alias("customer_id"),
        col("customer.region").alias("customer_region"),
        col("loan.amount").alias("loan_amount"),
        col("loan.term_months").alias("loan_term_months"),
        col("scoring.score").alias("score"),
        col("scoring.risk_level").alias("risk_level"),
        col("documents"),
        col("decision_status"),
        col("submitted_at")
    )

    df_flat.printSchema()
    print(f"Total records read: {df_flat.count()}")
    output_path = "s3a://dataproc-bucket/kafka-batch-output-flat"
    df_flat.write.mode("overwrite").parquet(output_path)
    print(f"Data saved to {output_path}")

if __name__ == "__main__":
    main()
