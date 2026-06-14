#!/usr/bin/env python3

from pyspark.sql import SparkSession
from pyspark.sql.functions import to_json, col, struct
import json
import uuid

def generate_loan_data(count=60000):
    data = []
    regions = ["DE-HE", "FR-PAR", "US-NY", "GB-LON", "ES-MAD"]
    risks = ["low", "medium", "high"]
    for i in range(count):
        record = {
            "application_id": f"loan_{uuid.uuid4().hex[:8]}",
            "customer": {
                "customer_id": f"cust_{i % 1000}",
                "region": regions[i % len(regions)]
            },
            "loan": {
                "amount": 1000 + (i % 5000) * 100,
                "term_months": 12 + (i % 48)
            },
            "scoring": {
                "score": 500 + (i % 300),
                "risk_level": risks[i % len(risks)]
            },
            "documents": [
                {"type": "passport", "status": "verified" if i % 2 == 0 else "pending"},
                {"type": "income_proof", "status": "missing" if i % 5 == 0 else "verified"}
            ],
            "decision_status": "manual_review" if i % 10 == 0 else "approved",
            "submitted_at": "2026-05-01T10:15:11Z"
        }
        data.append(record)
    return data

def main():
    spark = SparkSession.builder.appName("dataproc-kafka-write-app").getOrCreate()
    raw_data = generate_loan_data(60000)

    df = spark.createDataFrame(raw_data)

    df_kafka = df.select(to_json(struct([col(c).alias(c) for c in df.columns])).alias('value'))

    print(f"Generated {df.count()} records. Sending to Kafka...")

    df_kafka.write.format("kafka") \
        .option("kafka.bootstrap.servers", "rc1b-tbmlun786dms0qsg.mdb.yandexcloud.net:9091") \
        .option("topic", "dataproc-kafka-topic") \
        .option("kafka.security.protocol", "SASL_SSL") \
        .option("kafka.sasl.mechanism", "SCRAM-SHA-512") \
        .option("kafka.sasl.jaas.config",
                "org.apache.kafka.common.security.scram.ScramLoginModule required "
                "username=user1 "
                "password=password1 "
                ";") \
        .save()
    print("Successfully wrote data to Kafka!")

if __name__ == "__main__":
    main()
