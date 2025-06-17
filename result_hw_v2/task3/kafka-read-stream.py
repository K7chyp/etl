from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_date
from pyspark.sql.types import StructType, StringType, IntegerType, BooleanType

BUCKET = 'data-proc-buckettt'

def write(batch_df, batch_id):
    batch_df.write \
        .format("jdbc") \
        .option("url", "") \
        .option("dbtable", "transactions_stream") \
        .option("user", "") \
        .option("password", "") \
        .option("driver", "org.postgresql.Driver") \
        .mode("append") \
        .save()

def main():
    spark = SparkSession.builder \
        .appName("kafka-proc-sparkr") \
        .getOrCreate()

    schema = StructType() \
        .add("transaction_id", LongType()) \
        .add("user_id", LongType()) \
        .add("transaction_date", StringType()) \
        .add("amount", DoubleType()) \
        .add("merchant_id", LongType()) \
        .add("category", StringType())

    df = spark.readStream.format("kafka") \
        .option("kafka.bootstrap.servers", "rc1a-sp0t812fps48sn74.mdb.yandexcloud.net:9091") \
        .option("subscribe", "kafka-topiccc") \
        .option("kafka.security.protocol", "SASL_SSL") \
        .option("kafka.sasl.mechanism", "SCRAM-SHA-512") \
        .option("kafka.sasl.jaas.config",
                "org.apache.kafka.common.security.scram.ScramLoginModule required "
                "username= "
                "password= ") \
        .option("startingOffsets", "latest") \
        .load()

    transformed_df = df \
        .selectExpr("CAST(value AS STRING) as json_str") \
        .select(from_json(col("json_str"), schema).alias("data")) \
        .select("data.*") \
        .withColumn("transaction_date", to_date(col("transaction_date"), "yyyy-MM-dd")) \
        .withColumn("transaction_timestamp", current_timestamp()) \
        .withColumn("amount_category",
            when(col("amount") < 50, "small")
            .when(col("amount") < 200, "medium")
            .otherwise("large")) \
        .withColumn("transaction_season",
            when(month(col("transaction_date")).isin(12, 1, 2), "winter")
            .when(month(col("transaction_date")).isin(3, 4, 5), "spring")
            .when(month(col("transaction_date")).isin(6, 7, 8), "summer")
            .otherwise("autumn")) \
        .withColumn("user_age_group",
            when(col("user_id") % 10 < 3, "18-25")
            .when(col("user_id") % 10 < 6, "26-35")
            .when(col("user_id") % 10 < 9, "36-50")
            .otherwise("50+")) \
        .withColumn("is_weekend", dayofweek(col("transaction_date")).isin([1, 7])) \
        .withColumn("transaction_year", year(col("transaction_date"))) \
        .withColumn("transaction_month", month(col("transaction_date"))) \
        .withColumn("user_id_hash", sha2(col("user_id").cast("string"), 256)) \
        .withColumn("merchant_category",
            when((col("merchant_id") // 10) * 10 == 5001, "groceries")
            .when((col("merchant_id") // 10) * 10 == 5020, "electronics")
            .when((col("merchant_id") // 10) * 10 == 5030, "transport")
            .when((col("merchant_id") // 10) * 10 == 5040, "clothing")
            .when((col("merchant_id") // 10) * 10 == 5050, "entertainment")
            .when((col("merchant_id") // 10) * 10 == 5060, "utilities")
            .when((col("merchant_id") // 10) * 10 == 5070, "pharmacy")
            .when((col("merchant_id") // 10) * 10 == 5080, "education")
            .when((col("merchant_id") // 10) * 10 == 5090, "travel")
            .otherwise("other")) \
        .withColumn("composite_key", concat(col("user_id_hash"), lit("_"), col("transaction_date"))) \
        .filter(col("amount") > 0) \
        .withColumn("is_premium", (col("amount") > 300) & col("category").isin(["electronics", "travel"])) \
        .withColumn("customer_segment",
            when(col("amount") > 400, "VIP")
            .when(col("amount") > 200, "Premium")
            .otherwise("Standard")) \
        .withColumn("days_since_transaction", datediff(current_date(), col("transaction_date"))) \
        .withColumn("month_name", date_format(col("transaction_date"), "MMMM")) \
        .withColumn("category_short", substring(col("category"), 1, 3)) \
        .withColumn("seasonal_adjusted_amount",
            when(col("transaction_season") == "summer", col("amount") * 1.1)
            .otherwise(col("amount")))
    
    query = transformed_df.writeStream \
        .foreachBatch(write) \
        .option("checkpointLocation", f"s3a://{BUCKET}/kafka-postgres-checkpoint") \
        .trigger(processingTime="10 seconds") \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    main()