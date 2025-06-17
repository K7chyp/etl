import random
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    LongType,
    DoubleType,
    DateType,
    StringType,
)
from pyspark.sql.functions import (
    udf,
    year,
    month,
    dayofmonth,
    dayofweek,
    when,
    lit,
    sha2,
    concat,
)
from pyspark.sql import functions as F


from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date
from pyspark.sql.types import IntegerType, StringType, BooleanType
from pyspark.sql.utils import AnalysisException


spark = SparkSession.builder.appName("Parquet ETL with Logging to S3").getOrCreate()

SOURCE = "s3a://etl-buckettt/transactions_v2.csv"
TARGET = "s3a://etl-buckettt/transformed/transactions_v2_processed.parquet"


def map_merchant_category(merchant_id):
    base = (merchant_id // 10) * 10
    return merchant_categories.get(base, "other")

def get_season(date):
    month = date.month
    if month in [12, 1, 2]:
        return "winter"
    if month in [3, 4, 5]:
        return "spring"
    if month in [6, 7, 8]:
        return "summer"
    return "autumn"

df = spark.read.option("header", "true").option("inferSchema", "true").csv(SOURCE)

df = df.withColumn(
    "amount_category",
    when(F.col("amount") < 50, "small")
    .when(F.col("amount") < 200, "medium")
    .otherwise("large"),
)
season_udf = udf(get_season, StringType())
df = df.withColumn("transaction_season", season_udf(F.col("transaction_date")))
df = df.withColumn(
    "user_age_group",
    when(F.col("user_id") % 10 < 3, "18-25")
    .when(F.col("user_id") % 10 < 6, "26-35")
    .when(F.col("user_id") % 10 < 9, "36-50")
    .otherwise("50+"),
)

df = df.withColumn("is_weekend", F.dayofweek(F.col("transaction_date")).isin([1, 7]))
df = df.withColumn("transaction_year", F.year(F.col("transaction_date")))
df = df.withColumn("transaction_month", F.month(F.col("transaction_date")))
df = df.withColumn("user_id_hash", F.sha2(F.col("user_id").cast("string"), 256))
merchant_categories = {
    5001: "groceries",
    5020: "electronics",
    5030: "transport",
    5040: "clothing",
    5050: "entertainment",
    5060: "utilities",
    5070: "pharmacy",
    5080: "education",
    5090: "travel",
}
merchant_udf = udf(map_merchant_category, StringType())
df = df.withColumn("merchant_category", merchant_udf(F.col("merchant_id")))
df = df.withColumn(
    "composite_key",
    F.concat(F.col("user_id_hash"), F.lit("_"), F.col("transaction_date")),
)
df = df.filter(F.col("amount") > 0)
df = df.withColumn(
    "is_premium",
    (F.col("amount") > 300) & (F.col("category").isin(["electronics", "travel"])),
)
df = df.na.drop()
df.write.mode("overwrite").parquet(TARGET)
spark.stop()
