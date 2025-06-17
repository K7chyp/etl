import random
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, LongType, DoubleType, DateType, StringType
from pyspark.sql.functions import udf, year, month, dayofmonth, dayofweek, when, lit, sha2, concat
from pyspark.sql import functions as F

def prepare_table(spark, database, table):
    # Создание базы данных, если не существует
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {database}")
    
    # Создание таблицы в формате Iceberg
    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {database}.{table} (
        transaction_id LONG,
        user_id LONG,
        transaction_date DATE,
        amount DOUBLE,
        merchant_id LONG,
        category STRING,
        amount_category STRING,
        transaction_season STRING,
        user_age_group STRING,
        is_weekend BOOLEAN,
        transaction_year INT,
        transaction_month INT,
        user_id_hash STRING,
        merchant_category STRING
    ) USING iceberg
    PARTITIONED BY (transaction_year, transaction_month)
    """
    spark.sql(create_table_sql)
    
    # Очистка таблицы (если нужно)
    # spark.sql(f"TRUNCATE TABLE {database}.{table}")

def generate_data(spark, num_records):
    # Схема данных
    schema = StructType([
        StructField("transaction_id", LongType(), False),
        StructField("user_id", LongType(), False),
        StructField("transaction_date", DateType(), False),
        StructField("amount", DoubleType(), False),
        StructField("merchant_id", LongType(), False),
        StructField("category", StringType(), False)
    ])
    
    # Параметры генерации
    categories = ["food", "electronics", "transport", "shopping", 
                 "entertainment", "utilities", "health", "education", "travel"]
    start_date = datetime(2025, 1, 1)
    end_date = datetime(2025, 6, 1)
    date_range = (end_date - start_date).days
    
    # Генерация данных
    data = []
    for i in range(1, num_records + 1):
        user_id = random.randint(1001, 1100)
        days_diff = random.randint(0, date_range)
        transaction_date = (start_date + timedelta(days=days_diff)).date()
        amount = round(random.uniform(5.0, 500.0), 2)
        merchant_id = random.randint(5001, 5100)
        category = random.choice(categories)
        
        data.append((
            i, 
            user_id, 
            transaction_date, 
            float(amount), 
            merchant_id, 
            category
        ))
    
    # Создание DataFrame
    return spark.createDataFrame(data, schema=schema)

def transform_data(df):
    """Комплексные трансформации данных"""
    
    # 1. Категоризация суммы
    df = df.withColumn(
        "amount_category",
        when(F.col("amount") < 50, "small")
        .when(F.col("amount") < 200, "medium")
        .otherwise("large")
    )
    
    # 2. Определение сезона
    def get_season(date):
        month = date.month
        if month in [12, 1, 2]: return "winter"
        if month in [3, 4, 5]: return "spring"
        if month in [6, 7, 8]: return "summer"
        return "autumn"
    
    season_udf = udf(get_season, StringType())
    df = df.withColumn("transaction_season", season_udf(F.col("transaction_date")))
    
    # 3. Возрастная группа пользователя (на основе ID)
    df = df.withColumn(
        "user_age_group",
        when(F.col("user_id") % 10 < 3, "18-25")
        .when(F.col("user_id") % 10 < 6, "26-35")
        .when(F.col("user_id") % 10 < 9, "36-50")
        .otherwise("50+")
    )
    
    # 4. Определение выходных дней
    df = df.withColumn(
        "is_weekend",
        F.dayofweek(F.col("transaction_date")).isin([1, 7])  # 1=Вс, 7=Сб
    )
    
    # 5. Извлечение года и месяца
    df = df.withColumn("transaction_year", F.year(F.col("transaction_date")))
    df = df.withColumn("transaction_month", F.month(F.col("transaction_date")))
    
    # 6. Хеширование идентификатора пользователя
    df = df.withColumn("user_id_hash", F.sha2(F.col("user_id").cast("string"), 256))
    
    # 7. Категоризация мерчантов
    merchant_categories = {
        5001: "groceries", 5020: "electronics", 5030: "transport", 
        5040: "clothing", 5050: "entertainment", 5060: "utilities",
        5070: "pharmacy", 5080: "education", 5090: "travel"
    }
    
    def map_merchant_category(merchant_id):
        base = (merchant_id // 10) * 10
        return merchant_categories.get(base, "other")
    
    merchant_udf = udf(map_merchant_category, StringType())
    df = df.withColumn("merchant_category", merchant_udf(F.col("merchant_id")))
    
    # 8. Создание композитного ключа
    df = df.withColumn(
        "composite_key",
        F.concat(F.col("user_id_hash"), F.lit("_"), F.col("transaction_date"))
    )
    
    # 9. Фильтрация некорректных данных
    df = df.filter(F.col("amount") > 0)
    
    # 10. Добавление флага премиум-транзакции
    df = df.withColumn(
        "is_premium",
        (F.col("amount") > 300) & (F.col("category").isin(["electronics", "travel"]))
    )
    
    return df

def write_data(spark, database, table, df):
    # Оптимизированная запись данных в таблицу Iceberg
    table_full_name = f"{database}.{table}"
    
    # Запись с оптимизацией для партиционированных данных
    (df.write
     .format("iceberg")
     .mode("append")
     .partitionBy("transaction_year", "transaction_month")
     .saveAsTable(table_full_name))

def main():
    # Конфигурация
    database = "etl_database"
    table = "transactions_v2"
    num_records = 100_000  # 100 тысяч записей
    
    # Создание Spark-сессии с поддержкой Iceberg
    spark = (
        SparkSession.builder
        .appName('transactions_etl')
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")
        .config("spark.sql.catalog.spark_catalog.type", "hive")
        .config("spark.sql.optimizer.dynamicPartitionPruning.enabled", "true")
        .enableHiveSupport()
        .getOrCreate()
    )
    
    # Подготовка таблицы
    prepare_table(spark, database, table)
    
    # Генерация данных
    raw_df = generate_data(spark, num_records)
    
    # Трансформация данных
    transformed_df = transform_data(raw_df)
    
    # Запись данных
    write_data(spark, database, table, transformed_df)
    
    spark.stop()

if __name__ == '__main__':
    main()