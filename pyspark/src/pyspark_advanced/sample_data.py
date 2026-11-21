from datetime import date, datetime
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType, DateType,
    TimestampType
)


def create_sample_data(spark: SparkSession) -> tuple[DataFrame, DataFrame, DataFrame]:
    """Create deterministic synthetic customers, orders, and order events."""
    customer_schema = StructType([
        StructField("customer_id", IntegerType(), False),
        StructField("customer_name", StringType(), False),
        StructField("region", StringType(), True),
        StructField("segment", StringType(), True),
    ])
    customers = spark.createDataFrame([
        (1, "Asha", "South", "Retail"),
        (2, "Ravi", "West", "Business"),
        (3, "Mina", "North", "Retail"),
        (4, "John", "East", "Business"),
    ], customer_schema)

    order_schema = StructType([
        StructField("order_id", StringType(), False),
        StructField("customer_id", IntegerType(), False),
        StructField("order_date", DateType(), False),
        StructField("amount", DoubleType(), True),
        StructField("status", StringType(), True),
    ])
    orders = spark.createDataFrame([
        ("O-100", 1, date(2025, 1, 2), 120.0, "PLACED"),
        ("O-101", 1, date(2025, 1, 5), 80.0, "SHIPPED"),
        ("O-102", 2, date(2025, 1, 3), 250.0, "SHIPPED"),
        ("O-103", 2, date(2025, 1, 8), None, "PLACED"),
        ("O-104", 3, date(2025, 1, 4), 60.0, "CANCELLED"),
        ("O-105", 3, date(2025, 1, 9), 140.0, "SHIPPED"),
    ], order_schema)

    event_schema = StructType([
        StructField("order_id", StringType(), False),
        StructField("event_status", StringType(), True),
        StructField("event_timestamp", TimestampType(), False),
        StructField("event_sequence", IntegerType(), False),
    ])
    events = spark.createDataFrame([
        ("O-100", "PLACED", datetime(2025, 1, 2, 9, 0), 1),
        ("O-100", "SHIPPED", datetime(2025, 1, 3, 10, 0), 2),
        ("O-101", "PLACED", datetime(2025, 1, 5, 8, 0), 1),
        ("O-101", "SHIPPED", datetime(2025, 1, 6, 8, 0), 2),
        ("O-102", "PLACED", datetime(2025, 1, 3, 11, 0), 1),
        ("O-103", "PLACED", datetime(2025, 1, 8, 12, 0), 1),
        ("O-104", "PLACED", datetime(2025, 1, 4, 9, 0), 1),
        ("O-104", "CANCELLED", datetime(2025, 1, 5, 9, 0), 2),
        ("O-105", "PLACED", datetime(2025, 1, 9, 9, 0), 1),
    ], event_schema)

    return customers, orders, events
