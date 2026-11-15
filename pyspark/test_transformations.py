from datetime import datetime, date
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType, DoubleType, DateType

from pyspark_advanced.transformations import (
    latest_order_events, add_customer_metrics, standardize_status
)


def test_latest_event_per_order(spark):
    schema = StructType([
        StructField("order_id", StringType(), False),
        StructField("event_status", StringType(), True),
        StructField("event_timestamp", TimestampType(), False),
        StructField("event_sequence", IntegerType(), False),
    ])
    df = spark.createDataFrame([
        ("A", "PLACED", datetime(2025, 1, 1), 1),
        ("A", "SHIPPED", datetime(2025, 1, 2), 2),
    ], schema)

    rows = latest_order_events(df).collect()
    assert len(rows) == 1
    assert rows[0]["event_status"] == "SHIPPED"


def test_running_total_and_order_number(spark):
    schema = StructType([
        StructField("order_id", StringType(), False),
        StructField("customer_id", IntegerType(), False),
        StructField("order_date", DateType(), False),
        StructField("amount", DoubleType(), True),
    ])
    df = spark.createDataFrame([
        ("A", 1, date(2025, 1, 1), 10.0),
        ("B", 1, date(2025, 1, 2), 15.0),
    ], schema)

    rows = add_customer_metrics(df).orderBy("order_date").collect()
    assert rows[0]["customer_order_number"] == 1
    assert rows[1]["running_customer_spend"] == 25.0


def test_standardize_status(spark):
    df = spark.createDataFrame([(" shipped ",)], ["status"])
    result = standardize_status(df).first()["status"]
    assert result == "SHIPPED"
