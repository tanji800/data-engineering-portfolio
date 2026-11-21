from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F


def latest_order_events(events: DataFrame) -> DataFrame:
    """Return the latest event per order with deterministic tie-breaking."""
    window = (
        Window.partitionBy("order_id")
        .orderBy(F.col("event_timestamp").desc(), F.col("event_sequence").desc())
    )
    return (
        events
        .withColumn("_row_num", F.row_number().over(window))
        .filter(F.col("_row_num") == 1)
        .drop("_row_num")
    )


def enrich_orders(orders: DataFrame, customers: DataFrame) -> DataFrame:
    """Enrich orders with a small customer dimension using a broadcast join."""
    customer_dim = F.broadcast(customers.select(
        "customer_id", "customer_name", "region", "segment"
    ))
    return orders.join(customer_dim, on="customer_id", how="left")


def add_customer_metrics(orders: DataFrame) -> DataFrame:
    """Add running spend, order sequence, and customer ranking."""
    ordered_window = (
        Window.partitionBy("customer_id")
        .orderBy(F.col("order_date"), F.col("order_id"))
    )
    running_window = ordered_window.rowsBetween(
        Window.unboundedPreceding, Window.currentRow
    )
    ranking_window = Window.partitionBy("customer_id").orderBy(
        F.col("amount").desc_nulls_last(), F.col("order_id")
    )

    return (
        orders
        .withColumn("amount_clean", F.coalesce(F.col("amount"), F.lit(0.0)))
        .withColumn("customer_order_number", F.row_number().over(ordered_window))
        .withColumn("running_customer_spend",
                    F.sum("amount_clean").over(running_window))
        .withColumn("amount_rank", F.dense_rank().over(ranking_window))
    )


def standardize_status(df: DataFrame, column: str = "status") -> DataFrame:
    """Normalize status strings without mutating the input DataFrame."""
    return df.withColumn(column, F.upper(F.trim(F.col(column))))
