from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from .sample_data import create_sample_data
from .transformations import (
    latest_order_events, enrich_orders, add_customer_metrics, standardize_status
)
from .quality import required_field_violations, invalid_order_amount_count


def run_pipeline(spark: SparkSession, output_path: str) -> DataFrame:
    customers, orders, events = create_sample_data(spark)

    print("Required-field null counts:", required_field_violations(
        orders, ["order_id", "customer_id", "order_date"]
    ))
    print("Negative amount count:", invalid_order_amount_count(orders))

    latest_events = latest_order_events(events)
    enriched = enrich_orders(orders, customers)

    curated = (
        enriched
        .join(
            latest_events.select("order_id", F.col("event_status").alias("latest_event_status")),
            on="order_id",
            how="left"
        )
        .transform(standardize_status, column="status")
        .transform(add_customer_metrics)
        .filter(F.col("status") != "CANCELLED")
    )

    # Demonstrate persistence only because the DataFrame is both inspected and written.
    curated = curated.persist()
    try:
        print("Curated row count:", curated.count())
        print("\nPhysical plan:")
        curated.explain("formatted")

        # Repartition by a useful key for this demonstration. This causes a shuffle.
        output = curated.repartition(4, "region")
        output.write.mode("overwrite").partitionBy("region").parquet(output_path)
    finally:
        curated.unpersist()

    return curated
