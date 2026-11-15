from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def required_field_violations(df: DataFrame, required_columns: list[str]) -> dict[str, int]:
    """Count null values in required columns. Triggers one aggregate action."""
    expressions = [
        F.sum(F.when(F.col(name).isNull(), 1).otherwise(0)).alias(name)
        for name in required_columns
    ]
    row = df.agg(*expressions).first()
    return {name: int(row[name] or 0) for name in required_columns}


def invalid_order_amount_count(df: DataFrame) -> int:
    """Count negative amounts; null amounts are handled separately."""
    return df.filter(F.col("amount") < 0).count()
