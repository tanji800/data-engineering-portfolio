from pathlib import Path
from pyspark.sql import SparkSession

from pyspark_advanced.pipeline import run_pipeline


def main() -> None:
    root = Path(__file__).resolve().parent
    output_path = str(root / "output" / "curated_orders")

    spark = (
        SparkSession.builder
        .appName("PySparkAdvancedEngineering")
        .master("local[*]")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    try:
        result = run_pipeline(spark, output_path)
        print("\nCurated output preview:")
        result.show(truncate=False)
        print(f"\nParquet output written to: {output_path}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
