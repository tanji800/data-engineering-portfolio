from pyspark_advanced.quality import required_field_violations, invalid_order_amount_count


def test_required_field_violations(spark):
    df = spark.createDataFrame([(1, None), (2, "x")], ["id", "name"])
    assert required_field_violations(df, ["id", "name"]) == {"id": 0, "name": 1}


def test_negative_amount_count(spark):
    df = spark.createDataFrame([(10.0,), (-2.0,), (None,)], ["amount"])
    assert invalid_order_amount_count(df) == 1
