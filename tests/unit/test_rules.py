"""DataSentinel rule checks that need no database."""
from sentinel.checks.rules import grain_checks

TABLE = "local.rules.grain"


def test_grain_checks_report_nulls_and_duplicates(spark):
    df = spark.createDataFrame([("a",), ("a",), (None,)], "k string")
    results = {r.check: r for r in grain_checks(df, "t", ["k"])}
    assert results["ExpectColumnValuesToNotBeNull:k"].status == "fail"
    assert results["ExpectColumnValuesToBeUnique:k"].status == "fail"


def test_grain_checks_leave_no_cached_table_behind(spark):
    """A check must never change what later reads see.

    GX caches what it validates by default, and Spark then serves later reads of the same table
    from that cache, even reads of another branch. That once made a run build gold from the
    previous day's silver (docs/decisions.md). The steps follow two daily runs.
    """
    spark.sql("CREATE NAMESPACE IF NOT EXISTS local.rules")
    spark.sql(f"DROP TABLE IF EXISTS {TABLE}")
    spark.sql(f"CREATE TABLE {TABLE} (k string) USING iceberg "
              "TBLPROPERTIES ('format-version' = '2', 'write.wap.enabled' = 'true')")
    spark.sql(f"DELETE FROM {TABLE} WHERE false")
    try:
        for day, key in ((1, "a"), (2, "b")):
            branch = f"run_{day}"
            spark.sql(f"ALTER TABLE {TABLE} CREATE BRANCH {branch}")
            spark.conf.set("spark.wap.branch", branch)
            spark.createDataFrame([(key,)], "k string").createOrReplaceTempView("new_rows")
            spark.sql(f"MERGE INTO {TABLE} t USING new_rows s ON t.k = s.k "
                      "WHEN NOT MATCHED THEN INSERT *")
            assert spark.table(TABLE).count() == day, f"day {day} reads a stale table"
            grain_checks(spark.table(TABLE), "grain", ["k"])  # the audit
            spark.conf.unset("spark.wap.branch")
            spark.sql(f"CALL local.system.fast_forward('rules.grain', 'main', '{branch}')")
    finally:
        spark.conf.unset("spark.wap.branch")
