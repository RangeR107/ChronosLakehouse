"""Iceberg smoke test from the spec ("Getting started: week 1").

It proves the local stack can do every hard mechanism the pipeline relies on: create a
format-version 2 table, MERGE into it, write to a branch with spark.wap.branch, read that branch
three ways, and fast-forward main to it (write-audit-publish).

The first count matters most. The pipeline's jobs assume that while spark.wap.branch is set,
their reads come from the run's branch too. If that count is 1, the design must change: every
job would have to read its branch explicitly (spec, Publishing: run lifecycle, step 2).
"""
TABLE = "local.db.smoke"


def count(spark, sql):
    return spark.sql(sql).collect()[0][0]


def test_iceberg_merge_branch_and_fast_forward(spark):
    spark.sql("CREATE NAMESPACE IF NOT EXISTS local.db")
    spark.sql(f"DROP TABLE IF EXISTS {TABLE}")
    spark.sql(f"""
        CREATE TABLE {TABLE} (id INT, status STRING) USING iceberg
        TBLPROPERTIES ('format-version' = '2', 'write.wap.enabled' = 'true')
    """)
    assert spark.sql(f"SHOW TBLPROPERTIES {TABLE} ('format-version')").collect()[0][1] == "2"

    spark.sql(f"INSERT INTO {TABLE} VALUES (1, 'placed')")
    spark.sql(f"""
        MERGE INTO {TABLE} t
        USING (SELECT 1 AS id, 'delivered' AS status) s
        ON t.id = s.id
        WHEN MATCHED THEN UPDATE SET t.status = s.status
        WHEN NOT MATCHED THEN INSERT *
    """)
    rows = [tuple(r) for r in spark.sql(f"SELECT id, status FROM {TABLE}").collect()]
    assert rows == [(1, "delivered")], "MERGE should update the matched row in place"

    spark.sql(f"ALTER TABLE {TABLE} CREATE BRANCH audit")
    try:
        spark.sql("SET spark.wap.branch = audit")
        spark.sql(f"INSERT INTO {TABLE} VALUES (2, 'placed')")
        assert count(spark, f"SELECT count(*) FROM {TABLE}") == 2, (
            "Reads do NOT follow spark.wap.branch. Stop: jobs must read their branch "
            "explicitly, which changes the design."
        )
    finally:
        # Never leak the branch setting into other tests, even if the assertion fails.
        spark.sql("RESET spark.wap.branch")

    assert count(spark, f"SELECT count(*) FROM {TABLE}") == 1, "main must be untouched"
    assert count(spark, f"SELECT count(*) FROM {TABLE} VERSION AS OF 'audit'") == 2
    assert count(spark, f"SELECT count(*) FROM {TABLE}.branch_audit") == 2

    spark.sql("CALL local.system.fast_forward('db.smoke', 'main', 'audit')")
    assert count(spark, f"SELECT count(*) FROM {TABLE}") == 2, "main should now match the branch"
