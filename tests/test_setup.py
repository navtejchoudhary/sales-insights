"""Setup checks: config loads, names resolve per profile, Spark + Delta work."""

from sales_insights.common.config import load_config


def test_local_profile_names(cfg):
    assert cfg.profile == "local"
    assert cfg.table("gold", "fact_sales") == "gold.fact_sales"


def test_dev_profile_uses_unity_catalog():
    dev = load_config("dev")
    assert dev.table("gold", "fact_sales") == "sales_dev.gold.fact_sales"
    assert dev.landing_path.startswith("/Volumes/")


def test_business_settings(cfg):
    assert cfg.business["fiscal_year_start_month"] == 4
    assert cfg.business["reconciliation_tolerance_pct"] == 0.1


def test_spark_delta_roundtrip(spark, tmp_path):
    path = str(tmp_path / "delta_check")
    spark.range(3).write.format("delta").mode("overwrite").save(path)
    ids = sorted(r.id for r in spark.read.format("delta").load(path).collect())
    assert ids == [0, 1, 2]
