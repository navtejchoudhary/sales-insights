"""One way to get a SparkSession, on a Mac or on Databricks.

Pipeline code must always call get_spark() and never build its own session,
so the same code runs unchanged in both places (portability rule 4).
"""

from __future__ import annotations

from pyspark.sql import SparkSession

from sales_insights.common.config import Config, load_config


def _running_on_databricks() -> bool:
    import os

    return "DATABRICKS_RUNTIME_VERSION" in os.environ


def get_spark(cfg: Config | None = None, app_name: str = "sales-insights") -> SparkSession:
    cfg = cfg or load_config()

    if _running_on_databricks():
        # Databricks already provides a session with Delta and Unity Catalog.
        return SparkSession.builder.getOrCreate()

    # Local Mac: PySpark + delta-spark, tables stored under lake/
    from delta import configure_spark_with_delta_pip

    warehouse = cfg.path(cfg.lake_path or "lake")
    builder = (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.warehouse.dir", warehouse)
        .config("spark.sql.session.timeZone", cfg.business.get("timezone", "Asia/Colombo"))
        .config("spark.sql.shuffle.partitions", "4")  # small local data; keeps tests fast
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.driver.memory", "2g")  # default 1 GB ran at 95% heap during bronze
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def ensure_schemas(spark: SparkSession, cfg: Config) -> None:
    """Create bronze/silver/gold/ops schemas if they don't exist."""
    for layer in cfg.schemas:
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {cfg.schema(layer)}")
