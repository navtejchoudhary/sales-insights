"""Read and write lakehouse tables the same way on a Mac and on Databricks.

Pipeline code always goes through Lake, never through paths or table names directly:

    lake = Lake(spark, cfg)
    lake.append(df, "bronze", "orders")
    df = lake.read("bronze", "orders")

- local profile: path-based Delta tables under lake_path, e.g. lake/bronze/orders
  (a plain local Spark session forgets registered tables when it stops; paths do not)
- dev profile:   Unity Catalog tables, e.g. sales_dev.bronze.orders
"""

from __future__ import annotations

from pathlib import Path

from pyspark.sql import DataFrame, SparkSession

from sales_insights.common.config import Config


class Lake:
    def __init__(self, spark: SparkSession, cfg: Config, root: str | Path | None = None):
        self.spark = spark
        self.cfg = cfg
        self.by_path = cfg.catalog is None
        self.root = Path(root) if root else (Path(cfg.path(cfg.lake_path)) if cfg.lake_path else None)

    def location(self, layer: str, name: str) -> str:
        """Folder (local) or fully qualified table name (Databricks)."""
        if self.by_path:
            return str(self.root / self.cfg.schemas[layer] / name)
        return self.cfg.table(layer, name)

    def sql_name(self, layer: str, name: str) -> str:
        """How SQL refers to the table: delta.`/abs/path` locally, catalog.schema.table on Databricks."""
        if self.by_path:
            return f"delta.`{Path(self.location(layer, name)).resolve()}`"
        return self.location(layer, name)

    def exists(self, layer: str, name: str) -> bool:
        if self.by_path:
            return (Path(self.location(layer, name)) / "_delta_log").is_dir()
        return self.spark.catalog.tableExists(self.location(layer, name))

    def read(self, layer: str, name: str) -> DataFrame:
        if self.by_path:
            return self.spark.read.format("delta").load(self.location(layer, name))
        return self.spark.table(self.location(layer, name))

    def append(self, df: DataFrame, layer: str, name: str) -> None:
        """Append rows; new columns are added to the table (schema evolution)."""
        self._write(df.write.format("delta").mode("append").option("mergeSchema", "true"), layer, name)

    def overwrite(self, df: DataFrame, layer: str, name: str) -> None:
        self._write(df.write.format("delta").mode("overwrite").option("overwriteSchema", "true"), layer, name)

    def _write(self, writer, layer: str, name: str) -> None:
        if self.by_path:
            writer.save(self.location(layer, name))
        else:
            writer.saveAsTable(self.location(layer, name))

    def describe(self, layer: str, name: str, description: str, column_comments: dict[str, str]) -> None:
        """Table and column comments in Unity Catalog (Genie and Catalog Explorer show them).

        Locally the column comments already travel inside the Delta schema (see gold_model.with_comments);
        plain local Spark has no catalog to hold a table comment, so this is a no-op there.
        """
        if self.by_path:
            return
        table = self.location(layer, name)

        def q(text: str) -> str:
            return text.replace("\\", "\\\\").replace("'", "\\'")

        self.spark.sql(f"COMMENT ON TABLE {table} IS '{q(description)}'")
        for col, text in column_comments.items():
            self.spark.sql(f"ALTER TABLE {table} ALTER COLUMN `{col}` COMMENT '{q(text)}'")
