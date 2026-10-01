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
