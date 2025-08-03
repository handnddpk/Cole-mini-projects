from connectors.base import Source
from pyspark.sql import DataFrame, SparkSession
from typing import Dict, Any, Optional


class IcebergSource(Source):
    """Source implementation for Apache Iceberg tables."""
    
    def __init__(self, spark: SparkSession, config: Dict[str, Any]):
        super().__init__(spark, config)
        self.table = config.get('table')
        self.warehouse = config.get('warehouse')
        self.snapshot_id = config.get('snapshot_id')
        self.as_of = config.get('as_of')
        self.options = config.get('options', {})
        
        # Validate required config
        if not self.table:
            raise ValueError("Iceberg source requires 'table' to be specified")
        
        if not self.warehouse:
            raise ValueError("Iceberg source requires 'warehouse' to be specified")
        
        # Configure catalog if not already done
        self._configure_catalog()
    
    def _configure_catalog(self):
        """Configure the Iceberg catalog if needed."""
        current_catalog = self.spark.conf.get("spark.sql.default.catalog", None)
        
        if not current_catalog:
            self.spark.conf.set(
                "spark.sql.catalog.spark_catalog",
                "org.apache.iceberg.spark.SparkSessionCatalog"
            )
            self.spark.conf.set(
                "spark.sql.catalog.spark_catalog.type",
                "hadoop"
            )
            self.spark.conf.set(
                "spark.sql.catalog.spark_catalog.warehouse",
                self.warehouse
            )
    
    def read(self) -> DataFrame:
        """Read data from Iceberg table."""
        reader = self.spark.read.format("iceberg")
        
        # Apply snapshot id if specified
        if self.snapshot_id:
            reader = reader.option("snapshot-id", str(self.snapshot_id))
        
        # Apply as of timestamp if specified
        if self.as_of:
            reader = reader.option("as-of-timestamp", str(self.as_of))
        
        # Apply all additional read options
        for key, value in self.options.items():
            reader = reader.option(key, str(value))
        
        return reader.load(self.table)
    
    @classmethod
    def get_source_type(cls) -> str:
        return "iceberg"