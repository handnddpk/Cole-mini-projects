from connectors.base import Sink
from pyspark.sql import DataFrame, SparkSession
from typing import Dict, Any


class IcebergSink(Sink):
    """Sink implementation for Apache Iceberg tables."""
    
    def __init__(self, spark: SparkSession, config: Dict[str, Any]):
        super().__init__(spark, config)
        self.table_name = config.get('table')
        self.warehouse = config.get('warehouse')
        self.mode = config.get('mode', 'append')
        self.options = config.get('options', {})
        self.partition_by = config.get('partition_by', [])
        
        # Validate required config
        if not self.table_name:
            raise ValueError("Iceberg sink requires 'table' to be specified")
            
        # Set warehouse location if provided
        if self.warehouse:
            spark.conf.set("spark.sql.catalog.spark_catalog.warehouse", self.warehouse)
    
    def write(self, df: DataFrame) -> None:
        """Write DataFrame to Iceberg table."""
        write_options = {
            "format": "iceberg",
        }
        
        # Add any additional options
        write_options.update(self.options)
        
        writer = df.write \
            .format("iceberg") \
            .mode(self.mode) \
            .options(**write_options)
            
        if self.partition_by:
            writer = writer.partitionBy(*self.partition_by)
            
        writer.saveAsTable(self.table_name)
    
    @classmethod
    def get_sink_type(cls) -> str:
        return "iceberg"