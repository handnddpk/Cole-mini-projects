from connectors.base import Sink
from pyspark.sql import DataFrame, SparkSession
from typing import Dict, Any


class ClickhouseSink(Sink):
    """Sink implementation for Clickhouse."""
    
    def __init__(self, spark: SparkSession, config: Dict[str, Any]):
        super().__init__(spark, config)
        self.url = config.get('url')
        self.table = config.get('table')
        self.database = config.get('database')
        self.properties = config.get('properties', {})
        self.mode = config.get('mode', 'append')
        
        # Validate required config
        if not self.url or not self.table or not self.database:
            raise ValueError("Clickhouse sink requires 'url', 'table', and 'database' to be specified")
    
    def write(self, df: DataFrame) -> None:
        """Write DataFrame to Clickhouse table."""
        # Prepare JDBC URL with database
        jdbc_url = f"{self.url}/{self.database}"
        
        # Convert properties dict to options
        props = {}
        for key, value in self.properties.items():
            props[key] = str(value)
        
        # Add Clickhouse-specific driver if not specified
        if "driver" not in props:
            props["driver"] = "com.clickhouse.jdbc.ClickHouseDriver"
            
        df.write \
            .format("jdbc") \
            .option("url", jdbc_url) \
            .option("dbtable", self.table) \
            .mode(self.mode) \
            .options(**props) \
            .save()
    
    @classmethod
    def get_sink_type(cls) -> str:
        return "clickhouse"