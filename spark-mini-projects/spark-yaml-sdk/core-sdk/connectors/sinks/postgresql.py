from connectors.base import Sink
from pyspark.sql import DataFrame, SparkSession
from typing import Dict, Any


class PostgreSQLSink(Sink):
    """Sink implementation for PostgreSQL."""
    
    def __init__(self, spark: SparkSession, config: Dict[str, Any]):
        super().__init__(spark, config)
        self.table = config.get('table')
        self.url = config.get('url')
        self.properties = config.get('properties', {})
        self.mode = config.get('mode', 'append')
        
        # Validate required config
        if not self.table or not self.url:
            raise ValueError("PostgreSQL sink requires 'table' and 'url' to be specified")
    
    def write(self, df: DataFrame) -> None:
        """Write DataFrame to PostgreSQL table."""
        # Convert properties dict to Java Properties
        props = {}
        for key, value in self.properties.items():
            props[key] = str(value)
            
        df.write \
            .format("jdbc") \
            .option("url", self.url) \
            .option("dbtable", self.table) \
            .mode(self.mode) \
            .options(**props) \
            .save()
    
    @classmethod
    def get_sink_type(cls) -> str:
        return "postgresql"