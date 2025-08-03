from connectors.base import Sink
from pyspark.sql import DataFrame, SparkSession
from typing import Dict, Any


class FileSink(Sink):
    """Base sink implementation for file-based outputs."""
    
    def __init__(self, spark: SparkSession, config: Dict[str, Any]):
        super().__init__(spark, config)
        self.path = config.get('path')
        self.format = config.get('format', 'parquet')
        self.mode = config.get('mode', 'overwrite')
        self.options = config.get('options', {})
        self.partition_by = config.get('partition_by', [])
        
        # Validate required config
        if not self.path:
            raise ValueError("File sink requires 'path' to be specified")
    
    def write(self, df: DataFrame) -> None:
        """Write DataFrame to file storage."""
        writer = df.write \
            .format(self.format) \
            .mode(self.mode) \
            .options(**self.options)
            
        if self.partition_by:
            writer = writer.partitionBy(*self.partition_by)
            
        writer.save(self.path)
    
    @classmethod
    def get_sink_type(cls) -> str:
        return "file"