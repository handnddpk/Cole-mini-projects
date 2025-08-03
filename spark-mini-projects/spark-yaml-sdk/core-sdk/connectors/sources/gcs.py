from connectors.base import Source
from pyspark.sql import DataFrame, SparkSession
from typing import Dict, Any


class GCSSource(Source):
    """Source implementation for Google Cloud Storage."""
    
    def __init__(self, spark: SparkSession, config: Dict[str, Any]):
        super().__init__(spark, config)
        self.path = config.get('path')
        self.format = config.get('format', 'parquet')
        self.options = config.get('options', {})
        
        # Validate required config
        if not self.path:
            raise ValueError("GCS source requires 'path' to be specified")
    
    def read(self) -> DataFrame:
        """Read data from Google Cloud Storage."""
        reader = self.spark.read.format(self.format)
        
        # Apply all read options
        for key, value in self.options.items():
            reader = reader.option(key, value)
        
        return reader.load(self.path)
    
    @classmethod
    def get_source_type(cls) -> str:
        return "gcs"