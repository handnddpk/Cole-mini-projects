from connectors.base import Source
from pyspark.sql import DataFrame, SparkSession
from typing import Dict, Any, Optional


class MongoDBSource(Source):
    """Source implementation for MongoDB databases."""
    
    def __init__(self, spark: SparkSession, config: Dict[str, Any]):
        super().__init__(spark, config)
        self.uri = config.get('uri')
        self.database = config.get('database')
        self.collection = config.get('collection')
        self.pipeline = config.get('pipeline')
        self.options = config.get('options', {})
        
        # Validate required config
        if not self.uri:
            raise ValueError("MongoDB source requires 'uri' to be specified")
        
        if not self.database:
            raise ValueError("MongoDB source requires 'database' to be specified")
            
        if not self.collection:
            raise ValueError("MongoDB source requires 'collection' to be specified")
    
    def read(self) -> DataFrame:
        """Read data from MongoDB source."""
        # Start with base options
        read_options = {
            "connection.uri": self.uri,
            "database": self.database,
            "collection": self.collection
        }
        
        # Add pipeline if specified
        if self.pipeline:
            read_options["pipeline"] = str(self.pipeline)
        
        # Add any additional options
        read_options.update(self.options)
        
        return (self.spark.read
                .format("mongodb")
                .options(**read_options)
                .load())
    
    @classmethod
    def get_source_type(cls) -> str:
        return "mongodb"