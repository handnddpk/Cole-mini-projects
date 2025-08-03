from connectors.base import Sink
from pyspark.sql import DataFrame, SparkSession
from typing import Dict, Any


class MongoDBSink(Sink):
    """Sink implementation for MongoDB."""
    
    def __init__(self, spark: SparkSession, config: Dict[str, Any]):
        super().__init__(spark, config)
        self.uri = config.get('uri')
        self.database = config.get('database')
        self.collection = config.get('collection')
        self.mode = config.get('mode', 'append')
        self.options = config.get('options', {})
        
        # Validate required config
        if not self.uri or not self.database or not self.collection:
            raise ValueError("MongoDB sink requires 'uri', 'database', and 'collection' to be specified")
    
    def write(self, df: DataFrame) -> None:
        """Write DataFrame to MongoDB collection."""
        write_options = {
            "connection.uri": self.uri,
            "database": self.database,
            "collection": self.collection,
        }
        
        # Add any additional options
        write_options.update(self.options)
        
        df.write \
            .format("mongo") \
            .mode(self.mode) \
            .options(**write_options) \
            .save()
    
    @classmethod
    def get_sink_type(cls) -> str:
        return "mongodb"