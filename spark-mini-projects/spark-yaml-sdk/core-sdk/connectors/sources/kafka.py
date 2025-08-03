from connectors.base import Source
from pyspark.sql import DataFrame, SparkSession
from typing import Dict, Any, List, Optional


class KafkaSource(Source):
    """Source implementation for Apache Kafka streaming."""
    
    def __init__(self, spark: SparkSession, config: Dict[str, Any]):
        super().__init__(spark, config)
        self.bootstrap_servers = config.get('bootstrap_servers')
        self.topics = config.get('topics')
        self.subscribe_type = config.get('subscribe_type', 'subscribe')  # subscribe, subscribePattern, assign
        self.starting_offsets = config.get('starting_offsets', 'latest')  # latest, earliest, or JSON string
        self.ending_offsets = config.get('ending_offsets')  # latest, or JSON string
        self.options = config.get('options', {})
        self.streaming = config.get('streaming', True)
        
        # Validate required config
        if not self.bootstrap_servers:
            raise ValueError("Kafka source requires 'bootstrap_servers' to be specified")
        
        if not self.topics:
            raise ValueError("Kafka source requires 'topics' to be specified")
    
    def read(self) -> DataFrame:
        """Read data from Kafka topics."""
        # Set up base options
        options = {
            "kafka.bootstrap.servers": self.bootstrap_servers,
            self.subscribe_type: self.topics,
            "startingOffsets": self.starting_offsets
        }
        
        # Add ending offsets if specified (batch mode only)
        if self.ending_offsets and not self.streaming:
            options["endingOffsets"] = self.ending_offsets
        
        # Add any additional options
        options.update(self.options)
        
        # Choose the appropriate reader based on streaming mode
        if self.streaming:
            reader = self.spark.readStream
        else:
            reader = self.spark.read
        
        # Create and return the DataFrame
        return reader.format("kafka").options(**options).load()
    
    @classmethod
    def get_source_type(cls) -> str:
        return "kafka"