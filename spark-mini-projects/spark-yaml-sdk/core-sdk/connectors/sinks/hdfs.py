from connectors.base import Sink
from pyspark.sql import DataFrame, SparkSession
from typing import Dict, Any


class HDFSSink(Sink):
    """Sink implementation for Hadoop Distributed File System."""
    
    def __init__(self, spark: SparkSession, config: Dict[str, Any]):
        super().__init__(spark, config)
        self.path = config.get('path')
        self.format = config.get('format', 'parquet')
        self.mode = config.get('mode', 'overwrite')
        self.options = config.get('options', {})
        self.partition_by = config.get('partition_by', [])
        self.streaming = config.get('streaming', False)
        self.checkpoint_location = config.get('checkpoint_location')
        self.trigger = config.get('trigger', {})
        
        # Validate required config
        if not self.path:
            raise ValueError("HDFS sink requires 'path' to be specified")
        
        # For streaming mode, checkpoint location is required
        if self.streaming and not self.checkpoint_location:
            raise ValueError("HDFS streaming sink requires 'checkpoint_location' to be specified")
    
    def write(self, df: DataFrame) -> None:
        """Write DataFrame to HDFS."""
        # Set up options
        write_options = dict(self.options)
        
        if self.streaming:
            # Streaming write
            writer = df.writeStream \
                .format(self.format) \
                .outputMode(self.mode)
            
            # Add checkpoint location
            writer = writer.option("checkpointLocation", self.checkpoint_location)
            
            # Add trigger if specified
            if self.trigger:
                trigger_type = self.trigger.get('type')
                if trigger_type == 'processingTime':
                    interval = self.trigger.get('interval', '0 seconds')
                    writer = writer.trigger(processingTime=interval)
                elif trigger_type == 'once':
                    writer = writer.trigger(once=True)
                elif trigger_type == 'continuous':
                    interval = self.trigger.get('interval', '1 second')
                    writer = writer.trigger(continuous=interval)
            
            # Add partitioning if specified
            if self.partition_by:
                writer = writer.partitionBy(*self.partition_by)
            
            # Add any additional options
            for key, value in write_options.items():
                writer = writer.option(key, value)
            
            # Start the streaming query
            writer.start(self.path).awaitTermination()
        else:
            # Batch write
            writer = df.write \
                .format(self.format) \
                .mode(self.mode)
            
            # Add partitioning if specified
            if self.partition_by:
                writer = writer.partitionBy(*self.partition_by)
            
            # Add any additional options
            for key, value in write_options.items():
                writer = writer.option(key, value)
            
            # Execute the write
            writer.save(self.path)
    
    @classmethod
    def get_sink_type(cls) -> str:
        return "hdfs"