from connectors.base import Source
from pyspark.sql import DataFrame, SparkSession
from typing import Dict, Any, List, Optional


class JDBCSource(Source):
    """Source implementation for JDBC databases."""
    
    def __init__(self, spark: SparkSession, config: Dict[str, Any]):
        super().__init__(spark, config)
        self.url = config.get('url')
        self.table = config.get('table')
        self.query = config.get('query')
        self.properties = config.get('properties', {})
        self.partition_column = config.get('partition_column')
        self.num_partitions = config.get('num_partitions')
        self.lower_bound = config.get('lower_bound')
        self.upper_bound = config.get('upper_bound')
        
        # Validate required config
        if not self.url:
            raise ValueError("JDBC source requires 'url' to be specified")
        
        if not self.table and not self.query:
            raise ValueError("JDBC source requires either 'table' or 'query' to be specified")
    
    def read(self) -> DataFrame:
        """Read data from JDBC source."""
        # Convert properties dict to options
        props = {}
        for key, value in self.properties.items():
            props[key] = str(value)
        
        # Determine if we're using a table or query
        if self.query:
            props["dbtable"] = f"({self.query}) as query_alias"
        else:
            props["dbtable"] = self.table
        
        # Add URL
        props["url"] = self.url
        
        # Configure partitioning if specified
        if self.partition_column and self.num_partitions and self.lower_bound is not None and self.upper_bound is not None:
            props["partitionColumn"] = self.partition_column
            props["numPartitions"] = str(self.num_partitions)
            props["lowerBound"] = str(self.lower_bound)
            props["upperBound"] = str(self.upper_bound)
        
        return self.spark.read.format("jdbc").options(**props).load()
    
    @classmethod
    def get_source_type(cls) -> str:
        return "jdbc"