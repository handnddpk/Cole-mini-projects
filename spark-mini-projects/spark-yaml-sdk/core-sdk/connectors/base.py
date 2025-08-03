from abc import ABC, abstractmethod
from pyspark.sql import DataFrame, SparkSession
from typing import Dict, Any, Optional


class Source(ABC):
    """Abstract base class for all data sources."""
    
    def __init__(self, spark: SparkSession, config: Dict[str, Any]):
        """
        Initialize the source.
        
        Args:
            spark: Spark session
            config: Source configuration
        """
        self.spark = spark
        self.config = config
    
    @abstractmethod
    def read(self) -> DataFrame:
        """Read data from the source and return a DataFrame."""
        pass
    
    @classmethod
    def get_source_type(cls) -> str:
        """Get the type identifier for this source."""
        return cls.__name__.lower()


class Sink(ABC):
    """Abstract base class for all data sinks."""
    
    def __init__(self, spark: SparkSession, config: Dict[str, Any]):
        """
        Initialize the sink.
        
        Args:
            spark: Spark session
            config: Sink configuration
        """
        self.spark = spark
        self.config = config
    
    @abstractmethod
    def write(self, df: DataFrame) -> None:
        """Write the DataFrame to the sink."""
        pass
    
    @classmethod
    def get_sink_type(cls) -> str:
        """Get the type identifier for this sink."""
        return cls.__name__.lower()
