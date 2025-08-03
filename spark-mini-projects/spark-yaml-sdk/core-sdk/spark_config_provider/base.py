from abc import ABC, abstractmethod
from typing import Dict, Any

class SparkConfigProvider(ABC):
    """Base class for source/sink configuration providers."""
    
    @abstractmethod
    def get_spark_configs(self) -> Dict[str, str]:
        """Get required Spark configurations."""
        pass
    
    @abstractmethod
    def get_required_jars(self) -> list[str]:
        """Get required jar dependencies."""
        pass