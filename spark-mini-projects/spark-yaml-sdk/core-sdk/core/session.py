from pyspark.sql import SparkSession
from typing import Dict, Any, Optional


class SparkSessionManager:
    """Manages Spark sessions for ETL jobs."""
    
    def __init__(self, app_name: str, configs: Dict[str, Any] = None):
        """
        Initialize the session manager.
        
        Args:
            app_name: Name of the Spark application
            configs: Dictionary of Spark configurations
        """
        self.app_name = app_name
        self.configs = configs or {}
        self._session = None
    
    def create_session(self) -> SparkSession:
        """Create and configure a new Spark session."""
        builder = SparkSession.builder.appName(self.app_name)
        
        # Apply all configurations
        for key, value in self.configs.items():
            if value is not None:  # Only set non-None values
                builder = builder.config(key, str(value))
        
        self._session = builder.getOrCreate()
        return self._session
    
    def get_session(self) -> SparkSession:
        """Get the current Spark session or create a new one."""
        if self._session is None:
            return self.create_session()
        return self._session
    
    def stop_session(self) -> None:
        """Stop the current Spark session."""
        if self._session:
            self._session.stop()
            self._session = None