import json
import os
from typing import Dict, Any, Optional


class ConfigParser:
    """Parses and validates configuration files for Spark ETL jobs."""
    
    def __init__(self, config_path: str):
        self.config_path = config_path
        self.config_data = None
        
    def parse(self) -> Dict[str, Any]:
        """Parse the config file and return the configuration dictionary."""
        if self.config_path.endswith('.json'):
            return self._parse_json()
        else:
            raise ValueError(f"Unsupported config format: {self.config_path}")
            
    def _parse_json(self) -> Dict[str, Any]:
        """Parse a JSON config file."""
        with open(self.config_path, 'r') as f:
            self.config_data = json.load(f)
        return self.config_data
    
    def get_sql_path(self) -> Optional[str]:
        """Get the path to SQL file if specified in the config."""
        if not self.config_data:
            self.parse()
        
        sql_path = self.config_data.get('sql_file')
        if sql_path and not os.path.isabs(sql_path):
            # If relative path, resolve relative to config file location
            base_dir = os.path.dirname(os.path.abspath(self.config_path))
            return os.path.join(base_dir, sql_path)
        return sql_path
    
    def get_spark_configs(self) -> Dict[str, Any]:
        """Extract Spark configuration from the parsed config."""
        if not self.config_data:
            self.parse()
        
        return self.config_data.get('spark_config', {})
    
    def get_sources(self) -> Dict[str, Any]:
        """Extract source configurations."""
        if not self.config_data:
            self.parse()
        
        return self.config_data.get('sources', {})
    
    def get_sinks(self) -> Dict[str, Any]:
        """Extract sink configurations."""
        if not self.config_data:
            self.parse()
        
        return self.config_data.get('sinks', {})
    
    def get_transformations(self) -> Dict[str, Any]:
        """Extract transformation configurations."""
        if not self.config_data:
            self.parse()
        
        return self.config_data.get('transformations', {})