from typing import Dict, Any, List, Optional
import os
import importlib

from pyspark.sql import SparkSession, DataFrame
from config.parser import ConfigParser
from connectors.base import Source, Sink
from spark_config_provider import PROVIDERS, get_provider


class JobExecutor:
    """Executes Spark ETL jobs based on configuration."""
    
    def __init__(self, config_path: str):
        """
        Initialize the job executor.
        
        Args:
            config_path: Path to the job configuration file
        """
        self.config_parser = ConfigParser(config_path)
        self.config = self.config_parser.parse()
        self.job_name = self.config.get('job_name', 'spark-etl-job')
        self.job_type = self.config.get('job_type', 'batch')
        self.spark_configs = self._get_all_spark_configs()
        self.spark = None
        self.source_registry = {}
        self.sink_registry = {}
        
        # Register built-in sources and sinks
        self._register_connectors()

    def _get_all_spark_configs(self) -> Dict[str, str]:
        """Get all Spark configurations including source/sink specific ones."""
        # Get base configs from config file
        configs = self.config_parser.get_spark_configs() or {}
        
        # Get required jars and configs from sources
        sources = self.config_parser.get_sources()
        sinks = self.config_parser.get_sinks()
        
        all_required_jars = set()
        
        # Process sources
        for source_config in sources.values():
            source_type = source_config.get('type')
            if source_type:
                try:
                    provider = get_provider(source_type)
                    configs.update(provider.get_spark_configs())
                    all_required_jars.update(provider.get_required_jars())
                except ValueError:
                    continue
        
        # Process sinks
        for sink_config in sinks.values():
            sink_type = sink_config.get('type')
            if sink_type:
                try:
                    provider = get_provider(sink_type)
                    configs.update(provider.get_spark_configs())
                    all_required_jars.update(provider.get_required_jars())
                except ValueError:
                    continue
        
        # Add collected jars to spark.jars.packages
        # if all_required_jars:
        #     existing_jars = configs.get('spark.jars.packages', '')
        #     jar_list = list(filter(None, [existing_jars, *all_required_jars]))
        #     configs['spark.jars.packages'] = ','.join(jar_list)
        
        return configs
    
    def _register_connectors(self):
        """Register all available source and sink implementations."""
        # Implementation to dynamically discover and register connectors
        connectors_path = os.path.join(os.path.dirname(__file__), '..', 'connectors')
        
        # Register sources
        sources_path = os.path.join(connectors_path, 'sources')
        for filename in os.listdir(sources_path):
            if filename.endswith('.py') and not filename.startswith('__'):
                module_name = filename[:-3]  # Remove .py extension
                module_path = f"connectors.sources.{module_name}"
                module = importlib.import_module(module_path)
                
                # Find source classes in the module
                for attr_name in dir(module):
                    attr = getattr(module, attr_name)
                    if isinstance(attr, type) and issubclass(attr, Source) and attr != Source:
                        self.source_registry[attr.get_source_type()] = attr
        
        # Register sinks
        sinks_path = os.path.join(connectors_path, 'sinks')
        for filename in os.listdir(sinks_path):
            if filename.endswith('.py') and not filename.startswith('__'):
                module_name = filename[:-3]  # Remove .py extension
                module_path = f"connectors.sinks.{module_name}"
                module = importlib.import_module(module_path)
                
                # Find sink classes in the module
                for attr_name in dir(module):
                    attr = getattr(module, attr_name)
                    if isinstance(attr, type) and issubclass(attr, Sink) and attr != Sink:
                        self.sink_registry[attr.get_sink_type()] = attr
    
    def initialize_spark(self):
        """Initialize the Spark session for this job."""
        from core.session import SparkSessionManager
        
        session_manager = SparkSessionManager(
            app_name=self.job_name,
            configs=self.spark_configs
        )
        self.spark = session_manager.create_session()
        return self.spark
    
    def _create_source(self, name: str, config: Dict[str, Any]) -> Source:
        """Create a source instance from config."""
        source_type = config.get('type')
        if not source_type:
            raise ValueError(f"Source '{name}' must specify a 'type'")
        
        source_class = self.source_registry.get(source_type)
        if not source_class:
            raise ValueError(f"Unknown source type: {source_type}")
        
        return source_class(self.spark, config)
    
    def _create_sink(self, name: str, config: Dict[str, Any]) -> Sink:
        """Create a sink instance from config."""
        sink_type = config.get('type')
        if not sink_type:
            raise ValueError(f"Sink '{name}' must specify a 'type'")
        
        sink_class = self.sink_registry.get(sink_type)
        if not sink_class:
            raise ValueError(f"Unknown sink type: {sink_type}")
        
        return sink_class(self.spark, config)
    
    def execute(self):
        """Execute the job based on configuration."""
        if not self.spark:
            self.initialize_spark()
        
        # Read from sources
        source_dfs = {}
        for src_name, src_config in self.config_parser.get_sources().items():
            source = self._create_source(src_name, src_config)
            source_dfs[src_name] = source.read()
        
        # Apply transformations
        transformed_dfs = self._apply_transformations(source_dfs)
        
        # Write to sinks
        for sink_name, sink_config in self.config_parser.get_sinks().items():
            sink = self._create_sink(sink_name, sink_config)
            df_name = sink_config.get('input')
            if not df_name or df_name not in transformed_dfs:
                raise ValueError(f"Sink '{sink_name}' references unknown DataFrame: {df_name}")
            
            sink.write(transformed_dfs[df_name])
    
    def _apply_transformations(self, source_dfs: Dict[str, DataFrame]) -> Dict[str, DataFrame]:
        """Apply transformations to source DataFrames."""
        from core.transform import TransformationManager
        
        # Create a copy of source_dfs to start with
        result_dfs = source_dfs.copy()
        
        # Check if we have SQL file
        sql_path = self.config_parser.get_sql_path()
        if sql_path:
            transform_manager = TransformationManager(self.spark, sql_file=sql_path)
            # Register all source dataframes as temporary views
            for name, df in source_dfs.items():
                df.createOrReplaceTempView(name)
            
            # Execute SQL and add results to result_dfs
            sql_results = transform_manager.execute_sql_file()
            for name, df in sql_results.items():
                result_dfs[name] = df
        
        # Apply JSON-defined transformations
        transformations = self.config_parser.get_transformations()
        if transformations:
            transform_manager = TransformationManager(self.spark)
            transformed = transform_manager.apply_transformations(source_dfs, transformations)
            # Add transformed DFs to results
            result_dfs.update(transformed)
        
        return result_dfs