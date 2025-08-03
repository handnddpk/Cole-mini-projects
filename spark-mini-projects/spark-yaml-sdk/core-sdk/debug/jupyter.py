from typing import Dict, Any, Optional
import json
import os

from pyspark.sql import SparkSession
from config.parser import ConfigParser
from core.session import SparkSessionManager
from spark_config_provider import get_provider

class JupyterDebugger:
    """Helper class for debugging Spark ETL jobs in Jupyter notebooks."""
    
    def __init__(self, config_path: str):
        """
        Initialize the debugger.
        
        Args:
            config_path: Path to the job configuration file
        """
        self.config_path = config_path
        self.config_parser = ConfigParser(config_path)
        self.config = self.config_parser.parse()
        self.job_name = self.config.get('job_name', 'spark-debug-job')
        self.spark_configs = self._get_all_spark_configs()
        self.spark = None
        self.source_dfs = {}
        self.transformed_dfs = {}

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
        
    def initialize_spark(self) -> SparkSession:
        """Initialize and return a Spark session for debugging."""
        # Add debug-specific configs
        debug_configs = self.spark_configs.copy()
        # debug_configs.update({
        #     "spark.ui.enabled": "true",
        #     "spark.sql.repl.eagerEval.enabled": "true"
        # })
        
        session_manager = SparkSessionManager(
            app_name=f"{self.job_name}-debug",
            configs=debug_configs
        )
        session_manager.stop_session()
        self.spark = session_manager.create_session()
        return self.spark
    
    def load_source_data(self) -> Dict[str, Any]:
        """Load source data based on configuration."""
        if not self.spark:
            self.initialize_spark()
        
        from core.execution import JobExecutor
        
        # Create a job executor but only initialize sources
        executor = JobExecutor(self.config_path)
        executor.spark = self.spark
        
        # Load each source
        for src_name, src_config in self.config_parser.get_sources().items():
            source = executor._create_source(src_name, src_config)
            self.source_dfs[src_name] = source.read()
        
        return self.source_dfs
    
    def execute_transformations(self) -> Dict[str, Any]:
        """Execute transformations for debugging."""
        if not self.source_dfs:
            self.load_source_data()
        
        from core.transform import TransformationManager
        
        # Apply SQL file transformations if present
        sql_path = self.config_parser.get_sql_path()
        if sql_path:
            transform_manager = TransformationManager(self.spark, sql_file=sql_path)
            # Register all source dataframes as temporary views
            for name, df in self.source_dfs.items():
                df.createOrReplaceTempView(name)
            
            # Execute SQL
            sql_results = transform_manager.execute_sql_file()
            self.transformed_dfs.update(sql_results)
        
        # Apply JSON-defined transformations
        transformations = self.config_parser.get_transformations()
        if transformations:
            transform_manager = TransformationManager(self.spark)
            transformed = transform_manager.apply_transformations(self.source_dfs, transformations)
            self.transformed_dfs.update(transformed)
        
        return self.transformed_dfs
    
    def debug_job(self) -> Dict[str, Any]:
        """Run the full job in debug mode."""
        self.load_source_data()
        self.execute_transformations()
        
        return {
            "sources": self.source_dfs,
            "transformed": self.transformed_dfs
        }
    
    def get_dataframe(self, name: str) -> Optional[Any]:
        """Get a specific DataFrame by name."""
        return self.source_dfs.get(name) or self.transformed_dfs.get(name)
    
    def debug_write_operations(self, df_name: str = None) -> Dict[str, Any]:
        """
        Debug write operations for specified DataFrame or all transformed DataFrames.
        
        Args:
            df_name: Optional name of specific DataFrame to debug write operations
                    If None, debug all configured sinks
        
        Returns:
            Dictionary containing write operation debug information
        """
        if not self.transformed_dfs:
            self.execute_transformations()
        
        debug_results = {}
        sinks = self.config_parser.get_sinks()
        
        from core.execution import JobExecutor
        executor = JobExecutor(self.config_path)
        executor.spark = self.spark
        
        # Helper function to debug single write operation
        def _debug_sink(name: str, sink_config: Dict[str, Any], df: DataFrame) -> Dict[str, Any]:
            try:
                # Create sink instance but don't actually write
                sink = executor._create_sink(name, sink_config)
                
                # Get write operation details
                write_info = {
                    "sink_type": sink_config.get("type"),
                    "target": sink_config.get("path") or sink_config.get("table"),
                    "format": sink_config.get("format", "parquet"),
                    "mode": sink_config.get("mode", "overwrite"),
                    "partitioning": sink_config.get("partitioning"),
                    "options": sink_config.get("options", {}),
                }
                
                # Get DataFrame statistics
                df_stats = {
                    "num_rows": df.count(),
                    "num_partitions": df.rdd.getNumPartitions(),
                    "schema": df.schema.jsonValue(),
                    "sample_data": df.limit(5).toPandas().to_dict()
                }
                
                # Analyze potential write operation
                write_analysis = {
                    "estimated_size": df.queryExecution.optimizedPlan.stats.sizeInBytes,
                    "write_plan": df.write.format(write_info["format"])
                        ._jwrite.queryExecution().analyzed().toString()
                }
                
                return {
                    "write_config": write_info,
                    "dataframe_stats": df_stats,
                    "write_analysis": write_analysis,
                    "status": "success",
                    "message": "Write operation analyzed successfully"
                }
                
            except Exception as e:
                return {
                    "status": "error",
                    "message": str(e),
                    "traceback": traceback.format_exc()
                }
    
        # Debug specific DataFrame if provided
        if df_name:
            df = self.get_dataframe(df_name)
            if df is None:
                raise ValueError(f"DataFrame '{df_name}' not found")
            
            # Find matching sink configuration
            sink_config = sinks.get(df_name)
            if sink_config:
                debug_results[df_name] = _debug_sink(df_name, sink_config, df)
            else:
                raise ValueError(f"No sink configuration found for DataFrame '{df_name}'")
        
        # Debug all configured sinks
        else:
            for sink_name, sink_config in sinks.items():
                df = self.get_dataframe(sink_name)
                if df is not None:
                    debug_results[sink_name] = _debug_sink(sink_name, sink_config, df)
        
        return debug_results