from typing import Dict, Any, List, Optional
from pyspark.sql import SparkSession, DataFrame
import os


class TransformationManager:
    """Manages transformations on DataFrames."""
    
    def __init__(self, spark: SparkSession, sql_file: Optional[str] = None):
        """
        Initialize the transformation manager.
        
        Args:
            spark: SparkSession to use
            sql_file: Optional path to SQL file with transformations
        """
        self.spark = spark
        self.sql_file = sql_file
    
    def execute_sql_file(self) -> Dict[str, DataFrame]:
        """Execute SQL from file and return resulting DataFrames."""
        if not self.sql_file or not os.path.exists(self.sql_file):
            raise ValueError(f"SQL file not found: {self.sql_file}")
        
        # Read the SQL content
        with open(self.sql_file, 'r') as f:
            sql_content = f.read()
        
        # Split into individual statements (basic implementation - might need enhancement)
        statements = [stmt.strip() for stmt in sql_content.split(';') if stmt.strip()]
        
        results = {}
        for stmt in statements:
            # Check if this is a CREATE OR REPLACE TEMPORARY VIEW statement
            lower_stmt = stmt.lower()
            if "create" in lower_stmt and "temporary view" in lower_stmt:
                # Extract view name (very simplistic - would need better parsing in production)
                view_name = stmt.split("view", 1)[1].strip().split(" ", 1)[0].strip()
                
                # Execute the statement
                self.spark.sql(stmt)
                
                # Add the view to results
                results[view_name] = self.spark.table(view_name)
            else:
                # Just execute the statement
                self.spark.sql(stmt)
        
        return results
    
    def apply_transformations(self, 
                          source_dfs: Dict[str, DataFrame], 
                          transformations: Dict[str, Any]) -> Dict[str, DataFrame]:
        """
        Apply JSON-defined transformations to DataFrames.
        
        Args:
            source_dfs: Dictionary of source DataFrames
            transformations: Transformation configurations
            
        Returns:
            Dictionary of transformed DataFrames
        """
        results = {}
        
        # Register source DataFrames as temporary views
        for name, df in source_dfs.items():
            df.createOrReplaceTempView(name)
        
        # Apply each transformation
        for name, config in transformations.items():
            transform_type = config.get('type', 'sql')
            debug_cache = config.get('debug_cache', False)
            view_creation = config.get('view_creation', True)
            
            if transform_type == 'sql':
                sql_content = self._get_sql_content(config)
                result_df = self.spark.sql(sql_content)
                
            elif transform_type == 'filter':
                result_df = self._apply_filter(name, config, source_dfs)
                
            elif transform_type == 'select':
                result_df = self._apply_select(name, config, source_dfs)
                
            elif transform_type == 'join':
                result_df = self._apply_join(name, config, source_dfs, results)
                
            elif transform_type == 'group_by':
                result_df = self._apply_group_by(name, config, source_dfs)
                
            elif transform_type == 'window':
                result_df = self._apply_window(name, config, source_dfs)
                
            elif transform_type == 'union':
                result_df = self._apply_union(name, config, source_dfs, results)
                
            elif transform_type == 'repartition':
                result_df = self._apply_repartition(name, config, source_dfs)
                
            else:
                raise ValueError(f"Unknown transformation type: {transform_type}")
            
            if debug_cache:
                result_df = result_df.cache()
            if view_creation:
                result_df.createOrReplaceTempView(f"{name}")
            results[name] = result_df
            
        return results

    def _get_sql_content(self, config: Dict[str, Any]) -> str:
        """Get SQL content from either direct statement or file."""
        if 'sql' in config:
            return config['sql']
        elif 'sql_file' in config:
            sql_path = config['sql_file']
            if not os.path.exists(sql_path):
                raise ValueError(f"SQL file not found: {sql_path}")
            with open(sql_path, 'r') as f:
                return f.read()
        else:
            raise ValueError("SQL transformation must specify either 'sql' or 'sql_file'")
    
    def _apply_filter(self, name: str, config: Dict[str, Any], source_dfs: Dict[str, DataFrame]) -> DataFrame:
        """Apply filter transformation."""
        source = config.get('source')
        condition = config.get('condition')
        if not source or not condition:
            raise ValueError(f"Filter transformation '{name}' missing source or condition")
        
        source_df = source_dfs.get(source)
        if not source_df:
            raise ValueError(f"Unknown source DataFrame: {source}")
        
        return source_df.filter(condition)

    def _apply_select(self, name: str, config: Dict[str, Any], source_dfs: Dict[str, DataFrame]) -> DataFrame:
        """Apply select transformation."""
        source = config.get('source')
        columns = config.get('columns', ['*'])
        if not source:
            raise ValueError(f"Select transformation '{name}' missing source")
        
        source_df = source_dfs.get(source)
        if not source_df:
            raise ValueError(f"Unknown source DataFrame: {source}")
        
        return source_df.selectExpr(*columns)

    def _apply_join(self, name: str, config: Dict[str, Any], 
                    source_dfs: Dict[str, DataFrame], results: Dict[str, DataFrame]) -> DataFrame:
        """Apply join transformation."""
        left = config.get('left')
        right = config.get('right')
        join_type = config.get('join_type', 'inner')
        on = config.get('on')
        
        if not left or not right or not on:
            raise ValueError(f"Join transformation '{name}' missing required parameters")
        
        left_df = source_dfs.get(left) or results.get(left)
        right_df = source_dfs.get(right) or results.get(right)
        
        if not left_df or not right_df:
            raise ValueError(f"Unknown DataFrames in join: {left}, {right}")
        
        return left_df.join(right_df, on=on, how=join_type)

    def _apply_group_by(self, name: str, config: Dict[str, Any], source_dfs: Dict[str, DataFrame]) -> DataFrame:
        """Apply groupBy transformation."""
        source = config.get('source')
        group_by = config.get('group_by', [])
        agg_exprs = config.get('agg_expressions', {})
        
        if not source or not group_by or not agg_exprs:
            raise ValueError(f"GroupBy transformation '{name}' missing required parameters")
        
        source_df = source_dfs.get(source)
        if not source_df:
            raise ValueError(f"Unknown source DataFrame: {source}")
        
        return source_df.groupBy(*group_by).agg(agg_exprs)

    def _apply_window(self, name: str, config: Dict[str, Any], source_dfs: Dict[str, DataFrame]) -> DataFrame:
        """Apply window transformation."""
        from pyspark.sql import Window
        
        source = config.get('source')
        partition_by = config.get('partition_by', [])
        order_by = config.get('order_by', [])
        window_exprs = config.get('window_expressions', {})
        
        if not source or not window_exprs:
            raise ValueError(f"Window transformation '{name}' missing required parameters")
        
        source_df = source_dfs.get(source)
        if not source_df:
            raise ValueError(f"Unknown source DataFrame: {source}")
        
        window_spec = Window.partitionBy(*partition_by).orderBy(*order_by)
        return source_df.select("*", *[expr.over(window_spec).alias(alias) 
                                    for alias, expr in window_exprs.items()])

    def _apply_union(self, name: str, config: Dict[str, Any], 
                    source_dfs: Dict[str, DataFrame], results: Dict[str, DataFrame]) -> DataFrame:
        """Apply union transformation."""
        sources = config.get('sources', [])
        distinct = config.get('distinct', False)
        
        if not sources:
            raise ValueError(f"Union transformation '{name}' missing sources")
        
        dfs = []
        for source in sources:
            df = source_dfs.get(source) or results.get(source)
            if not df:
                raise ValueError(f"Unknown DataFrame in union: {source}")
            dfs.append(df)
        
        result = dfs[0]
        for df in dfs[1:]:
            result = result.unionByName(df) if distinct else result.unionAll(df)
        return result

    def _apply_repartition(self, name: str, config: Dict[str, Any], source_dfs: Dict[str, DataFrame]) -> DataFrame:
        """Apply repartition transformation."""
        source = config.get('source')
        num_partitions = config.get('num_partitions')
        columns = config.get('columns', [])
        
        if not source or not num_partitions:
            raise ValueError(f"Repartition transformation '{name}' missing required parameters")
        
        source_df = source_dfs.get(source)
        if not source_df:
            raise ValueError(f"Unknown source DataFrame: {source}")
        
        if columns:
            return source_df.repartition(num_partitions, *columns)
        return source_df.repartition(num_partitions)