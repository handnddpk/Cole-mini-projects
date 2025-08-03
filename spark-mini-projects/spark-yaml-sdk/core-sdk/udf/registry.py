import os
from typing import Dict, Any, List, Optional
from pyspark.sql import SparkSession


class UDFRegistry:
    """Manages registration of Java/Scala UDFs."""
    
    def __init__(self, spark: SparkSession):
        """
        Initialize the UDF registry.
        
        Args:
            spark: SparkSession to register UDFs with
        """
        self.spark = spark
    
    def register_jar_udfs(self, jar_paths: List[str]) -> None:
        """
        Register UDFs from JAR files.
        
        Args:
            jar_paths: List of paths to JAR files containing UDFs
        """
        # Add JARs to Spark context
        for jar_path in jar_paths:
            if not os.path.exists(jar_path):
                raise ValueError(f"JAR file not found: {jar_path}")
            self.spark.sparkContext.addFile(jar_path)
    
    def register_scala_function(self, function_name: str, function_class: str) -> None:
        """
        Register a Scala function as a UDF.
        
        Args:
            function_name: Name to register the UDF as
            function_class: Fully qualified class name of the Scala function
        """
        self.spark.udf.registerJavaFunction(function_name, function_class)
    
    def register_functions_from_config(self, udf_config: Dict[str, Any]) -> None:
        """
        Register UDFs based on configuration.
        
        Args:
            udf_config: Configuration for UDFs to register
        """
        # Register JAR files
        jar_paths = udf_config.get('jars', [])
        if jar_paths:
            self.register_jar_udfs(jar_paths)
        
        # Register individual functions
        functions = udf_config.get('functions', [])
        for func in functions:
            name = func.get('name')
            class_name = func.get('class')
            if name and class_name:
                self.register_scala_function(name, class_name)