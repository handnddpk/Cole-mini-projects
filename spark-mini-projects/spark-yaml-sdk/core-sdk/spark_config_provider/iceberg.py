from spark_config_provider.base import SparkConfigProvider
from typing import Dict, List

class IcebergConfigProvider(SparkConfigProvider):
    @classmethod
    def get_source_type(cls) -> str:
        return "iceberg"
    
    def get_spark_configs(self) -> Dict[str, str]:
        return {
            "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
            "spark.sql.catalog.spark_catalog": "org.apache.iceberg.spark.SparkSessionCatalog",
            "spark.sql.catalog.spark_catalog.type": "hadoop"
        }
    
    def get_required_jars(self) -> List[str]:
        return [
            "org.apache.iceberg:iceberg-spark-runtime-3.3_2.12:1.4.2"
        ]