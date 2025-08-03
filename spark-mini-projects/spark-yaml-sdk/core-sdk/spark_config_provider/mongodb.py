from spark_config_provider.base import SparkConfigProvider
from typing import Dict, List

class MongoDBConfigProvider(SparkConfigProvider):
    @classmethod
    def get_source_type(cls) -> str:
        return "mongodb"
    
    def get_spark_configs(self) -> Dict[str, str]:
        return {
            "spark.mongodb.input.uri": "mongodb://mongodb:27017",
            "spark.mongodb.output.uri": "mongodb://mongodb:27017"
        }
    
    def get_required_jars(self) -> List[str]:
        return [
            "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1"
        ]