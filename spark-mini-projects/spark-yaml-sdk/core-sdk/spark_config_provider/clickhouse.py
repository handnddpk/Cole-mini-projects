from spark_config_provider.base import SparkConfigProvider
from typing import Dict, List

class ClickHouseConfigProvider(SparkConfigProvider):
    @classmethod
    def get_source_type(cls) -> str:
        return "clickhouse"
    
    def get_spark_configs(self) -> Dict[str, str]:
        return {
            "spark.sql.execution.arrow.enabled": "true"
        }
    
    def get_required_jars(self) -> List[str]:
        return [
            "com.clickhouse:clickhouse-jdbc:0.3.2-patch11",
            "org.slf4j:slf4j-api:1.7.30",
            "org.lz4:lz4-java:1.8.0"
        ]