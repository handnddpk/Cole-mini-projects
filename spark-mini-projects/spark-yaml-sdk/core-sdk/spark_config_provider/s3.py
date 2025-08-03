from spark_config_provider.base import SparkConfigProvider
from typing import Dict, List

class S3ConfigProvider(SparkConfigProvider):
    @classmethod
    def get_source_type(cls) -> str:
        return "s3"
    
    def get_spark_configs(self) -> Dict[str, str]:
        return {
            "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
            "spark.hadoop.fs.s3a.path.style.access": "true"
        }
    
    def get_required_jars(self) -> List[str]:
        return [
            "org.apache.hadoop:hadoop-aws:3.3.2",
            "com.amazonaws:aws-java-sdk-bundle:1.11.1026"
        ]