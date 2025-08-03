from spark_config_provider.base import SparkConfigProvider
from spark_config_provider.mongodb import MongoDBConfigProvider
from spark_config_provider.iceberg import IcebergConfigProvider
from spark_config_provider.s3 import S3ConfigProvider
from spark_config_provider.clickhouse import ClickHouseConfigProvider

__all__ = [
    'SparkConfigProvider',
    'MongoDBConfigProvider',
    'IcebergConfigProvider',
    'S3ConfigProvider'
]

# Registry of all config providers
PROVIDERS = {
    provider.get_source_type(): provider
    for provider in [MongoDBConfigProvider, IcebergConfigProvider, S3ConfigProvider, ClickHouseConfigProvider]
}

def get_provider(source_type: str) -> SparkConfigProvider:
    """Get the configuration provider for a specific source type."""
    provider_class = PROVIDERS.get(source_type)
    if not provider_class:
        raise ValueError(f"No configuration provider found for source type: {source_type}")
    return provider_class()