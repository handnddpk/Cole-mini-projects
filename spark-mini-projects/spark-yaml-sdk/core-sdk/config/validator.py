from typing import Dict, Any, List, Tuple
import jsonschema


class ConfigValidator:
    """Validates ETL job configurations against schemas."""
    
    def __init__(self):
        # Define config schema
        self.schema = {
            "type": "object",
            "required": ["job_name", "job_type", "sources", "sinks"],
            "properties": {
                "job_name": {"type": "string"},
                "job_type": {"type": "string", "enum": ["batch", "streaming"]},
                "sources": {
                    "type": "object",
                    "minProperties": 1
                },
                "sinks": {
                    "type": "object",
                    "minProperties": 1
                },
                "transformations": {"type": "object"},
                "sql_file": {"type": "string"},
                "spark_config": {"type": "object"},
                "udf_jars": {
                    "type": "array",
                    "items": {"type": "string"}
                }
            }
        }
    
    def validate(self, config: Dict[str, Any]) -> Tuple[bool, List[str]]:
        """
        Validate a configuration against the schema.
        
        Args:
            config: The configuration dictionary to validate
            
        Returns:
            Tuple of (is_valid, error_messages)
        """
        try:
            jsonschema.validate(instance=config, schema=self.schema)
            return True, []
        except jsonschema.exceptions.ValidationError as e:
            return False, [str(e)]