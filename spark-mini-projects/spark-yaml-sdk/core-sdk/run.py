#!/usr/bin/env python3

import os
import sys
from core.execution import JobExecutor

# Set environment variables for database connections
os.environ['DB_USER'] = 'postgres'
os.environ['DB_PASSWORD'] = 'postgres'
os.environ['CH_USER'] = 'default'
os.environ['CH_PASSWORD'] = ''

# Check if config path is provided
if len(sys.argv) < 2:
    print("Usage: python run-etl-job.py <config_path>")
    sys.exit(1)

config_path = sys.argv[1]

# Initialize and run the job
executor = JobExecutor(config_path)
spark = executor.initialize_spark()
executor.execute()

print("ETL job completed successfully!")