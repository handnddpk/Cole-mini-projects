#!/bin/bash

# Make all scripts executable
chmod +x scripts/*.sh

echo "✅ All scripts are now executable!"
echo ""
echo "📋 Available scripts:"
echo "  ./scripts/setup.sh              - Initial setup and service startup"
echo "  ./scripts/start-streaming.sh    - Start Spark streaming jobs"  
echo "  ./scripts/start-ingestion.sh    - Start data ingestion from APIs"
echo "  ./scripts/run-ml-pipeline.sh    - Run machine learning pipeline"
echo "  ./scripts/make-executable.sh    - Make all scripts executable (this script)"
