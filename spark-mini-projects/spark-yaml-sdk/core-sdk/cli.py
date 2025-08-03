import argparse
import os

def main():
    """Command-line interface for Spark ETL jobs."""
    parser = argparse.ArgumentParser(description='Spark ETL Job Runner')
    parser.add_argument('config', help='Path to job configuration file')
    parser.add_argument('--debug', action='store_true', help='Run in debug mode')
    
    args = parser.parse_args()
    
    if not os.path.exists(args.config):
        print(f"Error: Config file not found: {args.config}")
        return 1
    
    if args.debug:
        print(f"Running in debug mode with config: {args.config}")
        from debug.jupyter import JupyterDebugger
        
        debugger = JupyterDebugger(args.config)
        results = debugger.debug_job()
        
        print("Debug run completed. Available DataFrames:")
        for name in results["sources"]:
            print(f"Source: {name}")
        for name in results["transformed"]:
            print(f"Transformed: {name}")
            
        # In a real CLI, you might want to provide options to inspect the DataFrames
    else:
        print(f"Executing job with config: {args.config}")
        from core.execution import JobExecutor
        
        executor = JobExecutor(args.config)
        executor.execute()
        print("Job execution completed")
    
    return 0

if __name__ == "__main__":
    exit(main())