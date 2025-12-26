"""
Script to run the elastic_test_pipeline locally without starting the API.
"""

from reflowfy.execution.local_executor import LocalExecutor
import elastic_test_pipeline as pipeline_module

# Get the pipeline instance
# Accessing it via the registry or directly if we knew the variable name 
# But since we import the module, the pipeline is registered.
# However, for LocalExecutor we need the pipeline instance.
# Let's just access it from the module if it's exposed globally
pipeline = pipeline_module.pipeline

# Or if not exposed, get from registry
# from reflowfy import pipeline_registry
# pipeline = pipeline_registry.get("elastic_test_pipeline")

print(f"🚀 Running pipeline: {pipeline.name}")

# Prepare runtime parameters
runtime_params = {
    "start_time": "2024-01-01T00:00:00",
    "end_time": "2024-12-31T23:59:59",
    "filter_status": "active"
}

# Create executor
executor = LocalExecutor(max_records=100)

# Execute
status = executor.execute(pipeline, runtime_params)

print("\n" + "="*30)
print(f"Final Status: {status.state.name}")
print(f"Records Processed: {status.metadata.get('records_sent', 0)}")
print("="*30)
