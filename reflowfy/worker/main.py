"""Worker entrypoint."""

import os
import signal
import sys
import importlib
import pkgutil
from pathlib import Path
from reflowfy.worker.consumer import KafkaJobConsumer


def discover_and_load_pipelines(module_name: str = "pipelines") -> int:
    """
    Auto-discover and import all pipeline modules from specified directory.
    This registers transformations so workers can use them.
    
    Args:
        module_name: Name of the module/directory containing pipelines
        
    Returns:
        Number of pipeline files loaded
    """
    loaded_count = 0
    
    try:
        # Try to import the pipelines package
        pipelines_package = importlib.import_module(module_name)
        package_path = Path(pipelines_package.__file__).parent
        
        print(f"Discovering pipelines in '{module_name}'...")
        
        # Import all Python files in the pipelines directory
        for _, module_name_inner, is_pkg in pkgutil.iter_modules([str(package_path)]):
            if not is_pkg:  # Only import Python files, not subdirectories
                try:
                    full_module = f"{module_name}.{module_name_inner}"
                    importlib.import_module(full_module)
                    print(f"  Loaded {module_name_inner}.py")
                    loaded_count += 1
                except Exception as e:
                    print(f"  Failed to load {module_name_inner}.py: {e}")
        
        if loaded_count == 0:
            print(f"  No pipeline files found in '{module_name}'")
        else:
            print(f"  Loaded {loaded_count} pipeline file(s)")
            
    except ImportError:
        print(f"  Module '{module_name}' not found - no pipelines loaded")
    
    return loaded_count


def handle_shutdown(signum, frame):
    """Handle graceful shutdown."""
    print("\nShutdown signal received, stopping worker...")
    sys.exit(0)


def main():
    """Worker main entry point."""
    import asyncio
    from reflowfy import __version__
    
    print("=" * 60)
    print("🚀 Starting Reflowfy Worker (Async)")
    print(f"📦 Version: {__version__}")
    print("=" * 60)
    
    # Register signal handlers
    signal.signal(signal.SIGTERM, handle_shutdown)
    signal.signal(signal.SIGINT, handle_shutdown)
    
    # Auto-discover and load pipelines (registers transformations)
    pipeline_module = os.getenv("PIPELINE_MODULE", "pipelines")
    discover_and_load_pipelines(pipeline_module)
    print()
    
    # Get configuration from environment
    kafka_bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    kafka_topic = os.getenv("KAFKA_TOPIC", "reflow.jobs")
    kafka_group_id = os.getenv("KAFKA_GROUP_ID", "reflowfy-workers")
    database_url = os.getenv("DATABASE_URL", "postgresql://reflowfy:reflowfy@localhost:5432/reflowfy")
    
    # SASL Authentication config
    security_protocol = os.getenv("KAFKA_SECURITY_PROTOCOL")
    sasl_mechanism = os.getenv("KAFKA_SASL_MECHANISM")
    sasl_username = os.getenv("KAFKA_SASL_USERNAME")
    sasl_password = os.getenv("KAFKA_SASL_PASSWORD")
    
    print(f"Kafka brokers: {kafka_bootstrap_servers}")
    print(f"Topic: {kafka_topic}")
    print(f"Consumer group: {kafka_group_id}")
    if sasl_username:
        print(f"SASL: {security_protocol or 'SASL_PLAINTEXT'} / {sasl_mechanism or 'SCRAM-SHA-256'}")
    print(f"Database: {database_url.split('@')[-1] if '@' in database_url else database_url}")  # Hide credentials
    print()
    
    # Create consumer
    consumer = KafkaJobConsumer(
        bootstrap_servers=kafka_bootstrap_servers,
        topic=kafka_topic,
        group_id=kafka_group_id,
        database_url=database_url,
        security_protocol=security_protocol,
        sasl_mechanism=sasl_mechanism,
        sasl_username=sasl_username,
        sasl_password=sasl_password,
    )
    
    try:
        print("Worker ready, waiting for jobs...\n")
        # Run async consumer in event loop
        asyncio.run(consumer.start())
    except KeyboardInterrupt:
        print("\nWorker stopped by user")
    except Exception as e:
        print(f"\nWorker crashed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()

