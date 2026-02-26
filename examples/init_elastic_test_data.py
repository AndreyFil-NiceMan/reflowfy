"""
Initialize Elasticsearch with test data for Reflofy testing.

This script:
1. Connects to Elasticsearch
2. Creates a test index with sample documents
3. Inserts 1000 test records with realistic data

Run this after starting Elasticsearch:
    docker compose -f docker-compose.elastic.yml up -d
    python examples/init_elastic_test_data.py
"""

from datetime import datetime, timedelta
import random
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk


# Sample data generators
FIRST_NAMES = [
    "Alice", "Bob", "Charlie", "Diana", "Eve", "Frank", "Grace", "Henry",
    "Ivy", "Jack", "Kate", "Leo", "Mia", "Noah", "Olivia", "Peter",
    "Quinn", "Rachel", "Sam", "Tina", "Uma", "Victor", "Wendy", "Xander",
    "Yara", "Zack"
]

LAST_NAMES = [
    "Anderson", "Brown", "Clark", "Davis", "Evans", "Foster", "Garcia",
    "Harris", "Ivanov", "Johnson", "Kim", "Lopez", "Martinez", "Nelson",
    "O'Brien", "Parker", "Quinn", "Rodriguez", "Smith", "Taylor",
    "Underwood", "Valdez", "Wilson", "Xavier", "Young", "Zhang"
]

EVENT_TYPES = [
    "user_login", "user_logout", "page_view", "button_click",
    "form_submit", "api_call", "error", "purchase", "search", "download"
]

STATUSES = ["active", "inactive", "pending", "archived"]

REGIONS = ["us-east", "us-west", "eu-central", "ap-south", "ap-east"]

PRODUCTS = [
    "Product A", "Product B", "Product C", "Premium Plan",
    "Basic Plan", "Enterprise Plan", "Service X", "Service Y"
]


def generate_user_data():
    """Generate random user data."""
    first_name = random.choice(FIRST_NAMES)
    last_name = random.choice(LAST_NAMES)
    
    return {
        "user_id": f"user_{random.randint(1000, 9999)}",
        "user_name": f"{first_name} {last_name}",
        "user_email": f"{first_name.lower()}.{last_name.lower()}@example.com",
        "user_region": random.choice(REGIONS),
    }


def generate_event_data(event_type):
    """Generate event-specific data."""
    if event_type == "purchase":
        return {
            "product": random.choice(PRODUCTS),
            "amount": round(random.uniform(10, 500), 2),
            "currency": "USD",
        }
    elif event_type == "search":
        return {
            "query": f"search term {random.randint(1, 100)}",
            "results_count": random.randint(0, 100),
        }
    elif event_type == "error":
        return {
            "error_code": f"ERR_{random.randint(100, 999)}",
            "error_message": "Sample error message",
            "severity": random.choice(["low", "medium", "high", "critical"]),
        }
    else:
        return {
            "component": random.choice(["frontend", "backend", "api", "database"]),
            "duration_ms": random.randint(10, 5000),
        }


def generate_test_documents(count=1000, start_date=None, end_date=None):
    """
    Generate test documents for Elasticsearch.
    
    Args:
        count: Number of documents to generate
        start_date: Start date for timestamp range
        end_date: End date for timestamp range
    
    Yields:
        Dictionary representing a test document
    """
    if start_date is None:
        start_date = datetime(2025, 1, 1)
    if end_date is None:
        end_date = datetime(2025, 12, 31)
    
    # Calculate time delta for random timestamps
    time_delta = (end_date - start_date).total_seconds()
    
    for i in range(count):
        # Generate random timestamp within range
        random_seconds = random.randint(0, int(time_delta))
        timestamp = start_date + timedelta(seconds=random_seconds)
        
        # Generate event type
        event_type = random.choice(EVENT_TYPES)
        
        # Generate document
        doc = {
            # Timestamp (Elasticsearch standard field)
            "@timestamp": timestamp.isoformat(),
            
            # User information
            **generate_user_data(),
            
            # Event information
            "event_type": event_type,
            "event_data": generate_event_data(event_type),
            
            # Status
            "status": random.choice(STATUSES),
            
            # Metadata
            "metadata": {
                "source": "test_generator",
                "version": "1.0",
                "tags": random.sample(["test", "demo", "sample", "production", "staging"], k=2),
                "priority": random.choice(["low", "normal", "high"]),
            },
            
            # Additional fields
            "session_id": f"session_{random.randint(10000, 99999)}",
            "request_id": f"req_{i}_{random.randint(1000, 9999)}",
            "processed": False,
        }
        
        yield doc


def init_elasticsearch_test_data(
    es_url="http://localhost:9200",
    index_name="reflofy-test-data",
    doc_count=1000,
):
    """
    Initialize Elasticsearch with test data.
    
    Args:
        es_url: Elasticsearch URL
        index_name: Index name to create
        doc_count: Number of documents to insert
    """
    print("=" * 60)
    print("🔧 Initializing Elasticsearch Test Data")
    print("=" * 60)
    print(f"Elasticsearch URL: {es_url}")
    print(f"Index name: {index_name}")
    print(f"Document count: {doc_count}")
    print()
    
    # Connect to Elasticsearch
    print("📡 Connecting to Elasticsearch...")
    es = Elasticsearch(hosts=[es_url])
    
    # Check connection
    if not es.ping():
        print("❌ Failed to connect to Elasticsearch")
        print("   Make sure Elasticsearch is running:")
        print("   docker compose -f docker-compose.elastic.yml up -d")
        return False
    
    print("✓ Connected successfully")
    print()
    
    # Delete index if it exists
    if es.indices.exists(index=index_name):
        print(f"🗑️  Deleting existing index: {index_name}")
        es.indices.delete(index=index_name)
        print("✓ Index deleted")
        print()
    
    # Create index with mapping
    print(f"📝 Creating index: {index_name}")
    
    index_mapping = {
        "mappings": {
            "properties": {
                "@timestamp": {"type": "date"},
                "user_id": {"type": "keyword"},
                "user_name": {"type": "text"},
                "user_email": {"type": "keyword"},
                "user_region": {"type": "keyword"},
                "event_type": {"type": "keyword"},
                "event_data": {"type": "object"},
                "status": {"type": "keyword"},
                "metadata": {"type": "object"},
                "session_id": {"type": "keyword"},
                "request_id": {"type": "keyword"},
                "processed": {"type": "boolean"},
            }
        }
    }
    
    es.indices.create(index=index_name, body=index_mapping)
    print("✓ Index created")
    print()
    
    # Generate and insert documents
    print(f"📦 Generating {doc_count} test documents...")
    docs = list(generate_test_documents(count=doc_count))
    print("✓ Documents generated")
    print()
    
    # Bulk insert
    print("💾 Inserting documents into Elasticsearch...")
    
    # Prepare bulk actions
    actions = [
        {
            "_index": index_name,
            "_source": doc,
        }
        for doc in docs
    ]
    
    # Execute bulk insert
    success, failed = bulk(es, actions, stats_only=True)
    
    print(f"✓ Inserted {success} documents")
    if failed > 0:
        print(f"⚠️  Failed: {failed} documents")
    print()
    
    # Refresh index to make documents searchable
    es.indices.refresh(index=index_name)
    
    # Get document count
    count_result = es.count(index=index_name)
    total_docs = count_result["count"]
    
    print("=" * 60)
    print("✅ Test Data Initialization Complete!")
    print("=" * 60)
    print(f"Index: {index_name}")
    print(f"Total documents: {total_docs}")
    print()
    print("📊 Sample statistics:")
    
    # Get some statistics
    for status in STATUSES:
        status_count = es.count(
            index=index_name,
            body={"query": {"term": {"status": status}}}
        )["count"]
        print(f"  - {status}: {status_count} documents")
    
    print()
    print("🎯 Next steps:")
    print("  1. View data in Kibana: http://localhost:5601")
    print("  2. Run test pipeline: python examples/run_simple_test.py")
    print("  3. Or import elastic_test_pipeline and test via API")
    print()
    
    return True


if __name__ == "__main__":
    init_elasticsearch_test_data()
