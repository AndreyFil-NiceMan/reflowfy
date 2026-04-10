"""
Initialize Elasticsearch test data for E2E tests.

Creates a test index with sample data in the e2e Elasticsearch cluster.

Usage:
    python -m tests.e2e.sources.init_elastic_test_data
    
    # Or with custom URL:
    ELASTICSEARCH_URL=http://localhost:9201 python -m tests.e2e.sources.init_elastic_test_data
"""

import os
import random
from datetime import datetime, timedelta
from elasticsearch import Elasticsearch

# Default URL for e2e tests
DEFAULT_ELASTICSEARCH_URL = "http://localhost:9201"
INDEX_NAME = "e2e-test-events"


def get_elasticsearch_url() -> str:
    """Get Elasticsearch URL."""
    return os.getenv("ELASTICSEARCH_URL", DEFAULT_ELASTICSEARCH_URL)


def create_index(client: Elasticsearch):
    """Create test index with mapping."""
    
    # Delete if exists
    if client.indices.exists(index=INDEX_NAME):
        client.indices.delete(index=INDEX_NAME)
        print(f"🗑️  Deleted existing index: {INDEX_NAME}")
    
    # Create with mapping
    mapping = {
        "mappings": {
            "properties": {
                "@timestamp": {"type": "date"},
                "event_type": {"type": "keyword"},
                "user_id": {"type": "integer"},
                "user_name": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
                "status": {"type": "keyword"},
                "amount": {"type": "float"},
                "metadata": {"type": "object"},
            }
        },
        "settings": {
            "number_of_shards": 1,
            "number_of_replicas": 0
        }
    }
    
    client.indices.create(index=INDEX_NAME, body=mapping)
    print(f"✅ Created index: {INDEX_NAME}")


def insert_sample_data(client: Elasticsearch, count: int = 500):
    """Insert sample documents."""
    
    event_types = ["purchase", "login", "logout", "view", "click", "signup"]
    statuses = ["active", "inactive", "pending", "completed"]
    first_names = ["Alice", "Bob", "Charlie", "Diana", "Eve", "Frank", "Grace", "Henry"]
    last_names = ["Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller"]
    
    base_date = datetime.now() - timedelta(days=90)
    
    # Bulk insert for efficiency
    actions = []
    
    for i in range(count):
        event_type = random.choice(event_types)
        timestamp = base_date + timedelta(
            days=random.randint(0, 90),
            hours=random.randint(0, 23),
            minutes=random.randint(0, 59)
        )
        
        doc = {
            "@timestamp": timestamp.isoformat(),
            "event_type": event_type,
            "user_id": random.randint(1, 100),
            "user_name": f"{random.choice(first_names)} {random.choice(last_names)}",
            "status": random.choice(statuses),
            "amount": round(random.uniform(10.0, 500.0), 2) if event_type == "purchase" else None,
            "metadata": {
                "source": "e2e_test",
                "batch": i // 100,
                "priority": random.choice(["low", "medium", "high"])
            }
        }
        
        actions.append({"index": {"_index": INDEX_NAME}})
        actions.append(doc)
        
        # Bulk insert every 100 documents
        if len(actions) >= 200:
            client.bulk(body=actions)
            actions = []
    
    # Insert remaining
    if actions:
        client.bulk(body=actions)
    
    # Refresh index
    client.indices.refresh(index=INDEX_NAME)
    
    print(f"✅ Inserted {count} documents")


def verify_data(client: Elasticsearch):
    """Verify inserted data."""
    
    # Count
    count = client.count(index=INDEX_NAME)["count"]
    
    # Aggregation by status
    agg_result = client.search(
        index=INDEX_NAME,
        body={
            "size": 0,
            "aggs": {
                "by_status": {
                    "terms": {"field": "status"}
                }
            }
        }
    )
    
    print("\n📊 Data summary:")
    print(f"   Total documents: {count}")
    print("   By status:")
    
    for bucket in agg_result["aggregations"]["by_status"]["buckets"]:
        print(f"     - {bucket['key']}: {bucket['doc_count']}")


def main():
    """Initialize test data."""
    print("🚀 Initializing Elasticsearch test data for E2E tests...\n")
    
    es_url = get_elasticsearch_url()
    print(f"📦 Connecting to: {es_url}")
    
    client = Elasticsearch(hosts=[es_url])
    
    try:
        # Check cluster health
        health = client.cluster.health()
        print(f"   Cluster status: {health['status']}")
        
        create_index(client)
        insert_sample_data(client, count=500)
        verify_data(client)
        print("\n✅ Elasticsearch test data initialization complete!")
    except Exception as e:
        print(f"\n❌ Error: {e}")
        raise
    finally:
        client.close()


if __name__ == "__main__":
    main()
