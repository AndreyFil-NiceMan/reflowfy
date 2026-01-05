"""
Initialize SQL test data for E2E tests.

Creates a test table with sample data in the e2e PostgreSQL database.

Usage:
    python -m tests.e2e.sources.init_sql_test_data
    
    # Or with custom connection:
    SQL_CONNECTION_URL=postgresql://user:pass@host:5432/db python -m tests.e2e.sources.init_sql_test_data
"""

import os
import random
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text

# Default connection URL for e2e tests
DEFAULT_CONNECTION_URL = "postgresql://reflowfy:reflowfy@localhost:5433/reflowfy_e2e"


def get_connection_url() -> str:
    """Get database connection URL."""
    return os.getenv("SQL_CONNECTION_URL", DEFAULT_CONNECTION_URL)


def create_test_table(engine):
    """Create test_events table."""
    with engine.connect() as conn:
        conn.execute(text("""
            DROP TABLE IF EXISTS test_events CASCADE;
        """))
        
        conn.execute(text("""
            CREATE TABLE test_events (
                id SERIAL PRIMARY KEY,
                event_type VARCHAR(50) NOT NULL,
                user_id INTEGER NOT NULL,
                user_name VARCHAR(100) NOT NULL,
                status VARCHAR(20) NOT NULL,
                amount DECIMAL(10, 2),
                created_at TIMESTAMP NOT NULL,
                metadata JSONB
            );
        """))
        
        conn.execute(text("""
            CREATE INDEX idx_test_events_created_at ON test_events(created_at);
            CREATE INDEX idx_test_events_status ON test_events(status);
            CREATE INDEX idx_test_events_user_id ON test_events(user_id);
        """))
        
        conn.commit()
        print("✅ Created test_events table")


def insert_sample_data(engine, count: int = 500):
    """Insert sample data into test_events table."""
    
    event_types = ["purchase", "login", "logout", "view", "click", "signup"]
    statuses = ["active", "inactive", "pending", "completed"]
    first_names = ["Alice", "Bob", "Charlie", "Diana", "Eve", "Frank", "Grace", "Henry"]
    last_names = ["Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller"]
    
    base_date = datetime.now() - timedelta(days=90)
    
    with engine.connect() as conn:
        for i in range(count):
            event_type = random.choice(event_types)
            user_id = random.randint(1, 100)
            user_name = f"{random.choice(first_names)} {random.choice(last_names)}"
            status = random.choice(statuses)
            amount = round(random.uniform(10.0, 500.0), 2) if event_type == "purchase" else None
            created_at = base_date + timedelta(
                days=random.randint(0, 90),
                hours=random.randint(0, 23),
                minutes=random.randint(0, 59)
            )
            metadata = {
                "source": "e2e_test",
                "batch": i // 100,
                "priority": random.choice(["low", "medium", "high"])
            }
            
            conn.execute(
                text("""
                    INSERT INTO test_events 
                    (event_type, user_id, user_name, status, amount, created_at, metadata)
                    VALUES (:event_type, :user_id, :user_name, :status, :amount, :created_at, :metadata)
                """),
                {
                    "event_type": event_type,
                    "user_id": user_id,
                    "user_name": user_name,
                    "status": status,
                    "amount": amount,
                    "created_at": created_at,
                    "metadata": str(metadata).replace("'", '"'),
                }
            )
        
        conn.commit()
        print(f"✅ Inserted {count} test events")


def verify_data(engine):
    """Verify inserted data."""
    with engine.connect() as conn:
        result = conn.execute(text("SELECT COUNT(*) FROM test_events"))
        count = result.scalar()
        
        result = conn.execute(text("""
            SELECT status, COUNT(*) as cnt 
            FROM test_events 
            GROUP BY status 
            ORDER BY cnt DESC
        """))
        
        print(f"\n📊 Data summary:")
        print(f"   Total records: {count}")
        print(f"   By status:")
        for row in result:
            print(f"     - {row[0]}: {row[1]}")


def main():
    """Initialize test data."""
    print("🚀 Initializing SQL test data for E2E tests...\n")
    
    connection_url = get_connection_url()
    print(f"📦 Connecting to: {connection_url}")
    
    engine = create_engine(connection_url)
    
    try:
        create_test_table(engine)
        insert_sample_data(engine, count=500)
        verify_data(engine)
        print("\n✅ SQL test data initialization complete!")
    except Exception as e:
        print(f"\n❌ Error: {e}")
        raise
    finally:
        engine.dispose()


if __name__ == "__main__":
    main()
