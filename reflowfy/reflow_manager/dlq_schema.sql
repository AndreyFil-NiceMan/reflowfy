-- DLQ (Dead Letter Queue) Database Schema
-- PostgreSQL

-- DLQ Jobs table
-- Stores jobs scheduled for later reflow
CREATE TABLE IF NOT EXISTS dlq_jobs (
    id SERIAL PRIMARY KEY,
    job_payload JSONB NOT NULL,
    pipeline_name VARCHAR(255) NOT NULL,
    scheduled_at TIMESTAMP NOT NULL,
    delay_minutes INTEGER NOT NULL,
    status VARCHAR(50) DEFAULT 'pending',  -- pending, processing, completed, failed
    retry_count INTEGER DEFAULT 0,
    max_retries INTEGER DEFAULT 5,
    error_message TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    processed_at TIMESTAMP,
    execution_id VARCHAR(255)
);

-- Archive table for permanently failed jobs
CREATE TABLE IF NOT EXISTS dlq_jobs_archive (
    id INTEGER PRIMARY KEY,
    job_payload JSONB NOT NULL,
    pipeline_name VARCHAR(255) NOT NULL,
    delay_minutes INTEGER NOT NULL,
    retry_count INTEGER NOT NULL,
    max_retries INTEGER NOT NULL,
    error_message TEXT,
    created_at TIMESTAMP NOT NULL,
    archived_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Indexes for DLQ jobs
CREATE INDEX IF NOT EXISTS idx_dlq_jobs_status_scheduled ON dlq_jobs(status, scheduled_at);
CREATE INDEX IF NOT EXISTS idx_dlq_jobs_pipeline ON dlq_jobs(pipeline_name);
CREATE INDEX IF NOT EXISTS idx_dlq_archive_pipeline ON dlq_jobs_archive(pipeline_name);


-- Trigger to update updated_at timestamp on dlq_jobs
CREATE OR REPLACE FUNCTION update_dlq_jobs_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS dlq_jobs_updated_at_trigger ON dlq_jobs;
CREATE TRIGGER dlq_jobs_updated_at_trigger
    BEFORE UPDATE ON dlq_jobs
    FOR EACH ROW
    EXECUTE FUNCTION update_dlq_jobs_updated_at();
