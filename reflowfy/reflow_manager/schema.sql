-- ReflowManager Database Schema
-- PostgreSQL

-- Executions table
-- Stores pipeline execution state and metadata
CREATE TABLE IF NOT EXISTS executions (
    execution_id VARCHAR(255) PRIMARY KEY,
    pipeline_name VARCHAR(255) NOT NULL,
    state VARCHAR(50) NOT NULL,  -- pending, running, paused, completed, failed
    total_jobs INTEGER DEFAULT 0,
    jobs_dispatched INTEGER DEFAULT 0,
    jobs_completed INTEGER DEFAULT 0,
    jobs_failed INTEGER DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
    completed_at TIMESTAMP,
    error_message TEXT,
    runtime_params JSONB
);

-- Indexes for executions
CREATE INDEX IF NOT EXISTS idx_executions_pipeline_name ON executions(pipeline_name);
CREATE INDEX IF NOT EXISTS idx_executions_state ON executions(state);
CREATE INDEX IF NOT EXISTS idx_executions_created_at ON executions(created_at);


-- Rate limit state table
-- Stores token bucket state for rate limiting
CREATE TABLE IF NOT EXISTS rate_limit_state (
    pipeline_name VARCHAR(255) PRIMARY KEY,
    tokens FLOAT NOT NULL,
    max_tokens FLOAT NOT NULL,
    refill_rate FLOAT NOT NULL,  -- tokens per second
    last_update TIMESTAMP NOT NULL
);


-- Jobs table
-- Stores job payloads and tracking data
CREATE TABLE IF NOT EXISTS jobs (
    job_id VARCHAR(255) PRIMARY KEY,
    execution_id VARCHAR(255) NOT NULL REFERENCES executions(execution_id) ON DELETE CASCADE,
    
    -- Job data
    job_payload JSONB NOT NULL,
    batch_number INTEGER,
    
    -- State tracking
    state VARCHAR(50) NOT NULL,  -- pending, dispatched, completed, failed
    
    -- Worker results
    processed_records INTEGER DEFAULT 0,
    error_message TEXT,
    stats JSONB,
    
    -- Timestamps
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
    dispatched_at TIMESTAMP,
    completed_at TIMESTAMP
);

-- Indexes for jobs
CREATE INDEX IF NOT EXISTS idx_jobs_execution_id ON jobs(execution_id);
CREATE INDEX IF NOT EXISTS idx_jobs_state ON jobs(state);
CREATE INDEX IF NOT EXISTS idx_jobs_batch_number ON jobs(batch_number);


-- Trigger to update updated_at timestamp on executions
CREATE OR REPLACE FUNCTION update_executions_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS executions_updated_at_trigger ON executions;
CREATE TRIGGER executions_updated_at_trigger
    BEFORE UPDATE ON executions
    FOR EACH ROW
    EXECUTE FUNCTION update_executions_updated_at();


-- Trigger to update updated_at timestamp on jobs
CREATE OR REPLACE FUNCTION update_jobs_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS jobs_updated_at_trigger ON jobs;
CREATE TRIGGER jobs_updated_at_trigger
    BEFORE UPDATE ON jobs
    FOR EACH ROW
    EXECUTE FUNCTION update_jobs_updated_at();


-- Migration: Drop old checkpoints table if exists
-- (Run this manually after verifying data is migrated)
-- DROP TABLE IF EXISTS checkpoints;
