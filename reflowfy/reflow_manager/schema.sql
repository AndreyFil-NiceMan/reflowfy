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


-- Checkpoints table
-- Stores checkpoint data for pause/resume functionality
CREATE TABLE IF NOT EXISTS checkpoints (
    id SERIAL PRIMARY KEY,
    execution_id VARCHAR(255) NOT NULL REFERENCES executions(execution_id) ON DELETE CASCADE,
    batch_id VARCHAR(255) NOT NULL,
    offset_data JSONB,  -- Source-specific offset/cursor information
    processed_records INTEGER DEFAULT 0,
    state VARCHAR(50) NOT NULL,  -- pending, processing, completed, failed
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
    error_message TEXT,
    stats JSONB  -- Detailed job statistics from worker
);

-- Indexes for checkpoints
CREATE INDEX IF NOT EXISTS idx_checkpoints_execution_id ON checkpoints(execution_id);
CREATE INDEX IF NOT EXISTS idx_checkpoints_batch_id ON checkpoints(batch_id);
CREATE INDEX IF NOT EXISTS idx_checkpoints_state ON checkpoints(state);


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
-- Stores job payloads persistently (not in RAM)
CREATE TABLE IF NOT EXISTS jobs (
    id SERIAL PRIMARY KEY,
    execution_id VARCHAR(255) NOT NULL REFERENCES executions(execution_id) ON DELETE CASCADE,
    batch_id VARCHAR(255) NOT NULL UNIQUE,
    job_payload JSONB NOT NULL,
    state VARCHAR(50) NOT NULL,  -- pending, dispatched, completed, failed
    batch_number INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
    dispatched_at TIMESTAMP,
    completed_at TIMESTAMP
);

-- Indexes for jobs
CREATE INDEX IF NOT EXISTS idx_jobs_execution_id ON jobs(execution_id);
CREATE INDEX IF NOT EXISTS idx_jobs_batch_id ON jobs(batch_id);
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

CREATE TRIGGER executions_updated_at_trigger
    BEFORE UPDATE ON executions
    FOR EACH ROW
    EXECUTE FUNCTION update_executions_updated_at();


-- Trigger to update updated_at timestamp on checkpoints
CREATE OR REPLACE FUNCTION update_checkpoints_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER checkpoints_updated_at_trigger
    BEFORE UPDATE ON checkpoints
    FOR EACH ROW
    EXECUTE FUNCTION update_checkpoints_updated_at();
