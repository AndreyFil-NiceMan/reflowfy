#!/bin/bash
# ==============================================================================
# Reflofy E2E Test Runner
#
# Starts all required services and runs E2E tests.
#
# Usage:
#   ./scripts/run_e2e_tests.sh              # Run all E2E tests
#   ./scripts/run_e2e_tests.sh sources      # Run only source tests
#   ./scripts/run_e2e_tests.sh destinations # Run only destination tests
#   ./scripts/run_e2e_tests.sh --no-docker  # Skip Docker Compose (services already running)
# ==============================================================================

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration
PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COMPOSE_FILE="$PROJECT_ROOT/docker-compose.e2e.yml"
MOCK_API_PID=""
MOCK_HTTP_PID=""

# Parse arguments
TEST_SUITE="all"
SKIP_DOCKER=false

for arg in "$@"; do
    case $arg in
        sources)
            TEST_SUITE="sources"
            ;;
        destinations)
            TEST_SUITE="destinations"
            ;;
        --no-docker)
            SKIP_DOCKER=true
            ;;
    esac
done

# Functions
log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

cleanup() {
    log_info "Cleaning up..."
    
    # Stop mock servers
    if [ -n "$MOCK_HTTP_PID" ] && kill -0 "$MOCK_HTTP_PID" 2>/dev/null; then
        log_info "Stopping mock HTTP server (PID: $MOCK_HTTP_PID)"
        kill "$MOCK_HTTP_PID" 2>/dev/null || true
    fi
    
    if [ "$SKIP_DOCKER" = false ]; then
        log_info "Stopping Docker Compose services..."
        docker-compose -f "$COMPOSE_FILE" down --remove-orphans 2>/dev/null || true
    fi
    
    log_success "Cleanup complete"
}

trap cleanup EXIT

wait_for_service() {
    local url=$1
    local name=$2
    local max_wait=${3:-60}
    
    log_info "Waiting for $name at $url..."
    
    local waited=0
    while [ $waited -lt $max_wait ]; do
        if curl -sf "$url" > /dev/null 2>&1; then
            log_success "$name is ready"
            return 0
        fi
        sleep 2
        waited=$((waited + 2))
    done
    
    log_error "$name not available after ${max_wait}s"
    return 1
}

# ==============================================================================
# Main Script
# ==============================================================================

echo ""
echo "=============================================="
echo "  Reflofy E2E Test Runner"
echo "=============================================="
echo ""

cd "$PROJECT_ROOT"

# Step 1: Start Docker Compose services
if [ "$SKIP_DOCKER" = false ]; then
    log_info "Starting Docker Compose services..."
    docker-compose -f "$COMPOSE_FILE" up -d
    
    # Wait for services to be healthy (Docker Compose handles health checks)
    log_info "Waiting for services to be healthy..."
    
    # Wait for PostgreSQL
    log_info "Waiting for PostgreSQL..."
    for i in $(seq 1 30); do
        if docker exec reflofy-e2e-postgres pg_isready -U reflowfy > /dev/null 2>&1; then
            log_success "PostgreSQL is ready"
            break
        fi
        if [ $i -eq 30 ]; then
            log_error "PostgreSQL not ready after 60s"
            exit 1
        fi
        sleep 2
    done
    
    # Wait for Elasticsearch
    wait_for_service "http://localhost:9201/_cluster/health" "Elasticsearch" 90 || exit 1
    
    # Wait for ReflowManager
    wait_for_service "http://localhost:8002/health" "ReflowManager" 60 || exit 1
else
    log_warning "Skipping Docker Compose (--no-docker flag)"
fi

# Step 2: Initialize test data
log_info "Initializing test data..."

# Initialize SQL test data
log_info "Initializing SQL test data..."
python -m tests.e2e.sources.init_sql_test_data || {
    log_warning "SQL initialization failed (may already be initialized)"
}

# Initialize Elasticsearch test data
log_info "Initializing Elasticsearch test data..."
python -m tests.e2e.sources.init_elastic_test_data || {
    log_warning "Elasticsearch initialization failed (may already be initialized)"
}

# Step 3: Wait for mock HTTP server
log_info "Waiting for mock HTTP server (running in Docker)..."
wait_for_service "http://localhost:8091/health" "Mock HTTP server" 60 || exit 1

# Step 4: Run tests
log_info "Running E2E tests..."
echo ""

case $TEST_SUITE in
    sources)
        log_info "Running source tests only..."
        pytest tests/e2e/sources/ -v --tb=short
        ;;
    destinations)
        log_info "Running destination tests only..."
        pytest tests/e2e/destinations/ -v --tb=short
        ;;
    all)
        log_info "Running all E2E tests..."
        pytest tests/e2e/ -v --tb=short
        ;;
esac

TEST_EXIT_CODE=$?

echo ""
if [ $TEST_EXIT_CODE -eq 0 ]; then
    log_success "All E2E tests passed!"
else
    log_error "Some E2E tests failed (exit code: $TEST_EXIT_CODE)"
fi

exit $TEST_EXIT_CODE
