#!/bin/bash
# ==============================================================================
# Reflofy E2E Test Runner
#
# Builds the package, installs it, starts services via CLI, and runs E2E tests.
#
# Usage:
#   ./scripts/run_e2e_tests.sh              # Run all E2E tests
#   ./scripts/run_e2e_tests.sh sources      # Run only source tests
#   ./scripts/run_e2e_tests.sh destinations # Run only destination tests
#   ./scripts/run_e2e_tests.sh dx           # Run only Developer Experience tests
#   ./scripts/run_e2e_tests.sh --no-docker  # Skip services start (assume running)
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
WORKSPACE="$PROJECT_ROOT/e2e_workspace"
DIST_DIR="$PROJECT_ROOT/dist"

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
        dx)
            TEST_SUITE="dx"
            ;;
        all)
             TEST_SUITE="all"
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
    
    if [ "$SKIP_DOCKER" = false ]; then
        if [ -d "$WORKSPACE" ]; then
            log_info "Stopping Docker Compose services..."
            (cd "$WORKSPACE" && docker compose down --remove-orphans 2>/dev/null || true)
            (cd "$WORKSPACE" && docker compose -f docker-compose.e2e-infra.yml down --remove-orphans 2>/dev/null || true)
        fi
    fi
    
    log_info "Removing workspace and dist folders..."
    rm -rf "$WORKSPACE"
    rm -rf "$DIST_DIR"
    
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
        echo -n "."
    done
    echo ""
    
    log_error "$name not available after ${max_wait}s"
    return 1
}

wait_for_kafka() {
    local max_wait=${1:-90}
    log_info "Waiting for Kafka to be healthy..."
    
    local waited=0
    while [ $waited -lt $max_wait ]; do
        if docker exec reflofy-e2e-kafka kafka-topics --list --bootstrap-server localhost:29092 > /dev/null 2>&1; then
            log_success "Kafka is ready"
            return 0
        fi
        sleep 3
        waited=$((waited + 3))
        echo -n "."
    done
    echo ""
    
    log_error "Kafka not ready after ${max_wait}s"
    log_info "Kafka logs:"
    docker logs reflofy-e2e-kafka 2>&1 | tail -30
    return 1
}

# ==============================================================================
# Main Script
# ==============================================================================

echo ""
echo "=============================================="
echo "  Reflofy E2E Test Runner (CLI Integration)"
echo "=============================================="
echo ""

cd "$PROJECT_ROOT"

# Step 0: Build and Install Package
log_info "Step 0: Building and Installing Reflowfy..."

if ! command -v python3 &> /dev/null; then
    log_error "python3 could not be found"
    exit 1
fi

# Clean dist
rm -rf "$DIST_DIR"
rm -rf "$WORKSPACE"

# Build package
log_info "Building package..."
python3 -m build || {
    log_error "Failed to build package. Make sure 'build' is installed (pip install build)"
    exit 1
}

# Install wheel
WHEEL_FILE=$(find "$DIST_DIR" -name "*.whl" | head -n 1)
if [ -z "$WHEEL_FILE" ]; then
    log_error "No wheel file found in $DIST_DIR"
    exit 1
fi

log_info "Installing $WHEEL_FILE..."
pip install --force-reinstall "$WHEEL_FILE" || {
    log_error "Failed to install package"
    exit 1
}
log_success "Reflowfy installed successfully"

# Step 1: Initialize Workspace
log_info "Step 1: Initializing E2E Workspace..."

mkdir -p "$WORKSPACE"
cd "$WORKSPACE"

# Run reflowfy init
log_info "Running reflowfy init..."
python3 -m reflowfy.cli.main init . --name e2e_pipeline || {
    log_error "reflowfy init failed"
    exit 1
}

# Verify files and directories exist
REQUIRED_DIRS=("pipelines" "sources" "destinations" "transformations" "queries")
for dir in "${REQUIRED_DIRS[@]}"; do
    if [ ! -d "$dir" ]; then
        log_error "Missing expected directory after init: $dir"
        exit 1
    fi
done

REQUIRED_FILES=("pipelines/e2e_pipeline.py" ".env" "Dockerfile.api" "Dockerfile.reflow-manager" "Dockerfile.worker" "docker-compose.yml")
for file in "${REQUIRED_FILES[@]}"; do
    if [ ! -f "$file" ]; then
        log_error "Missing expected file after init: $file"
        exit 1
    fi
done
log_success "Workspace initialized and verified"

# Step 2: Configure for E2E Testing
log_info "Step 2: Configuring Workspace for E2E..."

# Copy the wheel to workspace
WHEEL_FILENAME=$(basename "$WHEEL_FILE")
cp "$WHEEL_FILE" .

# Modify Dockerfiles to install from local wheel instead of PyPI
log_info "Modifying Dockerfiles to install from local wheel..."

for dockerfile in Dockerfile.api Dockerfile.reflow-manager Dockerfile.worker; do
    # Replace "RUN pip install --no-cache-dir reflowfy" with COPY wheel + install
    sed -i "s|RUN pip install --no-cache-dir reflowfy|COPY $WHEEL_FILENAME /tmp/$WHEEL_FILENAME\nRUN pip install --no-cache-dir /tmp/$WHEEL_FILENAME|" "$dockerfile"
    log_success "  Modified $dockerfile"
done

# Modify docker-compose.yml for E2E testing (different ports, container names, env vars)
log_info "Modifying docker-compose.yml for E2E..."

# Change container names to avoid conflicts
sed -i 's/container_name: reflowfy-/container_name: reflofy-e2e-/g' docker-compose.yml

# Change ports to E2E ports (5432->5433, 8001->8002, 8000->8003)
sed -i 's/"5432:5432"/"5433:5432"/g' docker-compose.yml
sed -i 's/"8001:8001"/"8002:8001"/g' docker-compose.yml
sed -i 's/"8000:8000"/"8003:8000"/g' docker-compose.yml
sed -i 's/"5050:80"/"5051:80"/g' docker-compose.yml

# Add environment variables for E2E test sources (elasticsearch, sql, mock servers)
sed -i '/EXECUTION_MODE: local/a\      ELASTICSEARCH_URL: http://reflofy-e2e-elasticsearch:9200\n      SQL_CONNECTION_URL: postgresql://reflowfy:reflowfy@postgres:5432/reflowfy\n      MOCK_HTTP_URL: http://reflofy-e2e-mock-http:8091/webhook\n      MOCK_API_URL: http://reflofy-e2e-mock-api:8092\n      DLQ_POLL_INTERVAL_SECONDS: 5' docker-compose.yml

# Point Kafka to the internal e2e-kafka container
sed -i 's/KAFKA_BOOTSTRAP_SERVERS: "ignored:9092"/KAFKA_BOOTSTRAP_SERVERS: "reflofy-e2e-kafka:29092"/g' docker-compose.yml

# Change PIPELINE_MODULE to load E2E test pipelines
sed -i 's/PIPELINE_MODULE: pipelines/PIPELINE_MODULE: tests.e2e.test_pipelines/g' docker-compose.yml

# Modify Dockerfiles to also COPY tests folder for E2E pipeline module
log_info "Adding tests folder to Dockerfiles..."
for dockerfile in Dockerfile.api Dockerfile.reflow-manager; do
    # Add COPY tests after COPY pipelines
    sed -i 's|COPY pipelines/ pipelines/|COPY pipelines/ pipelines/\nCOPY tests/ tests/|' "$dockerfile"
done

# Copy E2E test pipelines to pipelines/ folder
log_info "Copying E2E test pipelines..."
cp -r "$PROJECT_ROOT/tests/e2e/test_pipelines/"* pipelines/ || true

# Copy tests folder for mock servers and test pipelines
cp -r "$PROJECT_ROOT/tests" .

# Copy E2E infrastructure compose file
cp "$PROJECT_ROOT/docker-compose.e2e-infra.yml" .

# Make the workspace docker compose use the same network name as infra
# The infra compose creates "e2e_workspace_reflowfy-network" with driver: bridge.
# The workspace compose uses "reflowfy-network" which docker compose names as
# "<project>_reflowfy-network" = "e2e_workspace_reflowfy-network". These match!
# But to be safe, let's use external: true pointing to the infra-created network.
sed -i '/^networks:/,/^volumes:/{
  /driver: bridge/c\    name: e2e_workspace_reflowfy-network\n    external: true
}' docker-compose.yml

log_success "Workspace configured for E2E"

# Step 3: Run Services
if [ "$SKIP_DOCKER" = false ]; then
    log_info "Step 3: Starting Services..."
    
    # Clean up any leftover containers/networks from previous runs
    log_info "Cleaning up previous E2E state..."
    docker compose down --remove-orphans 2>/dev/null || true
    docker compose -f docker-compose.e2e-infra.yml down --remove-orphans 2>/dev/null || true
    docker network rm e2e_workspace_reflowfy-network 2>/dev/null || true

    # Start E2E infrastructure FIRST (creates the shared network, starts Kafka, ES, mocks)
    log_info "Starting E2E test infrastructure..."
    docker compose -f docker-compose.e2e-infra.yml up -d --build || {
        log_error "E2E infrastructure startup failed"
        exit 1
    }

    # Wait for Kafka to be fully healthy before starting main services
    wait_for_kafka 120 || exit 1

    # Create Kafka topics explicitly
    log_info "Creating Kafka topics..."
    docker exec reflofy-e2e-kafka kafka-topics --create \
        --topic reflow.jobs \
        --bootstrap-server localhost:29092 \
        --replication-factor 1 --partitions 1 \
        --if-not-exists 2>/dev/null || log_warning "Topic reflow.jobs might already exist"
    docker exec reflofy-e2e-kafka kafka-topics --create \
        --topic e2e-test-destination \
        --bootstrap-server localhost:29092 \
        --replication-factor 1 --partitions 1 \
        --if-not-exists 2>/dev/null || log_warning "Topic e2e-test-destination might already exist"
    log_success "Kafka topics created"

    # Start main services (they join the existing network)
    log_info "Starting main Reflowfy services..."
    python3 -m reflowfy.cli.main run --build --detach || {
        log_error "reflowfy run failed"
        exit 1
    }
    
    # Wait for services
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
    
    wait_for_service "http://localhost:9201/_cluster/health" "Elasticsearch" 90 || exit 1
    wait_for_service "http://localhost:8002/health" "ReflowManager" 120 || exit 1
    
    # Wait for mock services used in E2E
    wait_for_service "http://localhost:8091/health" "Mock HTTP server" 60 || exit 1
    wait_for_service "http://localhost:8092/health" "Mock API server" 60 || {
         log_warning "Mock API server not available"
    }

    # Initialize test data
    log_info "Initializing test data..."
    
    log_info "  - PostgreSQL data..."
    export SQL_CONNECTION_URL="postgresql://reflowfy:reflowfy@localhost:5433/reflowfy"
    python3 tests/e2e/sources/init_sql_test_data.py || log_warning "Failed to init SQL data"
    
    log_info "  - Elasticsearch data..."
    export ELASTICSEARCH_URL="http://localhost:9201"
    python3 tests/e2e/sources/init_elastic_test_data.py || log_warning "Failed to init Elastic data"
    
    # Export Kafka SASL config for tests running on host
    export E2E_KAFKA_SERVERS="localhost:9095"
    export KAFKA_SECURITY_PROTOCOL="SASL_PLAINTEXT"
    export KAFKA_SASL_MECHANISM="PLAIN"
    export KAFKA_SASL_USERNAME="admin"
    export KAFKA_SASL_PASSWORD="admin-secret"

else
    log_warning "Skipping Docker Start (--no-docker)"
fi

# Step 4: Run Tests
log_info "Step 4: Running E2E Tests..."

# We need to run tests from the Workspace or Project Root?
# The tests usually import 'reflowfy'. Since we installed it, it works anywhere.
# But existing tests might rely on relative paths for data files.
# Let's run from PROJECT_ROOT but pointing to the running services (configured via default ports)
cd "$PROJECT_ROOT"

case $TEST_SUITE in
    sources)
        pytest tests/e2e/sources/ -v --tb=short
        ;;
    destinations)
        pytest tests/e2e/destinations/ -v --tb=short -ra
        ;;
    dx)
        pytest tests/e2e/test_auto_registration.py tests/e2e/test_decorator_components.py tests/e2e/test_cli_scaffolding.py tests/e2e/test_cli_build.py tests/e2e/test_cli_run.py tests/e2e/test_cli_check.py tests/e2e/test_cli_deploy.py tests/e2e/test_cli_test.py -v --tb=short -ra
        ;;
    all)
        pytest tests/e2e/ -v --tb=short -ra
        ;;
esac

TEST_EXIT_CODE=$?

if [ $TEST_EXIT_CODE -eq 0 ]; then
    log_success "All E2E tests passed!"
else
    log_error "Some E2E tests failed (exit code: $TEST_EXIT_CODE)"
fi

exit $TEST_EXIT_CODE
