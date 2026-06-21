#!/bin/bash
# ==============================================================================
# Reflowfy base image builder (MAINTAINER ONLY)
#
# Builds and pushes the shared reflowfy-base image that end users consume as
# the FROM of their api/reflow-manager/worker Dockerfiles. Run this once per
# reflowfy release.
#
# Usage:
#   ./scripts/build_base_image.sh                       # build + push :<version> and :latest
#   REFLOWFY_VERSION=1.0.1 ./scripts/build_base_image.sh
#   REFLOWFY_BASE_IMAGE=myregistry.local/reflowfy-base ./scripts/build_base_image.sh
#   PYTHON_IMAGE=python:3.12-slim ./scripts/build_base_image.sh
#   ./scripts/build_base_image.sh --no-push             # build locally only
#
# Environment / .env:
#   REFLOWFY_BASE_IMAGE  Image repository (no tag). Default: reflowfy-base
#   REFLOWFY_VERSION     reflowfy version to bake in. Default: current package version
#   PYTHON_IMAGE         Python base image. Default: python:3.11-slim
# ==============================================================================

set -e

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT_ROOT"

# Load .env if present (for REFLOWFY_BASE_IMAGE / REGISTRY / PROJECT etc.)
if [ -f .env ]; then
    set -a
    # shellcheck disable=SC1091
    . ./.env
    set +a
fi

PUSH=true
for arg in "$@"; do
    case $arg in
        --no-push) PUSH=false ;;
    esac
done

# Image repository (strip any accidental :tag from REFLOWFY_BASE_IMAGE)
BASE_REPO="${REFLOWFY_BASE_IMAGE:-reflowfy-base}"
BASE_REPO="${BASE_REPO%%:*}"

PYTHON_IMAGE="${PYTHON_IMAGE:-python:3.11-slim}"

# Resolve reflowfy version: explicit env wins, else read from the package.
if [ -z "$REFLOWFY_VERSION" ]; then
    REFLOWFY_VERSION="$(uv run python -c 'import reflowfy; print(reflowfy.__version__)' 2>/dev/null || true)"
fi

if [ -z "$REFLOWFY_VERSION" ]; then
    echo "[WARN] Could not determine REFLOWFY_VERSION; tagging :latest only and installing latest reflowfy."
    TAGS=("${BASE_REPO}:latest")
    VERSION_BUILD_ARG=""
else
    TAGS=("${BASE_REPO}:${REFLOWFY_VERSION}" "${BASE_REPO}:latest")
    VERSION_BUILD_ARG="$REFLOWFY_VERSION"
fi

echo "=============================================="
echo "  Building reflowfy base image"
echo "    repo:          ${BASE_REPO}"
echo "    python image:  ${PYTHON_IMAGE}"
echo "    reflowfy:      ${REFLOWFY_VERSION:-latest}"
echo "    push:          ${PUSH}"
echo "=============================================="

TAG_ARGS=()
for t in "${TAGS[@]}"; do
    TAG_ARGS+=(-t "$t")
done

docker build \
    -f Dockerfile.base \
    --build-arg "PYTHON_IMAGE=${PYTHON_IMAGE}" \
    --build-arg "REFLOWFY_VERSION=${VERSION_BUILD_ARG}" \
    "${TAG_ARGS[@]}" \
    .

echo "[OK] Built: ${TAGS[*]}"

if [ "$PUSH" = true ]; then
    for t in "${TAGS[@]}"; do
        echo "[PUSH] $t"
        docker push "$t"
    done
    echo "[OK] Pushed: ${TAGS[*]}"
else
    echo "[SKIP] --no-push given; image left local."
fi
