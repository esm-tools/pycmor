#!/usr/bin/env bash
set -e

# Run pytest in Docker container matching CI environment
#
# Usage:
#   ./utils/run-pytest-in-docker.sh                    # Run doctests (default)
#   TEST_TYPE=unit ./utils/run-pytest-in-docker.sh     # Run unit tests
#   TEST_TYPE=integration ./utils/run-pytest-in-docker.sh
#   TEST_TYPE=meta ./utils/run-pytest-in-docker.sh
#   TEST_TYPE=all ./utils/run-pytest-in-docker.sh
#   PYTHON_VERSION=3.10 ./utils/run-pytest-in-docker.sh
#   BRANCH=main ./utils/run-pytest-in-docker.sh
#
# Environment variables:
#   PYTHON_VERSION - Python version (default: 3.11)
#   BRANCH - Git branch for image tag (default: prep-release)
#   TEST_TYPE - Test type: doctest, unit, integration, meta, all (default: doctest)
#   IMAGE - Full image name (overrides PYTHON_VERSION and BRANCH)

# Default values
PYTHON_VERSION="${PYTHON_VERSION:-3.11}"
BRANCH="${BRANCH:-prep-release}"
TEST_TYPE="${TEST_TYPE:-doctest}"
IMAGE="${IMAGE:-ghcr.io/esm-tools/pycmor-testground:py${PYTHON_VERSION}-${BRANCH}}"

# Create cache directory if it doesn't exist
CACHE_DIR="${HOME}/.cache/pycmor"
mkdir -p "${CACHE_DIR}"

# Determine pytest command based on test type
case "${TEST_TYPE}" in
  doctest)
    PYTEST_CMD="PYTHONPATH=src PYTHONLOGLEVEL=CRITICAL pytest -v --doctest-modules --cov=src/pycmor src/"
    ;;
  unit)
    PYTEST_CMD="pytest -vvv -s --cov=src/pycmor tests/unit/"
    ;;
  integration)
    PYTEST_CMD="pytest -vvv -s --cov=src/pycmor tests/integration/"
    ;;
  meta)
    PYTEST_CMD="pytest -vvv -s --cov=src/pycmor tests/meta/"
    ;;
  all)
    PYTEST_CMD="pytest -vvv -s --cov=src/pycmor tests/"
    ;;
  *)
    echo "Unknown test type: ${TEST_TYPE}"
    echo "Valid options: doctest, unit, integration, meta, all"
    exit 1
    ;;
esac

echo "Running ${TEST_TYPE} tests with Python ${PYTHON_VERSION}"
echo "Image: ${IMAGE}"
echo ""

docker run --rm \
  -e PREFECT_SERVER_EPHEMERAL_STARTUP_TIMEOUT_SECONDS=300 \
  -v "$(pwd):/workspace" \
  -v "${CACHE_DIR}:/root/.cache/pycmor" \
  "${IMAGE}" \
  bash -c "${PYTEST_CMD}"
