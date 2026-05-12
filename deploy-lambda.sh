#!/usr/bin/env bash
# =============================================================================
# FSAMP Processor - Lambda Deployment Script
# =============================================================================
# Build and deploy the processor Lambda function.
#
# Usage:
#   ./deploy-lambda.sh                    # Build and package only
#   ./deploy-lambda.sh deploy dev         # Deploy to dev environment
#   ./deploy-lambda.sh deploy prod        # Deploy to production
#   ./deploy-lambda.sh local              # Test locally with SAM
# =============================================================================

set -euo pipefail

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Configuration
FUNCTION_NAME_PREFIX="fsamp"
RUNTIME="python3.14"
ARCHITECTURE="arm64"
BUILD_DIR="build"
DIST_DIR="dist"

log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# -----------------------------------------------------------------------------
# Build Lambda Package
# -----------------------------------------------------------------------------
build_package() {
    log_info "Building Lambda deployment package..."

    # Clean previous builds
    rm -rf "${BUILD_DIR}" "${DIST_DIR}"
    mkdir -p "${BUILD_DIR}" "${DIST_DIR}"

    # Install dependencies
    log_info "Installing dependencies..."
    pip install --target "${BUILD_DIR}" --upgrade \
        --platform manylinux2014_aarch64 \
        --only-binary=:all: \
        -r <(pip-compile --quiet --output-file=- pyproject.toml 2>/dev/null || pip freeze)

    # If pip-compile not available, use pip directly
    if [ ! -d "${BUILD_DIR}/boto3" ]; then
        log_warn "Using pip install directly..."
        pip install --target "${BUILD_DIR}" .
    fi

    # Copy source code
    log_info "Copying source code..."
    cp -r src/processor "${BUILD_DIR}/"

    # Create ZIP
    log_info "Creating deployment package..."
    cd "${BUILD_DIR}"
    zip -r "../${DIST_DIR}/deployment.zip" . -x "*.pyc" -x "__pycache__/*" -x "*.dist-info/*"
    cd ..

    PACKAGE_SIZE=$(du -h "${DIST_DIR}/deployment.zip" | cut -f1)
    log_info "Package created: ${DIST_DIR}/deployment.zip (${PACKAGE_SIZE})"

    # Check size (Lambda limit is 50MB zipped, 250MB unzipped)
    PACKAGE_BYTES=$(stat -f%z "${DIST_DIR}/deployment.zip" 2>/dev/null || stat -c%s "${DIST_DIR}/deployment.zip")
    if [ "$PACKAGE_BYTES" -gt 52428800 ]; then
        log_error "Package exceeds Lambda 50MB limit! Consider using container image."
        exit 1
    fi
}

# -----------------------------------------------------------------------------
# Build Container Image (alternative deployment)
# -----------------------------------------------------------------------------
build_container() {
    log_info "Building Lambda container image..."

    docker build -f Dockerfile.lambda -t "fsamp-processor-lambda:latest" .

    log_info "Container image built: fsamp-processor-lambda:latest"
}

# -----------------------------------------------------------------------------
# Deploy to AWS Lambda
# -----------------------------------------------------------------------------
deploy() {
    local env="${1:-dev}"
    local function_name="${FUNCTION_NAME_PREFIX}-${env}-processor"

    log_info "Deploying to ${function_name}..."

    # Check if function exists
    if ! aws lambda get-function --function-name "${function_name}" &>/dev/null; then
        log_error "Function ${function_name} does not exist. Create it with Terraform first."
        exit 1
    fi

    # Update function code
    log_info "Updating function code..."
    aws lambda update-function-code \
        --function-name "${function_name}" \
        --zip-file "fileb://${DIST_DIR}/deployment.zip" \
        --architectures "${ARCHITECTURE}"

    # Wait for update to complete
    log_info "Waiting for update to complete..."
    aws lambda wait function-updated --function-name "${function_name}"

    # Publish new version (optional)
    log_info "Publishing new version..."
    VERSION=$(aws lambda publish-version \
        --function-name "${function_name}" \
        --description "Deployed at $(date -u +%Y-%m-%dT%H:%M:%SZ)" \
        --query 'Version' --output text)

    log_info "Deployed version: ${VERSION}"

    # Get function info
    aws lambda get-function --function-name "${function_name}" \
        --query '{FunctionName: Configuration.FunctionName, Runtime: Configuration.Runtime, MemorySize: Configuration.MemorySize, Timeout: Configuration.Timeout, LastModified: Configuration.LastModified}' \
        --output table
}

# -----------------------------------------------------------------------------
# Local Testing with SAM
# -----------------------------------------------------------------------------
test_local() {
    log_info "Starting local Lambda with SAM..."

    # Check if SAM is installed
    if ! command -v sam &>/dev/null; then
        log_error "AWS SAM CLI not installed. Install it: brew install aws-sam-cli"
        exit 1
    fi

    # Build with SAM
    log_info "Building with SAM..."
    sam build --use-container

    # Invoke locally
    log_info "Invoking Lambda locally..."
    sam local invoke ProcessorFunction \
        -e events/sample-sqs-event.json \
        --env-vars env.json
}

# -----------------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------------
main() {
    local command="${1:-build}"

    case "$command" in
        build)
            build_package
            ;;
        container)
            build_container
            ;;
        deploy)
            build_package
            deploy "${2:-dev}"
            ;;
        local)
            test_local
            ;;
        *)
            echo "Usage: $0 {build|container|deploy <env>|local}"
            exit 1
            ;;
    esac
}

main "$@"
