#!/bin/bash
# =============================================================================
# FSAMP Processor - Build Script
# =============================================================================
# Builds the Docker image for deployment.
# =============================================================================

set -e

# Configuration
IMAGE_NAME="${IMAGE_NAME:-fsamp-processor}"
IMAGE_TAG="${IMAGE_TAG:-latest}"
REGISTRY="${REGISTRY:-}"
BUILD_DATE=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
VCS_REF=$(git rev-parse --short HEAD 2>/dev/null || echo "unknown")

echo "=========================================="
echo "Building FSAMP Processor"
echo "=========================================="
echo "Image: ${IMAGE_NAME}:${IMAGE_TAG}"
echo "Build Date: ${BUILD_DATE}"
echo "Git Ref: ${VCS_REF}"
echo ""

# Build the image
docker build \
    --build-arg BUILD_DATE="${BUILD_DATE}" \
    --build-arg VCS_REF="${VCS_REF}" \
    --tag "${IMAGE_NAME}:${IMAGE_TAG}" \
    --tag "${IMAGE_NAME}:${VCS_REF}" \
    --file Dockerfile \
    .

echo ""
echo "Build complete!"
echo ""

# Tag for registry if specified
if [ -n "${REGISTRY}" ]; then
    echo "Tagging for registry: ${REGISTRY}"
    docker tag "${IMAGE_NAME}:${IMAGE_TAG}" "${REGISTRY}/${IMAGE_NAME}:${IMAGE_TAG}"
    docker tag "${IMAGE_NAME}:${VCS_REF}" "${REGISTRY}/${IMAGE_NAME}:${VCS_REF}"
    
    echo ""
    echo "To push to registry, run:"
    echo "  docker push ${REGISTRY}/${IMAGE_NAME}:${IMAGE_TAG}"
fi

echo ""
echo "To run locally:"
echo "  docker-compose up"
echo ""
echo "Or standalone:"
echo "  docker run --rm -it ${IMAGE_NAME}:${IMAGE_TAG}"
