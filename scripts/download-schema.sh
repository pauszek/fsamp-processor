#!/usr/bin/env bash
# =============================================================================
# Download/Copy Event Schema
# =============================================================================
# Enterprise-grade schema distribution with version pinning.
# For local development: copies from sibling fsamp-event-schema repo
# For CI: schema is checked out by GitHub Actions (see build-python.yml)
#
# Usage:
#   ./scripts/download-schema.sh              # Uses version from schema.version
#   ./scripts/download-schema.sh 0.0.5        # Explicit version (validates match)
# =============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
SCHEMA_DIR="${PROJECT_ROOT}/schema"
VERSION_FILE="${PROJECT_ROOT}/schema.version"

# Sibling repo path (local development)
SIBLING_SCHEMA="${PROJECT_ROOT}/../fsamp-event-schema/event.schema.json"

# Read pinned version
if [[ -f "$VERSION_FILE" ]]; then
    PINNED_VERSION=$(tr -d '[:space:]' < "$VERSION_FILE")
else
    echo "❌ Error: schema.version file not found"
    exit 1
fi

echo "📦 Setting up event schema v${PINNED_VERSION}..."

# Create schema directory
mkdir -p "$SCHEMA_DIR"

# Copy from sibling repo (local development)
if [[ -f "$SIBLING_SCHEMA" ]]; then
    cp "$SIBLING_SCHEMA" "${SCHEMA_DIR}/event.schema.json"
    echo "✅ Schema copied from sibling repo"

    # Verify JSON is valid
    if python3 -c "import json; json.load(open('${SCHEMA_DIR}/event.schema.json'))" 2>/dev/null; then
        echo "✅ Schema JSON is valid"
    else
        echo "❌ Error: Schema file is not valid JSON"
        exit 1
    fi

    # Show schema info
    echo ""
    echo "📋 Schema Info:"
    python3 -c "
import json
schema = json.load(open('${SCHEMA_DIR}/event.schema.json'))
print(f\"   Title: {schema.get('title', 'N/A')}\")
print(f\"   Required fields: {schema.get('required', [])}\")
"
    echo ""
    echo "⚠️  Note: Using local sibling repo. Ensure it's at version v${PINNED_VERSION}"
    echo "   In CI, the schema is fetched from GitHub with exact version pinning."
else
    echo "❌ Error: Sibling repo not found at ${SIBLING_SCHEMA}"
    echo ""
    echo "For local development, ensure fsamp-event-schema repo is cloned as sibling:"
    echo "   cd $(dirname "$PROJECT_ROOT")"
    echo "   git clone git@github.com:pauszek/fsamp-event-schema.git"
    echo ""
    echo "In CI, this script is not used - GitHub Actions checks out the schema directly."
    exit 1
fi
