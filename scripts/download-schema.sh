#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
SCHEMA_DIR="${PROJECT_ROOT}/schema"
VERSION_FILE="${PROJECT_ROOT}/schema.version"

SIBLING_SCHEMA="${PROJECT_ROOT}/../fsamp-event-schema/event.schema.json"

if [[ -f "$VERSION_FILE" ]]; then
    PINNED_VERSION=$(tr -d '[:space:]' < "$VERSION_FILE")
else
    echo "Error: schema.version file not found"
    exit 1
fi

echo "Setting up event schema v${PINNED_VERSION}..."

mkdir -p "$SCHEMA_DIR"

if [[ -f "$SIBLING_SCHEMA" ]]; then
    cp "$SIBLING_SCHEMA" "${SCHEMA_DIR}/event.schema.json"
    echo "Schema copied from sibling repo"

    if python3 -c "import json; json.load(open('${SCHEMA_DIR}/event.schema.json'))" 2>/dev/null; then
        echo "Schema JSON is valid"
    else
        echo "Error: Schema file is not valid JSON"
        exit 1
    fi

    echo ""
    echo "Schema Info:"
    python3 -c "
import json
schema = json.load(open('${SCHEMA_DIR}/event.schema.json'))
print(f\"   Title: {schema.get('title', 'N/A')}\")
print(f\"   Required fields: {schema.get('required', [])}\")
"
    echo ""
    echo "Note: Using local sibling repo. Ensure it's at version v${PINNED_VERSION}"
    echo "   In CI, the schema is fetched from GitHub with exact version pinning."
else
    echo "Error: Sibling repo not found at ${SIBLING_SCHEMA}"
    echo ""
    echo "For local development, ensure fsamp-event-schema repo is cloned as sibling:"
    echo "   cd $(dirname "$PROJECT_ROOT")"
    echo "   git clone git@github.com:pauszek/fsamp-event-schema.git"
    echo ""
    echo "In CI, this script is not used - GitHub Actions checks out the schema directly."
    exit 1
fi
