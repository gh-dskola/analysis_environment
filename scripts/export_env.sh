#!/bin/bash
#
# Export a conda/mamba environment to an environment.yaml file
#
# Usage: export_env.sh [-n ENV_NAME] [-o OUTPUT_FILE] [--names-only | --with-hashes]
#

set -euo pipefail

# Defaults
ENV_NAME=""
OUTPUT_FILE="environment.yaml"
EXPORT_MODE="versions"  # versions, names, hashes
YAML_NAME=""            # name to write in the YAML file (empty = no name)

usage() {
    cat << EOF
Usage: $(basename "$0") [-n ENV_NAME] [-o OUTPUT_FILE] [OPTIONS]

Export a conda/mamba environment to an environment.yaml file.

Arguments:
    -n, --name ENV_NAME       Name of the environment to export (default: current environment)
    -o, --output OUTPUT_FILE  Output file path (default: environment.yaml)

Export Options (mutually exclusive):
    --with-versions           Include package versions (default)
    --names-only              Export only package names, no versions
    --with-hashes             Include package versions and build hashes

Naming Options (mutually exclusive):
    --yaml-name NAME          Set the environment name in the YAML file
    --name-from-dir           Use current directory name as the YAML environment name

    -h, --help                Show this help message

Examples:
    $(basename "$0")                              # export current environment
    $(basename "$0") -n myenv                     # export named environment
    $(basename "$0") -o myenv.yaml --names-only   # names only, current env
    $(basename "$0") -n myenv --with-hashes       # with build hashes
    $(basename "$0") --name-from-dir              # use current dir as YAML name
    $(basename "$0") --yaml-name myproject        # set custom YAML name
EOF
    exit "${1:-0}"
}

# Parse arguments
while [[ $# -gt 0 ]]; do
    case "$1" in
        -n|--name)
            ENV_NAME="$2"
            shift 2
            ;;
        -o|--output)
            OUTPUT_FILE="$2"
            shift 2
            ;;
        --with-versions)
            EXPORT_MODE="versions"
            shift
            ;;
        --names-only)
            EXPORT_MODE="names"
            shift
            ;;
        --with-hashes)
            EXPORT_MODE="hashes"
            shift
            ;;
        --yaml-name)
            YAML_NAME="$2"
            shift 2
            ;;
        --name-from-dir)
            YAML_NAME="$(basename "$PWD")"
            shift
            ;;
        -h|--help)
            usage 0
            ;;
        *)
            echo "Error: Unknown option: $1" >&2
            usage 1
            ;;
    esac
done

# Default to current environment if not specified
if [[ -z "$ENV_NAME" ]]; then
    if [[ -z "${CONDA_DEFAULT_ENV:-}" ]]; then
        echo "Error: No environment specified and no active conda environment detected" >&2
        usage 1
    fi
    ENV_NAME="$CONDA_DEFAULT_ENV"
fi

# Detect conda or mamba
if command -v mamba &> /dev/null; then
    CONDA_CMD="mamba"
elif command -v conda &> /dev/null; then
    CONDA_CMD="conda"
else
    echo "Error: Neither conda nor mamba found in PATH" >&2
    exit 1
fi

echo "Using $CONDA_CMD to export environment '$ENV_NAME'..."

# Build export command based on mode
# All modes strip "name:" and "prefix:" lines, then optionally add custom name
case "$EXPORT_MODE" in
    versions)
        # Export with versions but without build hashes
        $CONDA_CMD env export -n "$ENV_NAME" --no-builds | \
            grep -v '^name:\|^prefix:' > "$OUTPUT_FILE"
        ;;
    names)
        # Export with only package names, no versions
        $CONDA_CMD env export -n "$ENV_NAME" --no-builds | \
            grep -v '^name:\|^prefix:' | \
            sed 's/=.*//' > "$OUTPUT_FILE"
        ;;
    hashes)
        # Export with full versions and build hashes
        $CONDA_CMD env export -n "$ENV_NAME" | \
            grep -v '^name:\|^prefix:' > "$OUTPUT_FILE"
        ;;
esac

# Prepend name if specified
if [[ -n "$YAML_NAME" ]]; then
    TMP_FILE=$(mktemp)
    echo "name: $YAML_NAME" > "$TMP_FILE"
    cat "$OUTPUT_FILE" >> "$TMP_FILE"
    mv "$TMP_FILE" "$OUTPUT_FILE"
fi

echo "Environment exported to: $OUTPUT_FILE"
