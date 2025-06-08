#!/bin/bash
set -e

# This script generates Go code from Protocol Buffer definitions for all APIs using bufbuild v2

# Use absolute path to buf to avoid any aliases or PATH issues
BUF_CMD="/usr/local/bin/buf"

# Check for buf command
if [ ! -x "$BUF_CMD" ]; then
    echo "Error: buf command not found at $BUF_CMD"
    echo "Please install buf from https://buf.build/docs/installation"
    exit 1
fi

# Verify buf version is 2.x
BUF_VERSION=$($BUF_CMD --version 2>/dev/null || echo "unknown")
if [[ ! "$BUF_VERSION" =~ ^1\.[0-9]+\.[0-9]+ ]]; then
    echo "Warning: Expected buf version 1.x but found $BUF_VERSION"
    echo "Some features may not work as expected."
fi

ROOT_DIR=$(pwd)
APIS_DIR="${ROOT_DIR}"

# Find all API directories with proto files and buf.yaml
MODULE_DIRS=$(find "${APIS_DIR}" -type f -name "buf.yaml" -exec dirname {} \; | sort | uniq)

if [ -z "$MODULE_DIRS" ]; then
    echo "No buf.yaml files found in ${APIS_DIR}"
    exit 0
fi

echo "Installing proto plugins required ..."
go install github.com/asynkron/protoactor-go/protobuf/protoc-gen-go-grain@latest

echo "Generating Go code for APIs..."

for MODULE_DIR in $MODULE_DIRS; do
    echo "Processing module: ${MODULE_DIR}"
    (
        cd "${MODULE_DIR}"
        
        # Generate code with buf v2
        echo "  Generating code..."
        $BUF_CMD generate
        echo "  Successfully generated code"
        
        # Format generated code if gen directory exists
        if [ -d "gen" ] && command -v gofmt &> /dev/null; then
            echo "  Formatting generated code..."
            find gen -name "*.go" -exec gofmt -w {} \;
        fi
    )
    
    if [ $? -ne 0 ]; then
        echo "Failed to generate code for ${MODULE_DIR}"
        exit 1
    fi
done

echo "All API code generation completed successfully"
