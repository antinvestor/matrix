#!/bin/bash

# This script generates Go code from the .proto files

set -eu

# Make sure required tools are installed
if ! command -v protoc &> /dev/null; then
    echo "Error: protoc is not installed. Please install protobuf compiler."
    exit 1
fi

# Check for Go plugins
if ! command -v protoc-gen-go &> /dev/null || ! command -v protoc-gen-go-grpc &> /dev/null; then
    echo "Installing protoc-gen-go and protoc-gen-go-grpc..."
    go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
    go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
fi

# Define proto files to compile
PROTO_FILES=(
    "syncapi/api/presence.proto"
)

# Generate Go code
for proto_file in "${PROTO_FILES[@]}"; do
    echo "Generating Go code from $proto_file..."
    
    # Extract directory from file path
    proto_dir=$(dirname "$proto_file")
    
    # Generate Go code
    protoc \
        --go_out=. \
        --go_opt=paths=source_relative \
        --go-grpc_out=. \
        --go-grpc_opt=paths=source_relative \
        "$proto_file"
    
    echo "Generated Go code from $proto_file"
done

echo "Code generation complete!"
