# Matrix API Definitions

This directory contains the API definitions for all Matrix services, organized as modular, self-contained Go modules.

## Directory Structure

```
apis/
├── buf.work.yaml         # Bufbuild workspace configuration
├── generate.sh                # Main script to generate code for all APIs
├── presence/                  # Presence API
│   └── v1/                    # Version 1
│       ├── buf.yaml           # Bufbuild configuration
│       ├── buf.gen.yaml       # Code generation configuration
│       ├── go.mod             # Go module definition
│       ├── presence.proto     # Protobuf definition
│       └── gen/               # Generated code (not committed)
├── [other-service]/
│   └── v1/
│       └── ...
└── ...
```

## API Organization

Each API is organized as follows:

1. **Service Directory**: Each Matrix service has its own directory (e.g., `presence`, `roomserver`)
2. **Version Directory**: Each version of an API has its own subdirectory (e.g., `v1`, `v2`)
3. **Module Files**: Each API version is a self-contained Go module with its own `go.mod`

## Code Generation

To generate code for all APIs across all versions:

```bash
# From the matrix root directory:
make generate-grpc

# OR directly:
cd apis
./generate.sh
```

The centralized script will:

- Automatically discover all API modules across all versions
- Generate code for each API using its buf.gen.yaml configuration
- Format the generated code for consistency

## Adding a New API

To add a new API:

1. Create a new directory structure: `apis/[service]/[version]/`
2. Create the following files:
    - `buf.yaml` - Bufbuild configuration
    - `buf.gen.yaml` - Code generation configuration
    - `go.mod` - Go module definition
    - `[service].proto` - Protobuf definition

The centralized generate.sh script will automatically detect and process the new API.

## Importing APIs

To use an API in a Matrix service:

```go
import (
    // Import the generated API package
    servicev1 "github.com/antinvestor/matrix/apis/[service]/[version]"
)
```

## Maintaining Backward Compatibility

When updating an API:

1. For minor changes, update the existing version
2. For breaking changes, create a new version directory (e.g., `v2`)
3. Support both versions during transition periods

## Requirements

- [buf](https://buf.build/docs/installation) - For generating code from protobuf definitions
- Go 1.18 or later
