# Migration Plan: NATS to gRPC for Presence Functionality

This document outlines the plan to migrate the presence functionality from NATS synchronous messaging to gRPC across
various services in Matrix.

## Overview

Currently, the following components use NATS for presence communication:

- `clientapi/routing/presence.go` - `GetPresence()`
- `federationapi/consumers/roomserver.go` - `sendPresence()`
- `syncapi/consumers/presence.go` - `RequestPresenceConsumer.Handle()`

The migration will introduce a gRPC interface while maintaining backward compatibility with NATS during the transition
period.

## Implementation Details

### 1. Modular API Structure

The gRPC services are organized as self-contained Go modules within the `apis` directory:

```
apis/
├── presence/
│   └── v1/             # Presence API version 1
│       ├── buf.yaml    # Bufbuild configuration
│       ├── buf.gen.yaml
│       ├── go.mod      # Self-contained Go module
│       ├── presence.proto
│       └── generate.sh
└── other-service/
    └── v1/
        └── ...
```

Each API module can be built independently using `buf` and imported as a Go module by any service that needs it.

### 2. Protocol Buffers Definition

The gRPC service for presence is defined in `apis/presence/v1/presence.proto`:

```protobuf
service PresenceService {
  rpc GetPresence(GetPresenceRequest) returns (GetPresenceResponse) {}
  rpc SendPresence(SendPresenceRequest) returns (SendPresenceResponse) {}
}
```

### 3. gRPC Server Implementation

The gRPC server is implemented in the SyncAPI component:

- `syncapi/api/presence_grpc.go` - Implements the gRPC service
- `syncapi/api/grpc_server.go` - Manages the gRPC server

### 4. gRPC Client Implementations

Clients are implemented in:

- `clientapi/routing/presence_grpc.go` - Client for the ClientAPI
- `federationapi/consumers/presence_grpc.go` - Client for the FederationAPI

### 5. Configuration

Added configuration options to enable/disable gRPC and maintain backward compatibility:

```yaml
syncapi:
  matrix:
    presence_grpc_enabled: true      # Enable gRPC for presence
    presence_grpc_port: 7563         # Port for the gRPC server
    enable_nats_for_presence: true   # Keep NATS for backward compatibility
```

### 6. Fallback Mechanism

All gRPC implementations include fallback to NATS if:

- gRPC is disabled in configuration
- gRPC connection fails

## Migration Steps

1. Deploy the new code with both gRPC and NATS enabled
2. Monitor for any issues in the gRPC implementation
3. Once stable, disable NATS by setting `enable_nats_for_presence: false`
4. Remove NATS code in a future update

## Build and Generation

The migration adds a new `generate-grpc` make target that uses `buf` to generate Go code from the proto files:

```shell
make generate-grpc
```

This will iterate through all API modules and run their individual `generate.sh` scripts.

## Testing Strategy

1. Unit tests for both gRPC server and client implementations
2. Integration tests with both gRPC and NATS enabled
3. Integration tests with only gRPC enabled
4. Performance comparison between NATS and gRPC implementations
