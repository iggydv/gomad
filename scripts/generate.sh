#!/usr/bin/env bash
# Generate Go code from .proto files
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

# Create output directories
mkdir -p "${PROJECT_ROOT}/gen/proto/models"
mkdir -p "${PROJECT_ROOT}/gen/proto/superpeerservice"
mkdir -p "${PROJECT_ROOT}/gen/proto/peerservice"
mkdir -p "${PROJECT_ROOT}/gen/proto/groupstorage"
mkdir -p "${PROJECT_ROOT}/gen/proto/peerstorage"

# Check protoc is installed
if ! command -v protoc &> /dev/null; then
    echo "protoc not found. Install with: brew install protobuf"
    exit 1
fi

# Check plugins
if ! command -v protoc-gen-go &> /dev/null; then
    echo "Installing protoc-gen-go..."
    go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
fi

if ! command -v protoc-gen-go-grpc &> /dev/null; then
    echo "Installing protoc-gen-go-grpc..."
    go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
fi

echo "Generating Go code from proto files..."

# GameObject (no gRPC services, messages only)
protoc \
    --go_out="${PROJECT_ROOT}/gen/proto/models" \
    --go_opt=paths=source_relative \
    -I "${PROJECT_ROOT}/proto" \
    -I "$(go env GOPATH)/pkg/mod/github.com/gogo/protobuf*/gogoproto" \
    "${PROJECT_ROOT}/proto/GameObject.proto" 2>/dev/null || \
protoc \
    --go_out="${PROJECT_ROOT}/gen/proto/models" \
    --go_opt=paths=source_relative \
    -I "${PROJECT_ROOT}/proto" \
    "${PROJECT_ROOT}/proto/GameObject.proto"

# SuperPeerService
protoc \
    --go_out="${PROJECT_ROOT}/gen/proto/superpeerservice" \
    --go_opt=paths=source_relative \
    --go-grpc_out="${PROJECT_ROOT}/gen/proto/superpeerservice" \
    --go-grpc_opt=paths=source_relative \
    -I "${PROJECT_ROOT}/proto" \
    "${PROJECT_ROOT}/proto/SuperPeerService.proto"

# PeerService
protoc \
    --go_out="${PROJECT_ROOT}/gen/proto/peerservice" \
    --go_opt=paths=source_relative \
    --go-grpc_out="${PROJECT_ROOT}/gen/proto/peerservice" \
    --go-grpc_opt=paths=source_relative \
    -I "${PROJECT_ROOT}/proto" \
    "${PROJECT_ROOT}/proto/PeerService.proto"

# GroupStorageService (imports GameObject.proto)
protoc \
    --go_out="${PROJECT_ROOT}/gen/proto/groupstorage" \
    --go_opt=paths=source_relative \
    --go_opt=MGameObject.proto=github.com/iggydv12/gomad/gen/proto/models \
    --go-grpc_out="${PROJECT_ROOT}/gen/proto/groupstorage" \
    --go-grpc_opt=paths=source_relative \
    --go-grpc_opt=MGameObject.proto=github.com/iggydv12/gomad/gen/proto/models \
    -I "${PROJECT_ROOT}/proto" \
    "${PROJECT_ROOT}/proto/GroupStorageService.proto"

# PeerStorageService (imports GameObject.proto)
protoc \
    --go_out="${PROJECT_ROOT}/gen/proto/peerstorage" \
    --go_opt=paths=source_relative \
    --go_opt=MGameObject.proto=github.com/iggydv12/gomad/gen/proto/models \
    --go-grpc_out="${PROJECT_ROOT}/gen/proto/peerstorage" \
    --go-grpc_opt=paths=source_relative \
    --go-grpc_opt=MGameObject.proto=github.com/iggydv12/gomad/gen/proto/models \
    -I "${PROJECT_ROOT}/proto" \
    "${PROJECT_ROOT}/proto/PeerStorageService.proto"

echo "Done! Generated files in ${PROJECT_ROOT}/gen/proto/"
