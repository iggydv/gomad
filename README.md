# Gomad — Nomad Geo-Spatial P2P Distributed Object Storage

Nomad is a geo-spatial peer-to-peer distributed object store built on the **Pithos architecture**. Nodes organise themselves into groups using **Voronoi tessellation** of a virtual 2-D world, discover each other via **libp2p mDNS + Kademlia DHT** (no ZooKeeper required), and replicate objects across the group with configurable quorum modes. Storage is backed by **PebbleDB** locally and exposed over **gRPC** and a **REST API**.

---

## Table of Contents

1. [Prerequisites](#prerequisites)
2. [Building](#building)
3. [Running the Application](#running-the-application)
   - [Locally (single node)](#locally-single-node)
   - [Local cluster with Docker Compose](#local-cluster-with-docker-compose)
   - [Kubernetes](#kubernetes)
4. [Configuration Reference](#configuration-reference)
5. [Running the Tests](#running-the-tests)
6. [Integrating into Existing Systems](#integrating-into-existing-systems)
   - [gRPC clients](#grpc-clients)
   - [REST API](#rest-api)
   - [Environment variable overrides](#environment-variable-overrides)
   - [Embedding the storage layer](#embedding-the-storage-layer)

---

## Prerequisites

| Tool | Minimum version | Purpose |
|------|-----------------|---------|
| Go | 1.23 | Build & test |
| protoc | any recent | Proto code generation |
| Docker / Docker Compose | any recent | Containerised cluster |
| kubectl | any recent | Kubernetes deployment (optional) |

Install the protoc Go plugins once:

```bash
go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
```

On macOS install `protoc` via Homebrew:

```bash
brew install protobuf
```

---

## Building

### 1. Generate protobuf code

```bash
bash scripts/generate.sh
```

Generated files land in `gen/proto/`.

### 2. Build the binary

```bash
go build -o nomad ./cmd/...
```

Or with size optimisations for production:

```bash
CGO_ENABLED=0 go build -ldflags="-s -w" -o nomad ./cmd/...
```

### 3. Build the Docker image

```bash
docker build -f deploy/Dockerfile -t gomad:latest .
```

The multi-stage Dockerfile runs proto generation, compiles a static binary, and produces a minimal Alpine image (~10 MB).

---

## Running the Application

### Locally (single node)

Start a **super-peer** (first node, becomes the group leader):

```bash
./nomad start --type super-peer
```

Start one or more **peers** (they discover the super-peer via mDNS/DHT and join the nearest group):

```bash
./nomad start --type peer
```

Pass a custom config file with `-c`:

```bash
./nomad start --type peer --config configs/config.yaml
```

Each node picks random free TCP ports for its REST API (8080–9999) and gRPC servers (5001–8999) and logs the chosen address on startup:

```
{"level":"info","msg":"Node running","role":"peer","REST":"0.0.0.0:8432"}
```

#### CLI flags

| Flag | Short | Default | Description |
|------|-------|---------|-------------|
| `--type` | `-t` | `peer` | Node role: `peer` or `super-peer` |
| `--config` | `-c` | `configs/config.yaml` | Path to YAML config file |

### Local cluster with Docker Compose

Starts 1 super-peer and 3 peers on a shared bridge network. Discovery is automatic via libp2p mDNS.

```bash
docker compose -f deploy/docker-compose.yml up --build
```

Stop and remove volumes:

```bash
docker compose -f deploy/docker-compose.yml down -v
```

### Kubernetes

Apply the manifests (requires a local image or pushed registry image tagged `gomad:latest`):

```bash
kubectl apply -f deploy/kubernetes/nomad.yaml
```

This creates:
- A **headless Service** (`nomad-headless`) exposing REST (8080), gRPC peer (6731), and gRPC storage (6735).
- A **StatefulSet** with 4 replicas, each with a 1 Gi PersistentVolumeClaim for Pebble data.

Scale the cluster:

```bash
kubectl scale statefulset nomad --replicas=8
```

The liveness probe hits `GET /nomad/storage/get/health` on port 8080.

---

## Configuration Reference

The default config file is `configs/config.yaml`. All keys can be overridden with environment variables (Viper's `AutomaticEnv`); replace dots with underscores and use uppercase, e.g. `NODE_STORAGE_REPLICATIONFACTOR=5`.

```yaml
node:
  maxPeers: 5                     # Maximum peers per group
  group:
    migration: false              # Enable peer migration between groups
    voronoiGrouping: false        # Use Voronoi for group assignment
  storage:
    replicationFactor: 3          # Number of group replicas per object
    storageMode: "fast"           # Write quorum: fast | safe
    retrievalMode: "fast"         # Read strategy: fast | safe | parallel
  networkHostnames:               # Override gRPC advertised addresses (optional)
    peerServer: ""
    peerStorageServer: ""
    overlayHostname: ""
    superPeerServer: ""
    groupStorageServer: ""
  world:
    height: 10.0                  # Virtual world height (Voronoi space)
    width: 10.0                   # Virtual world width  (Voronoi space)

schedule:
  healthCheck: 20s                # Peer health-check interval
  dhtBootstrap: 500s              # DHT re-bootstrap interval
  dbCleanup: 60s                  # Pebble compaction interval
  ledgerCleanup: 60s              # Expired ledger entry sweep
  repair: 6000s                   # Under-replicated object repair interval
  updatePosition: 10s             # Virtual position update (requires migration: true)
```

#### Storage modes

| Mode | Write behaviour | Read behaviour |
|------|-----------------|----------------|
| `fast` | Write locally, propagate async | Return first response from any replica |
| `safe` | Block until quorum of replicas confirm | Query replicas sequentially until found |
| `parallel` | — | Fan-out to all replicas, return fastest |

---

## Running the Tests

Run all unit tests:

```bash
go test ./...
```

Run a specific package with verbose output:

```bash
go test -v ./internal/ledger/...
go test -v ./internal/spatial/...
```

Run with race detection:

```bash
go test -race ./...
```

#### Test coverage

```bash
go test -coverprofile=coverage.out ./...
go tool cover -html=coverage.out
```

#### What is tested

| Package | Tests |
|---------|-------|
| `internal/ledger` | Add/retrieve objects, duplicate prevention, peer removal, TTL expiry sweep, repair detection, Populate/Snapshot |
| `internal/spatial` | Nearest super-peer lookup (Voronoi), AOI membership, random position bounds, 2-D distance, site removal, empty-set error |

No external services are required — all tests run in-process.

---

## Integrating into Existing Systems

### gRPC clients

Nomad exposes three gRPC services defined in `proto/`. Generate client stubs in any language supported by `protoc` and connect to the address logged at startup.

**Proto files:**

| File | Service | Purpose |
|------|---------|---------|
| `PeerService.proto` | `PeerService` | Join group, update position, health check |
| `SuperPeerService.proto` | `SuperPeerService` | Group membership management |
| `GroupStorageService.proto` | `GroupStorageService` | CRUD operations scoped to a peer group |
| `PeerStorageService.proto` | `PeerStorageService` | Overlay DHT storage operations |
| `GameObject.proto` | — | Shared message type |

**`GameObjectGrpc` message:**

```protobuf
message GameObjectGrpc {
  string id           = 1;   // Unique object key
  int64  creationTime = 2;   // Unix timestamp (seconds)
  int64  ttl          = 3;   // Expiry as Unix timestamp
  bytes  value        = 4;   // Arbitrary binary payload
  int64  lastModified = 5;   // Unix timestamp of last update
}
```

**Go example — store and retrieve an object:**

```go
import (
    "google.golang.org/grpc"
    pb "github.com/iggydv12/gomad/gen/proto/groupstorage"
    pbm "github.com/iggydv12/gomad/gen/proto/models"
)

conn, _ := grpc.Dial("localhost:6735", grpc.WithInsecure())
client := pb.NewGroupStorageServiceClient(conn)

// Store
client.Put(ctx, &pbm.GameObjectGrpc{
    Id:           "player:42",
    CreationTime: time.Now().Unix(),
    Ttl:          time.Now().Add(24 * time.Hour).Unix(),
    Value:        []byte(`{"x":3.5,"y":7.1}`),
})

// Retrieve
resp, _ := client.Get(ctx, &pb.GetRequest{Id: "player:42"})
```

**Generating stubs for another language (e.g. Python):**

```bash
protoc \
  --python_out=./client \
  --grpc_python_out=./client \
  -I proto \
  proto/GroupStorageService.proto proto/GameObject.proto
```

---

### REST API

Each node starts a Gin HTTP server on a random port in the 8080–9999 range (logged on startup). The Swagger UI is available at `/swagger/index.html`.

**Base path:** `http://<node-host>:<rest-port>/nomad/storage`

| Method | Path | Body / Params | Description |
|--------|------|---------------|-------------|
| `GET` | `/get/health` | — | Liveness check (returns 200 OK) |
| `GET` | `/get/:id` | — | Retrieve an object by ID |
| `POST` | `/put` | JSON `GameObjectGrpc` | Store a new object |
| `PUT` | `/update` | JSON `GameObjectGrpc` | Update an existing object |
| `DELETE` | `/delete/:id` | — | Remove an object by ID |

**cURL example:**

```bash
# Store
curl -X POST http://localhost:8432/nomad/storage/put \
  -H "Content-Type: application/json" \
  -d '{"id":"item:99","creationTime":1700000000,"ttl":1800000000,"value":"eyJ4IjoxfQ=="}'

# Retrieve
curl http://localhost:8432/nomad/storage/get/item:99

# Delete
curl -X DELETE http://localhost:8432/nomad/storage/delete/item:99
```

---

### Environment variable overrides

Any config key can be set via environment variables without modifying the YAML file. This is the recommended approach for container and CI deployments.

```bash
# Override via env (dots → underscores, uppercase)
NODE_MAXPEERS=10 \
NODE_STORAGE_REPLICATIONFACTOR=5 \
NODE_STORAGE_STORAGEMODE=safe \
NODE_STORAGE_RETRIEVALMODE=parallel \
NODE_WORLD_WIDTH=100.0 \
NODE_WORLD_HEIGHT=100.0 \
./nomad start --type peer
```

In Docker Compose or Kubernetes use the `env` / `env:` block as shown in `deploy/docker-compose.yml` and `deploy/kubernetes/nomad.yaml`.

---

### Embedding the storage layer

The `storage.PeerStorage` facade can be wired directly into a Go application without running the full node bootstrap. This is useful for unit tests or custom orchestration.

```go
import (
    "go.uber.org/zap"
    "github.com/iggydv12/gomad/internal/ledger"
    "github.com/iggydv12/gomad/internal/storage"
    "github.com/iggydv12/gomad/internal/storage/group"
    "github.com/iggydv12/gomad/internal/storage/local"
    pbm "github.com/iggydv12/gomad/gen/proto/models"
)

logger, _ := zap.NewDevelopment()
gl := ledger.NewGroupLedger()

ls := local.NewPebbleStorage("/tmp/my-app-pebble", logger)
gs := group.New(3 /* replication factor */, gl, logger)

ps := storage.New(ls, gs, nil /* no overlay */, gl, logger)
ps.Init("localhost:6735", "fast", "fast")
defer ps.Close()

// Store an object
ps.Put(&pbm.GameObjectGrpc{
    Id:           "session:abc",
    CreationTime: time.Now().Unix(),
    Ttl:          time.Now().Add(1 * time.Hour).Unix(),
    Value:        []byte("data"),
}, false /* groupEnabled */, false /* overlayEnabled */)

// Retrieve it
obj, err := ps.Get("session:abc", false, false)
```

The three storage tiers are independent:
- `local` — PebbleDB, always local to the process.
- `group` — gRPC fan-out to replica peers; requires live peer connections.
- `overlay` — libp2p DHT; requires a running `LibP2PDiscovery` host.

Pass `nil` for `overlay` or set `groupEnabled / overlayEnabled` to `false` to use only the local tier.
