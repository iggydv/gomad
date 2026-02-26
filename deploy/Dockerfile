# Multi-stage Go build — no JVM, no ZooKeeper needed
FROM golang:1.23-alpine AS builder

WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download

# Install protoc and plugins for code generation
RUN apk add --no-cache protobuf
RUN go install google.golang.org/protobuf/cmd/protoc-gen-go@latest && \
    go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest

COPY . .
RUN bash scripts/generate.sh 2>/dev/null || true
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o nomad ./cmd/nomad

# Final image — just the binary
FROM alpine:3.20
RUN apk --no-cache add ca-certificates

COPY --from=builder /app/nomad /nomad
COPY --from=builder /app/configs/ /configs/

EXPOSE 8080-9999

ENTRYPOINT ["/nomad", "start"]
CMD ["-t", "peer"]
