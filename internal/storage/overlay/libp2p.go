// Package overlay provides the libp2p Kademlia DHT implementation of DHTOverlayStorage.
package overlay

import (
	"context"
	"fmt"
	"time"

	dht "github.com/libp2p/go-libp2p-kad-dht"
	"github.com/libp2p/go-libp2p/core/host"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	pbm "github.com/iggydv12/gomad/gen/proto/models"
)

const dhtKeyPrefix = "/nomad/obj/"

// LibP2PStorage implements DHTOverlayStorage using the Kademlia DHT.
// Maps to TomP2POverlayStorage.java (the one that doesn't work on macOS).
type LibP2PStorage struct {
	host   host.Host
	dht    *dht.IpfsDHT
	logger *zap.Logger
}

// NewLibP2PStorage creates a LibP2PStorage backed by an already-started DHT.
func NewLibP2PStorage(h host.Host, d *dht.IpfsDHT, logger *zap.Logger) *LibP2PStorage {
	return &LibP2PStorage{host: h, dht: d, logger: logger}
}

// Init is a no-op here — the DHT is started by the discovery layer.
func (s *LibP2PStorage) Init() error { return nil }

// Close is a no-op — the DHT is closed by the discovery layer.
func (s *LibP2PStorage) Close() error { return nil }

// Put serializes the object and stores it under the DHT key.
func (s *LibP2PStorage) Put(obj *pbm.GameObjectGrpc) (bool, error) {
	data, err := proto.Marshal(obj)
	if err != nil {
		return false, fmt.Errorf("marshal: %w", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	key := dhtKeyPrefix + obj.Id
	if err := s.dht.PutValue(ctx, key, data); err != nil {
		return false, fmt.Errorf("dht put: %w", err)
	}
	s.logger.Debug("DHT put", zap.String("key", key))
	return true, nil
}

// Get retrieves and deserializes an object from the DHT.
func (s *LibP2PStorage) Get(id string) (*pbm.GameObjectGrpc, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	key := dhtKeyPrefix + id
	data, err := s.dht.GetValue(ctx, key)
	if err != nil {
		return nil, fmt.Errorf("dht get: %w", err)
	}
	obj := &pbm.GameObjectGrpc{}
	if err := proto.Unmarshal(data, obj); err != nil {
		return nil, fmt.Errorf("unmarshal: %w", err)
	}
	// Check for tombstone
	if obj.Id == "" {
		return nil, fmt.Errorf("tombstone: %s", id)
	}
	// Check TTL
	if obj.Ttl > 0 && obj.Ttl < time.Now().Unix() {
		return nil, fmt.Errorf("expired: %s", id)
	}
	return obj, nil
}

// Update overwrites the DHT record with a newer version.
func (s *LibP2PStorage) Update(obj *pbm.GameObjectGrpc) (bool, error) {
	return s.Put(obj)
}

// Delete writes a tombstone record (DHT doesn't support true deletion).
func (s *LibP2PStorage) Delete(id string) (bool, error) {
	// Write a tombstone with empty ID to signal deletion
	tombstone := &pbm.GameObjectGrpc{
		Id:           "",
		LastModified: time.Now().Unix(),
		Ttl:          time.Now().Unix() + 3600, // keep tombstone for 1h
	}
	data, err := proto.Marshal(tombstone)
	if err != nil {
		return false, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := s.dht.PutValue(ctx, dhtKeyPrefix+id, data); err != nil {
		return false, err
	}
	return true, nil
}
