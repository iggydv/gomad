// Package overlay defines the DHTOverlayStorage interface for inter-group distribution.
package overlay

import (
	pbm "github.com/iggydv12/nomad-go/gen/proto/models"
)

// DHTOverlayStorage is the interface for the DHT-backed inter-group storage layer.
// Maps to DHTOverlayStorage.java in the Java implementation.
type DHTOverlayStorage interface {
	// Init sets up the DHT connection.
	Init() error
	// Close tears down the DHT connection.
	Close() error
	// Put stores an object on the DHT.
	Put(obj *pbm.GameObjectGrpc) (bool, error)
	// Get retrieves an object from the DHT by ID.
	Get(id string) (*pbm.GameObjectGrpc, error)
	// Update overwrites an object on the DHT.
	Update(obj *pbm.GameObjectGrpc) (bool, error)
	// Delete writes a tombstone (DHT doesn't support true delete).
	Delete(id string) (bool, error)
}
