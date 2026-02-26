// Package local defines the LocalStorage interface for the authoritative object store.
package local

import (
	models "github.com/iggydv12/gomad/gen/proto/models"
)

// LocalStorage is the interface for the single-node authoritative KV store.
// Maps to LocalStorage.java in the Java implementation.
type LocalStorage interface {
	// Init opens/creates the underlying store.
	Init() error
	// Close flushes and closes the store.
	Close() error
	// Put stores a new object. Returns false if the key already exists.
	Put(obj *models.GameObjectGrpc) (bool, error)
	// Get retrieves an object by ID. Returns nil if not found.
	Get(id string) (*models.GameObjectGrpc, error)
	// Update overwrites an existing object.
	Update(obj *models.GameObjectGrpc) (bool, error)
	// Delete removes an object by ID.
	Delete(id string) (bool, error)
	// Truncate deletes all stored objects.
	Truncate() error
	// CleanExpired removes all entries whose TTL has passed.
	CleanExpired() (int, error)
}
