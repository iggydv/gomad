// Package storage provides the PeerStorage facade that wires all three storage tiers.
package storage

import (
	"context"
	"fmt"
	"sync"
	"time"

	"go.uber.org/zap"

	pbm "github.com/iggydv12/gomad/gen/proto/models"
	"github.com/iggydv12/gomad/internal/ledger"
	"github.com/iggydv12/gomad/internal/storage/group"
	"github.com/iggydv12/gomad/internal/storage/local"
	"github.com/iggydv12/gomad/internal/storage/overlay"
)

// PeerStorage is the facade that orchestrates local, group, and overlay storage.
// Maps to PeerStorage.java in the Java implementation.
type PeerStorage struct {
	mu           sync.RWMutex
	local        local.LocalStorage
	groupStorage *group.GroupStorage
	overlay      overlay.DHTOverlayStorage
	groupLedger  *ledger.GroupLedger
	storageMode  string
	retrievalMode string
	logger       *zap.Logger
	selfGSHost   string
	initialized  bool
}

// New creates a PeerStorage facade.
func New(ls local.LocalStorage, gs *group.GroupStorage, os overlay.DHTOverlayStorage,
	gl *ledger.GroupLedger, logger *zap.Logger) *PeerStorage {
	return &PeerStorage{
		local:        ls,
		groupStorage: gs,
		overlay:      os,
		groupLedger:  gl,
		storageMode:  "fast",
		retrievalMode: "fast",
		logger:       logger,
	}
}

// Init opens all storage backends.
func (ps *PeerStorage) Init(selfGSHost, storageMode, retrievalMode string) error {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	if err := ps.local.Init(); err != nil {
		return fmt.Errorf("local storage init: %w", err)
	}
	ps.selfGSHost = selfGSHost
	ps.storageMode = storageMode
	ps.retrievalMode = retrievalMode
	ps.groupStorage.SetSelfHost(selfGSHost)
	ps.initialized = true
	return nil
}

// Close shuts down all storage backends.
func (ps *PeerStorage) Close() error {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	if !ps.initialized {
		return nil
	}
	ps.groupStorage.CloseClients()
	if err := ps.local.Close(); err != nil {
		return err
	}
	ps.initialized = false
	return nil
}

// Put stores an object across all tiers.
func (ps *PeerStorage) Put(obj *pbm.GameObjectGrpc, groupEnabled, overlayEnabled bool) (bool, error) {
	if ps.groupLedger.ThisPeerContainsObject(ps.selfGSHost, obj.Id) {
		return false, fmt.Errorf("duplicate key: %s", obj.Id)
	}
	if obj.LastModified == 0 {
		obj.LastModified = obj.CreationTime
	}

	ok, err := ps.local.Put(obj)
	if !ok || err != nil {
		return false, err
	}

	// Notify group ledger
	ps.groupStorage.NotifyAllPeersObjectAdded(obj.Id, obj.Ttl, ps.selfGSHost)
	ps.groupLedger.AddToObjectLedger(obj.Id, ledger.MetaData{ID: ps.selfGSHost, TTL: obj.Ttl})
	ps.groupLedger.AddToPeerLedger(ps.selfGSHost, ledger.MetaData{ID: obj.Id, TTL: obj.Ttl})

	// Async overlay write
	if overlayEnabled {
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			_ = ctx
			if _, err := ps.overlay.Put(obj); err != nil {
				ps.logger.Warn("overlay put failed", zap.String("id", obj.Id), zap.Error(err))
			}
		}()
	}

	// Group write
	if groupEnabled {
		switch ps.storageMode {
		case "fast":
			ok, err = ps.groupStorage.FastPut(obj)
		default:
			ok, err = ps.groupStorage.SafePut(obj)
		}
		return ok, err
	}

	return true, nil
}

// LocalPut stores an object only locally (used during repair).
func (ps *PeerStorage) LocalPut(obj *pbm.GameObjectGrpc) (bool, error) {
	if ps.groupLedger.ThisPeerContainsObject(ps.selfGSHost, obj.Id) {
		ps.groupStorage.NotifyAllPeersObjectAdded(obj.Id, obj.Ttl, ps.selfGSHost)
		return false, nil
	}
	ok, err := ps.local.Put(obj)
	if ok {
		ps.groupStorage.NotifyAllPeersObjectAdded(obj.Id, obj.Ttl, ps.selfGSHost)
		ps.groupLedger.AddToObjectLedger(obj.Id, ledger.MetaData{ID: ps.selfGSHost, TTL: obj.Ttl})
		ps.groupLedger.AddToPeerLedger(ps.selfGSHost, ledger.MetaData{ID: obj.Id, TTL: obj.Ttl})
	}
	return ok, err
}

// Get retrieves an object, checking local → group → overlay.
func (ps *PeerStorage) Get(id string, groupEnabled, overlayEnabled bool) (*pbm.GameObjectGrpc, error) {
	if ps.groupLedger.ObjectLedgerContainsKey(id) {
		// Try local first
		obj, err := ps.local.Get(id)
		if err == nil {
			return obj, nil
		}
		// Local miss — try group/overlay
		if groupEnabled {
			switch ps.retrievalMode {
			case "fast":
				return ps.groupStorage.FastGet(id)
			case "parallel":
				return ps.groupStorage.ParallelGet(id)
			case "safe":
				return ps.groupStorage.SafeGet(id)
			default:
				return ps.groupStorage.FastGet(id)
			}
		}
	}

	if overlayEnabled {
		return ps.overlay.Get(id)
	}

	return nil, fmt.Errorf("not found: %s", id)
}

// Update updates an object across all tiers.
func (ps *PeerStorage) Update(obj *pbm.GameObjectGrpc) (bool, error) {
	if obj.LastModified == 0 {
		obj.LastModified = time.Now().Unix()
	}

	okLocal, errLocal := ps.local.Update(obj)
	okGroup, errGroup := ps.groupStorage.SafeUpdate(obj)
	if _, err := ps.overlay.Update(obj); err != nil {
		ps.logger.Warn("overlay update failed", zap.Error(err))
	}

	if errLocal != nil {
		return false, errLocal
	}
	if errGroup != nil {
		ps.logger.Warn("group update failed", zap.Error(errGroup))
	}

	if okLocal && okGroup {
		ttl := obj.Ttl - obj.CreationTime + obj.LastModified
		ps.groupStorage.NotifyAllPeersObjectAdded(obj.Id, ttl, ps.selfGSHost)
	}

	return okLocal, nil
}

// Delete removes an object from all tiers.
func (ps *PeerStorage) Delete(id string) (bool, error) {
	_, _ = ps.local.Delete(id)
	_, _ = ps.groupStorage.SafeDelete(id)
	_, _ = ps.overlay.Delete(id)
	ps.groupLedger.RemoveObjectFromGroupLedger(id)
	return true, nil
}

// SetStorageMode updates the write quorum mode.
func (ps *PeerStorage) SetStorageMode(mode string) {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	ps.storageMode = mode
}

// SetRetrievalMode updates the read quorum mode.
func (ps *PeerStorage) SetRetrievalMode(mode string) {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	ps.retrievalMode = mode
}

// TruncateLocal deletes all locally stored objects.
func (ps *PeerStorage) TruncateLocal() {
	if err := ps.local.Truncate(); err != nil {
		ps.logger.Error("truncate local failed", zap.Error(err))
	}
}

// UpdateGroupStorageClients replaces the set of group replica peers.
func (ps *PeerStorage) UpdateGroupStorageClients(hosts []string) {
	ps.groupStorage.UpdateClients(hosts)
}

// AddGroupStoragePeer adds a single peer to the group storage.
func (ps *PeerStorage) AddGroupStoragePeer(host string) {
	ps.groupStorage.AddPeer(host)
}

// RemoveGroupStoragePeer removes a peer from the group storage.
func (ps *PeerStorage) RemoveGroupStoragePeer(host string) {
	ps.groupStorage.RemovePeer(host)
}

// GroupStorage returns the underlying GroupStorage instance.
func (ps *PeerStorage) GroupStorage() *group.GroupStorage { return ps.groupStorage }

// NotifyAllPeersRemovePeer propagates peer removal to all group members.
func (ps *PeerStorage) NotifyAllPeersRemovePeer(peerID string) {
	ps.groupStorage.NotifyAllPeersRemovePeer(peerID)
}
