// Package ledger provides thread-safe in-memory tracking of objects and peers.
// It maps to GroupLedger.java and Ledger.java from the Java implementation.
package ledger

import (
	"sync"
	"time"
)

// GroupLedger tracks which peers store which objects and vice-versa.
// Equivalent to GroupLedger.java — uses two cross-reference ledgers.
type GroupLedger struct {
	mu           sync.RWMutex
	objectLedger map[string][]MetaData // objectID → []MetaData{peerID, ttl}
	peerLedger   map[string][]MetaData // peerID   → []MetaData{objectID, ttl}
}

// NewGroupLedger creates a new, empty GroupLedger.
func NewGroupLedger() *GroupLedger {
	return &GroupLedger{
		objectLedger: make(map[string][]MetaData),
		peerLedger:   make(map[string][]MetaData),
	}
}

// AddToObjectLedger records that peerID stores objectID with the given TTL.
func (gl *GroupLedger) AddToObjectLedger(objectID string, peer MetaData) bool {
	gl.mu.Lock()
	defer gl.mu.Unlock()
	if gl.containsEntry(gl.objectLedger, objectID, peer.ID) {
		return false
	}
	gl.objectLedger[objectID] = append(gl.objectLedger[objectID], peer)
	return true
}

// AddToPeerLedger records that peerID holds objectID with the given TTL.
func (gl *GroupLedger) AddToPeerLedger(peerID string, object MetaData) bool {
	gl.mu.Lock()
	defer gl.mu.Unlock()
	if gl.containsEntry(gl.peerLedger, peerID, object.ID) {
		return false
	}
	gl.peerLedger[peerID] = append(gl.peerLedger[peerID], object)
	return true
}

// containsEntry checks if a key-value pair exists (no TTL comparison). Must be called with lock held.
func (gl *GroupLedger) containsEntry(ledger map[string][]MetaData, key, id string) bool {
	entries, ok := ledger[key]
	if !ok {
		return false
	}
	for _, e := range entries {
		if e.ID == id {
			return true
		}
	}
	return false
}

// ObjectLedgerContainsKey returns true if the object is tracked in the object ledger.
func (gl *GroupLedger) ObjectLedgerContainsKey(objectID string) bool {
	gl.mu.RLock()
	defer gl.mu.RUnlock()
	_, ok := gl.objectLedger[objectID]
	return ok
}

// PeerLedgerContainsKey returns true if the peer has any entries.
func (gl *GroupLedger) PeerLedgerContainsKey(peerID string) bool {
	gl.mu.RLock()
	defer gl.mu.RUnlock()
	_, ok := gl.peerLedger[peerID]
	return ok
}

// ThisPeerContainsObject returns true if peerID stores objectID.
func (gl *GroupLedger) ThisPeerContainsObject(peerID, objectID string) bool {
	gl.mu.RLock()
	defer gl.mu.RUnlock()
	return gl.containsEntry(gl.peerLedger, peerID, objectID)
}

// CountReplicas returns how many peers currently store objectID.
func (gl *GroupLedger) CountReplicas(objectID string) int {
	gl.mu.RLock()
	defer gl.mu.RUnlock()
	return len(gl.objectLedger[objectID])
}

// GetObjectsNeedingRepair returns objects with fewer than rf replicas.
func (gl *GroupLedger) GetObjectsNeedingRepair(rf int) map[string]int {
	gl.mu.RLock()
	defer gl.mu.RUnlock()
	result := make(map[string]int)
	for objectID, peers := range gl.objectLedger {
		count := len(peers)
		if count < rf {
			result[objectID] = rf - count
		}
	}
	return result
}

// RemovePeersStoringObject returns peers from the list that do NOT already store objectID.
func (gl *GroupLedger) RemovePeersStoringObject(peers []string, objectID string) []string {
	gl.mu.RLock()
	defer gl.mu.RUnlock()
	var result []string
	for _, p := range peers {
		if !gl.containsEntry(gl.peerLedger, p, objectID) {
			result = append(result, p)
		}
	}
	return result
}

// RemovePeersNotStoringObject returns peers from the list that DO store objectID.
func (gl *GroupLedger) RemovePeersNotStoringObject(peers []string, objectID string) []string {
	gl.mu.RLock()
	defer gl.mu.RUnlock()
	var result []string
	for _, p := range peers {
		if gl.containsEntry(gl.peerLedger, p, objectID) {
			result = append(result, p)
		}
	}
	return result
}

// RemovePeerFromGroupLedger removes all entries for peerID from both ledgers.
func (gl *GroupLedger) RemovePeerFromGroupLedger(peerID string) {
	gl.mu.Lock()
	defer gl.mu.Unlock()
	// Remove from peer ledger
	delete(gl.peerLedger, peerID)
	// Remove peerID value from all object ledger entries
	for objectID, entries := range gl.objectLedger {
		filtered := entries[:0]
		for _, e := range entries {
			if e.ID != peerID {
				filtered = append(filtered, e)
			}
		}
		if len(filtered) == 0 {
			delete(gl.objectLedger, objectID)
		} else {
			gl.objectLedger[objectID] = filtered
		}
	}
}

// RemoveObjectFromGroupLedger removes all entries for objectID from both ledgers.
func (gl *GroupLedger) RemoveObjectFromGroupLedger(objectID string) {
	gl.mu.Lock()
	defer gl.mu.Unlock()
	delete(gl.objectLedger, objectID)
	for peerID, entries := range gl.peerLedger {
		filtered := entries[:0]
		for _, e := range entries {
			if e.ID != objectID {
				filtered = append(filtered, e)
			}
		}
		if len(filtered) == 0 {
			delete(gl.peerLedger, peerID)
		} else {
			gl.peerLedger[peerID] = filtered
		}
	}
}

// CleanExpired removes all entries whose TTL has passed. Returns true if anything was removed.
func (gl *GroupLedger) CleanExpired() bool {
	now := time.Now().Unix()
	gl.mu.Lock()
	defer gl.mu.Unlock()
	removed := false

	for key, entries := range gl.objectLedger {
		filtered := entries[:0]
		for _, e := range entries {
			if e.TTL == 0 || e.TTL > now {
				filtered = append(filtered, e)
			} else {
				removed = true
			}
		}
		if len(filtered) == 0 {
			delete(gl.objectLedger, key)
		} else {
			gl.objectLedger[key] = filtered
		}
	}

	for key, entries := range gl.peerLedger {
		filtered := entries[:0]
		for _, e := range entries {
			if e.TTL == 0 || e.TTL > now {
				filtered = append(filtered, e)
			} else {
				removed = true
			}
		}
		if len(filtered) == 0 {
			delete(gl.peerLedger, key)
		} else {
			gl.peerLedger[key] = filtered
		}
	}

	return removed
}

// ClearAll empties both ledgers.
func (gl *GroupLedger) ClearAll() {
	gl.mu.Lock()
	defer gl.mu.Unlock()
	gl.objectLedger = make(map[string][]MetaData)
	gl.peerLedger = make(map[string][]MetaData)
}

// GetAllGroupObjects returns a snapshot of all tracked object IDs.
func (gl *GroupLedger) GetAllGroupObjects() []string {
	gl.mu.RLock()
	defer gl.mu.RUnlock()
	keys := make([]string, 0, len(gl.objectLedger))
	for k := range gl.objectLedger {
		keys = append(keys, k)
	}
	return keys
}

// Populate sets both ledgers from the maps received in a JoinResponse.
// objectMap: objectID → []MetaData{peerID,ttl}
// peerMap:   peerID   → []MetaData{objectID,ttl}
func (gl *GroupLedger) Populate(objectMap, peerMap map[string][]MetaData) {
	gl.mu.Lock()
	defer gl.mu.Unlock()
	gl.objectLedger = objectMap
	gl.peerLedger = peerMap
}

// GetObjectLedgerSnapshot returns a copy of the object ledger (for gRPC serialization).
func (gl *GroupLedger) GetObjectLedgerSnapshot() map[string][]MetaData {
	gl.mu.RLock()
	defer gl.mu.RUnlock()
	snap := make(map[string][]MetaData, len(gl.objectLedger))
	for k, v := range gl.objectLedger {
		cp := make([]MetaData, len(v))
		copy(cp, v)
		snap[k] = cp
	}
	return snap
}

// GetPeerLedgerSnapshot returns a copy of the peer ledger.
func (gl *GroupLedger) GetPeerLedgerSnapshot() map[string][]MetaData {
	gl.mu.RLock()
	defer gl.mu.RUnlock()
	snap := make(map[string][]MetaData, len(gl.peerLedger))
	for k, v := range gl.peerLedger {
		cp := make([]MetaData, len(v))
		copy(cp, v)
		snap[k] = cp
	}
	return snap
}
