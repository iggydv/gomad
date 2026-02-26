// Package group implements the gRPC-based group replication storage layer.
package group

import (
	"context"
	"fmt"
	"sync"
	"time"

	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

	"github.com/iggydv12/nomad-go/api/grpc/clients"
	pbm "github.com/iggydv12/nomad-go/gen/proto/models"
	"github.com/iggydv12/nomad-go/internal/ledger"
)

// GroupStorage fans out storage operations to all replica peers in the group.
// Maps to GroupStorage.java in the Java implementation.
type GroupStorage struct {
	mu          sync.RWMutex
	peers       map[string]*clients.GroupStorageClient // target → client
	rf          int // replication factor
	spClient    *clients.SuperPeerClient
	logger      *zap.Logger
	groupLedger *ledger.GroupLedger
	selfHost    string
}

// New creates a GroupStorage.
func New(rf int, gl *ledger.GroupLedger, logger *zap.Logger) *GroupStorage {
	return &GroupStorage{
		peers:       make(map[string]*clients.GroupStorageClient),
		rf:          rf,
		groupLedger: gl,
		logger:      logger,
	}
}

// SetSuperPeerClient sets the super-peer client reference.
func (gs *GroupStorage) SetSuperPeerClient(c *clients.SuperPeerClient) {
	gs.mu.Lock()
	defer gs.mu.Unlock()
	gs.spClient = c
}

// SetSelfHost records our own group-storage hostname to avoid self-connections.
func (gs *GroupStorage) SetSelfHost(host string) {
	gs.mu.Lock()
	defer gs.mu.Unlock()
	gs.selfHost = host
}

// UpdateClients replaces the set of peer clients.
func (gs *GroupStorage) UpdateClients(hosts []string) {
	gs.mu.Lock()
	defer gs.mu.Unlock()
	// Close removed clients
	for target, c := range gs.peers {
		found := false
		for _, h := range hosts {
			if h == target {
				found = true
				break
			}
		}
		if !found {
			c.Close()
			delete(gs.peers, target)
		}
	}
	// Add new clients (skip self)
	for _, h := range hosts {
		if h == gs.selfHost {
			continue
		}
		if _, exists := gs.peers[h]; !exists {
			c, err := clients.NewGroupStorageClient(h, gs.logger)
			if err != nil {
				gs.logger.Warn("failed to create group storage client", zap.String("target", h), zap.Error(err))
				continue
			}
			gs.peers[h] = c
		}
	}
}

// AddPeer adds a single peer client.
func (gs *GroupStorage) AddPeer(host string) {
	gs.mu.Lock()
	defer gs.mu.Unlock()
	if host == gs.selfHost {
		return
	}
	if _, exists := gs.peers[host]; exists {
		return
	}
	c, err := clients.NewGroupStorageClient(host, gs.logger)
	if err != nil {
		gs.logger.Warn("failed to add group storage peer", zap.String("host", host), zap.Error(err))
		return
	}
	gs.peers[host] = c
}

// RemovePeer removes and closes a peer client.
func (gs *GroupStorage) RemovePeer(host string) {
	gs.mu.Lock()
	defer gs.mu.Unlock()
	if c, ok := gs.peers[host]; ok {
		c.Close()
		delete(gs.peers, host)
	}
}

// UpdateReplicationFactor refreshes the RF from the super-peer.
func (gs *GroupStorage) UpdateReplicationFactor(rf int) {
	gs.mu.Lock()
	defer gs.mu.Unlock()
	gs.rf = rf
}

// CloseClients closes all peer connections.
func (gs *GroupStorage) CloseClients() {
	gs.mu.Lock()
	defer gs.mu.Unlock()
	for _, c := range gs.peers {
		c.Close()
	}
	gs.peers = make(map[string]*clients.GroupStorageClient)
}

// GetClient returns the client for the given hostname, or nil if not found.
func (gs *GroupStorage) GetClient(host string) *clients.GroupStorageClient {
	gs.mu.RLock()
	defer gs.mu.RUnlock()
	return gs.peers[host]
}

// PeerList returns a snapshot of current peer hostnames.
func (gs *GroupStorage) PeerList() []string {
	gs.mu.RLock()
	defer gs.mu.RUnlock()
	list := make([]string, 0, len(gs.peers))
	for h := range gs.peers {
		list = append(list, h)
	}
	return list
}

// NotifyAllPeersObjectAdded notifies all peers to record object→peer in their ledgers.
func (gs *GroupStorage) NotifyAllPeersObjectAdded(objectID string, ttl int64, selfGSHost string) {
	gs.mu.RLock()
	peers := make([]*clients.GroupStorageClient, 0, len(gs.peers))
	for _, c := range gs.peers {
		peers = append(peers, c)
	}
	gs.mu.RUnlock()

	for _, c := range peers {
		_, err := c.AddToGroupLedger(objectID, selfGSHost, ttl)
		if err != nil {
			gs.logger.Warn("addToGroupLedger failed", zap.String("peer", c.Target()), zap.Error(err))
		}
	}
	// Also notify super-peer
	if gs.spClient != nil && gs.spClient.IsActive() {
		_, err := gs.spClient.AddObjectReference(objectID, selfGSHost, ttl)
		if err != nil {
			gs.logger.Warn("addObjectReference to SP failed", zap.Error(err))
		}
	}
}

// NotifyAllPeersRemovePeer notifies all peers to remove a peer from their ledgers.
func (gs *GroupStorage) NotifyAllPeersRemovePeer(peerID string) {
	gs.mu.RLock()
	peers := make([]*clients.GroupStorageClient, 0, len(gs.peers))
	for _, c := range gs.peers {
		peers = append(peers, c)
	}
	gs.mu.RUnlock()

	for _, c := range peers {
		_, err := c.RemovePeerGroupLedger(peerID)
		if err != nil {
			gs.logger.Warn("removePeerGroupLedger failed", zap.String("peer", c.Target()), zap.Error(err))
		}
	}
}

// FastPut stores obj on the first peer that responds with success.
func (gs *GroupStorage) FastPut(obj *pbm.GameObjectGrpc) (bool, error) {
	gs.mu.RLock()
	peers := gs.activePeers()
	gs.mu.RUnlock()

	if len(peers) == 0 {
		return true, nil // no replicas to send to
	}

	type result struct {
		ok  bool
		err error
	}
	ch := make(chan result, len(peers))
	ctx, cancel := context.WithTimeout(context.Background(), 2500*time.Millisecond)
	defer cancel()

	for _, c := range peers {
		go func(c *clients.GroupStorageClient) {
			ok, err := c.Put(obj)
			ch <- result{ok, err}
		}(c)
	}

	select {
	case r := <-ch:
		return r.ok, r.err
	case <-ctx.Done():
		return false, fmt.Errorf("fast put timed out")
	}
}

// SafePut fans out to all peers and waits for RF ACKs.
func (gs *GroupStorage) SafePut(obj *pbm.GameObjectGrpc) (bool, error) {
	gs.mu.RLock()
	peers := gs.activePeers()
	gs.mu.RUnlock()

	if len(peers) == 0 {
		return true, nil
	}

	eg, ctx := errgroup.WithContext(context.Background())
	ctx, cancel := context.WithTimeout(ctx, 2500*time.Millisecond)
	defer cancel()
	_ = ctx

	for _, c := range peers {
		c := c
		eg.Go(func() error {
			ok, err := c.Put(obj)
			if err != nil {
				return err
			}
			if !ok {
				return fmt.Errorf("put rejected by %s", c.Target())
			}
			return nil
		})
	}
	if err := eg.Wait(); err != nil {
		gs.logger.Warn("safe put: not all peers succeeded", zap.Error(err))
		return false, nil
	}
	return true, nil
}

// FastGet retrieves obj from the first responding peer.
func (gs *GroupStorage) FastGet(id string) (*pbm.GameObjectGrpc, error) {
	gs.mu.RLock()
	peers := gs.activePeers()
	gs.mu.RUnlock()

	type result struct {
		obj *pbm.GameObjectGrpc
		err error
	}
	ch := make(chan result, len(peers))
	ctx, cancel := context.WithTimeout(context.Background(), 2500*time.Millisecond)
	defer cancel()

	for _, c := range peers {
		go func(c *clients.GroupStorageClient) {
			obj, err := c.Get(id)
			if obj != nil {
				ch <- result{obj, nil}
			} else {
				ch <- result{nil, err}
			}
		}(c)
	}

	received := 0
	for received < len(peers) {
		select {
		case r := <-ch:
			received++
			if r.obj != nil {
				return r.obj, nil
			}
		case <-ctx.Done():
			return nil, fmt.Errorf("fast get timed out for %s", id)
		}
	}
	return nil, fmt.Errorf("not found: %s", id)
}

// ParallelGet is like FastGet but returns the first non-nil response.
func (gs *GroupStorage) ParallelGet(id string) (*pbm.GameObjectGrpc, error) {
	return gs.FastGet(id)
}

// SafeGet queries all peers and returns the majority-quorum result.
func (gs *GroupStorage) SafeGet(id string) (*pbm.GameObjectGrpc, error) {
	gs.mu.RLock()
	peers := gs.activePeers()
	gs.mu.RUnlock()

	if len(peers) == 0 {
		return nil, fmt.Errorf("no peers available")
	}

	type result struct {
		obj *pbm.GameObjectGrpc
		err error
	}
	ch := make(chan result, len(peers))
	ctx, cancel := context.WithTimeout(context.Background(), 2500*time.Millisecond)
	defer cancel()

	for _, c := range peers {
		go func(c *clients.GroupStorageClient) {
			obj, err := c.Get(id)
			ch <- result{obj, err}
		}(c)
	}

	// Collect quorum (RF/2+1)
	quorum := gs.rf/2 + 1
	counts := make(map[string]int)
	var best *pbm.GameObjectGrpc

	for i := 0; i < len(peers); i++ {
		select {
		case r := <-ch:
			if r.obj != nil {
				counts[r.obj.Id]++
				if counts[r.obj.Id] >= quorum {
					best = r.obj
				}
			}
		case <-ctx.Done():
			break
		}
	}

	if best != nil {
		return best, nil
	}
	return nil, fmt.Errorf("quorum not reached for %s", id)
}

// SafeUpdate fans out update to all peers.
func (gs *GroupStorage) SafeUpdate(obj *pbm.GameObjectGrpc) (bool, error) {
	gs.mu.RLock()
	peers := gs.activePeers()
	gs.mu.RUnlock()

	eg, _ := errgroup.WithContext(context.Background())
	for _, c := range peers {
		c := c
		eg.Go(func() error {
			_, err := c.Update(obj)
			return err
		})
	}
	if err := eg.Wait(); err != nil {
		return false, err
	}
	return true, nil
}

// SafeDelete fans out delete to all peers.
func (gs *GroupStorage) SafeDelete(id string) (bool, error) {
	gs.mu.RLock()
	peers := gs.activePeers()
	gs.mu.RUnlock()

	eg, _ := errgroup.WithContext(context.Background())
	for _, c := range peers {
		c := c
		eg.Go(func() error {
			_, err := c.Delete(id)
			return err
		})
	}
	if err := eg.Wait(); err != nil {
		return false, err
	}
	return true, nil
}

// CheckClient verifies the connection to a specific peer by hostname.
func (gs *GroupStorage) CheckClient(host string) {
	gs.mu.RLock()
	c, ok := gs.peers[host]
	gs.mu.RUnlock()
	if !ok {
		return
	}
	ok2, err := c.HealthCheck()
	if err != nil || !ok2 {
		gs.logger.Warn("peer health check failed — reconnecting", zap.String("host", host))
		gs.RemovePeer(host)
		gs.AddPeer(host)
	}
}

// activePeers returns active clients (must be called with read lock held OR snapshot).
func (gs *GroupStorage) activePeers() []*clients.GroupStorageClient {
	list := make([]*clients.GroupStorageClient, 0, len(gs.peers))
	for _, c := range gs.peers {
		if c.IsActive() {
			list = append(list, c)
		}
	}
	return list
}
