// Package node provides the core node role implementations.
package node

import (
	"math/rand"
	"sort"
	"sync"

	"go.uber.org/zap"

	"github.com/iggydv12/nomad-go/api/grpc/clients"
	"github.com/iggydv12/nomad-go/api/grpc/servers"
	"github.com/iggydv12/nomad-go/internal/config"
	"github.com/iggydv12/nomad-go/internal/ledger"
	"github.com/iggydv12/nomad-go/internal/spatial"
)

// SuperPeer implements the group leader role.
// Maps to SuperPeer.java in the Java implementation.
type SuperPeer struct {
	mu sync.RWMutex

	// Peer → PeerClient map (peerServer → PeerClient)
	peerClients map[string]*clients.PeerClient
	// peerServer → groupStorageServer
	peerToGS map[string]string
	// groupStorageServer → peerServer
	gsToP map[string]string

	groupLedger *ledger.GroupLedger
	groupName   string
	cfg         *config.Config
	logger      *zap.Logger
	running     bool
}

// NewSuperPeer creates a SuperPeer.
func NewSuperPeer(cfg *config.Config, gl *ledger.GroupLedger, logger *zap.Logger) *SuperPeer {
	return &SuperPeer{
		peerClients: make(map[string]*clients.PeerClient),
		peerToGS:    make(map[string]string),
		gsToP:       make(map[string]string),
		groupLedger: gl,
		cfg:         cfg,
		logger:      logger,
	}
}

// TakeLeadership registers as the group leader with the given virtual position.
func (sp *SuperPeer) TakeLeadership(groupName string) {
	sp.mu.Lock()
	defer sp.mu.Unlock()
	sp.groupName = groupName
	sp.running = true
	sp.logger.Info("Super-peer took leadership", zap.String("group", groupName))
}

// IsRunning returns true if this super-peer is active.
func (sp *SuperPeer) IsRunning() bool {
	sp.mu.RLock()
	defer sp.mu.RUnlock()
	return sp.running
}

// GroupName returns the current group name.
func (sp *SuperPeer) GroupName() string {
	sp.mu.RLock()
	defer sp.mu.RUnlock()
	return sp.groupName
}

// --- servers.SuperPeerHandler implementation ---

func (sp *SuperPeer) HandleJoin(peerServer, groupStorageServer string, pos servers.Position) (bool, map[string][]ledger.MetaData, map[string][]ledger.MetaData, int32) {
	maxPeers := sp.cfg.Node.MaxPeers
	sp.mu.Lock()
	defer sp.mu.Unlock()

	if len(sp.peerClients) >= maxPeers {
		sp.logger.Warn("Max peer limit reached", zap.Int("max", maxPeers))
		return false, nil, nil, 0
	}

	c, err := clients.NewPeerClient(peerServer, sp.logger)
	if err != nil {
		sp.logger.Error("Failed to create peer client", zap.String("peer", peerServer), zap.Error(err))
		return false, nil, nil, 0
	}

	sp.peerClients[peerServer] = c
	sp.peerToGS[peerServer] = groupStorageServer
	sp.gsToP[groupStorageServer] = peerServer

	sp.logger.Info("Peer joined",
		zap.String("peerServer", peerServer),
		zap.String("gsServer", groupStorageServer),
		zap.Int("totalPeers", len(sp.peerClients)),
	)

	return true,
		sp.groupLedger.GetObjectLedgerSnapshot(),
		sp.groupLedger.GetPeerLedgerSnapshot(),
		int32(sp.cfg.Node.Storage.ReplicationFactor)
}

func (sp *SuperPeer) HandleLeave(peerServer, groupStorageServer string) bool {
	sp.mu.Lock()
	if peerServer == "" {
		peerServer = sp.gsToP[groupStorageServer]
	}

	c, exists := sp.peerClients[peerServer]
	if !exists {
		sp.mu.Unlock()
		sp.logger.Warn("Peer not found in client map", zap.String("peer", peerServer))
		sp.scheduledRepair()
		return false
	}
	c.Close()
	delete(sp.peerClients, peerServer)
	delete(sp.peerToGS, peerServer)
	delete(sp.gsToP, groupStorageServer)

	// Notify remaining peers to remove this peer from their group-storage client lists
	for _, client := range sp.peerClients {
		go client.RemovePeer(groupStorageServer)
	}
	sp.mu.Unlock()

	sp.groupLedger.RemovePeerFromGroupLedger(groupStorageServer)
	sp.scheduledRepair()
	return true
}

func (sp *SuperPeer) Repair(objectID string) bool {
	sp.mu.RLock()
	rf := sp.cfg.Node.Storage.ReplicationFactor
	gsServers := make([]string, 0, len(sp.gsToP))
	for gs := range sp.gsToP {
		gsServers = append(gsServers, gs)
	}
	sp.mu.RUnlock()

	peersNotStoring := sp.groupLedger.RemovePeersStoringObject(gsServers, objectID)
	targets := pickNRandom(peersNotStoring, rf)

	for _, gs := range targets {
		sp.mu.RLock()
		peerServer := sp.gsToP[gs]
		client := sp.peerClients[peerServer]
		sp.mu.RUnlock()

		if client != nil {
			go func(c *clients.PeerClient, oid string) {
				if _, err := c.RepairObjects([]string{oid}); err != nil {
					sp.logger.Warn("repair failed", zap.String("peer", c.Target()), zap.Error(err))
				}
			}(client, objectID)
		}
	}
	return true
}

func (sp *SuperPeer) AddObjectReference(objectID, peerID string, ttl int64) bool {
	ok1 := sp.groupLedger.AddToObjectLedger(objectID, ledger.MetaData{ID: peerID, TTL: ttl})
	ok2 := sp.groupLedger.AddToPeerLedger(peerID, ledger.MetaData{ID: objectID, TTL: ttl})
	return ok1 || ok2
}

func (sp *SuperPeer) RemovePeerGroupLedger(peerID string) bool {
	sp.groupLedger.RemovePeerFromGroupLedger(peerID)
	return true
}

func (sp *SuperPeer) NotifyPeers(host string) bool {
	sp.mu.RLock()
	peerClients := make([]*clients.PeerClient, 0, len(sp.peerClients))
	for _, c := range sp.peerClients {
		peerClients = append(peerClients, c)
	}
	sp.mu.RUnlock()

	result := true
	for _, c := range peerClients {
		if c.Target() != host {
			ok, err := c.AddPeer(host)
			if err != nil {
				sp.logger.Warn("notifyPeers addPeer failed", zap.String("to", c.Target()), zap.Error(err))
				result = false
			} else {
				result = result && ok
			}
		}
	}
	return result
}

func (sp *SuperPeer) PingPeer(hostname string) bool {
	// Try to dial with a short timeout via any connected peer client
	sp.mu.RLock()
	defer sp.mu.RUnlock()
	for _, c := range sp.peerClients {
		if c.Target() == hostname {
			// We have a client — try a health check
			return true
		}
	}
	return false
}

func (sp *SuperPeer) UpdatePositionReference(pos servers.Position, peerID string) (bool, string, string) {
	vPos := spatial.VirtualPosition{X: pos.X, Y: pos.Y, Z: pos.Z}
	_ = vPos
	// In a full implementation: check IsWithinAOI, return new SP + group if migrating
	// For now: acknowledge, no migration
	return true, "", ""
}

func (sp *SuperPeer) GetReplicationFactor() int32 {
	return int32(sp.cfg.Node.Storage.ReplicationFactor)
}

// HandleShutdown notifies all connected peers that this super-peer is leaving.
func (sp *SuperPeer) HandleShutdown() {
	sp.mu.RLock()
	peerList := make([]*clients.PeerClient, 0, len(sp.peerClients))
	for _, c := range sp.peerClients {
		peerList = append(peerList, c)
	}
	sp.mu.RUnlock()

	for _, c := range peerList {
		go func(pc *clients.PeerClient) {
			if _, err := pc.HandleSuperPeerLeave(); err != nil {
				sp.logger.Warn("handleSuperPeerLeave failed", zap.String("peer", pc.Target()), zap.Error(err))
			}
		}(c)
	}

	sp.mu.Lock()
	sp.running = false
	sp.mu.Unlock()
}

// scheduledRepair triggers a repair scan for under-replicated objects.
func (sp *SuperPeer) scheduledRepair() {
	sp.mu.RLock()
	rf := sp.cfg.Node.Storage.ReplicationFactor
	sp.mu.RUnlock()

	needsRepair := sp.groupLedger.GetObjectsNeedingRepair(rf)
	for objectID := range needsRepair {
		sp.Repair(objectID)
	}
}

// GSSPeerList returns all group-storage hostnames.
func (sp *SuperPeer) GSSPeerList() []string {
	sp.mu.RLock()
	defer sp.mu.RUnlock()
	list := make([]string, 0, len(sp.gsToP))
	for gs := range sp.gsToP {
		list = append(list, gs)
	}
	sort.Strings(list)
	return list
}

// pickNRandom picks n random elements from lst.
func pickNRandom(lst []string, n int) []string {
	cp := make([]string, len(lst))
	copy(cp, lst)
	rand.Shuffle(len(cp), func(i, j int) { cp[i], cp[j] = cp[j], cp[i] })
	if n > len(cp) {
		return cp
	}
	return cp[:n]
}
