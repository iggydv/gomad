package node

import (
	"math/rand"
	"sync"
	"time"

	retry "github.com/avast/retry-go/v4"
	"github.com/iggydv12/gomad/internal/api/grpc/clients"
	"github.com/iggydv12/gomad/internal/discovery"
	"go.uber.org/zap"

	"github.com/iggydv12/gomad/internal/config"
	"github.com/iggydv12/gomad/internal/ledger"
	"github.com/iggydv12/gomad/internal/spatial"
	"github.com/iggydv12/gomad/internal/storage"
)

// Peer implements the regular storage peer role.
// Maps to Peer.java in the Java implementation.
type Peer struct {
	mu sync.RWMutex

	id              string // assigned by super-peer
	superPeerID     string
	group           string
	peerServer      string // hostname:port for PeerService
	gsHost          string // hostname:port for GroupStorageService
	virtualPos      spatial.VirtualPosition
	superPeerClient *clients.SuperPeerClient
	peerStorage     *storage.PeerStorage
	groupLedger     *ledger.GroupLedger
	cfg             *config.Config
	logger          *zap.Logger
	active          bool

	// Set by the controller to enable election and role transitions.
	disc               *discovery.LibP2PDiscovery
	transitionCallback func(transitionRequest)
}

// NewPeer creates a Peer.
func NewPeer(id string, cfg *config.Config, ps *storage.PeerStorage, gl *ledger.GroupLedger, logger *zap.Logger) *Peer {
	return &Peer{
		id:          id,
		cfg:         cfg,
		peerStorage: ps,
		groupLedger: gl,
		logger:      logger,
	}
}

// SetAddresses configures this peer's network addresses.
func (p *Peer) SetAddresses(peerServer, gsHost string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.peerServer = peerServer
	p.gsHost = gsHost
}

// SetDisc injects the discovery layer so the peer can run elections.
func (p *Peer) SetDisc(d *discovery.LibP2PDiscovery) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.disc = d
}

// SetTransitionCallback registers the controller callback invoked when this
// peer needs a role transition (election win or reconnect).
func (p *Peer) SetTransitionCallback(cb func(transitionRequest)) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.transitionCallback = cb
}

// SetPosition sets the virtual position.
func (p *Peer) SetPosition(pos spatial.VirtualPosition) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.virtualPos = pos
}

// JoinGroup connects to the super-peer at spTarget and joins the group.
func (p *Peer) JoinGroup(group, spTarget string) error {
	p.mu.Lock()
	p.group = group
	p.mu.Unlock()

	err := retry.Do(func() error {
		return p.joinGroupOnce(spTarget)
	},
		retry.Attempts(3),
		retry.Delay(1*time.Second),
		retry.MaxDelay(30*time.Second),
		retry.OnRetry(func(n uint, err error) {
			p.logger.Warn("Join retry", zap.Uint("attempt", n), zap.Error(err))
		}),
	)
	if err != nil {
		return err
	}
	p.active = true
	return nil
}

func (p *Peer) joinGroupOnce(spTarget string) error {
	spClient, err := clients.NewSuperPeerClient(spTarget, p.logger)
	if err != nil {
		return err
	}

	pos := clients.Position{X: p.virtualPos.X, Y: p.virtualPos.Y, Z: p.virtualPos.Z}
	accepted, objLedger, peerLedger, rf, err := spClient.JoinGroup(p.peerServer, p.gsHost, pos)
	if err != nil {
		return err
	}
	if !accepted {
		return &joinRejectedError{}
	}

	p.mu.Lock()
	p.superPeerClient = spClient
	p.cfg.Node.Storage.ReplicationFactor = int(rf)
	p.mu.Unlock()

	p.groupLedger.Populate(objLedger, peerLedger)

	// Notify super-peer of our group storage address
	if _, err := spClient.NotifyPeers(p.gsHost); err != nil {
		p.logger.Warn("notifyPeers failed", zap.Error(err))
	}

	p.logger.Info("Joined group successfully",
		zap.String("group", p.group),
		zap.String("superPeer", spTarget),
		zap.Int("rf", int(rf)),
	)
	return nil
}

// --- servers.PeerHandler implementation ---

func (p *Peer) AddGroupStoragePeer(host string) bool {
	p.mu.RLock()
	self := p.gsHost
	p.mu.RUnlock()

	if host == self {
		return false
	}
	p.peerStorage.AddGroupStoragePeer(host)
	return true
}

func (p *Peer) RemoveGroupStoragePeer(host string) bool {
	p.peerStorage.RemoveGroupStoragePeer(host)
	return true
}

func (p *Peer) HandleSuperPeerLeave() bool {
	p.logger.Info("Super-peer has left — starting election")

	p.mu.Lock()
	group := p.group
	pos := p.virtualPos
	disc := p.disc
	cb := p.transitionCallback
	p.active = false // deactivate during election; schedulers will idle
	p.mu.Unlock()

	if disc == nil || cb == nil {
		// No discovery / controller wired — nothing to do.
		return true
	}

	go func() {
		// Jitter (0–200 ms) to reduce thundering-herd.
		jitter := time.Duration(rand.Int63n(int64(200 * time.Millisecond)))
		time.Sleep(jitter)

		won, err := disc.TryLeaderElection(group, pos)
		if err != nil {
			p.logger.Warn("leader election error", zap.Error(err))
		}
		if won {
			cb(transitionRequest{
				toSuperPeer: true,
				permanent:   false,
				groupName:   group,
			})
		} else {
			p.reconnectToNewSuperPeer()
		}
	}()
	return true
}

// reconnectToNewSuperPeer polls FindSuperPeers with exponential back-off and
// enqueues a peer transition once a super-peer is discovered.
func (p *Peer) reconnectToNewSuperPeer() {
	p.mu.RLock()
	disc := p.disc
	cb := p.transitionCallback
	group := p.group
	p.mu.RUnlock()

	if disc == nil || cb == nil {
		return
	}

	delays := []time.Duration{1, 2, 4, 8, 16}
	for _, d := range delays {
		sps, err := disc.FindSuperPeers()
		if err == nil && len(sps) > 0 {
			cb(transitionRequest{toSuperPeer: false, superPeers: sps})
			return
		}
		p.logger.Info("reconnect: no super-peer yet, retrying",
			zap.Duration("backoff", d*time.Second))
		time.Sleep(d * time.Second)
	}

	// Last attempt
	sps, err := disc.FindSuperPeers()
	if err == nil && len(sps) > 0 {
		cb(transitionRequest{toSuperPeer: false, superPeers: sps})
		return
	}

	// All retries exhausted — self-elect as provisional SP.
	p.logger.Warn("reconnect: no super-peer found after all retries — self-electing",
		zap.String("group", group))
	cb(transitionRequest{
		toSuperPeer: true,
		permanent:   false,
		groupName:   group,
	})
}


func (p *Peer) RepairObjects(objectIDs []string) bool {
	for _, objectID := range objectIDs {
		obj, err := p.peerStorage.Get(objectID, true, false)
		if err != nil {
			p.logger.Warn("repair: object not found", zap.String("id", objectID))
			continue
		}
		if _, err := p.peerStorage.LocalPut(obj); err != nil {
			p.logger.Warn("repair: local put failed", zap.String("id", objectID), zap.Error(err))
		}
	}
	return true
}

// --- servers.GroupStorageHandler implementation ---

func (p *Peer) Get(id string) (any, error) {
	return p.peerStorage.Get(id, false, false)
}

// Close gracefully leaves the group.
func (p *Peer) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.active {
		return nil
	}

	// Notify peers to remove from their ledgers
	p.peerStorage.NotifyAllPeersRemovePeer(p.gsHost)

	// Tell super-peer we're leaving
	if p.superPeerClient != nil {
		if _, err := p.superPeerClient.LeaveGroup(p.peerServer, p.gsHost); err != nil {
			p.logger.Warn("leave group failed", zap.Error(err))
		}
		p.superPeerClient.Close()
	}

	p.groupLedger.ClearAll()
	p.peerStorage.TruncateLocal()
	p.active = false
	return nil
}

// IsActive returns whether the peer is active.
func (p *Peer) IsActive() bool {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.active
}

// UpdatePosition sends a position update to the super-peer, triggering migration if needed.
func (p *Peer) UpdatePosition(pos spatial.VirtualPosition) {
	p.mu.Lock()
	p.virtualPos = pos
	sp := p.superPeerClient
	peerID := p.id
	p.mu.Unlock()

	if sp == nil || !sp.IsActive() {
		return
	}

	clientPos := clients.Position{X: pos.X, Y: pos.Y, Z: pos.Z}
	ack, newSP, newGroup, err := sp.UpdatePosition(peerID, clientPos)
	if err != nil {
		p.logger.Warn("position update failed", zap.Error(err))
		return
	}
	if ack && newSP != "" && newGroup != "" {
		p.logger.Info("Migration triggered",
			zap.String("from", p.group),
			zap.String("to", newGroup),
			zap.String("newSP", newSP),
		)
		go p.migrate(newSP, newGroup)
	}
}

// migrate moves this peer to a new group managed by newSP.
func (p *Peer) migrate(newSP, newGroup string) {
	p.logger.Info("Starting migration", zap.String("to", newGroup))

	p.mu.Lock()
	oldGS := p.gsHost
	oldPS := p.peerServer
	oldSP := p.superPeerClient
	p.mu.Unlock()

	// 1. Notify current group peers to remove us
	p.peerStorage.NotifyAllPeersRemovePeer(oldGS)

	// 2. Tell current super-peer we're leaving
	if oldSP != nil {
		if _, err := oldSP.LeaveGroup(oldPS, oldGS); err != nil {
			p.logger.Warn("leave old group failed", zap.Error(err))
		}
		oldSP.Close()
	}

	// 3. Clear local state
	p.groupLedger.ClearAll()
	p.peerStorage.TruncateLocal()

	// 4. Join new group
	if err := p.JoinGroup(newGroup, newSP); err != nil {
		p.logger.Error("migration join failed", zap.Error(err))
	}
}

// PeerStorageGet implements a direct Get for the peer storage REST/gRPC handler.
func (p *Peer) PeerStorageGet(id string) (any, error) {
	return p.peerStorage.Get(id, true, true)
}

// PeerStorageDelete implements a direct Delete for the peer storage handler.
func (p *Peer) PeerStorageDelete(id string) (bool, error) {
	return p.peerStorage.Delete(id)
}

// joinRejectedError is returned when the super-peer rejects a join request.
type joinRejectedError struct{}

func (e *joinRejectedError) Error() string { return "join rejected by super-peer (max peers reached)" }
