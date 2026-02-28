// Package node provides the bootstrap pipeline for Nomad nodes.
package node

import (
	"context"
	"fmt"
	"math/rand"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	servers2 "github.com/iggydv12/gomad/internal/api/grpc/servers"
	"go.uber.org/zap"

	pbm "github.com/iggydv12/gomad/gen/proto/models"
	"github.com/iggydv12/gomad/internal/config"
	"github.com/iggydv12/gomad/internal/discovery"
	"github.com/iggydv12/gomad/internal/ledger"
	"github.com/iggydv12/gomad/internal/spatial"
	"github.com/iggydv12/gomad/internal/storage"
	"github.com/iggydv12/gomad/internal/storage/group"
	"github.com/iggydv12/gomad/internal/storage/local"
	"github.com/iggydv12/gomad/internal/storage/overlay"
)

// Controller bootstraps the node, wires all components, and runs until shutdown.
type Controller struct {
	cfg      *config.Config
	nodeType ComponentType
	logger   *zap.Logger
	peerID   string

	// Core components — created once at boot and shared across role transitions.
	voronoi     *spatial.VoronoiWrapper
	groupLedger *ledger.GroupLedger
	groupStore  *group.GroupStorage
	peerStore   *storage.PeerStorage
	disc        *discovery.LibP2PDiscovery
	virtualPos  spatial.VirtualPosition

	// FSM
	stateMu         sync.RWMutex
	state           NodeState
	activePeer      *Peer
	activeSuperPeer *SuperPeer

	// Scheduler lifecycle — cancelled and recreated on each role transition.
	schedCtx    context.Context
	schedCancel context.CancelFunc

	// Transition dispatch — buffered=1 so callers never block.
	transitionCh chan transitionRequest

	// Network addresses — fixed at boot.
	peerServiceBind string // bind addr for PeerService  (5001–6999)
	spServiceBind   string // bind addr for SuperPeerService (7000–8999)
	gsAddrBind      string // bind addr for GroupStorageService (9000–9999)
	peerServerAddr  string // advertised PeerService addr
	spServerAddr    string // advertised SuperPeerService addr
	gsAddr          string // advertised GroupStorageService addr
	restAddr        string // REST API bind addr
}

// NewController creates a Controller for the given node type.
func NewController(cfg *config.Config, nodeType ComponentType, logger *zap.Logger) *Controller {
	return &Controller{
		cfg:          cfg,
		nodeType:     nodeType,
		logger:       logger,
		peerID:       fmt.Sprintf("node-%d", rand.Int63()),
		state:        StateDiscovering,
		transitionCh: make(chan transitionRequest, 1),
	}
}

// Run bootstraps all components and blocks until SIGINT/SIGTERM.
func (c *Controller) Run(ctx context.Context) error {
	c.logger.Info("Starting Nomad node",
		zap.String("role", c.nodeType.String()),
		zap.String("peerID", c.peerID),
	)

	// --- 1. Group ledger ---
	c.groupLedger = ledger.NewGroupLedger()

	// --- 2. Voronoi ---
	c.voronoi = spatial.NewVoronoiWrapper(c.cfg.Node.World.Width, c.cfg.Node.World.Height)

	// --- 3. libp2p discovery ---
	c.disc = discovery.NewLibP2PDiscovery(
		c.cfg.Node.World.Width,
		c.cfg.Node.World.Height,
		&voronoiAdapter{voronoi: c.voronoi},
		c.logger,
	)
	if err := c.disc.Init(); err != nil {
		return fmt.Errorf("discovery init: %w", err)
	}
	defer c.disc.Close()

	// --- 4. Storage ---
	dbPath := fmt.Sprintf("/tmp/nomad-pebble-%s", c.peerID)
	ls := local.NewPebbleStorage(dbPath, c.logger)
	c.groupStore = group.New(c.cfg.Node.Storage.ReplicationFactor, c.groupLedger, c.logger)
	var dhtOverlay overlay.DHTOverlayStorage
	dhtOverlay = overlay.NewLibP2PStorage(c.disc.GetHost(), c.disc.GetDHT(), c.logger)
	c.peerStore = storage.New(ls, c.groupStore, dhtOverlay, c.groupLedger, c.logger)

	// --- 5. Virtual position ---
	c.virtualPos = spatial.NewRandomPosition(c.cfg.Node.World.Width, c.cfg.Node.World.Height)

	// --- 6. Port allocation (separate ranges for each service) ---
	primaryIP := c.disc.PrimaryIP()

	peerSvcPort := randomPort(5001, 6999)
	spSvcPort := randomPort(7000, 8999)
	gsPort := randomPort(9000, 9999)
	restPort := randomPort(10000, 10999)

	c.peerServiceBind = fmt.Sprintf("0.0.0.0:%d", peerSvcPort)
	c.spServiceBind = fmt.Sprintf("0.0.0.0:%d", spSvcPort)
	c.gsAddrBind = fmt.Sprintf("0.0.0.0:%d", gsPort)
	c.peerServerAddr = fmt.Sprintf("%s:%d", primaryIP, peerSvcPort)
	c.spServerAddr = fmt.Sprintf("%s:%d", primaryIP, spSvcPort)
	c.gsAddr = fmt.Sprintf("%s:%d", primaryIP, gsPort)
	c.restAddr = fmt.Sprintf("0.0.0.0:%d", restPort)

	// Initialize storage (uses gsAddr for self-identity in the group ledger).
	if err := c.peerStore.Init(c.gsAddr, c.cfg.Node.Storage.StorageMode, c.cfg.Node.Storage.RetrievalMode); err != nil {
		return fmt.Errorf("storage init: %w", err)
	}
	defer c.peerStore.Close()

	c.disc.SetGroupStorageHostname(c.gsAddr)
	c.disc.SetSuperPeerHostname(c.spServerAddr)

	// --- 7. Start all gRPC servers at boot (gated handlers) ---
	//
	// All three servers start regardless of initial role.  Gated handlers
	// check the current NodeState and delegate to the active component or
	// return a graceful "unavailable" response.
	grpcPeer, err := servers2.NewPeerServiceServer(
		&gatedPeerHandler{ctrl: c}, c.logger,
	).Serve(c.peerServiceBind)
	if err != nil {
		return fmt.Errorf("PeerService gRPC serve: %w", err)
	}
	defer grpcPeer.Stop()

	grpcSP, err := servers2.NewSuperPeerServiceServer(
		&gatedSuperPeerHandler{ctrl: c}, c.groupLedger, c.logger,
	).Serve(c.spServiceBind)
	if err != nil {
		return fmt.Errorf("SuperPeerService gRPC serve: %w", err)
	}
	defer grpcSP.Stop()

	grpcGS, err := servers2.NewGroupStorageServiceServer(
		&gatedGroupStorageHandler{ctrl: c}, c.logger,
	).Serve(c.gsAddrBind)
	if err != nil {
		return fmt.Errorf("GroupStorageService gRPC serve: %w", err)
	}
	defer grpcGS.Stop()

	c.logger.Info("All gRPC servers listening",
		zap.String("peer", c.peerServiceBind),
		zap.String("superPeer", c.spServiceBind),
		zap.String("groupStorage", c.gsAddrBind),
	)

	// --- 8. Start transition dispatch loop ---
	go c.runTransitionLoop(ctx)

	// --- 9. Dynamic bootstrap ---
	groupName := fmt.Sprintf("group-%d", rand.Int63()&0xFFFF)
	if err := c.bootstrapDynamic(ctx, groupName); err != nil {
		return fmt.Errorf("bootstrap: %w", err)
	}

	// --- 10. REST API ---
	c.startRESTServer(c.restAddr, c.peerStore, c.groupLedger)

	c.logger.Info("Node running",
		zap.String("state", c.getState().String()),
		zap.String("REST", c.restAddr),
	)

	// --- 11. Wait for shutdown ---
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	select {
	case <-sigCh:
		c.logger.Info("Shutdown signal received")
	case <-ctx.Done():
		c.logger.Info("Context cancelled")
	}

	return nil
}

// --- FSM helpers ---

func (c *Controller) getState() NodeState {
	c.stateMu.RLock()
	defer c.stateMu.RUnlock()
	return c.state
}

func (c *Controller) setState(s NodeState) {
	c.stateMu.Lock()
	c.state = s
	c.stateMu.Unlock()
}

func (c *Controller) getActiveSuperPeer() *SuperPeer {
	c.stateMu.RLock()
	defer c.stateMu.RUnlock()
	if c.state.IsActingSuperPeer() {
		return c.activeSuperPeer
	}
	return nil
}

func (c *Controller) getActivePeer() *Peer {
	c.stateMu.RLock()
	defer c.stateMu.RUnlock()
	// Also allow access during StateElecting so the gated handler can forward
	// the HandleSuperPeerLeave call to the peer object.
	if c.state == StatePeer || c.state == StateElecting {
		return c.activePeer
	}
	return nil
}

// onPeerRequestsTransition is the callback given to Peer; it enqueues a
// transition without blocking (the drop case is safe because buffered=1 and
// only one outstanding transition makes sense at a time).
func (c *Controller) onPeerRequestsTransition(req transitionRequest) {
	select {
	case c.transitionCh <- req:
	default:
		c.logger.Warn("Transition channel full — dropping request")
	}
}

// --- Dynamic bootstrap ---

// bootstrapDynamic replaces the old static bootstrapSuperPeer / bootstrapPeer
// pair. It implements the initial state-machine transition:
//
//	--type super-peer → permanent SP immediately.
//	--type peer       → poll for SPs for DiscoveryWindow; if none found, apply
//	                    jitter then re-check; become provisional SP if still empty.
func (c *Controller) bootstrapDynamic(ctx context.Context, groupName string) error {
	if c.nodeType == RoleSuperPeer {
		return c.transitionToSuperPeer(ctx, true, groupName)
	}

	window := c.cfg.Bootstrap.DiscoveryWindow
	interval := 500 * time.Millisecond
	deadline := time.Now().Add(window)

	var superPeers []discovery.SuperPeerInfo
	for time.Now().Before(deadline) {
		sps, _ := c.disc.FindSuperPeers()
		if len(sps) > 0 {
			superPeers = sps
			break
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(interval):
		}
	}

	if len(superPeers) > 0 {
		return c.transitionToPeer(ctx, superPeers)
	}

	// No super-peer found — apply random jitter (0–500 ms) to reduce
	// thundering-herd when multiple peers start simultaneously.
	jitter := time.Duration(rand.Int63n(int64(500 * time.Millisecond)))
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(jitter):
	}

	// Re-check after jitter; another node may have won the race.
	sps, _ := c.disc.FindSuperPeers()
	if len(sps) > 0 {
		return c.transitionToPeer(ctx, sps)
	}

	// Still no super-peer — become provisional SP.
	c.logger.Info("No super-peer found — assuming provisional SP role")
	return c.transitionToSuperPeer(ctx, false, groupName)
}

// --- Role transitions ---

// transitionToSuperPeer tears down any active peer, creates a SuperPeer,
// and starts SP schedulers. If provisional=false the node will never demote.
func (c *Controller) transitionToSuperPeer(ctx context.Context, permanent bool, groupName string) error {
	c.logger.Info("Transitioning to super-peer",
		zap.Bool("permanent", permanent),
		zap.String("group", groupName),
	)

	// Snapshot and clear active roles outside the lock to avoid slow operations
	// while holding it.
	c.stateMu.Lock()
	oldPeer := c.activePeer
	oldSP := c.activeSuperPeer
	oldCancel := c.schedCancel
	c.activePeer = nil
	c.activeSuperPeer = nil
	if permanent {
		c.state = StatePermanentSuperPeer
	} else {
		c.state = StateProvisionalSuperPeer
	}
	c.stateMu.Unlock()

	if oldCancel != nil {
		oldCancel()
	}
	if oldPeer != nil {
		oldPeer.Close() // best-effort; SP may already be gone
	}
	if oldSP != nil {
		oldSP.HandleShutdown()
	}

	sp := NewSuperPeer(c.cfg, c.groupLedger, c.logger)

	c.stateMu.Lock()
	c.activeSuperPeer = sp
	c.stateMu.Unlock()

	sp.TakeLeadership(groupName, !permanent)

	// Publish presence in DHT.
	if err := c.disc.RegisterAsSuperPeer(groupName, c.virtualPos); err != nil {
		c.logger.Warn("DHT super-peer registration failed", zap.Error(err))
	}

	// Update Voronoi with our own site.
	c.voronoi.AddSitePoint(spatial.SitePoint{
		PeerID:    c.peerID,
		GroupName: groupName,
		GSHost:    c.gsAddr,
		Position:  c.virtualPos,
	})

	// Start SP schedulers under a fresh child context.
	schedCtx, schedCancel := context.WithCancel(ctx)
	c.stateMu.Lock()
	c.schedCtx = schedCtx
	c.schedCancel = schedCancel
	c.stateMu.Unlock()

	c.startSuperPeerSchedulers(schedCtx, sp, c.groupLedger, c.groupStore, c.disc, groupName, c.virtualPos)

	// If provisional, poll DHT for a dedicated SP that should take over.
	if !permanent {
		go c.monitorForDedicatedSuperPeer(schedCtx)
	}

	c.logger.Info("Now acting as super-peer",
		zap.Bool("permanent", permanent),
		zap.String("group", groupName),
		zap.String("spAddr", c.spServerAddr),
	)
	return nil
}

// transitionToPeer tears down any active SP, creates a Peer, and joins the
// nearest super-peer.
func (c *Controller) transitionToPeer(ctx context.Context, superPeers []discovery.SuperPeerInfo) error {
	c.logger.Info("Transitioning to peer role",
		zap.Int("availableSPs", len(superPeers)),
	)

	c.stateMu.Lock()
	oldSP := c.activeSuperPeer
	oldPeer := c.activePeer
	oldCancel := c.schedCancel
	c.activeSuperPeer = nil
	c.activePeer = nil
	c.state = StatePeer
	c.stateMu.Unlock()

	if oldCancel != nil {
		oldCancel()
	}
	if oldSP != nil {
		oldSP.HandleShutdown()
	}
	if oldPeer != nil {
		oldPeer.Close() // best-effort
	}

	// Clear ledger and rebuild Voronoi from discovered super-peers.
	c.groupLedger.ClearAll()

	sites := make([]spatial.SitePoint, len(superPeers))
	for i, sp := range superPeers {
		sites[i] = spatial.SitePoint{
			PeerID:    sp.PeerID,
			GroupName: sp.GroupName,
			Multiaddr: sp.Multiaddr,
			SPHost:    sp.SPHost,
			GSHost:    sp.GSHost,
			Position:  sp.Position,
		}
	}
	c.voronoi.UpdateSitePoints(sites)

	nearest, err := c.voronoi.FindNearestSuperPeer(c.virtualPos)
	if err != nil {
		return fmt.Errorf("find nearest super-peer: %w", err)
	}

	peer := NewPeer(c.peerID, c.cfg, c.peerStore, c.groupLedger, c.logger)
	peer.SetAddresses(c.peerServerAddr, c.gsAddr)
	peer.SetPosition(c.virtualPos)
	peer.SetDisc(c.disc)
	peer.SetTransitionCallback(c.onPeerRequestsTransition)

	c.stateMu.Lock()
	c.activePeer = peer
	c.stateMu.Unlock()

	if err := peer.JoinGroup(nearest.GroupName, nearest.SPHost); err != nil {
		return fmt.Errorf("join group: %w", err)
	}

	// Start peer schedulers under a fresh child context.
	schedCtx, schedCancel := context.WithCancel(ctx)
	c.stateMu.Lock()
	c.schedCtx = schedCtx
	c.schedCancel = schedCancel
	c.stateMu.Unlock()

	c.startPeerSchedulers(schedCtx, peer, c.peerStore, c.groupLedger, c.disc)

	c.logger.Info("Now acting as peer",
		zap.String("group", nearest.GroupName),
		zap.String("superPeer", nearest.SPHost),
	)
	return nil
}

// --- Transition dispatch ---

// runTransitionLoop processes role-transition requests from the transitionCh.
// It runs in its own goroutine to avoid deadlocks from gRPC handler goroutines
// calling transition methods directly.
func (c *Controller) runTransitionLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case req := <-c.transitionCh:
			if req.toSuperPeer {
				groupName := req.groupName
				if groupName == "" {
					groupName = fmt.Sprintf("group-%d", rand.Int63()&0xFFFF)
				}
				if err := c.transitionToSuperPeer(ctx, req.permanent, groupName); err != nil {
					c.logger.Error("transition to super-peer failed", zap.Error(err))
				}
			} else {
				sps := req.superPeers
				if len(sps) == 0 {
					// Caller didn't supply super-peers; do a live query.
					sps, _ = c.disc.FindSuperPeers()
				}
				if len(sps) == 0 {
					c.logger.Warn("transitionToPeer: no super-peer found — retrying as provisional SP")
					gn := fmt.Sprintf("group-%d", rand.Int63()&0xFFFF)
					if err := c.transitionToSuperPeer(ctx, false, gn); err != nil {
						c.logger.Error("fallback transition to SP failed", zap.Error(err))
					}
					continue
				}
				if err := c.transitionToPeer(ctx, sps); err != nil {
					c.logger.Error("transition to peer failed", zap.Error(err))
				}
			}
		}
	}
}

// --- Provisional SP monitor ---

// monitorForDedicatedSuperPeer polls the DHT every ProvisionalPollInterval.
// When it detects a foreign super-peer it hands off leadership and enqueues a
// peer transition.  It exits when ctx is cancelled (i.e., when the SP role
// ends for any reason).
func (c *Controller) monitorForDedicatedSuperPeer(ctx context.Context) {
	ticker := time.NewTicker(c.cfg.Bootstrap.ProvisionalPollInterval)
	defer ticker.Stop()
	myID := c.disc.GetHost().ID().String()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			sps, _ := c.disc.FindSuperPeers()
			for _, sp := range sps {
				if sp.PeerID != myID {
					c.logger.Info("Provisional SP: dedicated SP detected — demoting",
						zap.String("newSP", sp.SPHost),
						zap.String("newSPPeerID", sp.PeerID),
					)
					c.stateMu.RLock()
					activeSP := c.activeSuperPeer
					c.stateMu.RUnlock()
					if activeSP != nil {
						activeSP.HandOffAndDemote(sp.SPHost)
					}
					select {
					case c.transitionCh <- transitionRequest{
						toSuperPeer: false,
						superPeers:  sps,
					}:
					default:
					}
					return
				}
			}
		}
	}
}

// --- Schedulers ---

func (c *Controller) startSuperPeerSchedulers(ctx context.Context, sp *SuperPeer, gl *ledger.GroupLedger, gs *group.GroupStorage, disc *discovery.LibP2PDiscovery, groupName string, pos spatial.VirtualPosition) {
	sched := c.cfg.Schedule

	go func() {
		ticker := time.NewTicker(sched.LedgerCleanup)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				gl.CleanExpired()
			}
		}
	}()

	go func() {
		ticker := time.NewTicker(sched.Repair)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if sp.IsRunning() {
					sp.scheduledRepair()
				}
			}
		}
	}()
}

func (c *Controller) startPeerSchedulers(ctx context.Context, p *Peer, ps *storage.PeerStorage, gl *ledger.GroupLedger, disc *discovery.LibP2PDiscovery) {
	sched := c.cfg.Schedule

	go func() {
		ticker := time.NewTicker(sched.HealthCheck)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if p.IsActive() {
					c.performHealthCheck(ps)
				}
			}
		}
	}()

	if c.cfg.Node.Group.Migration {
		go func() {
			ticker := time.NewTicker(sched.UpdatePosition)
			defer ticker.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					if p.IsActive() {
						newPos := spatial.NewRandomPosition(c.cfg.Node.World.Width, c.cfg.Node.World.Height)
						p.UpdatePosition(newPos)
					}
				}
			}
		}()
	}

	go func() {
		ticker := time.NewTicker(sched.DBCleanup)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				gl.CleanExpired()
			}
		}
	}()
}

func (c *Controller) performHealthCheck(ps *storage.PeerStorage) {
	peerList := ps.GroupStorage().PeerList()
	if len(peerList) == 0 {
		return
	}
	target := peerList[rand.Intn(len(peerList))]
	client := ps.GroupStorage().GetClient(target)
	if client == nil {
		return
	}
	ok, err := client.HealthCheck()
	if err != nil || !ok {
		c.logger.Warn("Health check failed", zap.String("peer", target))
		ps.GroupStorage().CheckClient(target)
	}
}

func (c *Controller) startRESTServer(addr string, ps *storage.PeerStorage, gl *ledger.GroupLedger) {
	c.logger.Info("REST API starting", zap.String("addr", addr))
}

// --- Gated handlers ---
//
// Each gated handler wraps the real handler and forwards calls only when the
// node is in the appropriate role.  This allows all three gRPC servers to
// remain bound and accept connections across role transitions.

// gatedSuperPeerHandler forwards SuperPeerService calls to the active SuperPeer.
type gatedSuperPeerHandler struct{ ctrl *Controller }

func (g *gatedSuperPeerHandler) HandleJoin(peerServer, gsServer string, pos servers2.Position) (bool, map[string][]ledger.MetaData, map[string][]ledger.MetaData, int32) {
	if sp := g.ctrl.getActiveSuperPeer(); sp != nil {
		return sp.HandleJoin(peerServer, gsServer, pos)
	}
	return false, nil, nil, 0
}
func (g *gatedSuperPeerHandler) HandleLeave(peerServer, gsServer string) bool {
	if sp := g.ctrl.getActiveSuperPeer(); sp != nil {
		return sp.HandleLeave(peerServer, gsServer)
	}
	return false
}
func (g *gatedSuperPeerHandler) Repair(objectID string) bool {
	if sp := g.ctrl.getActiveSuperPeer(); sp != nil {
		return sp.Repair(objectID)
	}
	return false
}
func (g *gatedSuperPeerHandler) AddObjectReference(objectID, peerID string, ttl int64) bool {
	if sp := g.ctrl.getActiveSuperPeer(); sp != nil {
		return sp.AddObjectReference(objectID, peerID, ttl)
	}
	return false
}
func (g *gatedSuperPeerHandler) RemovePeerGroupLedger(peerID string) bool {
	if sp := g.ctrl.getActiveSuperPeer(); sp != nil {
		return sp.RemovePeerGroupLedger(peerID)
	}
	return false
}
func (g *gatedSuperPeerHandler) NotifyPeers(host string) bool {
	if sp := g.ctrl.getActiveSuperPeer(); sp != nil {
		return sp.NotifyPeers(host)
	}
	return false
}
func (g *gatedSuperPeerHandler) PingPeer(hostname string) bool {
	if sp := g.ctrl.getActiveSuperPeer(); sp != nil {
		return sp.PingPeer(hostname)
	}
	return false
}
func (g *gatedSuperPeerHandler) UpdatePositionReference(pos servers2.Position, peerID string) (bool, string, string) {
	if sp := g.ctrl.getActiveSuperPeer(); sp != nil {
		return sp.UpdatePositionReference(pos, peerID)
	}
	return false, "", ""
}
func (g *gatedSuperPeerHandler) GetReplicationFactor() int32 {
	if sp := g.ctrl.getActiveSuperPeer(); sp != nil {
		return sp.GetReplicationFactor()
	}
	return 0
}

// gatedPeerHandler forwards PeerService calls to the active Peer.
type gatedPeerHandler struct{ ctrl *Controller }

func (g *gatedPeerHandler) AddGroupStoragePeer(host string) bool {
	if p := g.ctrl.getActivePeer(); p != nil {
		return p.AddGroupStoragePeer(host)
	}
	return false
}
func (g *gatedPeerHandler) RemoveGroupStoragePeer(host string) bool {
	if p := g.ctrl.getActivePeer(); p != nil {
		return p.RemoveGroupStoragePeer(host)
	}
	return false
}
func (g *gatedPeerHandler) HandleSuperPeerLeave() bool {
	// Set StateElecting before forwarding so the gated SP handler blocks
	// incoming join requests during the election window.
	g.ctrl.stateMu.Lock()
	if g.ctrl.state == StatePeer {
		g.ctrl.state = StateElecting
	}
	p := g.ctrl.activePeer
	g.ctrl.stateMu.Unlock()

	if p != nil {
		return p.HandleSuperPeerLeave()
	}
	return false
}
func (g *gatedPeerHandler) RepairObjects(objectIDs []string) bool {
	if p := g.ctrl.getActivePeer(); p != nil {
		return p.RepairObjects(objectIDs)
	}
	return false
}

// gatedGroupStorageHandler always delegates to the shared PeerStorage +
// GroupLedger; it logs a warning during the election window.
type gatedGroupStorageHandler struct{ ctrl *Controller }

func (g *gatedGroupStorageHandler) warnIfElecting() {
	g.ctrl.stateMu.RLock()
	s := g.ctrl.state
	g.ctrl.stateMu.RUnlock()
	if s == StateElecting {
		g.ctrl.logger.Warn("GroupStorage operation during election window — local only")
	}
}
func (g *gatedGroupStorageHandler) Get(id string) (*pbm.GameObjectGrpc, error) {
	g.warnIfElecting()
	return g.ctrl.peerStore.Get(id, false, false)
}
func (g *gatedGroupStorageHandler) Put(obj *pbm.GameObjectGrpc) (bool, error) {
	g.warnIfElecting()
	return g.ctrl.peerStore.Put(obj, false, false)
}
func (g *gatedGroupStorageHandler) Update(obj *pbm.GameObjectGrpc) (bool, error) {
	g.warnIfElecting()
	return g.ctrl.peerStore.Update(obj)
}
func (g *gatedGroupStorageHandler) Delete(id string) (bool, error) {
	g.warnIfElecting()
	return g.ctrl.peerStore.Delete(id)
}
func (g *gatedGroupStorageHandler) AddToGroupLedger(objectID, peerID string, ttl int64) bool {
	ok1 := g.ctrl.groupLedger.AddToObjectLedger(objectID, ledger.MetaData{ID: peerID, TTL: ttl})
	ok2 := g.ctrl.groupLedger.AddToPeerLedger(peerID, ledger.MetaData{ID: objectID, TTL: ttl})
	return ok1 || ok2
}
func (g *gatedGroupStorageHandler) RemovePeerGroupLedger(peerID string) bool {
	g.ctrl.groupLedger.RemovePeerFromGroupLedger(peerID)
	return true
}

// --- Adapters ---

// voronoiAdapter satisfies discovery.VoronoiQuerier using spatial.VoronoiWrapper.
type voronoiAdapter struct {
	voronoi  *spatial.VoronoiWrapper
	selfSite spatial.SitePoint
}

func (a *voronoiAdapter) IsWithinAOI(pos spatial.VirtualPosition) bool {
	return a.voronoi.IsWithinAOI(a.selfSite, pos)
}

func (a *voronoiAdapter) FindNeighbour(pos spatial.VirtualPosition) (discovery.SuperPeerInfo, error) {
	site, err := a.voronoi.FindNeighbour(pos)
	if err != nil {
		return discovery.SuperPeerInfo{}, err
	}
	return discovery.SuperPeerInfo{
		PeerID:    site.PeerID,
		Multiaddr: site.Multiaddr,
		Position:  site.Position,
		GroupName: site.GroupName,
	}, nil
}

// --- Utilities ---

// randomPort picks a random free TCP port in [min, max).
func randomPort(min, max int) int {
	for {
		port := min + rand.Intn(max-min)
		l, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
		if err == nil {
			l.Close()
			return port
		}
	}
}

