// Package node provides the bootstrap pipeline for Nomad nodes.
package node

import (
	"context"
	"fmt"
	"math/rand"
	"net"
	"os"
	"os/signal"
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
}

// NewController creates a Controller for the given node type.
func NewController(cfg *config.Config, nodeType ComponentType, logger *zap.Logger) *Controller {
	return &Controller{
		cfg:      cfg,
		nodeType: nodeType,
		logger:   logger,
		peerID:   fmt.Sprintf("node-%d", rand.Int63()),
	}
}

// Run bootstraps all components and blocks until SIGINT/SIGTERM.
func (c *Controller) Run(ctx context.Context) error {
	c.logger.Info("Starting Nomad node",
		zap.String("role", c.nodeType.String()),
		zap.String("peerID", c.peerID),
	)

	// --- 1. Set up group ledger ---
	gl := ledger.NewGroupLedger()

	// --- 2. Set up Voronoi ---
	voronoi := spatial.NewVoronoiWrapper(c.cfg.Node.World.Width, c.cfg.Node.World.Height)

	// --- 3. Set up libp2p discovery ---
	disc := discovery.NewLibP2PDiscovery(
		c.cfg.Node.World.Width,
		c.cfg.Node.World.Height,
		&voronoiAdapter{voronoi: voronoi},
		c.logger,
	)
	if err := disc.Init(); err != nil {
		return fmt.Errorf("discovery init: %w", err)
	}
	defer disc.Close()

	// --- 4. Set up storage ---
	dbPath := fmt.Sprintf("/tmp/nomad-pebble-%s", c.peerID)
	ls := local.NewPebbleStorage(dbPath, c.logger)
	gs := group.New(c.cfg.Node.Storage.ReplicationFactor, gl, c.logger)
	var dhtOverlay overlay.DHTOverlayStorage
	if disc != nil {
		dhtOverlay = overlay.NewLibP2PStorage(disc.GetHost(), disc.GetDHT(), c.logger)
	}
	ps := storage.New(ls, gs, dhtOverlay, gl, c.logger)

	// --- 5. Assign random virtual position ---
	virtualPos := spatial.NewRandomPosition(c.cfg.Node.World.Width, c.cfg.Node.World.Height)

	// --- 6. Assign ports ---
	peerServerAddr := "0.0.0.0:" + fmt.Sprintf("%d", randomPort(5001, 8999))
	gsAddr := "0.0.0.0:" + fmt.Sprintf("%d", randomPort(5001, 8999))
	restAddr := "0.0.0.0:" + fmt.Sprintf("%d", randomPort(8080, 9999))

	// Initialize storage
	if err := ps.Init(gsAddr, c.cfg.Node.Storage.StorageMode, c.cfg.Node.Storage.RetrievalMode); err != nil {
		return fmt.Errorf("storage init: %w", err)
	}
	defer ps.Close()

	disc.SetGroupStorageHostname(gsAddr)

	// --- 7. Determine role and bootstrap ---
	var role ComponentType
	superPeers, _ := disc.FindSuperPeers()
	if c.nodeType == RoleSuperPeer || len(superPeers) == 0 {
		role = RoleSuperPeer
		c.logger.Info("Assuming super-peer role")
		if err := c.bootstrapSuperPeer(disc, voronoi, gl, gs, ps, virtualPos, peerServerAddr, gsAddr, restAddr); err != nil {
			return err
		}
	} else {
		role = RolePeer
		c.logger.Info("Assuming peer role")
		if err := c.bootstrapPeer(disc, voronoi, gl, ps, virtualPos, peerServerAddr, gsAddr, restAddr, superPeers); err != nil {
			return err
		}
	}

	c.logger.Info("Node running",
		zap.String("role", role.String()),
		zap.String("REST", restAddr),
	)

	// --- 8. Wait for shutdown signal ---
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

func (c *Controller) bootstrapSuperPeer(
	disc *discovery.LibP2PDiscovery,
	vw *spatial.VoronoiWrapper,
	gl *ledger.GroupLedger,
	gs *group.GroupStorage,
	ps *storage.PeerStorage,
	pos spatial.VirtualPosition,
	peerServerAddr, gsAddr, restAddr string,
) error {
	groupName := fmt.Sprintf("group-%d", rand.Int63()&0xFFFF)
	sp := NewSuperPeer(c.cfg, gl, c.logger)
	sp.TakeLeadership(groupName)

	// Register in DHT
	if err := disc.RegisterAsSuperPeer(groupName, pos); err != nil {
		c.logger.Warn("DHT registration failed", zap.Error(err))
	}

	// Update Voronoi
	vw.AddSitePoint(spatial.SitePoint{
		PeerID:    c.peerID,
		GroupName: groupName,
		GSHost:    gsAddr,
		Position:  pos,
	})

	// Start gRPC servers
	spSrv := servers2.NewSuperPeerServiceServer(sp, gl, c.logger)
	grpcSP, err := spSrv.Serve(peerServerAddr)
	if err != nil {
		return fmt.Errorf("superpeer gRPC serve: %w", err)
	}
	defer grpcSP.Stop()

	gsSrv := servers2.NewGroupStorageServiceServer(&groupStorageHandler{ps: ps, gl: gl}, c.logger)
	grpcGS, err := gsSrv.Serve(gsAddr)
	if err != nil {
		return fmt.Errorf("group storage gRPC serve: %w", err)
	}
	defer grpcGS.Stop()

	// Start schedulers
	c.startSuperPeerSchedulers(context.Background(), sp, gl, gs, disc, groupName, pos)

	// Start REST API
	c.startRESTServer(restAddr, ps, gl)

	return nil
}

func (c *Controller) bootstrapPeer(
	disc *discovery.LibP2PDiscovery,
	vw *spatial.VoronoiWrapper,
	gl *ledger.GroupLedger,
	ps *storage.PeerStorage,
	pos spatial.VirtualPosition,
	peerServerAddr, gsAddr, restAddr string,
	superPeers []discovery.SuperPeerInfo,
) error {
	// Find nearest super-peer via Voronoi
	sites := make([]spatial.SitePoint, len(superPeers))
	for i, sp := range superPeers {
		sites[i] = spatial.SitePoint{
			PeerID:    sp.PeerID,
			GroupName: sp.GroupName,
			Multiaddr: sp.Multiaddr,
			Position:  sp.Position,
		}
	}
	vw.UpdateSitePoints(sites)

	nearest, err := vw.FindNearestSuperPeer(pos)
	if err != nil {
		return fmt.Errorf("find nearest super-peer: %w", err)
	}

	peer := NewPeer(c.peerID, c.cfg, ps, gl, c.logger)
	peer.SetAddresses(peerServerAddr, gsAddr)
	peer.SetPosition(pos)

	if err := peer.JoinGroup(nearest.GroupName, nearest.Multiaddr); err != nil {
		return fmt.Errorf("join group: %w", err)
	}

	// Start gRPC PeerService server
	peerSrv := servers2.NewPeerServiceServer(peer, c.logger)
	grpcPeer, err := peerSrv.Serve(peerServerAddr)
	if err != nil {
		return fmt.Errorf("peer gRPC serve: %w", err)
	}
	defer grpcPeer.Stop()

	// Start GroupStorage gRPC server (this peer also serves group storage)
	gsSrv := servers2.NewGroupStorageServiceServer(&groupStorageHandler{ps: ps, gl: gl}, c.logger)
	grpcGS, err := gsSrv.Serve(gsAddr)
	if err != nil {
		return fmt.Errorf("group storage gRPC serve: %w", err)
	}
	defer grpcGS.Stop()

	// Start schedulers
	c.startPeerSchedulers(context.Background(), peer, ps, gl, disc)

	// Start REST API
	c.startRESTServer(restAddr, ps, gl)

	return nil
}

func (c *Controller) startSuperPeerSchedulers(ctx context.Context, sp *SuperPeer, gl *ledger.GroupLedger, gs *group.GroupStorage, disc *discovery.LibP2PDiscovery, groupName string, pos spatial.VirtualPosition) {
	sched := c.cfg.Schedule

	// Ledger cleanup
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

	// Scheduled repair
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

	// Health check
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

	// Position update
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

	// DB cleanup
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
	// REST router is started in api/rest package — wired here
	c.logger.Info("REST API starting", zap.String("addr", addr))
	// Actual gin server is started by the rest package
}

// randomPort picks a random free TCP port in the given range.
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

// --- Adapters to satisfy interface constraints ---

// groupStorageHandler adapts PeerStorage to the servers.GroupStorageHandler interface.
type groupStorageHandler struct {
	ps *storage.PeerStorage
	gl *ledger.GroupLedger
}

func (h *groupStorageHandler) Get(id string) (*pbm.GameObjectGrpc, error) {
	return h.ps.Get(id, false, false)
}

func (h *groupStorageHandler) Put(obj *pbm.GameObjectGrpc) (bool, error) {
	return h.ps.Put(obj, false, false)
}

func (h *groupStorageHandler) Update(obj *pbm.GameObjectGrpc) (bool, error) {
	return h.ps.Update(obj)
}

func (h *groupStorageHandler) Delete(id string) (bool, error) {
	return h.ps.Delete(id)
}

func (h *groupStorageHandler) AddToGroupLedger(objectID, peerID string, ttl int64) bool {
	ok1 := h.gl.AddToObjectLedger(objectID, ledger.MetaData{ID: peerID, TTL: ttl})
	ok2 := h.gl.AddToPeerLedger(peerID, ledger.MetaData{ID: objectID, TTL: ttl})
	return ok1 || ok2
}

func (h *groupStorageHandler) RemovePeerGroupLedger(peerID string) bool {
	h.gl.RemovePeerFromGroupLedger(peerID)
	return true
}

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
