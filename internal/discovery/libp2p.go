// Package discovery provides the libp2p-backed peer discovery implementation.
// Replaces ZookeeperDirectoryServerClient.java — no external service needed.
package discovery

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	dht "github.com/libp2p/go-libp2p-kad-dht"
	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/p2p/discovery/mdns"
	"github.com/multiformats/go-multiaddr"
	"go.uber.org/zap"

	"github.com/iggydv12/gomad/internal/spatial"
)

const (
	dhtLeaderPrefix    = "/nomad/leader/"
	dhtSuperPeerPrefix = "/nomad/superpeer/"
	mdnsServiceTag     = "nomad-discovery"
	leaderTTL          = 120 * time.Second
	refreshInterval    = 60 * time.Second
)

// voronoiRecord is serialized into the DHT for super-peer discovery.
type voronoiRecord struct {
	PeerID    string  `json:"peerID"`
	Multiaddr string  `json:"multiaddr"`
	SPHost    string  `json:"spHost"`    // SuperPeerService gRPC address (host:port)
	X         float64 `json:"x"`
	Y         float64 `json:"y"`
	GroupName string  `json:"groupName"`
	GSHost    string  `json:"gsHost"`    // GroupStorageService gRPC address (host:port)
}

// LibP2PDiscovery implements PeerDiscovery using libp2p mDNS + Kademlia DHT.
type LibP2PDiscovery struct {
	mu            sync.RWMutex
	host          host.Host
	dht           *dht.IpfsDHT
	logger        *zap.Logger
	groupName     string
	gsHost        string // GroupStorageService advertised gRPC address
	spHost        string // SuperPeerService advertised gRPC address
	voronoi       VoronoiQuerier
	worldW        float64
	worldH        float64
	cancelRefresh context.CancelFunc
}

// VoronoiQuerier is fulfilled by spatial.VoronoiWrapper.
type VoronoiQuerier interface {
	IsWithinAOI(pos spatial.VirtualPosition) bool
	FindNeighbour(pos spatial.VirtualPosition) (SuperPeerInfo, error)
}

// NewLibP2PDiscovery creates a LibP2PDiscovery.
func NewLibP2PDiscovery(worldW, worldH float64, voronoi VoronoiQuerier, logger *zap.Logger) *LibP2PDiscovery {
	return &LibP2PDiscovery{
		worldW:  worldW,
		worldH:  worldH,
		voronoi: voronoi,
		logger:  logger,
	}
}

// nomadValidator accepts any value stored under the /nomad/ DHT namespace.
type nomadValidator struct{}

func (nomadValidator) Validate(_ string, _ []byte) error { return nil }
func (nomadValidator) Select(_ string, vals [][]byte) (int, error) {
	if len(vals) == 0 {
		return 0, fmt.Errorf("no values")
	}
	return 0, nil
}

// Init creates the libp2p host, starts mDNS discovery, and bootstraps the Kademlia DHT.
func (d *LibP2PDiscovery) Init() error {
	h, err := libp2p.New(
		libp2p.ListenAddrStrings("/ip4/0.0.0.0/tcp/0"),
		libp2p.NATPortMap(),
	)
	if err != nil {
		return fmt.Errorf("libp2p host: %w", err)
	}
	d.host = h

	// Start Kademlia DHT in server mode.
	// Use a custom protocol prefix (/nomad) so the DHT is isolated from the
	// IPFS network and is not subject to the /ipfs validator constraint
	// (/pk and /ipns only). With a custom prefix we can register our own
	// /nomad/* key validators freely.
	kadDHT, err := dht.New(context.Background(), h,
		dht.Mode(dht.ModeServer),
		dht.ProtocolPrefix("/nomad"),
		dht.NamespacedValidator("nomad", nomadValidator{}),
	)
	if err != nil {
		return fmt.Errorf("kademlia dht: %w", err)
	}
	if err := kadDHT.Bootstrap(context.Background()); err != nil {
		d.logger.Warn("DHT bootstrap failed (will retry)", zap.Error(err))
	}
	d.dht = kadDHT

	// Start mDNS for local subnet discovery
	mdnsService := mdns.NewMdnsService(h, mdnsServiceTag, &mdnsNotifee{discovery: d, logger: d.logger})
	if err := mdnsService.Start(); err != nil {
		d.logger.Warn("mDNS start failed (LAN discovery disabled)", zap.Error(err))
	}

	d.logger.Info("libp2p discovery started",
		zap.String("peerID", h.ID().String()),
		zap.Strings("addrs", addrsToStrings(h.Addrs())),
	)
	return nil
}

// RegisterAsSuperPeer publishes this node as a super-peer in the DHT.
func (d *LibP2PDiscovery) RegisterAsSuperPeer(groupName string, pos spatial.VirtualPosition) error {
	d.mu.Lock()
	d.groupName = groupName
	d.mu.Unlock()

	rec := voronoiRecord{
		PeerID:    d.host.ID().String(),
		Multiaddr: d.primaryMultiaddr(),
		SPHost:    d.spHost,
		X:         pos.X,
		Y:         pos.Y,
		GroupName: groupName,
		GSHost:    d.gsHost,
	}
	data, err := json.Marshal(rec)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	key := dhtSuperPeerPrefix + d.host.ID().String()
	if err := d.dht.PutValue(ctx, key, data); err != nil {
		return fmt.Errorf("register super-peer: %w", err)
	}
	// Also publish leader record
	if err := d.dht.PutValue(ctx, dhtLeaderPrefix+groupName, data); err != nil {
		d.logger.Warn("DHT leader record put failed", zap.Error(err))
	}

	// Start background refresh
	d.startLeaderRefresh(groupName, pos)

	d.logger.Info("Registered as super-peer",
		zap.String("group", groupName),
		zap.Float64("x", pos.X),
		zap.Float64("y", pos.Y),
	)
	return nil
}

// startLeaderRefresh keeps the DHT leader record alive every 60s.
func (d *LibP2PDiscovery) startLeaderRefresh(groupName string, pos spatial.VirtualPosition) {
	if d.cancelRefresh != nil {
		d.cancelRefresh()
	}
	ctx, cancel := context.WithCancel(context.Background())
	d.cancelRefresh = cancel

	go func() {
		ticker := time.NewTicker(refreshInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if err := d.RegisterAsSuperPeer(groupName, pos); err != nil {
					d.logger.Warn("DHT leadership refresh failed", zap.Error(err))
				}
			}
		}
	}()
}

// FindSuperPeers queries the DHT for all known super-peers.
func (d *LibP2PDiscovery) FindSuperPeers() ([]SuperPeerInfo, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// We scan the local routing table for peers that have published records
	// In practice, super-peers have put values under /nomad/superpeer/<peerID>
	peers := d.host.Network().Peers()
	var results []SuperPeerInfo

	for _, p := range peers {
		key := dhtSuperPeerPrefix + p.String()
		data, err := d.dht.GetValue(ctx, key)
		if err != nil {
			continue
		}
		var rec voronoiRecord
		if err := json.Unmarshal(data, &rec); err != nil {
			continue
		}
		results = append(results, SuperPeerInfo{
			PeerID:    rec.PeerID,
			Multiaddr: rec.Multiaddr,
			SPHost:    rec.SPHost,
			GSHost:    rec.GSHost,
			Position:  spatial.VirtualPosition{X: rec.X, Y: rec.Y},
			GroupName: rec.GroupName,
		})
	}

	return results, nil
}

// JoinGroup announces this node as a member of groupName.
func (d *LibP2PDiscovery) JoinGroup(groupName string) error {
	d.mu.Lock()
	d.groupName = groupName
	d.mu.Unlock()
	d.logger.Info("Joined group", zap.String("group", groupName))
	return nil
}

// GetGroupMembers returns the current members for groupName from the DHT.
func (d *LibP2PDiscovery) GetGroupMembers(groupName string) ([]string, error) {
	// Use the routing table peers that share a group record
	peers := d.host.Network().Peers()
	var members []string
	for _, p := range peers {
		members = append(members, p.String())
	}
	return members, nil
}

// RefreshLeadership re-publishes the leader record.
func (d *LibP2PDiscovery) RefreshLeadership(groupName string, pos spatial.VirtualPosition) error {
	return d.RegisterAsSuperPeer(groupName, pos)
}

// SetGroupStorageHostname sets the advertised GroupStorageService gRPC address.
func (d *LibP2PDiscovery) SetGroupStorageHostname(host string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.gsHost = host
}

// SetSuperPeerHostname sets the advertised SuperPeerService gRPC address.
func (d *LibP2PDiscovery) SetSuperPeerHostname(host string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.spHost = host
}

// PrimaryIP returns the first non-loopback IPv4 address that libp2p is
// listening on, falling back to 127.0.0.1. Use this to build advertised
// gRPC addresses that remote nodes can actually reach.
func (d *LibP2PDiscovery) PrimaryIP() string {
	for _, addr := range d.host.Addrs() {
		s := addr.String() // e.g. /ip4/172.20.0.2/tcp/45063
		if strings.HasPrefix(s, "/ip4/") {
			parts := strings.Split(s, "/")
			if len(parts) >= 3 && parts[2] != "127.0.0.1" {
				return parts[2]
			}
		}
	}
	return "127.0.0.1"
}

// GetGroupName returns the current group name.
func (d *LibP2PDiscovery) GetGroupName() string {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.groupName
}

// SetGroupName sets the group name.
func (d *LibP2PDiscovery) SetGroupName(name string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.groupName = name
}

// IsWithinAOI delegates to the Voronoi wrapper.
func (d *LibP2PDiscovery) IsWithinAOI(pos spatial.VirtualPosition) bool {
	if d.voronoi == nil {
		return true
	}
	return d.voronoi.IsWithinAOI(pos)
}

// GetNeighbouringLeaderData returns the closest neighbouring super-peer.
func (d *LibP2PDiscovery) GetNeighbouringLeaderData(pos spatial.VirtualPosition) (SuperPeerInfo, error) {
	if d.voronoi == nil {
		return SuperPeerInfo{}, nil
	}
	return d.voronoi.FindNeighbour(pos)
}

// GetGroupStorageHostnames returns group storage hostnames from DHT records.
func (d *LibP2PDiscovery) GetGroupStorageHostnames(groupName string) ([]string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	peers := d.host.Network().Peers()
	var hosts []string

	for _, p := range peers {
		key := dhtSuperPeerPrefix + p.String()
		data, err := d.dht.GetValue(ctx, key)
		if err != nil {
			continue
		}
		var rec voronoiRecord
		if err := json.Unmarshal(data, &rec); err != nil {
			continue
		}
		if rec.GroupName == groupName && rec.GSHost != "" {
			hosts = append(hosts, rec.GSHost)
		}
	}

	// Deterministic order
	sort.Strings(hosts)
	return hosts, nil
}

// GetDHT returns the underlying DHT instance for use by the overlay storage.
func (d *LibP2PDiscovery) GetDHT() *dht.IpfsDHT { return d.dht }

// GetHost returns the libp2p host.
func (d *LibP2PDiscovery) GetHost() host.Host { return d.host }

// TryLeaderElection attempts to become the group leader.
// Returns true if this node wins (lowest PeerID deterministic election).
func (d *LibP2PDiscovery) TryLeaderElection(groupName string, pos spatial.VirtualPosition) (bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Check if a leader already exists
	key := dhtLeaderPrefix + groupName
	data, err := d.dht.GetValue(ctx, key)
	if err == nil {
		var rec voronoiRecord
		if jsonErr := json.Unmarshal(data, &rec); jsonErr == nil {
			// Leader exists — check if reachable
			if rec.PeerID != d.host.ID().String() {
				// Another peer claims leadership
				d.logger.Info("Leader already exists for group", zap.String("group", groupName), zap.String("leader", rec.PeerID))
				return false, nil
			}
		}
	}

	// No leader or we are the leader candidate — check peers in the group
	// The lowest PeerID among candidates wins (deterministic)
	peers := d.host.Network().Peers()
	myID := d.host.ID().String()
	winner := myID

	for _, p := range peers {
		if p.String() < winner {
			winner = p.String()
		}
	}

	if winner == myID {
		d.logger.Info("Won leader election", zap.String("group", groupName))
		return true, d.RegisterAsSuperPeer(groupName, pos)
	}

	d.logger.Info("Lost leader election", zap.String("winner", winner))
	return false, nil
}

// Close shuts down the libp2p host and DHT.
func (d *LibP2PDiscovery) Close() error {
	if d.cancelRefresh != nil {
		d.cancelRefresh()
	}
	if d.dht != nil {
		d.dht.Close()
	}
	if d.host != nil {
		return d.host.Close()
	}
	return nil
}

// primaryMultiaddr returns the first public (or loopback) multiaddr as a string.
func (d *LibP2PDiscovery) primaryMultiaddr() string {
	addrs := d.host.Addrs()
	if len(addrs) == 0 {
		return ""
	}
	return addrs[0].String() + "/p2p/" + d.host.ID().String()
}

func addrsToStrings(addrs []multiaddr.Multiaddr) []string {
	s := make([]string, len(addrs))
	for i, a := range addrs {
		s[i] = a.String()
	}
	return s
}

// mdnsNotifee handles mDNS peer discovery notifications.
type mdnsNotifee struct {
	discovery *LibP2PDiscovery
	logger    *zap.Logger
}

func (n *mdnsNotifee) HandlePeerFound(pi peer.AddrInfo) {
	n.logger.Info("mDNS: found peer", zap.String("peerID", pi.ID.String()))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := n.discovery.host.Connect(ctx, pi); err != nil {
		n.logger.Warn("mDNS connect failed", zap.String("peer", pi.ID.String()), zap.Error(err))
	}
}
