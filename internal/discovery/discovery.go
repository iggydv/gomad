// Package discovery defines the peer discovery interface.
package discovery

import "github.com/iggydv12/gomad/internal/spatial"

// SuperPeerInfo holds information about a discovered super-peer.
type SuperPeerInfo struct {
	PeerID   string
	Multiaddr string
	Position  spatial.VirtualPosition
	GroupName string
}

// PeerDiscovery is the interface for P2P discovery and group membership.
// Replaces ZookeeperDirectoryServerClient.java (~500KB).
type PeerDiscovery interface {
	// Init starts the discovery subsystem (libp2p host, mDNS, DHT).
	Init() error
	// RegisterAsSuperPeer publishes this node as a super-peer with given position.
	RegisterAsSuperPeer(groupName string, pos spatial.VirtualPosition) error
	// FindSuperPeers queries the DHT for all known super-peers.
	FindSuperPeers() ([]SuperPeerInfo, error)
	// JoinGroup announces this node as a provider of the given group key.
	JoinGroup(groupName string) error
	// GetGroupMembers returns the current members of the group.
	GetGroupMembers(groupName string) ([]string, error)
	// RefreshLeadership re-publishes the leader DHT record (call every 60s).
	RefreshLeadership(groupName string, pos spatial.VirtualPosition) error
	// SetGroupStorageHostname sets the group storage address for DHT advertisement.
	SetGroupStorageHostname(host string)
	// GetGroupName returns the name of the group this node belongs to.
	GetGroupName() string
	// SetGroupName sets the group name.
	SetGroupName(name string)
	// IsWithinAOI checks if the given position falls within this super-peer's Voronoi cell.
	IsWithinAOI(pos spatial.VirtualPosition) bool
	// GetNeighbouringLeaderData returns the nearest neighbour super-peer for migration.
	GetNeighbouringLeaderData(pos spatial.VirtualPosition) (SuperPeerInfo, error)
	// GetGroupStorageHostnames returns the group storage hostnames for all group members.
	GetGroupStorageHostnames(groupName string) ([]string, error)
	// Close shuts down the discovery subsystem.
	Close() error
}
