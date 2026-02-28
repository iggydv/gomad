package node

import "github.com/iggydv12/gomad/internal/discovery"

// transitionRequest is sent over the controller's transition channel to
// request an asynchronous role change.
type transitionRequest struct {
	toSuperPeer bool
	permanent   bool                   // only meaningful when toSuperPeer=true
	groupName   string                 // only meaningful when toSuperPeer=true
	superPeers  []discovery.SuperPeerInfo // only meaningful when toSuperPeer=false
}

// NodeState represents the finite-state-machine state of this node's role.
type NodeState int32

const (
	// StateDiscovering is the initial state; role is not yet determined.
	StateDiscovering NodeState = iota
	// StateProvisionalSuperPeer means this node is acting as SP because the
	// network was empty at startup (--type peer) or it won an election; it
	// will yield to a dedicated SP if one joins.
	StateProvisionalSuperPeer
	// StatePermanentSuperPeer means this node was started with --type super-peer
	// and will never demote itself.
	StatePermanentSuperPeer
	// StatePeer means this node is a regular storage peer inside a group.
	StatePeer
	// StateElecting is a transient state during leader election.
	StateElecting
)

// IsActingSuperPeer returns true when the node is serving as a super-peer
// (provisional or permanent).
func (s NodeState) IsActingSuperPeer() bool {
	return s == StateProvisionalSuperPeer || s == StatePermanentSuperPeer
}

// IsStable returns true when the node has settled into a non-transient role.
func (s NodeState) IsStable() bool {
	return s == StatePeer || s.IsActingSuperPeer()
}

func (s NodeState) String() string {
	switch s {
	case StateDiscovering:
		return "discovering"
	case StateProvisionalSuperPeer:
		return "provisional-super-peer"
	case StatePermanentSuperPeer:
		return "permanent-super-peer"
	case StatePeer:
		return "peer"
	case StateElecting:
		return "electing"
	default:
		return "unknown"
	}
}
