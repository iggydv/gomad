package ledger

// MetaData represents a ledger entry (peer ID or object ID with TTL)
type MetaData struct {
	ID  string
	TTL int64 // absolute expiry Unix timestamp (seconds)
}

// ComponentType represents the node's role in the system
type ComponentType int

const (
	RoleNone      ComponentType = iota
	RolePeer                    // regular peer
	RoleSuperPeer               // super-peer / group leader
)

func (c ComponentType) String() string {
	switch c {
	case RolePeer:
		return "peer"
	case RoleSuperPeer:
		return "super-peer"
	default:
		return "none"
	}
}
