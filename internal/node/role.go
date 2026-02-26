package node

// ComponentType represents the role of this node.
type ComponentType int

const (
	RoleNone      ComponentType = iota
	RolePeer                    // Regular storage peer
	RoleSuperPeer               // Group leader / super-peer
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
