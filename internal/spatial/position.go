// Package spatial handles virtual position generation and geometry helpers.
package spatial

import (
	"math"
	"math/rand"
)

// VirtualPosition represents a peer's 3D virtual position in the game world.
type VirtualPosition struct {
	X, Y, Z float64
}

// NewRandomPosition generates a random 2D position within world bounds (Z=0).
func NewRandomPosition(worldWidth, worldHeight float64) VirtualPosition {
	return VirtualPosition{
		X: rand.Float64() * worldWidth,
		Y: rand.Float64() * worldHeight,
		Z: 0,
	}
}

// Distance2D returns the Euclidean distance ignoring Z.
func (p VirtualPosition) Distance2D(other VirtualPosition) float64 {
	dx := p.X - other.X
	dy := p.Y - other.Y
	return math.Sqrt(dx*dx + dy*dy)
}
