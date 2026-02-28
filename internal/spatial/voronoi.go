// Package spatial provides Voronoi-based group assignment.
// Maps to VoronoiWrapper.java (Tektosyne-based) in the Java implementation.
package spatial

import (
	"fmt"
	"math"
	"sync"

	"github.com/derekmu/voronoi"
)

// SitePoint represents a super-peer's position and metadata in the Voronoi diagram.
type SitePoint struct {
	PeerID    string
	GroupName string
	Multiaddr string // libp2p p2p multiaddr
	SPHost    string // SuperPeerService gRPC address (host:port)
	GSHost    string // GroupStorageService gRPC address (host:port)
	Position  VirtualPosition
}

// VoronoiWrapper manages the Voronoi diagram for group assignment.
// Maps to VoronoiWrapper.java in the Java implementation.
type VoronoiWrapper struct {
	mu      sync.RWMutex
	sites   []SitePoint
	diagram *voronoi.Diagram
	worldW  float64
	worldH  float64
}

// NewVoronoiWrapper creates a VoronoiWrapper for the given world dimensions.
func NewVoronoiWrapper(worldW, worldH float64) *VoronoiWrapper {
	return &VoronoiWrapper{worldW: worldW, worldH: worldH}
}

// UpdateSitePoints rebuilds the Voronoi diagram from new super-peer positions.
func (vw *VoronoiWrapper) UpdateSitePoints(sites []SitePoint) {
	vw.mu.Lock()
	defer vw.mu.Unlock()
	vw.sites = sites
	vw.rebuild()
}

// AddSitePoint adds a single super-peer and rebuilds the diagram.
func (vw *VoronoiWrapper) AddSitePoint(sp SitePoint) {
	vw.mu.Lock()
	defer vw.mu.Unlock()
	// Replace if same PeerID
	for i, s := range vw.sites {
		if s.PeerID == sp.PeerID {
			vw.sites[i] = sp
			vw.rebuild()
			return
		}
	}
	vw.sites = append(vw.sites, sp)
	vw.rebuild()
}

// rebuild recomputes the Voronoi diagram. Must be called with lock held.
func (vw *VoronoiWrapper) rebuild() {
	if len(vw.sites) == 0 {
		vw.diagram = nil
		return
	}
	points := make([]voronoi.Vertex, len(vw.sites))
	for i, s := range vw.sites {
		points[i] = voronoi.Vertex{X: s.Position.X, Y: s.Position.Y}
	}
	bbox := voronoi.NewBBox(0, vw.worldW, 0, vw.worldH)
	vw.diagram = voronoi.ComputeDiagram(points, bbox, true)
}

// FindNearestSuperPeer returns the super-peer whose site is closest to pos.
// Uses brute-force nearest-site lookup (sufficient for small N super-peers).
func (vw *VoronoiWrapper) FindNearestSuperPeer(pos VirtualPosition) (SitePoint, error) {
	vw.mu.RLock()
	defer vw.mu.RUnlock()
	if len(vw.sites) == 0 {
		return SitePoint{}, fmt.Errorf("no super-peers registered")
	}
	best := vw.sites[0]
	bestDist := pos.Distance2D(best.Position)
	for _, s := range vw.sites[1:] {
		if d := pos.Distance2D(s.Position); d < bestDist {
			bestDist = d
			best = s
		}
	}
	return best, nil
}

// IsWithinAOI returns true if pos is closest to selfSite (i.e., within this super-peer's Voronoi cell).
func (vw *VoronoiWrapper) IsWithinAOI(selfSite SitePoint, pos VirtualPosition) bool {
	nearest, err := vw.FindNearestSuperPeer(pos)
	if err != nil {
		return true // only node — accept all
	}
	return nearest.PeerID == selfSite.PeerID
}

// GetNeighbours returns the group names of adjacent Voronoi regions.
func (vw *VoronoiWrapper) GetNeighbours(groupName string) []string {
	vw.mu.RLock()
	defer vw.mu.RUnlock()
	if vw.diagram == nil {
		return nil
	}
	// Find the site index for this group
	siteIdx := -1
	for i, s := range vw.sites {
		if s.GroupName == groupName {
			siteIdx = i
			break
		}
	}
	if siteIdx < 0 {
		return nil
	}

	// Collect edge-sharing neighbours from the diagram
	seen := make(map[string]bool)
	var neighbours []string
	for _, edge := range vw.diagram.Edges {
		if edge.LeftCell == nil || edge.RightCell == nil {
			continue
		}
		li := cellIndex(vw.diagram, edge.LeftCell)
		ri := cellIndex(vw.diagram, edge.RightCell)
		if li == siteIdx && ri >= 0 && ri < len(vw.sites) {
			n := vw.sites[ri].GroupName
			if !seen[n] {
				seen[n] = true
				neighbours = append(neighbours, n)
			}
		} else if ri == siteIdx && li >= 0 && li < len(vw.sites) {
			n := vw.sites[li].GroupName
			if !seen[n] {
				seen[n] = true
				neighbours = append(neighbours, n)
			}
		}
	}
	return neighbours
}

// FindNeighbour returns the adjacent super-peer nearest to pos (for migration).
func (vw *VoronoiWrapper) FindNeighbour(pos VirtualPosition) (SitePoint, error) {
	vw.mu.RLock()
	defer vw.mu.RUnlock()
	if len(vw.sites) < 2 {
		return SitePoint{}, fmt.Errorf("not enough super-peers for migration")
	}
	// Find the nearest super-peer excluding ourselves
	// (caller is the current SP; we want the one pos has migrated to)
	var best SitePoint
	bestDist := math.MaxFloat64
	for _, s := range vw.sites {
		d := pos.Distance2D(s.Position)
		if d < bestDist {
			bestDist = d
			best = s
		}
	}
	return best, nil
}

// AllSites returns a snapshot of all registered site points.
func (vw *VoronoiWrapper) AllSites() []SitePoint {
	vw.mu.RLock()
	defer vw.mu.RUnlock()
	cp := make([]SitePoint, len(vw.sites))
	copy(cp, vw.sites)
	return cp
}

// RemoveSite removes the site with the given PeerID.
func (vw *VoronoiWrapper) RemoveSite(peerID string) {
	vw.mu.Lock()
	defer vw.mu.Unlock()
	filtered := vw.sites[:0]
	for _, s := range vw.sites {
		if s.PeerID != peerID {
			filtered = append(filtered, s)
		}
	}
	vw.sites = filtered
	vw.rebuild()
}

// cellIndex finds the index in vw.sites that corresponds to a given Voronoi cell.
func cellIndex(d *voronoi.Diagram, cell *voronoi.Cell) int {
	for i, c := range d.Cells {
		if c == cell {
			return i
		}
	}
	return -1

}
