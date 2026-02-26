package spatial_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/iggydv12/nomad-go/internal/spatial"
)

func TestFindNearestSuperPeer(t *testing.T) {
	vw := spatial.NewVoronoiWrapper(10.0, 10.0)

	sp1 := spatial.SitePoint{PeerID: "sp1", GroupName: "group-1", Position: spatial.VirtualPosition{X: 2, Y: 2}}
	sp2 := spatial.SitePoint{PeerID: "sp2", GroupName: "group-2", Position: spatial.VirtualPosition{X: 8, Y: 8}}
	vw.UpdateSitePoints([]spatial.SitePoint{sp1, sp2})

	// A position near sp1
	nearest, err := vw.FindNearestSuperPeer(spatial.VirtualPosition{X: 1, Y: 1})
	require.NoError(t, err)
	assert.Equal(t, "sp1", nearest.PeerID)

	// A position near sp2
	nearest, err = vw.FindNearestSuperPeer(spatial.VirtualPosition{X: 9, Y: 9})
	require.NoError(t, err)
	assert.Equal(t, "sp2", nearest.PeerID)
}

func TestIsWithinAOI(t *testing.T) {
	vw := spatial.NewVoronoiWrapper(10.0, 10.0)

	sp1 := spatial.SitePoint{PeerID: "sp1", GroupName: "g1", Position: spatial.VirtualPosition{X: 2, Y: 2}}
	sp2 := spatial.SitePoint{PeerID: "sp2", GroupName: "g2", Position: spatial.VirtualPosition{X: 8, Y: 8}}
	vw.UpdateSitePoints([]spatial.SitePoint{sp1, sp2})

	// Position close to sp1 should be within sp1's AOI
	assert.True(t, vw.IsWithinAOI(sp1, spatial.VirtualPosition{X: 1, Y: 1}))
	assert.False(t, vw.IsWithinAOI(sp1, spatial.VirtualPosition{X: 9, Y: 9}))
}

func TestNewRandomPosition(t *testing.T) {
	for i := 0; i < 100; i++ {
		pos := spatial.NewRandomPosition(10.0, 10.0)
		assert.GreaterOrEqual(t, pos.X, 0.0)
		assert.Less(t, pos.X, 10.0)
		assert.GreaterOrEqual(t, pos.Y, 0.0)
		assert.Less(t, pos.Y, 10.0)
	}
}

func TestDistance2D(t *testing.T) {
	p1 := spatial.VirtualPosition{X: 0, Y: 0}
	p2 := spatial.VirtualPosition{X: 3, Y: 4}
	assert.InDelta(t, 5.0, p1.Distance2D(p2), 0.0001)
}

func TestRemoveSite(t *testing.T) {
	vw := spatial.NewVoronoiWrapper(10.0, 10.0)
	sp1 := spatial.SitePoint{PeerID: "sp1", Position: spatial.VirtualPosition{X: 2, Y: 2}}
	sp2 := spatial.SitePoint{PeerID: "sp2", Position: spatial.VirtualPosition{X: 8, Y: 8}}
	vw.UpdateSitePoints([]spatial.SitePoint{sp1, sp2})

	vw.RemoveSite("sp1")
	sites := vw.AllSites()
	assert.Len(t, sites, 1)
	assert.Equal(t, "sp2", sites[0].PeerID)
}

func TestNoSuperPeersError(t *testing.T) {
	vw := spatial.NewVoronoiWrapper(10.0, 10.0)
	_, err := vw.FindNearestSuperPeer(spatial.VirtualPosition{X: 5, Y: 5})
	assert.Error(t, err)
}
