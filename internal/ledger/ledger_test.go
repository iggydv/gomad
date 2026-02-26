package ledger_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/iggydv12/nomad-go/internal/ledger"
)

func TestAddAndRetrieve(t *testing.T) {
	gl := ledger.NewGroupLedger()

	// Add object reference
	ok := gl.AddToObjectLedger("obj1", ledger.MetaData{ID: "peer1", TTL: time.Now().Add(10 * time.Minute).Unix()})
	require.True(t, ok)

	// Add peer reference
	ok = gl.AddToPeerLedger("peer1", ledger.MetaData{ID: "obj1", TTL: time.Now().Add(10 * time.Minute).Unix()})
	require.True(t, ok)

	assert.True(t, gl.ObjectLedgerContainsKey("obj1"))
	assert.True(t, gl.PeerLedgerContainsKey("peer1"))
	assert.True(t, gl.ThisPeerContainsObject("peer1", "obj1"))
	assert.Equal(t, 1, gl.CountReplicas("obj1"))
}

func TestDuplicatePrevented(t *testing.T) {
	gl := ledger.NewGroupLedger()
	md := ledger.MetaData{ID: "peer1", TTL: time.Now().Add(10 * time.Minute).Unix()}

	ok1 := gl.AddToObjectLedger("obj1", md)
	ok2 := gl.AddToObjectLedger("obj1", md) // duplicate
	assert.True(t, ok1)
	assert.False(t, ok2)
	assert.Equal(t, 1, gl.CountReplicas("obj1"))
}

func TestRemovePeer(t *testing.T) {
	gl := ledger.NewGroupLedger()
	gl.AddToObjectLedger("obj1", ledger.MetaData{ID: "peer1", TTL: time.Now().Add(time.Hour).Unix()})
	gl.AddToPeerLedger("peer1", ledger.MetaData{ID: "obj1", TTL: time.Now().Add(time.Hour).Unix()})

	gl.RemovePeerFromGroupLedger("peer1")
	assert.False(t, gl.PeerLedgerContainsKey("peer1"))
	assert.Equal(t, 0, gl.CountReplicas("obj1"))
}

func TestCleanExpired(t *testing.T) {
	gl := ledger.NewGroupLedger()
	// Already expired
	gl.AddToObjectLedger("obj-expired", ledger.MetaData{ID: "peer1", TTL: time.Now().Add(-time.Second).Unix()})
	// Still valid
	gl.AddToObjectLedger("obj-valid", ledger.MetaData{ID: "peer1", TTL: time.Now().Add(time.Hour).Unix()})

	removed := gl.CleanExpired()
	assert.True(t, removed)
	assert.False(t, gl.ObjectLedgerContainsKey("obj-expired"))
	assert.True(t, gl.ObjectLedgerContainsKey("obj-valid"))
}

func TestGetObjectsNeedingRepair(t *testing.T) {
	gl := ledger.NewGroupLedger()
	// obj1 has 2 replicas, need 3
	gl.AddToObjectLedger("obj1", ledger.MetaData{ID: "peer1", TTL: time.Now().Add(time.Hour).Unix()})
	gl.AddToObjectLedger("obj1", ledger.MetaData{ID: "peer2", TTL: time.Now().Add(time.Hour).Unix()})

	needs := gl.GetObjectsNeedingRepair(3)
	assert.Contains(t, needs, "obj1")
	assert.Equal(t, 1, needs["obj1"]) // need 1 more replica
}

func TestRemovePeersStoringObject(t *testing.T) {
	gl := ledger.NewGroupLedger()
	gl.AddToPeerLedger("peer1", ledger.MetaData{ID: "obj1", TTL: time.Now().Add(time.Hour).Unix()})

	// peer1 stores obj1, peer2 does not
	notStoring := gl.RemovePeersStoringObject([]string{"peer1", "peer2"}, "obj1")
	assert.Equal(t, []string{"peer2"}, notStoring)
}

func TestPopulateAndSnapshot(t *testing.T) {
	gl := ledger.NewGroupLedger()
	objMap := map[string][]ledger.MetaData{
		"obj1": {{ID: "peer1", TTL: time.Now().Add(time.Hour).Unix()}},
	}
	peerMap := map[string][]ledger.MetaData{
		"peer1": {{ID: "obj1", TTL: time.Now().Add(time.Hour).Unix()}},
	}
	gl.Populate(objMap, peerMap)

	snap := gl.GetObjectLedgerSnapshot()
	assert.Contains(t, snap, "obj1")
}
