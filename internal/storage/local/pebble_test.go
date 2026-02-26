package local_test

import (
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	pbm "github.com/iggydv12/nomad-go/gen/proto/models"
	"github.com/iggydv12/nomad-go/internal/storage/local"
)

func setupPebble(t *testing.T) *local.PebbleStorage {
	t.Helper()
	dir := t.TempDir()
	logger, _ := zap.NewDevelopment()
	s := local.NewPebbleStorage(dir+"/test-pebble", logger)
	require.NoError(t, s.Init())
	t.Cleanup(func() {
		s.Close()
		os.RemoveAll(dir)
	})
	return s
}

func TestPebblePutGet(t *testing.T) {
	s := setupPebble(t)

	obj := &pbm.GameObjectGrpc{
		Id:           "test-obj-1",
		CreationTime: time.Now().Unix(),
		Ttl:          time.Now().Add(time.Hour).Unix(),
		Value:        []byte("hello world"),
		LastModified: time.Now().Unix(),
	}

	ok, err := s.Put(obj)
	require.NoError(t, err)
	assert.True(t, ok)

	got, err := s.Get(obj.Id)
	require.NoError(t, err)
	assert.Equal(t, obj.Id, got.Id)
	assert.Equal(t, obj.Value, got.Value)
}

func TestPebbleDuplicatePut(t *testing.T) {
	s := setupPebble(t)
	obj := &pbm.GameObjectGrpc{Id: "dup", Ttl: time.Now().Add(time.Hour).Unix(), Value: []byte("x")}

	ok1, _ := s.Put(obj)
	ok2, _ := s.Put(obj) // should be rejected
	assert.True(t, ok1)
	assert.False(t, ok2)
}

func TestPebbleUpdate(t *testing.T) {
	s := setupPebble(t)
	obj := &pbm.GameObjectGrpc{Id: "upd", Ttl: time.Now().Add(time.Hour).Unix(), Value: []byte("v1")}
	s.Put(obj)

	obj.Value = []byte("v2")
	ok, err := s.Update(obj)
	require.NoError(t, err)
	assert.True(t, ok)

	got, err := s.Get("upd")
	require.NoError(t, err)
	assert.Equal(t, []byte("v2"), got.Value)
}

func TestPebbleDelete(t *testing.T) {
	s := setupPebble(t)
	obj := &pbm.GameObjectGrpc{Id: "del", Ttl: time.Now().Add(time.Hour).Unix(), Value: []byte("bye")}
	s.Put(obj)

	ok, err := s.Delete("del")
	require.NoError(t, err)
	assert.True(t, ok)

	_, err = s.Get("del")
	assert.Error(t, err)
}

func TestPebbleTTLExpiry(t *testing.T) {
	s := setupPebble(t)
	// Expired
	expired := &pbm.GameObjectGrpc{Id: "expired", Ttl: time.Now().Add(-time.Second).Unix(), Value: []byte("x")}
	s.Put(expired)

	// Valid
	valid := &pbm.GameObjectGrpc{Id: "valid", Ttl: time.Now().Add(time.Hour).Unix(), Value: []byte("y")}
	s.Put(valid)

	count, err := s.CleanExpired()
	require.NoError(t, err)
	assert.Equal(t, 1, count)

	_, err = s.Get("expired")
	assert.Error(t, err)
	_, err = s.Get("valid")
	assert.NoError(t, err)
}
