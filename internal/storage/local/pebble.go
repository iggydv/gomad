// Package local provides the Pebble-backed implementation of LocalStorage.
package local

import (
	"errors"
	"fmt"
	"time"

	"github.com/cockroachdb/pebble"
	models "github.com/iggydv12/gomad/gen/proto/models"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

// PebbleStorage is a Pebble LSM-tree backed LocalStorage.
// Replaces both H2 and RocksDB backends from the Java version.
type PebbleStorage struct {
	db     *pebble.DB
	path   string
	logger *zap.Logger
}

// NewPebbleStorage creates a PebbleStorage instance (not yet opened).
func NewPebbleStorage(dbPath string, logger *zap.Logger) *PebbleStorage {
	return &PebbleStorage{
		path:   dbPath,
		logger: logger,
	}
}

// Init opens the Pebble database.
func (p *PebbleStorage) Init() error {
	opts := &pebble.Options{
		Logger: &pebbleLogger{p.logger},
	}
	db, err := pebble.Open(p.path, opts)
	if err != nil {
		return fmt.Errorf("pebble open %s: %w", p.path, err)
	}
	p.db = db
	p.logger.Info("Pebble storage opened", zap.String("path", p.path))
	return nil
}

// Close flushes and closes the database.
func (p *PebbleStorage) Close() error {
	if p.db != nil {
		return p.db.Close()
	}
	return nil
}

// Put stores an object. Returns (false, nil) if the key already exists.
func (p *PebbleStorage) Put(obj *models.GameObjectGrpc) (bool, error) {
	key := []byte(obj.Id)
	// Check for existing key
	_, closer, err := p.db.Get(key)
	if err == nil {
		closer.Close()
		return false, nil // already exists
	}
	if err != pebble.ErrNotFound {
		return false, fmt.Errorf("pebble get check: %w", err)
	}

	data, err := proto.Marshal(obj)
	if err != nil {
		return false, fmt.Errorf("marshal: %w", err)
	}
	if err := p.db.Set(key, data, pebble.Sync); err != nil {
		return false, fmt.Errorf("pebble set: %w", err)
	}
	return true, nil
}

// Get retrieves an object by ID.
func (p *PebbleStorage) Get(id string) (*models.GameObjectGrpc, error) {
	data, closer, err := p.db.Get([]byte(id))
	if errors.Is(err, pebble.ErrNotFound) {
		return nil, fmt.Errorf("not found: %s", id)
	}
	if err != nil {
		return nil, fmt.Errorf("pebble get: %w", err)
	}
	defer closer.Close()

	obj := &models.GameObjectGrpc{}
	if err := proto.Unmarshal(data, obj); err != nil {
		return nil, fmt.Errorf("unmarshal: %w", err)
	}

	// Check TTL expiry
	if obj.Ttl > 0 && obj.Ttl < time.Now().Unix() {
		return nil, fmt.Errorf("not found: %s (expired)", id)
	}

	return obj, nil
}

// Update overwrites an existing object.
func (p *PebbleStorage) Update(obj *models.GameObjectGrpc) (bool, error) {
	data, err := proto.Marshal(obj)
	if err != nil {
		return false, fmt.Errorf("marshal: %w", err)
	}
	if err := p.db.Set([]byte(obj.Id), data, pebble.Sync); err != nil {
		return false, fmt.Errorf("pebble set: %w", err)
	}
	return true, nil
}

// Delete removes an object by ID.
func (p *PebbleStorage) Delete(id string) (bool, error) {
	if err := p.db.Delete([]byte(id), pebble.Sync); err != nil {
		return false, fmt.Errorf("pebble delete: %w", err)
	}
	return true, nil
}

// Truncate deletes all stored objects by range-deleting all keys.
func (p *PebbleStorage) Truncate() error {
	// Scan all keys and delete them
	iter, err := p.db.NewIter(nil)
	if err != nil {
		return fmt.Errorf("pebble iter: %w", err)
	}
	defer iter.Close()

	var keys [][]byte
	for iter.First(); iter.Valid(); iter.Next() {
		k := make([]byte, len(iter.Key()))
		copy(k, iter.Key())
		keys = append(keys, k)
	}
	if err := iter.Error(); err != nil {
		return err
	}

	batch := p.db.NewBatch()
	defer batch.Close()
	for _, k := range keys {
		if err := batch.Delete(k, nil); err != nil {
			return err
		}
	}
	return batch.Commit(pebble.Sync)
}

// CleanExpired removes all entries whose TTL has passed.
// Returns the count of removed entries.
func (p *PebbleStorage) CleanExpired() (int, error) {
	now := time.Now().Unix()
	iter, err := p.db.NewIter(nil)
	if err != nil {
		return 0, fmt.Errorf("pebble iter: %w", err)
	}
	defer iter.Close()

	var expired [][]byte
	for iter.First(); iter.Valid(); iter.Next() {
		data := iter.Value()
		obj := &models.GameObjectGrpc{}
		if err := proto.Unmarshal(data, obj); err != nil {
			continue
		}
		if obj.Ttl > 0 && obj.Ttl < now {
			k := make([]byte, len(iter.Key()))
			copy(k, iter.Key())
			expired = append(expired, k)
		}
	}
	if err := iter.Error(); err != nil {
		return 0, err
	}

	if len(expired) == 0 {
		return 0, nil
	}

	batch := p.db.NewBatch()
	defer batch.Close()
	for _, k := range expired {
		if err := batch.Delete(k, nil); err != nil {
			return 0, err
		}
	}
	if err := batch.Commit(pebble.Sync); err != nil {
		return 0, err
	}

	p.logger.Info("Cleaned expired objects", zap.Int("count", len(expired)))
	return len(expired), nil
}

// pebbleLogger adapts zap.Logger to the pebble.Logger interface.
type pebbleLogger struct {
	z *zap.Logger
}

func (l *pebbleLogger) Infof(format string, args ...any) {
	l.z.Sugar().Infof(format, args...)
}

func (l *pebbleLogger) Fatalf(format string, args ...any) {
	l.z.Sugar().Fatalf(format, args...)
}
