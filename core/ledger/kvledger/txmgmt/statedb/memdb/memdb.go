// Package memdb contains an in-memory implementation of statedb.VersionedDB used by
// tests that need a fully in-memory ledger without any filesystem dependencies.
package memdb

import (
	"sort"
	"sync"

	"github.com/hyperledger/fabric/core/ledger/internal/version"
	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/statedb"
)

// NewProvider creates a VersionedDBProvider whose databases are kept entirely in memory.
// It is intended for tests that want a ledger with no files on disk and no external
// services (e.g. CouchDB).
func NewProvider() *Provider {
	return &Provider{dbs: map[string]*DB{}}
}

// Provider is an in-memory implementation of statedb.VersionedDBProvider.
type Provider struct {
	mu  sync.Mutex
	dbs map[string]*DB
}

// GetDBHandle returns the in-memory database for the given id, creating it on first use.
func (p *Provider) GetDBHandle(id string, _ statedb.NamespaceProvider) (statedb.VersionedDB, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if db, ok := p.dbs[id]; ok {
		return db, nil
	}
	db := &DB{
		data: map[string]map[string]*statedb.VersionedValue{},
	}
	p.dbs[id] = db
	return db, nil
}

// ImportFromSnapshot loads the given key-values into the in-memory database and sets
// the savepoint.
func (p *Provider) ImportFromSnapshot(id string, savepoint *version.Height, itr statedb.FullScanIterator) error {
	dbHandle, err := p.GetDBHandle(id, nil)
	if err != nil {
		return err
	}
	for {
		kv, err := itr.Next()
		if err != nil {
			return err
		}
		if kv == nil {
			break
		}
		batch := statedb.NewUpdateBatch()
		batch.Put(kv.Namespace, kv.Key, kv.Value, kv.Version)
		if err := dbHandle.ApplyUpdates(batch, savepoint); err != nil {
			return err
		}
	}
	itr.Close()
	return nil
}

// BytesKeySupported returns true as the in-memory database accepts arbitrary key bytes.
func (p *Provider) BytesKeySupported() bool {
	return true
}

// Close releases all in-memory databases.
func (p *Provider) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.dbs = map[string]*DB{}
}

// Drop removes the in-memory database for the given id.
func (p *Provider) Drop(id string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	delete(p.dbs, id)
	return nil
}

// DB is an in-memory implementation of statedb.VersionedDB.
type DB struct {
	mu        sync.RWMutex
	data      map[string]map[string]*statedb.VersionedValue
	savepoint *version.Height
}

// GetState returns the value stored for the given namespace and key.
func (d *DB) GetState(namespace string, key string) (*statedb.VersionedValue, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	if v, ok := d.data[namespace][key]; ok {
		return v, nil
	}
	return nil, nil
}

// GetVersion returns the version of the given namespace and key.
func (d *DB) GetVersion(namespace string, key string) (*version.Height, error) {
	v, err := d.GetState(namespace, key)
	if err != nil || v == nil {
		return nil, err
	}
	return v.Version, nil
}

// GetStateMultipleKeys returns the values for multiple keys.
func (d *DB) GetStateMultipleKeys(namespace string, keys []string) ([]*statedb.VersionedValue, error) {
	values := make([]*statedb.VersionedValue, len(keys))
	for i, key := range keys {
		v, err := d.GetState(namespace, key)
		if err != nil {
			return nil, err
		}
		values[i] = v
	}
	return values, nil
}

// GetStateRangeScanIterator returns an iterator over the keys in [startKey, endKey).
func (d *DB) GetStateRangeScanIterator(namespace string, startKey string, endKey string) (statedb.ResultsIterator, error) {
	return d.newIterator(namespace, startKey, endKey, 0), nil
}

// GetStateRangeScanIteratorWithPagination is the paginated variant of GetStateRangeScanIterator.
func (d *DB) GetStateRangeScanIteratorWithPagination(namespace string, startKey string, endKey string, pageSize int32) (statedb.QueryResultsIterator, error) {
	return d.newIterator(namespace, startKey, endKey, pageSize), nil
}

// ExecuteQuery is not supported by the in-memory database.
func (d *DB) ExecuteQuery(namespace, query string) (statedb.ResultsIterator, error) {
	return nil, nil
}

// ExecuteQueryWithPagination is not supported by the in-memory database.
func (d *DB) ExecuteQueryWithPagination(namespace, query, bookmark string, pageSize int32) (statedb.QueryResultsIterator, error) {
	return nil, nil
}

// ApplyUpdates applies all the updates in the batch and records the given savepoint.
// Deletes (entries with a nil value) remove the key from the in-memory database.
func (d *DB) ApplyUpdates(batch *statedb.UpdateBatch, height *version.Height) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	for ns, nsUpdates := range batch.Updates {
		if _, ok := d.data[ns]; !ok {
			d.data[ns] = map[string]*statedb.VersionedValue{}
		}
		for key, vv := range nsUpdates.M {
			if vv.Value == nil {
				delete(d.data[ns], key)
				continue
			}
			d.data[ns][key] = vv
		}
	}
	if height != nil {
		d.savepoint = height
	}
	return nil
}

// GetLatestSavePoint returns the height up to which the in-memory database is consistent.
func (d *DB) GetLatestSavePoint() (*version.Height, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.savepoint, nil
}

// ValidateKeyValue accepts every key and value in the in-memory database.
func (d *DB) ValidateKeyValue(key string, value []byte) error {
	return nil
}

// BytesKeySupported returns true.
func (d *DB) BytesKeySupported() bool {
	return true
}

// GetFullScanIterator returns an iterator over all the committed key-values in the
// lexical order of <Namespace, Key>.
func (d *DB) GetFullScanIterator(skipNamespace func(string) bool) (statedb.FullScanIterator, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	namespaces := make([]string, 0, len(d.data))
	for ns := range d.data {
		if skipNamespace != nil && skipNamespace(ns) {
			continue
		}
		namespaces = append(namespaces, ns)
	}
	sort.Strings(namespaces)
	refs := make([]kvRef, 0)
	for _, ns := range namespaces {
		for _, key := range d.sortedKeysLocked(ns) {
			refs = append(refs, kvRef{ns: ns, key: key})
		}
	}
	return &fullScanIterator{db: d, refs: refs}, nil
}

// Open is a no-op for the in-memory database.
func (d *DB) Open() error { return nil }

// Close is a no-op for the in-memory database.
func (d *DB) Close() {}

// sortedKeysLocked returns the keys of a namespace in lexical order. The caller is
// expected to hold at least a read lock.
func (d *DB) sortedKeysLocked(namespace string) []string {
	keys := make([]string, 0, len(d.data[namespace]))
	for k := range d.data[namespace] {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func (d *DB) newIterator(namespace, startKey, endKey string, pageSize int32) *resultsIterator {
	d.mu.RLock()
	defer d.mu.RUnlock()
	keys := make([]string, 0)
	for _, k := range d.sortedKeysLocked(namespace) {
		if startKey != "" && k < startKey {
			continue
		}
		if endKey != "" && k >= endKey {
			continue
		}
		keys = append(keys, k)
	}
	if pageSize > 0 && int(pageSize) < len(keys) {
		keys = keys[:pageSize]
	}
	return &resultsIterator{
		db:        d,
		namespace: namespace,
		keys:      keys,
	}
}

type kvRef struct {
	ns  string
	key string
}

type resultsIterator struct {
	db        *DB
	namespace string
	keys      []string
	nextIndex int
}

func (itr *resultsIterator) Next() (*statedb.VersionedKV, error) {
	if itr.nextIndex >= len(itr.keys) {
		return nil, nil
	}
	key := itr.keys[itr.nextIndex]
	itr.nextIndex++
	v, err := itr.db.GetState(itr.namespace, key)
	if err != nil {
		return nil, err
	}
	return &statedb.VersionedKV{
		CompositeKey:   &statedb.CompositeKey{Namespace: itr.namespace, Key: key},
		VersionedValue: v,
	}, nil
}

func (itr *resultsIterator) Close() {}

// GetBookmarkAndClose returns an empty bookmark as pagination bookmarks are not supported.
func (itr *resultsIterator) GetBookmarkAndClose() string {
	return ""
}

type fullScanIterator struct {
	db        *DB
	refs      []kvRef
	nextIndex int
}

func (itr *fullScanIterator) Next() (*statedb.VersionedKV, error) {
	if itr.nextIndex >= len(itr.refs) {
		return nil, nil
	}
	ref := itr.refs[itr.nextIndex]
	itr.nextIndex++
	v, err := itr.db.GetState(ref.ns, ref.key)
	if err != nil {
		return nil, err
	}
	return &statedb.VersionedKV{
		CompositeKey:   &statedb.CompositeKey{Namespace: ref.ns, Key: ref.key},
		VersionedValue: v,
	}, nil
}

func (itr *fullScanIterator) Close() {}
