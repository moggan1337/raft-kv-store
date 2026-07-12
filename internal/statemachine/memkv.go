package statemachine

import (
	"encoding/json"
	"fmt"
	"sync"
)

// Op is the JSON-encoded operation applied to the KV store via the Raft log.
type Op struct {
	Op string `json:"op"` // "put" | "delete"
	K  string `json:"k"`
	V  string `json:"v,omitempty"`
}

// MemKV is a thread-safe in-memory key-value store used as the Raft
// state machine. Operations are submitted as JSON-encoded bytes
// (e.g. `{"op":"put","k":"foo","v":"bar"}`).
type MemKV struct {
	mu sync.RWMutex
	m  map[string][]byte
}

func NewMemKV() *MemKV { return &MemKV{m: make(map[string][]byte)} }

// Apply executes a JSON-encoded op against the store. Returns an
// error if the op is unknown or the JSON is malformed.
func (k *MemKV) Apply(cmd []byte) (result []byte, err error) {
	var o Op
	if err := json.Unmarshal(cmd, &o); err != nil {
		return nil, fmt.Errorf("decode op: %w", err)
	}
	switch o.Op {
	case "put":
		k.mu.Lock()
		k.m[o.K] = []byte(o.V)
		k.mu.Unlock()
		return []byte("OK"), nil
	case "delete":
		k.mu.Lock()
		delete(k.m, o.K)
		k.mu.Unlock()
		return []byte("OK"), nil
	}
	return nil, fmt.Errorf("unknown op: %s", o.Op)
}

// Get returns the value for k, or nil if absent.
func (k *MemKV) Get(key string) []byte {
	k.mu.RLock()
	defer k.mu.RUnlock()
	return k.m[key]
}

// Snapshot returns a serialised copy of the store.
func (k *MemKV) Snapshot() ([]byte, error) {
	k.mu.RLock()
	defer k.mu.RUnlock()
	return json.Marshal(k.m)
}

// Restore replaces the entire store from a snapshot.
func (k *MemKV) Restore(b []byte) error {
	k.mu.Lock()
	defer k.mu.Unlock()
	k.m = make(map[string][]byte)
	return json.Unmarshal(b, &k.m)
}
