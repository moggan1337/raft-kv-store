package raft

import (
	"encoding/json"
	"os"
	"path/filepath"
)

// persistedState bundles the volatile Node state we want to recover after
// a restart. We persist the entire log + the term/votedFor so a Node can
// resume from the same point.
type persistedState struct {
	CurrentTerm uint64     `json:"current_term"`
	VotedFor    int        `json:"voted_for"`
	Log         []LogEntry `json:"log"`
}

// Persist writes the Node's current state to a JSON file at `path`.
// Atomic via temp-file + rename so a crash mid-write doesn't corrupt
// the existing state file.
func (n *Node) Persist(path string) error {
	n.mu.Lock()
	ps := persistedState{
		CurrentTerm: n.term,
		VotedFor:    n.votedFor,
		Log:         append([]LogEntry(nil), n.log...),
	}
	n.mu.Unlock()

	data, err := json.Marshal(ps)
	if err != nil {
		return err
	}
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

// Recover reads state from `path` and restores Node's persistent fields.
// Returns nil if the file doesn't exist (fresh start).
func (n *Node) Recover(path string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	var ps persistedState
	if err := json.Unmarshal(data, &ps); err != nil {
		return err
	}
	n.mu.Lock()
	n.term = ps.CurrentTerm
	n.votedFor = ps.VotedFor
	n.log = ps.Log
	if len(n.log) == 0 {
		n.log = []LogEntry{{Term: 0, Index: 0, Type: EntryNoop}}
	}
	// On recovery, treat all log entries as committed so the applier
	// replays them. For a single-node cluster this is always safe
	// (entries were durably Persisted after the leader committed them).
	// For multi-node, a smarter approach would persist the commit
	// index separately and use it to bound replay.
	n.commitIndex = uint64(len(n.log) - 1)
	n.lastApplied = 0
	n.mu.Unlock()
	return nil
}
