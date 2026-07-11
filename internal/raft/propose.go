package raft

import (
	"context"
	"errors"
	"fmt"
)

var ErrNotLeader = errors.New("not leader")

// Propose attempts to append a new command to the log.
// On the leader, this appends locally and is replicated to peers via
// the leader's AppendEntries loop. On a non-leader, it returns ErrNotLeader
// (in production, a real client would redirect to the leader).
// ProposeNotify is the awaitable variant that waits for commit.
func (n *Node) Propose(ctx context.Context, cmd []byte) error {
	n.mu.Lock()
	if n.role != RoleLeader {
		n.mu.Unlock()
		return ErrNotLeader
	}
	e := LogEntry{
		Term:  n.term,
		Index: uint64(len(n.log)),
		Type:  EntryCommand,
		Data:  cmd,
	}
	n.log = append(n.log, e)
	n.mu.Unlock()
	// Signal replication loop
	select {
	case n.proposeCh <- struct{}{}:
	default:
	}
	return nil
}

// ProposeNotify appends the command and blocks until it is committed (or
// context is cancelled, or timeout elapses).
func (n *Node) ProposeNotify(ctx context.Context, cmd []byte) (uint64, error) {
	n.mu.Lock()
	if n.role != RoleLeader {
		n.mu.Unlock()
		return 0, ErrNotLeader
	}
	e := LogEntry{
		Term:  n.term,
		Index: uint64(len(n.log)),
		Type:  EntryCommand,
		Data:  cmd,
	}
	n.log = append(n.log, e)
	n.mu.Unlock()
	select {
	case n.proposeCh <- struct{}{}:
	default:
	}

	// Wait for commit
	deadline := time.NewTimer(2 * time.Second)
	defer deadline.Stop()
	for {
		n.mu.Lock()
		if n.lastApplied >= e.Index {
			n.mu.Unlock()
			return e.Index, nil
		}
		n.mu.Unlock()
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		case <-deadline.C:
			return 0, fmt.Errorf("propose timeout")
		case <-time.After(10 * time.Millisecond):
		}
	}
}
