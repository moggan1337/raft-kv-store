package raft

import (
	"sync"
	"time"
)

type ApplyFunc func(index uint64, cmd []byte)

type Applier struct {
	mu     sync.Mutex
	n      *Node
	apply  ApplyFunc
	stop   chan struct{}
	done   chan struct{}
	stopped bool
}

func NewApplier(n *Node, f ApplyFunc) *Applier {
	return &Applier{
		n:     n,
		apply: f,
		stop:  make(chan struct{}),
		done:  make(chan struct{}),
	}
}

func (a *Applier) Run() {
	defer close(a.done)
	t := time.NewTicker(10 * time.Millisecond)
	defer t.Stop()
	for {
		select {
		case <-a.stop:
			return
		case <-t.C:
			a.tick()
		}
	}
}

func (a *Applier) Stop() {
	a.mu.Lock()
	if a.stopped {
		a.mu.Unlock()
		<-a.done
		return
	}
	a.stopped = true
	close(a.stop)
	a.mu.Unlock()
	<-a.done
}

func (a *Applier) tick() {
	a.n.mu.Lock()
	if a.apply == nil {
		a.n.mu.Unlock()
		return
	}
	// Collect indices to apply while holding lock
	var toApply []LogEntry
	for a.n.lastApplied < a.n.commitIndex {
		a.n.lastApplied++
		e := a.n.log[a.n.lastApplied]
		if e.Type == EntryCommand {
			toApply = append(toApply, e)
		}
	}
	a.n.mu.Unlock()

	// Apply outside the lock to avoid holding it during user callbacks
	for _, e := range toApply {
		a.apply(e.Index, e.Data)
	}
}
