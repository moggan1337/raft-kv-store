package raft

import "time"

type ApplyFunc func(index uint64, cmd []byte)

type Applier struct {
	n     *Node
	apply ApplyFunc
	stop  chan struct{}
	done  chan struct{}
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
	close(a.stop)
	<-a.done
}

func (a *Applier) tick() {
	a.n.mu.Lock()
	defer a.n.mu.Unlock()
	if a.apply == nil {
		return
	}
	for a.n.lastApplied < a.n.commitIndex {
		a.n.lastApplied++
		e := a.n.log[a.n.lastApplied]
		if e.Type == EntryCommand {
			a.mu.Unlock()
			a.apply(a.n.lastApplied, e.Data)
			a.mu.Lock()
		}
	}
}
