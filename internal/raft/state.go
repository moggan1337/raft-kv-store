package raft

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
)

type Role uint8

const (
	RoleFollower Role = iota
	RoleCandidate
	RoleLeader
)

func (r Role) String() string {
	switch r {
	case RoleFollower:
		return "follower"
	case RoleCandidate:
		return "candidate"
	case RoleLeader:
		return "leader"
	}
	return "unknown"
}

type PersistentState struct {
	CurrentTerm uint64
	VotedFor    int
	Log         []LogEntry
}

func (p PersistentState) Encode() ([]byte, error) {
	var buf bytes.Buffer
	if err := binary.Write(&buf, binary.BigEndian, p.CurrentTerm); err != nil {
		return nil, err
	}
	if err := binary.Write(&buf, binary.BigEndian, int64(p.VotedFor)); err != nil {
		return nil, err
	}
	if err := gob.NewEncoder(&buf).Encode(p.Log); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (p *PersistentState) Decode(b []byte) error {
	buf := bytes.NewReader(b)
	if err := binary.Read(buf, binary.BigEndian, &p.CurrentTerm); err != nil {
		return err
	}
	var vf int64
	if err := binary.Read(buf, binary.BigEndian, &vf); err != nil {
		return err
	}
	p.VotedFor = int(vf)
	return gob.NewDecoder(buf).Decode(&p.Log)
}

type VolatileState struct {
	CommitIndex uint64
	LastApplied uint64
}
