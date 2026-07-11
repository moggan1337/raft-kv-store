package raft

import (
	"encoding/binary"
	"fmt"
)

type EntryType uint8

const (
	EntryCommand EntryType = 1
	EntryNoop    EntryType = 2
	EntryConfig  EntryType = 3
)

type LogEntry struct {
	Term  uint64
	Index uint64
	Type  EntryType
	Data  []byte
}

func (e LogEntry) Encode() []byte {
	// [term 8][index 8][type 1][len 4][data]
	buf := make([]byte, 8+8+1+4+len(e.Data))
	binary.BigEndian.PutUint64(buf[0:8], e.Term)
	binary.BigEndian.PutUint64(buf[8:16], e.Index)
	buf[16] = byte(e.Type)
	binary.BigEndian.PutUint32(buf[17:21], uint32(len(e.Data)))
	copy(buf[21:], e.Data)
	return buf
}

func (e *LogEntry) Decode(b []byte) error {
	if len(b) < 21 {
		return fmt.Errorf("entry too short: %d", len(b))
	}
	e.Term = binary.BigEndian.Uint64(b[0:8])
	e.Index = binary.BigEndian.Uint64(b[8:16])
	e.Type = EntryType(b[16])
	n := binary.BigEndian.Uint32(b[17:21])
	if uint32(len(b)-21) < n {
		return fmt.Errorf("data truncated: have %d want %d", len(b)-21, n)
	}
	e.Data = make([]byte, n)
	copy(e.Data, b[21:21+n])
	return nil
}
