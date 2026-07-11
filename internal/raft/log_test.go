package raft

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestLogEntryEncode(t *testing.T) {
	e := LogEntry{Term: 5, Index: 12, Type: EntryNoop, Data: []byte("hi")}
	b := e.Encode()
	assert.NotEmpty(t, b)

	var d LogEntry
	assert.NoError(t, d.Decode(b))
	assert.Equal(t, e, d)
}

func TestLogEntryRejectsTruncated(t *testing.T) {
	var d LogEntry
	assert.Error(t, d.Decode([]byte{0x00}))
}
