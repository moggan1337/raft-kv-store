package statemachine

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPutThenGet(t *testing.T) {
	kv := NewMemKV()
	r, err := kv.Apply([]byte(`{"op":"put","k":"foo","v":"bar"}`))
	require.NoError(t, err)
	assert.Equal(t, []byte("OK"), r)
	assert.Equal(t, []byte("bar"), kv.Get("foo"))
}

func TestDelete(t *testing.T) {
	kv := NewMemKV()
	_, _ = kv.Apply([]byte(`{"op":"put","k":"a","v":"1"}`))
	r, err := kv.Apply([]byte(`{"op":"delete","k":"a"}`))
	require.NoError(t, err)
	assert.Equal(t, []byte("OK"), r)
	assert.Nil(t, kv.Get("a"))
}

func TestSnapshotRoundtrip(t *testing.T) {
	kv := NewMemKV()
	_, _ = kv.Apply([]byte(`{"op":"put","k":"x","v":"y"}`))
	_, _ = kv.Apply([]byte(`{"op":"put","k":"a","v":"b"}`))
	snap, err := kv.Snapshot()
	require.NoError(t, err)

	kv2 := NewMemKV()
	require.NoError(t, kv2.Restore(snap))
	assert.Equal(t, []byte("y"), kv2.Get("x"))
	assert.Equal(t, []byte("b"), kv2.Get("a"))
}

func TestUnknownOp(t *testing.T) {
	kv := NewMemKV()
	_, err := kv.Apply([]byte(`{"op":"bogus","k":"a"}`))
	assert.Error(t, err)
}
