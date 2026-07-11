package wal

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestReplay(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "wal.log")
	w, err := Open(path, false)
	require.NoError(t, err)
	for i := 0; i < 5; i++ {
		_, err := w.Append([]byte{byte(i)})
		require.NoError(t, err)
	}
	require.NoError(t, w.Sync())
	require.NoError(t, w.Close())

	records, lastOff, err := Replay(path)
	require.NoError(t, err)
	assert.Len(t, records, 5)
	assert.Greater(t, lastOff, uint64(0))
	assert.Equal(t, []byte{0}, records[0])
}
