package wal

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRoundTripRecords(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(filepath.Join(dir, "wal.log"), false)
	require.NoError(t, err)

	payloads := [][]byte{[]byte("alpha"), []byte("beta"), []byte("gamma")}
	for _, p := range payloads {
		_, err := w.Append(p)
		require.NoError(t, err)
	}
	require.NoError(t, w.Sync())
	require.NoError(t, w.Close())

	r, err := Open(filepath.Join(dir, "wal.log"), true)
	require.NoError(t, err)
	defer r.Close()

	var got [][]byte
	for {
		rec, err := r.Next()
		if err != nil {
			break
		}
		got = append(got, rec)
	}
	assert.Equal(t, payloads, got)
}

func TestCorruptRecordDetected(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "wal.log")
	w, err := Open(path, false)
	require.NoError(t, err)
	_, err = w.Append([]byte("x"))
	require.NoError(t, err)
	require.NoError(t, w.Sync())
	require.NoError(t, w.Close())

	b, err := readFile(path)
	require.NoError(t, err)
	// Corrupt the first byte of the CRC. Frame layout is [crc 4][len 4][data N]
	// so byte 0 is always within the CRC for any payload.
	b[0] ^= 0xFF
	require.NoError(t, writeFile(path, b))

	r, err := Open(path, true)
	require.NoError(t, err)
	defer r.Close()
	_, err = r.Next()
	assert.Error(t, err)
}
