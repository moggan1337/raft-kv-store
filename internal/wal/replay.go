package wal

import (
	"os"
)

func Replay(path string) ([][]byte, uint64, error) {
	if _, err := os.Stat(path); err != nil {
		if os.IsNotExist(err) {
			return nil, 0, nil
		}
		return nil, 0, err
	}
	r, err := Open(path, true)
	if err != nil {
		return nil, 0, err
	}
	defer r.Close()

	var out [][]byte
	var lastOff uint64
	for {
		rec, err := r.Next()
		if err != nil {
			break
		}
		out = append(out, rec)
		lastOff += 8 + uint64(len(rec))
	}
	return out, lastOff, nil
}
