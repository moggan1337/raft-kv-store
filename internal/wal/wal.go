package wal

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"sync"
)

type WAL struct {
	mu       sync.Mutex
	path     string
	f        *os.File
	bw       *bufio.Writer
	readOnly bool
}

func Open(path string, readOnly bool) (*WAL, error) {
	flag := os.O_CREATE | os.O_RDWR
	if readOnly {
		flag = os.O_RDONLY
	}
	f, err := os.OpenFile(path, flag, 0o644)
	if err != nil {
		return nil, err
	}
	w := &WAL{path: path, f: f, readOnly: readOnly}
	if !readOnly {
		w.bw = bufio.NewWriter(f)
	}
	return w, nil
}

func (w *WAL) Append(data []byte) (uint64, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.readOnly {
		return 0, fmt.Errorf("wal: read-only")
	}
	crc := crc32.ChecksumIEEE(data)
	off, _ := w.f.Seek(0, io.SeekEnd)
	hdr := make([]byte, 8)
	binary.BigEndian.PutUint32(hdr[0:4], crc)
	binary.BigEndian.PutUint32(hdr[4:8], uint32(len(data)))
	if _, err := w.bw.Write(hdr); err != nil {
		return 0, err
	}
	if _, err := w.bw.Write(data); err != nil {
		return 0, err
	}
	return uint64(off), nil
}

func (w *WAL) Sync() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.bw.Flush(); err != nil {
		return err
	}
	return w.f.Sync()
}

func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.bw != nil {
		if err := w.bw.Flush(); err != nil {
			return err
		}
	}
	return w.f.Close()
}

func (w *WAL) Next() ([]byte, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	hdr := make([]byte, 8)
	if _, err := io.ReadFull(w.f, hdr); err != nil {
		return nil, err
	}
	crc := binary.BigEndian.Uint32(hdr[0:4])
	n := binary.BigEndian.Uint32(hdr[4:8])
	data := make([]byte, n)
	if _, err := io.ReadFull(w.f, data); err != nil {
		return nil, err
	}
	if crc32.ChecksumIEEE(data) != crc {
		return nil, fmt.Errorf("wal: crc mismatch")
	}
	return data, nil
}

func readFile(p string) ([]byte, error)  { return os.ReadFile(p) }
func writeFile(p string, b []byte) error  { return os.WriteFile(p, b, 0o644) }
