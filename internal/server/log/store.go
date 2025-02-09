package log

import (
	"bufio"
	"encoding/binary"
	"os"
	"sync"
)

var (
	enc = binary.BigEndian
)

const (
	lenWidth = 8
)

// Store—the file we store records in
type store struct {
	*os.File               // Embedded file for persistent storage
	mu       sync.Mutex    // For thread-safe operations
	buf      *bufio.Writer // Buffered writer for performance
	size     uint64        // Tracks total size of the store
}

// Wraper around a file - with file size
func newStore(f *os.File) (*store, error) {
	fi, err := os.Stat(f.Name())
	if err != nil {
		return nil, err
	}
	size := uint64(fi.Size())
	return &store{
		File: f,
		size: size,
		buf:  bufio.NewWriter(f),
	}, nil
}

// Persists the given bytes to the store
func (s *store) Append(p []byte) (n uint64, pos uint64, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	pos = s.size
	//First write the length of the record, so when we read we kno how many bytes to read
	if err := binary.Write(s.buf, enc, uint64(len(p))); err != nil {
		return 0, 0, err
	}
	//write actual record data
	w, err := s.buf.Write(p)
	if err != nil {
		return 0, 0, err
	}
	//calc total bytes written
	w += lenWidth
	s.size += uint64(w)
	return uint64(w), pos, nil
}

// Read returns the record stored at the given position
func (s *store) Read(pos uint64) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Flush the write buffer to ensure we can read the latest data
	if err := s.buf.Flush(); err != nil {
		return nil, err
	}

	// Get the size of the record
	size := make([]byte, lenWidth)
	if _, err := s.File.ReadAt(size, int64(pos)); err != nil {
		return nil, err
	}

	// Convert the size bytes to uint64
	recordSize := enc.Uint64(size)

	// Read the record data
	record := make([]byte, recordSize)
	if _, err := s.File.ReadAt(record, int64(pos+lenWidth)); err != nil {
		return nil, err
	}

	return record, nil
}

// Read len p bytes into p beginning at the off offset
func (s *store) ReadAt(p []byte, off int64) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.buf.Flush(); err != nil {
		return 0, err
	}
	return s.File.ReadAt(p, off)
}

// Close persists any buffered data before closing the file
func (s *store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	err := s.buf.Flush()
	if err != nil {
		return err
	}
	return s.File.Close()
}
