package store

import "sync"

// Version: how new a value is. Time is from the Lamport clock, Node breaks ties.
type Version struct {
	Time uint64
	Node int
}

// Newer: higher Time wins; same Time, higher Node wins.
func (v Version) Newer(other Version) bool {
	if v.Time != other.Time {
		return v.Time > other.Time
	}
	return v.Node > other.Node
}

// Entry: what we store per key. Deleted=true is a tombstone (delete marker).
type Entry struct {
	Value   string
	Version Version
	Deleted bool
}

type Store struct {
	mu   sync.RWMutex // one writer or many readers at a time
	data map[string]Entry
}

func NewStore() *Store {
	return &Store{data: make(map[string]Entry)}
}

// Get: return the entry for key, tombstones included.
func (s *Store) Get(key string) (Entry, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	entry, ok := s.data[key]
	return entry, ok
}

// Apply: save entry only if it's newer than what we have. Returns true if saved.
func (s *Store) Apply(key string, entry Entry) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	current, ok := s.data[key]
	if ok && !entry.Version.Newer(current.Version) {
		return false
	}
	s.data[key] = entry
	return true
}
