package store

import (
	"maps"
	"slices"
	"sync"
)

// Vector: vector clock, one counter per node ID: how many writes from each node a value has seen.
type Vector map[int]uint64

// Covers: v has seen everything other has (every slot >=).
func (v Vector) Covers(other Vector) bool {
	for node, t := range other {
		if v[node] < t {
			return false
		}
	}
	return true
}

// Entry: one value of a key. Deleted=true is a tombstone (delete marker).
type Entry struct {
	Value   string
	Clock   Vector
	Deleted bool
}

// Merge: combine two lists of values for a key; drop any value another one has seen.
// What's left are concurrent values (usually just one).
func Merge(a, b []Entry) []Entry {
	all := append(slices.Clone(a), b...)
	var kept []Entry
	for i, e := range all {
		seen := false
		for j, o := range all {
			// o has seen e: drop e (for two copies of the same write, keep the first).
			if j != i && o.Clock.Covers(e.Clock) && (j < i || !e.Clock.Covers(o.Clock)) {
				seen = true
				break
			}
		}
		if !seen {
			kept = append(kept, e)
		}
	}
	return kept
}

type Store struct {
	mu   sync.RWMutex // one writer or many readers at a time
	data map[string][]Entry
}

func NewStore() *Store {
	return &Store{data: make(map[string][]Entry)}
}

// Get: return the values for key, tombstones included (nil if none).
func (s *Store) Get(key string) []Entry {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.data[key]
}

// All: copy of every key's values, for anti-entropy.
func (s *Store) All() map[string][]Entry {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return maps.Clone(s.data)
}

// Apply: merge incoming values into what we have for key.
func (s *Store) Apply(key string, incoming []Entry) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.data[key] = Merge(s.data[key], incoming)
}
