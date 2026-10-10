package store

import (
	"maps"
	"slices"
	"sync"
)

// Vector: one counter per node ID, e.g. {1: 2} = "node 1's first 2 writes".
type Vector map[int]uint64

// Dot: the ID of one write: the node that made it and that node's write count.
type Dot struct {
	Node    int
	Counter uint64
}

// Entry: one value of a key. Context = the writes it had seen. Deleted=true is a tombstone.
type Entry struct {
	Value   string
	Dot     Dot
	Context Vector
	Deleted bool
}

// Seen: e had seen write d when it was written.
func (e Entry) Seen(d Dot) bool {
	return e.Context[d.Node] >= d.Counter
}

// Context: every write these values had seen, plus the values themselves.
func Context(entries []Entry) Vector {
	v := Vector{}
	for _, e := range entries {
		for node, t := range e.Context {
			v[node] = max(v[node], t)
		}
		v[e.Dot.Node] = max(v[e.Dot.Node], e.Dot.Counter)
	}
	return v
}

// Merge: combine two lists of values for a key; drop any value another one has seen.
// What's left are concurrent values (usually just one).
func Merge(a, b []Entry) []Entry {
	all := append(slices.Clone(a), b...)
	var kept []Entry
	for i, e := range all {
		seen := slices.ContainsFunc(all, func(o Entry) bool { return o.Seen(e.Dot) })
		dup := slices.ContainsFunc(all[:i], func(o Entry) bool { return o.Dot == e.Dot })
		if !seen && !dup {
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
