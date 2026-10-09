package store

import "testing"

var (
	a = Entry{Value: "a", Clock: Vector{1: 1}}       // written on node 1
	b = Entry{Value: "b", Clock: Vector{3: 1}}       // written on node 3, without seeing a
	c = Entry{Value: "c", Clock: Vector{1: 1, 3: 2}} // written on node 3 after seeing a and b
)

func values(entries []Entry) []string {
	var vs []string
	for _, e := range entries {
		vs = append(vs, e.Value)
	}
	return vs
}

func TestConcurrentValuesAreBothKept(t *testing.T) {
	got := Merge([]Entry{a}, []Entry{b})
	if len(got) != 2 {
		t.Fatalf("got %v, want both a and b", values(got))
	}
}

func TestNewerValueReplacesSeenOnes(t *testing.T) {
	got := Merge(Merge([]Entry{a}, []Entry{b}), []Entry{c})
	if len(got) != 1 || got[0].Value != "c" {
		t.Fatalf("got %v, want only c", values(got))
	}
}

func TestOlderWriteIsIgnored(t *testing.T) {
	// Writes can arrive out of order over the network.
	s := NewStore()
	s.Apply("x", []Entry{c})
	s.Apply("x", []Entry{a})

	if got := s.Get("x"); len(got) != 1 || got[0].Value != "c" {
		t.Fatalf("older write changed the key: got %v", values(got))
	}
}

func TestMergeRules(t *testing.T) {
	// Order and duplicates don't matter.
	if got := Merge([]Entry{b}, []Entry{a}); len(got) != 2 {
		t.Fatalf("b then a = %v, want both", values(got))
	}
	if got := Merge([]Entry{a}, []Entry{a}); len(got) != 1 {
		t.Fatalf("a merged with itself = %v, want one a", values(got))
	}
}

func TestTombstoneBlocksOldValue(t *testing.T) {
	// A node that missed the delete sends back the old value later.
	s := NewStore()
	s.Apply("x", []Entry{a})
	s.Apply("x", []Entry{{Deleted: true, Clock: Vector{1: 2}}})
	s.Apply("x", []Entry{a})

	if got := s.Get("x"); len(got) != 1 || !got[0].Deleted {
		t.Fatalf("deleted key came back: %v", values(got))
	}
}
