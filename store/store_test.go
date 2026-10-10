package store

import "testing"

var (
	a = Entry{Value: "a", Dot: Dot{1, 1}}                             // node 1's 1st write
	b = Entry{Value: "b", Dot: Dot{3, 1}}                             // node 3's 1st write, without seeing a
	c = Entry{Value: "c", Dot: Dot{3, 2}, Context: Vector{1: 1, 3: 1}} // written after seeing a and b
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

func TestUnseenWriteOnSameNodeIsKept(t *testing.T) {
	// Bob writes x after seeing a; Alice, who only saw a, writes through the same node later.
	bob := Entry{Value: "bob", Dot: Dot{1, 2}, Context: Vector{1: 1}}
	alice := Entry{Value: "alice", Dot: Dot{1, 3}, Context: Vector{1: 1}}
	got := Merge([]Entry{a, bob}, []Entry{alice})
	if len(got) != 2 {
		t.Fatalf("got %v, want bob and alice (alice never saw bob)", values(got))
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
	s.Apply("x", []Entry{{Deleted: true, Dot: Dot{1, 2}, Context: Vector{1: 1}}})
	s.Apply("x", []Entry{a})

	if got := s.Get("x"); len(got) != 1 || !got[0].Deleted {
		t.Fatalf("deleted key came back: %v", values(got))
	}
}
