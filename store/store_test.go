package store

import "testing"

func TestNewerWriteWins(t *testing.T) {
	s := NewStore()
	s.Apply("x", Entry{Value: "1", Version: Version{Time: 1, Node: 1}})
	s.Apply("x", Entry{Value: "2", Version: Version{Time: 2, Node: 1}})

	entry, _ := s.Get("x")
	if entry.Value != "2" {
		t.Fatalf("got %q, want the newer value %q", entry.Value, "2")
	}
}

func TestOlderWriteIsIgnored(t *testing.T) {
	// Writes can arrive out of order over the network.
	s := NewStore()
	s.Apply("x", Entry{Value: "new", Version: Version{Time: 5, Node: 1}})
	stored := s.Apply("x", Entry{Value: "old", Version: Version{Time: 3, Node: 1}})

	entry, _ := s.Get("x")
	if stored || entry.Value != "new" {
		t.Fatalf("older write replaced newer one: got %q", entry.Value)
	}
}

func TestTieBrokenByNodeID(t *testing.T) {
	// Same Time on two nodes: higher node ID wins, whatever the arrival order.
	a := Entry{Value: "from-node-1", Version: Version{Time: 4, Node: 1}}
	b := Entry{Value: "from-node-2", Version: Version{Time: 4, Node: 2}}

	s1 := NewStore()
	s1.Apply("x", a)
	s1.Apply("x", b)

	s2 := NewStore()
	s2.Apply("x", b)
	s2.Apply("x", a)

	e1, _ := s1.Get("x")
	e2, _ := s2.Get("x")
	if e1.Value != "from-node-2" || e2.Value != "from-node-2" {
		t.Fatalf("nodes disagree: %q vs %q", e1.Value, e2.Value)
	}
}

func TestTombstoneBlocksOldValue(t *testing.T) {
	// A node that missed the delete sends back the old value later.
	s := NewStore()
	s.Apply("x", Entry{Value: "5", Version: Version{Time: 1, Node: 1}})
	s.Apply("x", Entry{Deleted: true, Version: Version{Time: 2, Node: 1}})
	s.Apply("x", Entry{Value: "5", Version: Version{Time: 1, Node: 1}})

	entry, _ := s.Get("x")
	if !entry.Deleted {
		t.Fatalf("deleted key came back with value %q", entry.Value)
	}
}
