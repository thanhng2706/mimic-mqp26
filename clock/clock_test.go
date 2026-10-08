package clock

import "testing"

func TestTickGoesUp(t *testing.T) {
	var c Clock
	if a, b := c.Tick(), c.Tick(); b <= a {
		t.Fatalf("second tick %d not after first %d", b, a)
	}
}

func TestObserveJumpsAhead(t *testing.T) {
	// This node is at 1 and sees a write from another node at time 10.
	// Its next write must be newer than that one.
	var c Clock
	c.Tick()
	c.Observe(10)
	if got := c.Tick(); got <= 10 {
		t.Fatalf("tick after observing 10 = %d, want > 10", got)
	}
}

func TestObserveNeverGoesBack(t *testing.T) {
	var c Clock
	c.Observe(10)
	c.Observe(3)
	if got := c.Tick(); got != 11 {
		t.Fatalf("tick = %d, want 11", got)
	}
}
