package main

import (
	"net"
	"strings"
	"testing"
	"time"
)

// cluster runs N nodes in this process, each on its own local port.
type cluster struct {
	t         *testing.T
	nodes     []*node
	listeners []net.Listener
}

func startCluster(t *testing.T, size, w, r int) *cluster {
	t.Helper()
	c := &cluster{t: t}

	var addrs []string
	for range size {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		c.listeners = append(c.listeners, l)
		addrs = append(addrs, l.Addr().String())
	}
	for i := range size {
		n, err := newNode(i+1, addrs, w, r)
		if err != nil {
			t.Fatal(err)
		}
		c.nodes = append(c.nodes, n)
		go serve(c.listeners[i], n)
	}
	t.Cleanup(func() {
		for _, l := range c.listeners {
			l.Close()
		}
	})
	return c
}

// send runs a client command against node i (1-based); returns the reply and GET's context.
func (c *cluster) send(i int, line string) (string, string) {
	c.t.Helper()
	resp, err := call(c.nodes[i-1].addr(), line)
	if err != nil {
		c.t.Fatalf("node %d: %v", i, err)
	}
	reply, context, _ := strings.Cut(resp, " CONTEXT ")
	return reply, context
}

// stop makes node i unreachable, like a crash or a network partition.
func (c *cluster) stop(i int) {
	c.listeners[i-1].Close()
}

// restart makes node i reachable again, keeping whatever it had stored.
func (c *cluster) restart(i int) {
	c.t.Helper()
	l, err := net.Listen("tcp", c.nodes[i-1].addr())
	if err != nil {
		c.t.Fatal(err)
	}
	c.listeners[i-1] = l
	go serve(l, c.nodes[i-1])
}

func TestWriteOnOneNodeReadOnAnother(t *testing.T) {
	c := startCluster(t, 3, 2, 2)

	if got, _ := c.send(1, "SET x 5"); got != "OK" {
		t.Fatalf("SET = %q", got)
	}
	if got, _ := c.send(3, "GET x"); got != "VALUE 5" {
		t.Fatalf("GET on node 3 = %q, want VALUE 5", got)
	}
}

func TestLaterWriteWinsAcrossCoordinators(t *testing.T) {
	c := startCluster(t, 3, 2, 2)

	c.send(1, "SET x 1")
	c.send(2, "SET x 2")
	if got, _ := c.send(3, "GET x"); got != "VALUE 2" {
		t.Fatalf("GET = %q, want VALUE 2", got)
	}
}

func TestWriteAfterAckedWriteWins(t *testing.T) {
	// Node 1 misses the first write, so its clock is behind. Its own
	// write must still get a bigger timestamp than the acked one.
	c := startCluster(t, 3, 2, 2)
	c.stop(1)
	c.send(2, "SET x 1")
	c.restart(1)

	c.send(1, "SET x 2")
	if got, _ := c.send(3, "GET x"); got != "VALUE 2" {
		t.Fatalf("GET = %q, want VALUE 2 (the later write)", got)
	}
}

func TestDeleteIsReplicated(t *testing.T) {
	c := startCluster(t, 3, 2, 2)

	c.send(1, "SET x 5")
	if got, _ := c.send(2, "DEL x"); got != "OK" {
		t.Fatalf("DEL = %q", got)
	}
	if got, _ := c.send(3, "GET x"); got != "NOT_FOUND" {
		t.Fatalf("GET after DEL = %q, want NOT_FOUND", got)
	}
}

func TestWorksWithOneNodeDown(t *testing.T) {
	c := startCluster(t, 3, 2, 2)
	c.stop(3)

	if got, _ := c.send(1, "SET x 5"); got != "OK" {
		t.Fatalf("SET with 2 of 3 up = %q, want OK", got)
	}
	if got, _ := c.send(2, "GET x"); got != "VALUE 5" {
		t.Fatalf("GET with 2 of 3 up = %q, want VALUE 5", got)
	}
}

func TestRefusesWritesWithoutQuorum(t *testing.T) {
	// W=2 but only 1 node is up: the system picks consistency over
	// availability and refuses the write.
	c := startCluster(t, 3, 2, 2)
	c.stop(2)
	c.stop(3)

	got, _ := c.send(1, "SET x 5")
	if got != "ERROR only 1 of 2 required replicas acked" {
		t.Fatalf("SET with 1 of 3 up = %q, want quorum error", got)
	}
}

func TestReadRepairFixesStaleNode(t *testing.T) {
	c := startCluster(t, 3, 2, 3)
	c.stop(3)
	c.send(1, "SET x 5") // node 3 misses this write
	c.restart(3)

	if c.nodes[2].store.Get("x") != nil {
		t.Fatal("node 3 should not have x yet")
	}
	// R=3, so this read hears from node 3 and repairs it.
	if got, _ := c.send(1, "GET x"); got != "VALUE 5" {
		t.Fatalf("GET = %q, want VALUE 5", got)
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if e := c.nodes[2].store.Get("x"); len(e) == 1 && e[0].Value == "5" {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("read repair did not copy x to node 3")
}

func TestAntiEntropyFixesStaleNode(t *testing.T) {
	c := startCluster(t, 3, 2, 2)
	c.stop(3)
	c.send(1, "SET x 5") // node 3 misses this write
	c.restart(3)

	c.nodes[0].antiEntropy() // one round, no GET needed
	if e := c.nodes[2].store.Get("x"); len(e) != 1 || e[0].Value != "5" {
		t.Fatal("anti-entropy did not copy x to node 3")
	}
}

func TestConcurrentWritesAreBothKept(t *testing.T) {
	// W=1, R=1 and a network split: each side accepts a write without seeing the other.
	c := startCluster(t, 3, 1, 1)
	c.stop(2)
	c.stop(3)
	c.send(1, "SET x a") // only node 1 has a
	c.stop(1)
	c.restart(3)
	c.send(3, "SET x b") // node 3 never saw a
	c.restart(1)
	c.restart(2)

	// Heal: anti-entropy spreads both values everywhere.
	c.nodes[0].antiEntropy()
	c.nodes[2].antiEntropy()
	if got, _ := c.send(2, "GET x"); got != "VALUES a b" {
		t.Fatalf("GET = %q, want VALUES a b (neither write lost)", got)
	}

	// The next SET has seen both, so it replaces both.
	c.send(2, "SET x c")
	c.nodes[1].antiEntropy() // W=1: make sure every node has c before reading with R=1
	if got, _ := c.send(1, "GET x"); got != "VALUE c" {
		t.Fatalf("GET after resolving = %q, want VALUE c", got)
	}
}

func TestWriteKeepsValuesTheClientDidNotSee(t *testing.T) {
	c := startCluster(t, 3, 2, 2)
	c.send(1, "SET x a")
	_, context := c.send(1, "GET x") // Alice reads a
	c.send(1, "SET x bob")            // Bob writes through the same node; Alice doesn't see it
	c.send(1, "SET x alice "+context) // Alice writes, based on what she read

	if got, _ := c.send(3, "GET x"); got != "VALUES alice bob" {
		t.Fatalf("GET = %q, want VALUES alice bob (Alice never saw bob)", got)
	}
}
