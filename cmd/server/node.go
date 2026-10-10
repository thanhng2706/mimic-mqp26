package main

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"strings"
	"time"

	"kvstore/clock"
	"kvstore/store"
)

// callTimeout: max wait per node, so a dead node can't hang us.
const callTimeout = time.Second

// node: one server. Holds a full copy of the data; any node can coordinate.
type node struct {
	id    int
	nodes []string // every node's address, including this one
	w, r  int
	store *store.Store
	clock clock.Clock // counts this node's writes: our slot in every vector clock
}

func newNode(id int, nodes []string, w, r int) (*node, error) {
	if id < 1 || id > len(nodes) {
		return nil, fmt.Errorf("-id must be between 1 and %d", len(nodes))
	}
	if w < 1 || w > len(nodes) || r < 1 || r > len(nodes) {
		return nil, fmt.Errorf("-w and -r must be between 1 and %d", len(nodes))
	}
	return &node{id: id, nodes: nodes, w: w, r: r, store: store.NewStore()}, nil
}

func (n *node) addr() string {
	return n.nodes[n.id-1]
}

// write: SET/DEL. context = what the client read (nil: replace everything we find).
// Send to all nodes at once, succeed after W acks.
func (n *node) write(key, value string, deleted bool, context store.Vector) error {
	// ASK first: catch our counter up so this write gets a new, unique ID.
	var found []store.Entry
	for _, res := range n.gather(key, n.r) {
		found = append(found, res.entries...)
	}
	if context == nil {
		context = store.Context(found)
	}
	n.clock.Observe(max(store.Context(found)[n.id], context[n.id]))
	entries := []store.Entry{{
		Value:   value,
		Dot:     store.Dot{Node: n.id, Counter: n.clock.Tick()},
		Context: context,
		Deleted: deleted,
	}}

	// Send it to every node in parallel.
	results := make(chan error, len(n.nodes))
	for _, addr := range n.nodes {
		go func() {
			results <- n.put(addr, key, entries)
		}()
	}

	// Count acks; done as soon as we have W.
	acks := 0
	for range n.nodes {
		if err := <-results; err != nil {
			log.Printf("write %s: %v", key, err)
			continue
		}
		acks++
		if acks == n.w {
			return nil
		}
	}
	return fmt.Errorf("only %d of %d required replicas acked", acks, n.w)
}

type readResult struct {
	addr    string
	entries []store.Entry
	err     error
}

// read: GET. Ask all nodes, wait for R answers, merge them, repair the nodes that answered.
func (n *node) read(key string) ([]store.Entry, error) {
	// Ask the nodes; refuse if fewer than R answer.
	answers := n.gather(key, n.r)
	if len(answers) < n.r {
		return nil, fmt.Errorf("only %d of %d required replicas answered", len(answers), n.r)
	}

	// Merge all answers: keeps the newest value, or several if they're concurrent.
	var merged []store.Entry
	for _, res := range answers {
		merged = store.Merge(merged, res.entries)
	}

	// Read repair: send the merged values back; Apply ignores what a node already has.
	for _, res := range answers {
		if len(merged) > 0 {
			go func() {
				if err := n.put(res.addr, key, merged); err != nil {
					log.Printf("read repair %s on %s: %v", key, res.addr, err)
				}
			}()
		}
	}
	return merged, nil
}

// gather: ask all nodes for key at once, return the first `need` answers.
func (n *node) gather(key string, need int) []readResult {
	results := make(chan readResult, len(n.nodes))
	for _, addr := range n.nodes {
		go func() {
			entries, err := n.fetch(addr, key)
			results <- readResult{addr, entries, err}
		}()
	}

	var answers []readResult
	for range n.nodes {
		res := <-results
		if res.err != nil {
			log.Printf("read %s: %v", key, res.err)
			continue
		}
		answers = append(answers, res)
		if len(answers) == need {
			break
		}
	}
	return answers
}

// antiEntropy: TELL every other node all our entries; their Apply merges them in.
func (n *node) antiEntropy() {
	all := n.store.All()
	for _, addr := range n.nodes {
		if addr == n.addr() {
			continue
		}
		for key, entries := range all {
			if n.put(addr, key, entries) != nil {
				break // node is down, try again next round
			}
		}
	}
}

// put: send PUT to one node (merge these values).
func (n *node) put(addr, key string, entries []store.Entry) error {
	resp, err := call(addr, "PUT "+key+" "+formatEntries(entries))
	if err != nil {
		return err
	}
	if resp != "ACK" {
		return fmt.Errorf("%s: %s", addr, resp)
	}
	return nil
}

// fetch: send READ to one node (what do you have for key?).
func (n *node) fetch(addr, key string) ([]store.Entry, error) {
	resp, err := call(addr, "READ "+key)
	if err != nil {
		return nil, err
	}
	if resp == "NONE" {
		return nil, nil
	}
	fields := strings.Fields(resp)
	if len(fields) < 2 || fields[0] != "ENTRY" {
		return nil, fmt.Errorf("%s: unexpected reply %q", addr, resp)
	}
	entries, err := parseEntries(fields[1:])
	if err != nil {
		return nil, fmt.Errorf("%s: %v", addr, err)
	}
	return entries, nil
}

// call: send one line over TCP, return the reply.
func call(addr, line string) (string, error) {
	conn, err := net.DialTimeout("tcp", addr, callTimeout)
	if err != nil {
		return "", err
	}
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(callTimeout))

	if _, err := fmt.Fprintln(conn, line); err != nil {
		return "", err
	}
	resp, err := bufio.NewReader(conn).ReadString('\n')
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(resp), nil
}

// formatVector / parseVector: Vector <-> text, e.g. "1:2,3:1" ("" is empty).
func formatVector(v store.Vector) string {
	var slots []string
	for node, t := range v {
		slots = append(slots, fmt.Sprintf("%d:%d", node, t))
	}
	return strings.Join(slots, ",")
}

func parseVector(s string) (store.Vector, error) {
	v := store.Vector{}
	for slot := range strings.SplitSeq(s, ",") {
		var node int
		var t uint64
		if _, err := fmt.Sscanf(slot, "%d:%d", &node, &t); slot != "" && err != nil {
			return nil, fmt.Errorf("bad vector %q", s)
		}
		v[node] = max(v[node], t)
	}
	delete(v, 0) // from an empty slot
	return v, nil
}

// formatEntries / parseEntries: entries <-> text, one "dot|context/deleted/value" each,
// e.g. "1:3|1:1/0/cat" = node 1's 3rd write, had seen node 1's 1st, value cat.
func formatEntries(entries []store.Entry) string {
	var parts []string
	for _, e := range entries {
		deleted := 0
		if e.Deleted {
			deleted = 1
		}
		dot := formatVector(store.Vector{e.Dot.Node: e.Dot.Counter})
		parts = append(parts, fmt.Sprintf("%s|%s/%d/%s", dot, formatVector(e.Context), deleted, e.Value))
	}
	return strings.Join(parts, " ")
}

func parseEntries(fields []string) ([]store.Entry, error) {
	var entries []store.Entry
	for _, f := range fields {
		parts := strings.SplitN(f, "/", 3)
		dot, context, ok := strings.Cut(parts[0], "|")
		if len(parts) != 3 || !ok {
			return nil, fmt.Errorf("bad entry %q", f)
		}
		e := store.Entry{Deleted: parts[1] == "1", Value: parts[2]}
		if _, err := fmt.Sscanf(dot, "%d:%d", &e.Dot.Node, &e.Dot.Counter); err != nil {
			return nil, fmt.Errorf("bad dot %q", dot)
		}
		var err error
		if e.Context, err = parseVector(context); err != nil {
			return nil, err
		}
		entries = append(entries, e)
	}
	return entries, nil
}
