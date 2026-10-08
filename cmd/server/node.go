package main

import (
	"bufio"
	"errors"
	"fmt"
	"log"
	"net"
	"strconv"
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
	clock clock.Clock
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

// write: SET/DEL. Send to all nodes at once, succeed after W acks.
func (n *node) write(key, value string, deleted bool) error {
	// Catch our clock up to the newest version first, so this write wins.
	for _, res := range n.gather(key, n.r) {
		if res.found {
			n.clock.Observe(res.entry.Version.Time)
		}
	}

	// Make the new entry with a fresh version.
	entry := store.Entry{
		Value:   value,
		Deleted: deleted,
		Version: store.Version{Time: n.clock.Tick(), Node: n.id},
	}

	// Send it to every node in parallel.
	results := make(chan error, len(n.nodes))
	for _, addr := range n.nodes {
		go func() {
			results <- n.put(addr, key, entry)
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
	addr  string
	entry store.Entry
	found bool
	err   error
}

// read: GET. Ask all nodes, wait for R answers, return the newest, repair stale nodes.
func (n *node) read(key string) (store.Entry, bool, error) {
	// Ask the nodes; refuse if fewer than R answer.
	answers := n.gather(key, n.r)
	if len(answers) < n.r {
		return store.Entry{}, false, fmt.Errorf("only %d of %d required replicas answered", len(answers), n.r)
	}

	// Pick the newest answer.
	var newest store.Entry
	found := false
	for _, res := range answers {
		if res.found && (!found || res.entry.Version.Newer(newest.Version)) {
			newest, found = res.entry, true
		}
	}
	if !found {
		return store.Entry{}, false, nil
	}
	n.clock.Observe(newest.Version.Time)

	// Read repair: send the newest entry to nodes that had something older.
	for _, res := range answers {
		if !res.found || newest.Version.Newer(res.entry.Version) {
			go func() {
				if err := n.put(res.addr, key, newest); err != nil {
					log.Printf("read repair %s on %s: %v", key, res.addr, err)
				}
			}()
		}
	}
	return newest, true, nil
}

// gather: ask all nodes for key at once, return the first `need` answers.
func (n *node) gather(key string, need int) []readResult {
	results := make(chan readResult, len(n.nodes))
	for _, addr := range n.nodes {
		go func() {
			entry, found, err := n.fetch(addr, key)
			results <- readResult{addr, entry, found, err}
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

// put: send PUT to one node (store this entry).
func (n *node) put(addr, key string, entry store.Entry) error {
	resp, err := call(addr, "PUT "+key+" "+formatEntry(entry))
	if err != nil {
		return err
	}
	if resp != "ACK" {
		return fmt.Errorf("%s: %s", addr, resp)
	}
	return nil
}

// fetch: send READ to one node (what do you have for key?).
func (n *node) fetch(addr, key string) (store.Entry, bool, error) {
	resp, err := call(addr, "READ "+key)
	if err != nil {
		return store.Entry{}, false, err
	}
	if resp == "NONE" {
		return store.Entry{}, false, nil
	}
	fields := strings.Fields(resp)
	if len(fields) < 4 || fields[0] != "ENTRY" {
		return store.Entry{}, false, fmt.Errorf("%s: unexpected reply %q", addr, resp)
	}
	entry, err := parseEntry(fields[1:])
	if err != nil {
		return store.Entry{}, false, fmt.Errorf("%s: %v", addr, err)
	}
	return entry, true, nil
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

// formatEntry / parseEntry: Entry <-> text "time node deleted [value]".
func formatEntry(e store.Entry) string {
	deleted := 0
	if e.Deleted {
		deleted = 1
	}
	return fmt.Sprintf("%d %d %d %s", e.Version.Time, e.Version.Node, deleted, e.Value)
}

func parseEntry(fields []string) (store.Entry, error) {
	if len(fields) < 3 {
		return store.Entry{}, errors.New("entry needs time, node and deleted")
	}
	t, err := strconv.ParseUint(fields[0], 10, 64)
	if err != nil {
		return store.Entry{}, fmt.Errorf("bad time %q", fields[0])
	}
	nodeID, err := strconv.Atoi(fields[1])
	if err != nil {
		return store.Entry{}, fmt.Errorf("bad node %q", fields[1])
	}
	entry := store.Entry{
		Version: store.Version{Time: t, Node: nodeID},
		Deleted: fields[2] == "1",
	}
	if len(fields) > 3 {
		entry.Value = fields[3]
	}
	return entry, nil
}
