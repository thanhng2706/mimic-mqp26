package main

import (
	"bufio"
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"slices"
	"strings"
	"time"

	"kvstore/store"
)

// syncEvery: how often each node runs anti-entropy.
const syncEvery = 2 * time.Second

func main() {
	id := flag.Int("id", 1, "this node's ID, 1-based position in -nodes")
	nodesFlag := flag.String("nodes", "localhost:9090,localhost:9091,localhost:9092",
		"comma-separated addresses of every node in the cluster, including this one")
	w := flag.Int("w", 2, "write quorum: replicas that must ack a write")
	r := flag.Int("r", 2, "read quorum: replicas that must answer a read")
	flag.Parse()

	addrs := strings.Split(*nodesFlag, ",")
	n, err := newNode(*id, addrs, *w, *r)
	if err != nil {
		log.Fatal(err)
	}

	listener, err := net.Listen("tcp", n.addr())
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	defer listener.Close()

	log.Printf("node %d listening on %s (N=%d, W=%d, R=%d)", n.id, n.addr(), len(n.nodes), n.w, n.r)
	if n.w+n.r <= len(n.nodes) {
		log.Printf("warning: W+R <= N, read and write quorums may not overlap, so reads can be stale")
	}

	go func() {
		for range time.Tick(syncEvery) {
			n.antiEntropy()
		}
	}()
	serve(listener, n)
}

func serve(listener net.Listener, n *node) {
	for {
		conn, err := listener.Accept()
		if errors.Is(err, net.ErrClosed) {
			return
		}
		if err != nil {
			log.Printf("accept error: %v", err)
			continue
		}
		go handleConn(conn, n)
	}
}

func handleConn(conn net.Conn, n *node) {
	defer conn.Close()

	scanner := bufio.NewScanner(conn)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		response := handleCommand(line, n)
		fmt.Fprintln(conn, response)
	}
}

// handleCommand runs one command. GET, SET and DEL come from clients and
// make this node the coordinator. PUT and READ come from other nodes and
// only touch the local store.
func handleCommand(line string, n *node) string {
	fields := strings.Fields(line)
	if len(fields) == 0 {
		return "ERROR empty command"
	}

	cmd := strings.ToUpper(fields[0])

	switch cmd {
	case "GET":
		if len(fields) != 2 {
			return "ERROR usage: GET key"
		}
		entries, err := n.read(fields[1])
		if err != nil {
			return "ERROR " + err.Error()
		}
		var values []string
		for _, e := range entries {
			if !e.Deleted {
				values = append(values, e.Value)
			}
		}
		slices.Sort(values)
		// CONTEXT: what this read saw; pass it to the next SET/DEL so it replaces only these.
		context := " CONTEXT " + formatVector(store.Context(entries))
		switch len(values) {
		case 0:
			return "NOT_FOUND"
		case 1:
			return "VALUE " + values[0] + context
		default: // concurrent writes: show all, the next SET replaces them
			return "VALUES " + strings.Join(values, " ") + context
		}

	case "SET", "DEL":
		// SET key value [context] / DEL key [context]
		args := fields[1:]
		want := 2
		if cmd == "DEL" {
			want = 1
		}
		if len(args) != want && len(args) != want+1 {
			return "ERROR usage: SET key value [context] / DEL key [context]"
		}
		var context store.Vector
		if len(args) > want {
			var err error
			if context, err = parseVector(args[want]); err != nil {
				return "ERROR " + err.Error()
			}
		}
		value := ""
		if cmd == "SET" {
			value = args[1]
		}
		if err := n.write(args[0], value, cmd == "DEL", context); err != nil {
			return "ERROR " + err.Error()
		}
		return "OK"

	case "PUT":
		if len(fields) < 3 {
			return "ERROR usage: PUT key dot|context/deleted/value..."
		}
		entries, err := parseEntries(fields[2:])
		if err != nil {
			return "ERROR " + err.Error()
		}
		n.clock.Observe(store.Context(entries)[n.id]) // after a restart, don't reuse our old write IDs
		n.store.Apply(fields[1], entries)
		return "ACK"

	case "READ":
		if len(fields) != 2 {
			return "ERROR usage: READ key"
		}
		entries := n.store.Get(fields[1])
		if len(entries) == 0 {
			return "NONE"
		}
		return "ENTRY " + formatEntries(entries)

	default:
		return "ERROR unknown command"
	}
}
