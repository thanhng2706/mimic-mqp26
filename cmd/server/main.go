package main

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"strings"

	"kvstore/store"
)

func main() {
	s := store.NewStore()

	listener, err := net.Listen("tcp", ":9090")
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	defer listener.Close()

	log.Println("kvstore server listening on :9090")

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Printf("accept error: %v", err)
			continue
		}
		go handleConn(conn, s)
	}
}

func handleConn(conn net.Conn, s *store.Store) {
	defer conn.Close()

	scanner := bufio.NewScanner(conn)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		response := handleCommand(line, s)
		fmt.Fprintln(conn, response)
	}
}

func handleCommand(line string, s *store.Store) string {
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
		value, ok := s.Get(fields[1])
		if !ok {
			return "NOT_FOUND"
		}
		return "VALUE " + value

	case "SET":
		if len(fields) != 3 {
			return "ERROR usage: SET key value"
		}
		s.Set(fields[1], fields[2])
		return "OK"

	case "DEL":
		if len(fields) != 2 {
			return "ERROR usage: DEL key"
		}
		if s.Delete(fields[1]) {
			return "OK"
		}
		return "NOT_FOUND"

	default:
		return "ERROR unknown command"
	}
}
