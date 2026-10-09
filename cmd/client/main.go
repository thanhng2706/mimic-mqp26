package main

import (
	"bufio"
	"flag"
	"fmt"
	"net"
	"os"
	"strings"
)

func main() {
	addr := flag.String("addr", "localhost:9090", "address of the node to send the command to")
	flag.Parse()
	args := flag.Args()

	if len(args) < 2 {
		fmt.Println("usage: client [-addr host:port] <get|set|del> <key> [value]")
		os.Exit(1)
	}

	conn, err := net.Dial("tcp", *addr)
	if err != nil {
		fmt.Println("failed to connect:", err)
		os.Exit(1)
	}
	defer conn.Close()

	// Send the command as typed; the server checks it.
	fmt.Fprintln(conn, strings.Join(args, " "))

	response, err := bufio.NewReader(conn).ReadString('\n')
	if err != nil {
		fmt.Println("failed to read response:", err)
		os.Exit(1)
	}

	fmt.Print(response)
}
