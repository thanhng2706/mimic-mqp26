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

	cmd := strings.ToUpper(args[0])
	key := args[1]

	var message string
	switch cmd {
	case "GET", "DEL":
		message = fmt.Sprintf("%s %s", cmd, key)
	case "SET":
		if len(args) < 3 {
			fmt.Println("usage: client set <key> <value>")
			os.Exit(1)
		}
		value := args[2]
		message = fmt.Sprintf("%s %s %s", cmd, key, value)
	default:
		fmt.Println("unknown command:", args[0])
		os.Exit(1)
	}

	conn, err := net.Dial("tcp", *addr)
	if err != nil {
		fmt.Println("failed to connect:", err)
		os.Exit(1)
	}
	defer conn.Close()

	fmt.Fprintln(conn, message)

	response, err := bufio.NewReader(conn).ReadString('\n')
	if err != nil {
		fmt.Println("failed to read response:", err)
		os.Exit(1)
	}

	fmt.Print(response)
}
