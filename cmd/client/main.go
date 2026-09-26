package main

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"strings"
)

func main() {
	if len(os.Args) < 3 {
		fmt.Println("usage: client <get|set|del> <key> [value]")
		os.Exit(1)
	}

	cmd := strings.ToUpper(os.Args[1])
	key := os.Args[2]

	var message string
	switch cmd {
	case "GET", "DEL":
		message = fmt.Sprintf("%s %s", cmd, key)
	case "SET":
		if len(os.Args) < 4 {
			fmt.Println("usage: client set <key> <value>")
			os.Exit(1)
		}
		value := os.Args[3]
		message = fmt.Sprintf("%s %s %s", cmd, key, value)
	default:
		fmt.Println("unknown command:", os.Args[1])
		os.Exit(1)
	}

	conn, err := net.Dial("tcp", "localhost:9090")
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
