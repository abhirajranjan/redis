package main

import (
	"fmt"
	"io"

	// Uncomment this block to pass the first stage
	"net"
	"os"
)

func main() {
	// You can use print statements as follows for debugging, they'll be visible when running tests.
	fmt.Println("Logs from your program will appear here!")

	// Uncomment this block to pass the first stage
	//
	l, err := net.Listen("tcp", "0.0.0.0:6379")
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}
	conn, err := l.Accept()
	if err != nil {
		fmt.Println("Error accepting connection: ", err.Error())
		os.Exit(1)
	}

	var b []byte
	for {
		b = make([]byte, 1024)
		_, err = conn.Read(b)
		if err == io.EOF {
			break
		}
		if err != nil {
			fmt.Println("error reading from client: ", err)
			os.Exit(1)
		}

		if _, err := conn.Write([]byte("+PONG\r\n")); err != nil {
			fmt.Println("error writing to client: ", err)
			os.Exit(1)
		}
	}
}
