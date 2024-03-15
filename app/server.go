package main

import (
	"fmt"
	"io"

	"net"
	"os"
)

func main() {
	// You can use print statements as follows for debugging, they'll be visible when running tests.
	fmt.Println("Logs from your program will appear here!")

	l, err := net.Listen("tcp", fmt.Sprintf("0.0.0.0:%d", Config.Server.Port))
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}

	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
		}

		go handleConn(conn)
	}

}

func handleConn(conn net.Conn) {
	for {
		data, err := ParseResp(conn)
		if err == io.EOF {
			break
		}
		if err != nil {
			fmt.Println(err)
			conn.Write([]byte(fmt.Sprintf("-ERR %s\r\n", err)))
			return
		}

		arr, ok := data.(Array)
		if !ok {
			fmt.Println("cannot convert cmd to array")
			conn.Write([]byte("cannot convert cmd to array"))
		}

		if err := HandleFunc(arr, conn); err != nil {
			fmt.Println(err)
		}
	}
}
