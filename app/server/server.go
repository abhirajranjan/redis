package server

import (
	"fmt"
	"io"
	"strconv"
	"strings"

	"net"
	"os"

	"github.com/codecrafters-io/redis-starter-go/app/config"
	"github.com/codecrafters-io/redis-starter-go/app/replication"
	"github.com/codecrafters-io/redis-starter-go/app/resp"
	"github.com/codecrafters-io/redis-starter-go/app/store"
)

type server struct {
	replication replication.Replication[[]byte]
	store       *store.Store
}

func NewServer() *server {
	repl := replication.Replication[[]byte]{}
	repl.Init()

	store := store.NewStore()

	return &server{
		replication: repl,
		store:       store,
	}
}

func (s server) Run() {
	l, err := net.Listen("tcp", fmt.Sprintf("0.0.0.0:%d", config.Server.Port))
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}

	if config.Replication.Role == config.RoleSlave {
		go replication.InitSlave(s.handleConn)
	}

	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
		}

		go s.handleConn(conn)
	}
}

func (s server) handleConn(conn net.Conn) {
	defer conn.Close()
	for {
		data, err := resp.Parse(conn)
		if err == io.EOF {
			break
		}
		if err != nil {
			conn.Write([]byte(fmt.Sprintf("-ERR %s\r\n", err)))
			continue
		}

		arr, ok := data.(resp.Array)
		if !ok {
			fmt.Println("cannot convert cmd to array")
			conn.Write([]byte("cannot convert cmd to array"))
		}

		fmt.Println("handle: ", strconv.Quote(string(arr.Bytes())))
		s.publishMessage(arr)
		if err := s.handleFunc(arr, conn); err != nil {
			fmt.Println(err)
		}
	}
}

func (s server) publishMessage(cmd resp.Array) {
	if config.Replication.Role != config.RoleMaster {
		return
	}

	if iswriteCMD(cmd) {
		fmt.Println("pub: ", strconv.Quote(string(cmd.Bytes())))
		s.replication.Publish(cmd.Bytes())
	}
}

func iswriteCMD(cmd resp.Array) bool {
	if len(cmd) == 0 {
		return false
	}

	s, ok := String(cmd[0])
	if !ok {
		return false
	}

	switch strings.ToLower(s) {
	case "set":
		return true
	default:
		return false
	}
}
