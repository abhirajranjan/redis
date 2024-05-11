package server

import (
	"fmt"
	"io"
	"strconv"
	"strings"
	"sync/atomic"

	"net"
	"os"

	"github.com/codecrafters-io/redis-starter-go/app/command"
	"github.com/codecrafters-io/redis-starter-go/app/config"
	"github.com/codecrafters-io/redis-starter-go/app/replication"
	"github.com/codecrafters-io/redis-starter-go/app/resp"
	"github.com/codecrafters-io/redis-starter-go/app/store"
)

type server struct {
	replication replication.Replication[[]byte]
	store       *store.Store

	config         *config.Config
	bytesProcessed atomic.Int64

	cmdRunner command.Command
}

func NewServer(config *config.Config) *server {
	repl := replication.Replication[[]byte]{}
	repl.Init()

	store := store.NewStore()
	s := &server{
		replication: repl,
		store:       store,
		config:      config,
	}

	s.cmdRunner = s.commandRunner()
	return s
}

func (s *server) Run() {
	l, err := net.Listen("tcp", net.JoinHostPort("0.0.0.0", s.config.Server.Port))
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}

	if s.config.Replication.Role == config.RoleSlave {
		go replication.InitSlave(&replication.SlaveConfig{
			FnCmd: s.handleConn,
			Host:  s.config.Replication.Host,
			Port:  s.config.Replication.Port,
		})
	}

	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
		}

		go s.handleConn(conn)
	}
}

func (s *server) handleConn(conn net.Conn) {
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
			conn.Write([]byte(fmt.Sprintf("-ERR %s\r\n", "cannot convert cmd to array")))
			continue
		}

		b := arr.Bytes()
		fmt.Println("handle: ", strconv.Quote(string(b)))

		if err := s.cmdRunner.Run(arr, conn); err != nil {
			fmt.Println(err)
			conn.Write([]byte(fmt.Sprintf("-ERR %s\r\n", "unknown command")))
		} else {
			s.publishMessage(arr)
		}

		s.bytesProcessed.Add(int64(len(b)))
	}
}

func (s *server) publishMessage(cmd resp.Array) {
	if s.config.Replication.Role != config.RoleMaster {
		return
	}

	if iswriteCMD(cmd) {
		b := cmd.Bytes()
		fmt.Println("pub: ", strconv.Quote(string(b)))

		s.replication.Publish(b)
	}
}

func iswriteCMD(cmd resp.Array) bool {
	if len(cmd) == 0 {
		return false
	}

	s, ok := resp.IsString(cmd[0])
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
