package server

import (
	"errors"
	"fmt"
	"io"
	"strings"

	"net"
	"os"

	"github.com/codecrafters-io/redis-starter-go/app/commandHandler"
	"github.com/codecrafters-io/redis-starter-go/app/config"

	"github.com/codecrafters-io/redis-starter-go/pkg/replication"
	"github.com/codecrafters-io/redis-starter-go/pkg/resp"
	"github.com/codecrafters-io/redis-starter-go/pkg/store"
)

type CmdHandler interface {
	HandleCmd(conn io.ReadWriter) (arr resp.Array, err error)
}

type server struct {
	store          *store.Store
	stateConfig    *serverStateConfig[[]byte]
	commandHandler CmdHandler
	replication    replication.Replication[[]byte]
}

func NewServer(config *config.Config) *server {
	var (
		store = store.NewStore()
		repl  = replication.New[[]byte]()
		cfg   = &serverStateConfig[[]byte]{Config: config, repl: repl}
	)

	s := &server{
		store:          store,
		commandHandler: commandHandler.NewCommandHandler(store, repl, cfg),
		stateConfig:    cfg,
		replication:    repl,
	}

	return s
}

func (s *server) Run() {
	l, err := net.Listen("tcp", net.JoinHostPort("0.0.0.0", s.stateConfig.Server.Port))
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}

	if s.stateConfig.ReplicationRole() != config.RoleSlave && s.stateConfig.ReplicationRole() != config.RoleMaster {
		fmt.Println("incorrect replication provided, starting as master")
		s.stateConfig.Replication.Role = config.RoleMaster
	}

	fmt.Println(s.stateConfig.ReplicationRole())

	switch s.stateConfig.ReplicationRole() {
	case config.RoleMaster:
		for {
			conn, err := l.Accept()
			if err != nil {
				fmt.Println("Error accepting connection: ", err.Error())
			}

			go s.handleMasterConn(conn)
		}

	case config.RoleSlave:
		go s.initSlave()

		for {
			conn, err := l.Accept()
			if err != nil {
				fmt.Println("Error accepting connection: ", err.Error())
			}

			go s.handleSlaveConn(conn)
		}
	}
}

func (s *server) handleMasterConn(conn io.ReadWriteCloser) {
	defer conn.Close()

	fmt.Println("master")
	for {
		arr, err := s.commandHandler.HandleCmd(conn)
		s.stateConfig.bytesProcessed.Add(int64(len(arr.Bytes())))

		if errors.Is(err, commandHandler.ErrConnectionClose) {
			break
		}
		if err != nil {
			fmt.Println(err.Error())
			continue
		}

		s.publishMessage(arr)
	}
}

func (s *server) publishMessage(cmd resp.Array) {
	if s.stateConfig.ReplicationRole() != config.RoleMaster {
		return
	}

	if iswriteCMD(cmd) {
		b := cmd.Bytes()
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
