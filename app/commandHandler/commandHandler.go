package commandHandler

import (
	"fmt"
	"io"
	"log"
	"time"

	"github.com/codecrafters-io/redis-starter-go/app/config"
	"github.com/codecrafters-io/redis-starter-go/pkg/command"
	"github.com/codecrafters-io/redis-starter-go/pkg/resp"
	"github.com/codecrafters-io/redis-starter-go/pkg/store"
	"github.com/pkg/errors"
)

var ErrConnectionClose = errors.New("close connection request")

type serverConfig interface {
	ReplicationRole() config.Role
	MasterReplId() string
	MasterReplOffset() int64
	BytesProcessed() int64
}

type replication interface {
	StartSync(w io.ReadWriter)
	NumProcessedCmd(atleastAck int64, timeout time.Duration) int64
}

type CommandHandler struct {
	store     *store.Store
	cmdRunner *command.Command
	cfg       serverConfig
	repl      replication
}

func NewCommandHandler(store *store.Store, cfg serverConfig, repl replication) *CommandHandler {
	s := &CommandHandler{
		store: store,
		cfg:   cfg,
		repl:  repl,
		cmdRunner: &command.Command{
			RunFn: func(_ resp.Array, w io.Writer) error {
				err := errors.New("invalid Command")
				w.Write(resp.SimpleError(err.Error()).Bytes())
				return err
			},
		},
	}

	s.initCommandRunner(s.cmdRunner)
	return s
}

// if alive returns false means connection should be closed
func (c *CommandHandler) HandleCmd(conn io.ReadWriter) (arr resp.Array, err error) {
	data, err := resp.Parse(conn)
	if err == io.EOF {
		return nil, ErrConnectionClose
	}
	if err != nil {
		conn.Write([]byte(fmt.Sprintf("-ERR %s\r\n", err)))
		return nil, err
	}

	arr, ok := data.(resp.Array)
	if !ok {
		err := errors.New("cannot convert cmd to array")
		conn.Write([]byte(fmt.Sprintf("-ERR %s\r\n", err.Error())))
		return nil, err
	}

	log.Printf("%s: Replication: recv command: %#v\n", string(c.cfg.ReplicationRole()), arr)

	if err := c.cmdRunner.Run(arr, conn); err != nil {
		conn.Write([]byte(fmt.Sprintf("-ERR %s\r\n", "unknown command")))
		return arr, err
	}

	return arr, nil
}
