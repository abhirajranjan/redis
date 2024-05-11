package commandHandler

import (
	"fmt"
	"io"
	"strconv"

	"github.com/codecrafters-io/redis-starter-go/app/config"
	"github.com/codecrafters-io/redis-starter-go/pkg/command"
	"github.com/codecrafters-io/redis-starter-go/pkg/replication"
	"github.com/codecrafters-io/redis-starter-go/pkg/resp"
	"github.com/codecrafters-io/redis-starter-go/pkg/store"
	"github.com/pkg/errors"
)

var ErrConnectionClose = errors.New("close connection request")

type cfg interface {
	ReplicationRole() config.Role
	MasterReplId() string
	MasterReplOffset() int64
	BytesProcessed() int64
}

type CommandHandler[T ~[]byte] struct {
	store       *store.Store
	cmdRunner   *command.Command
	replication replication.Replication[T]
	cfg         cfg
}

func NewCommandHandler[T ~[]byte](store *store.Store, replication replication.Replication[T], cfg cfg) *CommandHandler[T] {
	s := &CommandHandler[T]{
		store:       store,
		replication: replication,

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
func (c *CommandHandler[T]) HandleCmd(conn io.ReadWriter) (arr resp.Array, err error) {
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

	b := arr.Bytes()
	fmt.Println("handle: ", strconv.Quote(string(b)))

	if err := c.cmdRunner.Run(arr, conn); err != nil {
		conn.Write([]byte(fmt.Sprintf("-ERR %s\r\n", "unknown command")))
		return arr, err
	}

	return arr, nil
}
