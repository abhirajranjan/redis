package replication

import (
	"io"
)

type regularCommand struct {
	Data io.Reader
}

func (r *regularCommand) CmdType() string {
	return "regular"
}

var _ cmd = (*regularCommand)(nil)

func (r *regularCommand) Read(p []byte) (n int, err error) {
	return r.Data.Read(p)
}

type numProcessedCmd struct {
	nonce int64
}

var _ cmd = (*numProcessedCmd)(nil)

func (r *numProcessedCmd) CmdType() string {
	return "numProcessed"
}
