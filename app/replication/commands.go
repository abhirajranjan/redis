package replication

type regularCommand struct {
	Data []byte
}

func (r *regularCommand) CmdType() string {
	return "regular"
}

var _ cmd = (*regularCommand)(nil)

type numProcessedCmd struct {
	nonce int64
}

var _ cmd = (*numProcessedCmd)(nil)

func (r *numProcessedCmd) CmdType() string {
	return "numProcessed"
}
