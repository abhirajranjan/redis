package replication

import (
	"fmt"
	"io"
	"net"
	"strconv"

	"github.com/codecrafters-io/redis-starter-go/app/config"
	"github.com/codecrafters-io/redis-starter-go/app/resp"
	"github.com/pkg/errors"
)

func HandleReplication() {
	tcpAddr, err := net.ResolveTCPAddr("tcp", config.Replication.Host+":"+strconv.FormatInt(config.Replication.Port, 10))
	if err != nil {
		fmt.Println(err, "ResolveTCPAddr failed")
		return
	}

	conn, err := net.DialTCP("tcp", nil, tcpAddr)
	if err != nil {
		fmt.Println(err, "Dial failed")
		return
	}
	defer conn.Close()

	if err := ping(conn); err != nil {
		fmt.Println(err)
		return
	}
}

func ping(rw io.ReadWriter) error {
	arr := resp.Array{
		resp.BulkString{Str: "PING"},
	}

	_, err := rw.Write(arr.Bytes())
	if err != nil {
		return errors.Wrap(err, "ping")
	}

	cmd, err := resp.Parse(rw)
	if err != nil {
		return errors.Wrap(err, "resp.Parse")
	}

	fmt.Println(strconv.Quote(string(cmd.Bytes())))
	return nil
}
