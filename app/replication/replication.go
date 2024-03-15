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
	if err := replconf(conn); err != nil {
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

func replconf(rw io.ReadWriter) error {
	arr := resp.Array{
		resp.BulkString{Str: "REPLCONF"},
		resp.BulkString{Str: "listening-port"},
		resp.BulkString{Str: strconv.FormatInt(config.Server.Port, 10)},
	}

	_, err := rw.Write(arr.Bytes())
	if err != nil {
		return errors.Wrap(err, "replconf")
	}

	cmd, err := resp.Parse(rw)
	if err != nil {
		return errors.Wrap(err, "resp.Parse")
	}

	fmt.Println(strconv.Quote(string(cmd.Bytes())))

	arr = resp.Array{
		resp.BulkString{Str: "REPLCONF"},
		resp.BulkString{Str: "capa"},
		resp.BulkString{Str: "psync2"},
	}

	_, err = rw.Write(arr.Bytes())
	if err != nil {
		return errors.Wrap(err, "replconf")
	}

	cmd, err = resp.Parse(rw)
	if err != nil {
		return errors.Wrap(err, "resp.Parse")
	}

	fmt.Println(strconv.Quote(string(cmd.Bytes())))
	return nil
}
