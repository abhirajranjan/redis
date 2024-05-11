package replication

import (
	"fmt"
	"io"
	"net"
	"strconv"

	"github.com/codecrafters-io/redis-starter-go/app/resp"
	"github.com/pkg/errors"
)

type SlaveConfig struct {
	FnCmd func(net.Conn)
	Host  string
	Port  string
}

type slave struct {
	*SlaveConfig
}

func InitSlave(config *SlaveConfig) {
	slave := slave{
		SlaveConfig: config,
	}

	tcpAddr, err := net.ResolveTCPAddr("tcp", net.JoinHostPort(config.Host, config.Port))
	if err != nil {
		panic(errors.Wrap(err, "ResolveTCPAddr failed"))
	}

	conn, err := net.DialTCP("tcp", nil, tcpAddr)
	if err != nil {
		panic(errors.Wrap(err, "Dial failed"))
	}

	if err := slave.ping(conn); err != nil {
		fmt.Println(err)
		conn.Close()
		return
	}
	if err := slave.replconf(conn); err != nil {
		fmt.Println(err)
		conn.Close()
		return
	}
	if err := slave.psync(conn); err != nil {
		fmt.Println(err)
		conn.Close()
		return
	}

	config.FnCmd(conn)
}

func (s *slave) ping(rw io.ReadWriter) error {
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

func (s *slave) replconf(rw io.ReadWriter) error {
	arr := resp.Array{
		resp.BulkString{Str: "REPLCONF"},
		resp.BulkString{Str: "listening-port"},
		resp.BulkString{Str: s.Port},
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
		resp.BulkString{Str: "eof"},
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

func (s *slave) psync(rw io.ReadWriter) error {
	arr := resp.Array{
		resp.BulkString{Str: "PSYNC"},
		resp.BulkString{Str: "?"},
		resp.BulkString{Str: "-1"},
	}

	_, err := rw.Write(arr.Bytes())
	if err != nil {
		return errors.Wrap(err, "psync")
	}

	cmd, err := resp.Parse(rw)
	if err != nil {
		return errors.Wrap(err, "resp.Parse")
	}

	fmt.Println(strconv.Quote(string(cmd.Bytes())))

	var b [1]byte
	if _, err := rw.Read(b[:]); err != nil {
		return errors.Wrap(err, "error reading rdb first byte")
	}

	if b[0] != '$' {
		return errors.New("expecting $ prefix")
	}

	len, err := resp.ParseInt(rw)
	if err != nil {
		return errors.New("expecting int")
	}

	bin := make([]byte, len)
	if _, err := rw.Read(bin); err != nil {
		return errors.Wrap(err, "error reading rdb")
	}

	return nil
}
