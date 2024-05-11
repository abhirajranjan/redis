package server

import (
	"fmt"
	"io"
	"net"
	"strconv"

	"github.com/codecrafters-io/redis-starter-go/app/commandHandler"
	"github.com/codecrafters-io/redis-starter-go/pkg/resp"
	"github.com/pkg/errors"
)

func (s *server) initSlave() {
	tcpAddr, err := net.ResolveTCPAddr("tcp", net.JoinHostPort(s.stateConfig.Replication.Host, s.stateConfig.Replication.Port))
	if err != nil {
		panic(errors.Wrap(err, "ResolveTCPAddr failed"))
	}

	conn, err := net.DialTCP("tcp", nil, tcpAddr)
	if err != nil {
		panic(errors.Wrap(err, "Dial failed"))
	}

	if err := s.ping(conn); err != nil {
		fmt.Println(err)
		conn.Close()
		return
	}
	if err := s.replconf(conn); err != nil {
		fmt.Println(err)
		conn.Close()
		return
	}
	if err := s.psync(conn); err != nil {
		fmt.Println(err)
		conn.Close()
		return
	}

	s.handleSlaveConn(conn)
}

func (s *server) handleSlaveConn(conn io.ReadWriteCloser) {
	defer conn.Close()

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
	}
}

func (s *server) ping(rw io.ReadWriter) error {
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

func (s *server) replconf(rw io.ReadWriter) error {
	arr := resp.Array{
		resp.BulkString{Str: "REPLCONF"},
		resp.BulkString{Str: "listening-port"},
		resp.BulkString{Str: s.stateConfig.Server.Port},
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

func (s *server) psync(rw io.ReadWriter) error {
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
