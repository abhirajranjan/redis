package server

import (
	"encoding/hex"
	"fmt"
	"io"
	"strings"
	"syscall"
	"time"

	"github.com/codecrafters-io/redis-starter-go/app/command"
	"github.com/codecrafters-io/redis-starter-go/app/config"
	"github.com/codecrafters-io/redis-starter-go/app/resp"
	"github.com/codecrafters-io/redis-starter-go/app/store"
	"github.com/pkg/errors"
)

var ErrUnknownCMD = errors.New("unknown command")

var CommandRunner = command.Command{
	RunFn: func(_ resp.Array, w io.Writer) error {
		err := errors.New("invalid Command")
		w.Write(resp.SimpleError(err.Error()).Bytes())
		return err
	},
}

func initCommands(s *server) {
	CommandRunner.AddCommand(&command.Command{
		Name:  "ping",
		RunFn: s.ping,
	})

	CommandRunner.AddCommand(&command.Command{
		Name:  "echo",
		RunFn: s.echo,
	})

	CommandRunner.AddCommand(&command.Command{
		Name:  "get",
		RunFn: s.get,
	})

	CommandRunner.AddCommand(&command.Command{
		Name:  "set",
		RunFn: s.set,
	})

	CommandRunner.AddCommand(initInfo(s))
	CommandRunner.AddCommand(initReplConf(s))

	CommandRunner.AddCommand(&command.Command{
		Name:  "psync",
		RunFn: s.psync,
	})
}

func initInfo(s *server) *command.Command {
	info := command.Command{
		Name:  "info",
		RunFn: s.info,
	}

	info.AddCommand(&command.Command{
		Name:  "replication",
		RunFn: s.infoReplication,
	})

	return &info
}

func initReplConf(s *server) *command.Command {
	replConf := command.Command{
		Name:  "replconf",
		RunFn: s.replconf,
	}

	replConf.AddCommand(&command.Command{
		Name:  "listening-port",
		RunFn: s.replConfListeningPort,
	})

	replConf.AddCommand(&command.Command{
		Name:  "getack",
		RunFn: s.replConfGetack,
	})

	replConf.AddCommand(&command.Command{
		Name:  "capa",
		RunFn: s.replConfCapa,
	})

	return &replConf
}

func (s *server) ping(_ resp.Array, w io.Writer) error {
	_, err := w.Write(resp.BulkString{Str: "PONG"}.Bytes())
	return err
}

func (s *server) echo(arr resp.Array, w io.Writer) error {
	if len(arr) == 0 {
		_, err := w.Write(resp.SimpleString("").Bytes())
		return err
	}

	if len(arr) == 1 {
		_, err := w.Write(arr[0].Bytes())
		return err
	}

	_, err := w.Write(arr.Bytes())
	return err
}

func (s *server) get(arr resp.Array, w io.Writer) error {
	if len(arr) != 1 {
		err := errors.New("ERR only 1 arg needed")
		w.Write(resp.SimpleError(err.Error()).Bytes())
		return err
	}

	val, ok := s.store.Get(arr[0])
	if !ok {
		if _, err := w.Write(resp.BulkString{IsNull: true}.Bytes()); err != nil {
			return err
		}
		return nil
	}

	_, err := w.Write(val.Bytes())
	return err
}

func parseSetParam(key, val resp.CMD, p *store.SetParam) error {
	k, ok := resp.IsString(key)
	if !ok {
		return errors.Errorf("ERR %s not string", key)
	}

	v, ok := resp.IsString(val)
	if !ok {
		return errors.Errorf("ERR %s not string", val)
	}

	k = strings.ToLower(k)
	switch k {
	case "px":
		d, err := time.ParseDuration(v + "ms")
		if err != nil {
			return errors.Errorf("ERR %s invalid duration", v)
		}
		p.Ex = time.Now().Add(d)
		return nil

	default:
		return errors.Errorf("ERR invalid param %s", k)
	}
}

func (s *server) set(arr resp.Array, w io.Writer) error {
	param := store.SetParam{}
	if len(arr) < 2 {
		err := errors.New("ERR k,v args needed")
		w.Write(resp.SimpleError(err.Error()).Bytes())
		return err
	}

	for i := 2; i < len(arr)-1; i += 2 {
		if err := parseSetParam(arr[i], arr[i+1], &param); err != nil {
			w.Write(resp.SimpleError(err.Error()).Bytes())
			return err
		}
	}

	s.store.Set(arr[0], arr[1], &param)
	_, err := w.Write(resp.SimpleString("OK").Bytes())
	return err
}

func (srv *server) info(arr resp.Array, w io.Writer) error {
	builder := strings.Builder{}
	if err := srv.infoReplicationString(&builder); err != nil {
		return errors.Wrap(err, "info")
	}

	_, err := w.Write(resp.BulkString{Str: builder.String()}.Bytes())
	return err
}

func (s *server) infoReplication(arr resp.Array, w io.Writer) error {
	builder := strings.Builder{}
	if err := s.infoReplicationString(&builder); err != nil {
		return err
	}

	_, err := w.Write(resp.BulkString{Str: builder.String()}.Bytes())
	return err
}

func (s *server) infoReplicationString(w io.Writer) error {
	if _, err := io.WriteString(w, "# Replication\n"); err != nil {
		return errors.Wrap(err, "replication")
	}

	if _, err := io.WriteString(w, fmt.Sprintf("role:%s\n", config.Replication.Role)); err != nil {
		return errors.Wrap(err, "replication")
	}

	if _, err := io.WriteString(w, fmt.Sprintf("master_replid:%s\n", config.Replication.MasterReplId)); err != nil {
		return errors.Wrap(err, "replication")
	}

	if _, err := io.WriteString(w, fmt.Sprintf("master_repl_offset:%d\n", config.Replication.MasterReplOffset)); err != nil {
		return errors.Wrap(err, "replication")
	}

	return nil
}

func (s *server) replconf(arr resp.Array, w io.Writer) error {
	w.Write(resp.SimpleError("ERR incorrect number of arguments").Bytes())
	return nil
}

func (s *server) replConfListeningPort(arr resp.Array, w io.Writer) error {
	if len(arr) < 1 {
		w.Write(resp.SimpleError("ERR incorrect number of arguments").Bytes())
		return nil
	}

	fmt.Println("port", arr[0])
	w.Write(resp.SimpleString("OK").Bytes())
	return nil
}

func (s *server) replConfCapa(arr resp.Array, w io.Writer) error {
	w.Write(resp.SimpleString("OK").Bytes())
	return nil
}

func (s *server) replConfGetack(arr resp.Array, w io.Writer) error {
	if len(arr) == 0 {
		return errors.New("expected atleast one args")
	}

	offset, ok := resp.IsString(arr[0])
	if !ok {
		return errors.New("GetAck: expected string arg")
	}

	if offset == "*" {
		w.Write(resp.Array{
			resp.BulkString{Str: "REPLCONF"},
			resp.BulkString{Str: "ACK"},
			resp.BulkString{Str: "*"},
		}.Bytes())
		return nil
	}

	return nil
}

func (s *server) psync(arr resp.Array, w io.Writer) error {
	if len(arr) < 2 {
		w.Write(resp.SimpleError("ERR incorrect number of arguments").Bytes())
		return nil
	}

	replid, ok := resp.IsString(arr[0])
	if !ok {
		err := errors.Errorf("ERR expected string type %s", arr[0])
		w.Write(resp.SimpleError(err.Error()).Bytes())
		return err
	}

	offset, ok := resp.IsInt(arr[1])
	if !ok {
		err := errors.Errorf("ERR expected int type %s", arr[1])
		w.Write(resp.SimpleError(err.Error()).Bytes())
		return err
	}

	if replid == "?" {
		replid = config.Replication.MasterReplId
	}

	if offset == -1 {
		offset = int64(config.Replication.MasterReplOffset)
	}

	w.Write(resp.SimpleString(fmt.Sprintf("FULLRESYNC %s %d", replid, offset)).Bytes())

	bin, err := hex.DecodeString("524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2")
	if err != nil {
		panic(err)
	}

	ch := s.replication.Subscribe()

	wbin := []byte(fmt.Sprintf("$%d\r\n", len(bin)))
	wbin = append(wbin, bin...)
	w.Write(wbin)

	for {
		data := <-ch
		if _, err := w.Write(data); err != nil {
			if errors.Is(err, syscall.ECONNRESET) {
				fmt.Println("unsub")
				s.replication.Unsubscribe(ch)
				break
			}

			fmt.Println("error replicating:", err)
		}
	}

	return nil
}
