package server

import (
	"encoding/hex"
	"fmt"
	"io"
	"strings"
	"syscall"
	"time"

	"github.com/codecrafters-io/redis-starter-go/app/config"
	"github.com/codecrafters-io/redis-starter-go/app/resp"
	"github.com/codecrafters-io/redis-starter-go/app/store"
	"github.com/pkg/errors"
)

var ErrUnknownCMD = errors.New("unknown command")

func (s server) handleFunc(args resp.Array, w io.Writer) error {
	if len(args) < 1 {
		return ErrUnknownCMD
	}

	cmd, ok := String(args[0])
	if !ok {
		return errors.WithMessagef(ErrUnknownCMD, "'%s' error casting to string", args[0])
	}

	args = args[1:]
	switch strings.ToLower(cmd) {
	case "ping":
		return s.ping(args, w)
	case "echo":
		return s.echo(args, w)
	case "get":
		return s.get(args, w)
	case "set":
		return s.set(args, w)
	case "info":
		return s.info(args, w)
	case "replconf":
		return s.replconf(args, w)
	case "psync":
		return s.psync(args, w)
	}
	return errors.WithMessagef(ErrUnknownCMD, "%s", args[0])
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
	str := ""
	if len(arr) == 0 {
		arr = append(arr, resp.SimpleString("replication"))
	}

	for _, v := range arr {
		s, ok := String(v)
		if !ok {
			err := errors.New("ERR require string type")
			w.Write(resp.SimpleError(err.Error()).Bytes())
			return err
		}

		if s == "replication" {
			str += srv.repl()
		}
	}

	_, err := w.Write(resp.BulkString{Str: str}.Bytes())
	return err
}

func (s *server) repl() string {
	b := strings.Builder{}
	b.WriteString("# Replication\n")
	b.WriteString(fmt.Sprintf("role:%s\n", config.Replication.Role))
	b.WriteString(fmt.Sprintf("master_replid:%s\n", config.Replication.MasterReplId))
	b.WriteString(fmt.Sprintf("master_repl_offset:%d\n", config.Replication.MasterReplOffset))
	return b.String()
}

func parseSetParam(key, val resp.CMD, p *store.SetParam) error {
	k, ok := String(key)
	if !ok {
		return errors.Errorf("ERR %s not string", key)
	}

	v, ok := String(val)
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

func (s *server) replconf(arr resp.Array, w io.Writer) error {
	if len(arr) < 1 {
		w.Write(resp.SimpleError("ERR incorrect number of arguments").Bytes())
		return nil
	}

	cmd, ok := String(arr[0])
	if !ok {
		err := errors.Errorf("ERR expected string type %s", arr[0])
		w.Write(resp.SimpleError(err.Error()).Bytes())
		return err
	}
	switch strings.ToLower(cmd) {
	case "listening-port":
		if len(arr) < 2 {
			w.Write(resp.SimpleError("ERR incorrect number of arguments").Bytes())
			return nil
		}

		fmt.Println("port", arr[1])
		w.Write(resp.SimpleString("OK").Bytes())
		return nil

	case "capa":
		w.Write(resp.SimpleString("OK").Bytes())
		return nil
	}
	return nil
}

func (s *server) psync(arr resp.Array, w io.Writer) error {
	if len(arr) < 2 {
		w.Write(resp.SimpleError("ERR incorrect number of arguments").Bytes())
		return nil
	}

	replid, ok := String(arr[0])
	if !ok {
		err := errors.Errorf("ERR expected string type %s", arr[0])
		w.Write(resp.SimpleError(err.Error()).Bytes())
		return err
	}

	offset, ok := Int(arr[1])
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
