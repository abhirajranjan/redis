package main

import (
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/codecrafters-io/redis-starter-go/app/config"
	"github.com/codecrafters-io/redis-starter-go/app/resp"
	"github.com/pkg/errors"
)

var ErrUnknownCMD = errors.New("unknown command")

func HandleFunc(args resp.Array, w io.Writer) error {
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
		return ping(args, w)
	case "echo":
		return echo(args, w)
	case "get":
		return get(args, w)
	case "set":
		return set(args, w)
	case "info":
		return info(args, w)
	case "replconf":
		return replconf(args, w)
	case "psync":
		return psync(args, w)
	}
	return errors.WithMessagef(ErrUnknownCMD, "%s", args[0])
}

func ping(_ resp.Array, w io.Writer) error {
	_, err := w.Write(resp.BulkString{Str: "PONG"}.Bytes())
	return err
}

func echo(arr resp.Array, w io.Writer) error {
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

func get(arr resp.Array, w io.Writer) error {
	if len(arr) != 1 {
		err := errors.New("ERR only 1 arg needed")
		w.Write(resp.SimpleError(err.Error()).Bytes())
		return err
	}

	val, ok := Store.Get(arr[0])
	if !ok {
		if _, err := w.Write(resp.BulkString{IsNull: true}.Bytes()); err != nil {
			return err
		}
		return nil
	}

	_, err := w.Write(val.Bytes())
	return err
}

func set(arr resp.Array, w io.Writer) error {
	param := SetParam{}
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

	Store.Set(arr[0], arr[1], &param)
	_, err := w.Write(resp.SimpleString("OK").Bytes())
	return err
}

func info(arr resp.Array, w io.Writer) error {
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
			str += repl()
		}
	}

	_, err := w.Write(resp.BulkString{Str: str}.Bytes())
	return err
}

func repl() string {
	b := strings.Builder{}
	b.WriteString("# Replication\n")
	b.WriteString(fmt.Sprintf("role:%s\n", config.Replication.Role))
	b.WriteString(fmt.Sprintf("master_replid:%s\n", config.Replication.MasterReplId))
	b.WriteString(fmt.Sprintf("master_repl_offset:%d\n", config.Replication.MasterReplOffset))
	return b.String()
}

func parseSetParam(key, val resp.CMD, p *SetParam) error {
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

func replconf(arr resp.Array, w io.Writer) error {
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

func psync(arr resp.Array, w io.Writer) error {
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
	return nil
}

func String(val resp.CMD) (string, bool) {
	switch v := val.(type) {
	case resp.SimpleString:
		return string(v), true
	case resp.BulkString:
		return v.Str, true
	default:
		return "", false
	}
}

func Int(val resp.CMD) (int64, bool) {
	if v, ok := val.(resp.Int); ok {
		return int64(v), true
	}

	cmd, ok := String(val)
	if ok {
		i, err := strconv.ParseInt(cmd, 10, 64)
		if err != nil {
			return 0, false
		}
		return i, true
	}

	return 0, false
}
