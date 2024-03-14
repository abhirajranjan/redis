package main

import (
	"io"
	"strings"

	"github.com/pkg/errors"
)

var ErrUnknownCMD = errors.New("unknown command")

func HandleFunc(args Array, w io.Writer) error {
	if len(args) < 1 {
		return ErrUnknownCMD
	}

	cmd, ok := args[0].(BulkString)
	if !ok {
		return errors.WithMessagef(ErrUnknownCMD, "'%s' error casting to string", args[0])
	}

	args = args[1:]
	switch strings.ToLower(cmd.string) {
	case "ping":
		return ping(args, w)
	case "echo":
		return echo(args, w)
	case "get":
		return get(args, w)
	case "set":
		return set(args, w)
	}
	return errors.WithMessagef(ErrUnknownCMD, "%s", args[0])
}

func ping(_ Array, w io.Writer) error {
	_, err := w.Write(BulkString{string: "PONG"}.Bytes())
	return err
}

func echo(arr Array, w io.Writer) error {
	if len(arr) == 0 {
		_, err := w.Write(SimpleString("").Bytes())
		return err
	}

	if len(arr) == 1 {
		_, err := w.Write(arr[0].Bytes())
		return err
	}

	_, err := w.Write(arr.Bytes())
	return err
}

func get(arr Array, w io.Writer) error {
	if len(arr) != 1 {
		err := errors.New("ERR only 1 arg needed")
		w.Write(SimpleError(err.Error()).Bytes())
		return err
	}

	val, ok := Store.Get(arr[0])
	if !ok {
		if _, err := w.Write(BulkString{IsNull: true}.Bytes()); err != nil {
			return err
		}
		return nil
	}

	_, err := w.Write(val.Bytes())
	return err
}

func set(arr Array, w io.Writer) error {
	if len(arr) != 2 {
		err := errors.New("ERR only k,v args needed")
		w.Write(SimpleError(err.Error()).Bytes())
		return err
	}

	Store.Set(arr[0], arr[1])
	_, err := w.Write(SimpleString("OK").Bytes())
	return err
}

func String(val CMD) (string, bool) {
	switch v := val.(type) {
	case SimpleString:
		return string(v), true
	case BulkString:
		return v.string, true
	default:
		return "", false
	}
}
