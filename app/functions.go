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
	switch strings.ToLower(string(cmd)) {
	case "ping":
		return ping(args, w)
	case "echo":
		return echo(args, w)
	}
	return errors.WithMessagef(ErrUnknownCMD, "%s", args[0])
}

func ping(_ Array, w io.Writer) error {
	_, err := w.Write(BulkString("PONG").Bytes())
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
