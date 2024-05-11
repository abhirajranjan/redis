package command

import (
	"io"
	"strings"

	"github.com/codecrafters-io/redis-starter-go/pkg/resp"
	"github.com/pkg/errors"
)

type Command struct {
	// case insensitive
	Name  string
	RunFn func(resp.Array, io.Writer) error

	subCmd map[string]*Command
}

func (f *Command) AddCommand(c *Command) {
	name := normalizeString(c.Name)
	if f.subCmd == nil {
		f.subCmd = make(map[string]*Command)
	}
	f.subCmd[name] = c
}

func (f *Command) Run(arr resp.Array, w io.Writer) error {
	if f.subCmd == nil || len(f.subCmd) == 0 || len(arr) == 0 {
		return f.RunFn(arr, w)
	}

	s, ok := resp.IsString(arr[0])
	if !ok {
		return errors.Errorf("command %v expected to be ~string", arr[0])
	}

	s = normalizeString(s)
	cmd, ok := f.subCmd[s]
	if !ok {
		return errors.Errorf("invalid command %s", s)
	}

	return cmd.Run(arr[1:], w)
}

func normalizeString(s string) string {
	return strings.ToLower(s)
}
