package server

import (
	"strconv"

	"github.com/codecrafters-io/redis-starter-go/app/resp"
)

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
