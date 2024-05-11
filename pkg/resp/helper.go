package resp

import (
	"strconv"
)

func IsString(val CMD) (string, bool) {
	switch v := val.(type) {
	case SimpleString:
		return string(v), true
	case BulkString:
		return v.Str, true
	case Int:
		return strconv.FormatInt(int64(v), 10), true
	default:
		return "", false
	}
}

func IsInt(val CMD) (int64, bool) {
	if v, ok := val.(Int); ok {
		return int64(v), true
	}

	cmd, ok := IsString(val)
	if ok {
		i, err := strconv.ParseInt(cmd, 10, 64)
		if err != nil {
			return 0, false
		}
		return i, true
	}

	return 0, false
}
