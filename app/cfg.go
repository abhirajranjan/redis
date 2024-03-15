package main

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"

	"github.com/pkg/errors"
)

type Role string

var (
	RoleMaster  Role        = "master"
	RoleSlave   Role        = "slave"
	Replication replication = replication{Role: RoleMaster}
)

type replication struct {
	Role Role `resp:"role"`
	host string
	port int
}

func (r *replication) Set(s string) error {
	b, a, _ := strings.Cut(s, " ")
	port, err := strconv.ParseInt(a, 10, 64)
	if err != nil {
		return errors.New("replica port should be int")
	}

	r.host = b
	r.port = int(port)
	r.Role = RoleSlave
	return nil
}

func (r replication) String() string {
	b := strings.Builder{}
	b.WriteString("# Replication")

	rv := reflect.ValueOf(r)
	rt := reflect.TypeOf(r)

	var (
		key string
		val string
	)

	for i := 0; i < rv.NumField(); i++ {
		fv := rv.Field(i)
		ft := rt.Field(i)

		if !ft.IsExported() {
			continue
		}
		tg, ok := rt.Field(i).Tag.Lookup("resp")
		if !ok {
			continue
		}

		key = tg

		switch fv.Kind() {
		case reflect.String:
			val = fv.String()

		case reflect.Int:
			valInt := fv.Int()
			val = strconv.FormatInt(valInt, 10)
		}

		b.WriteString(fmt.Sprintf("\n%s:%s\n", key, val))
	}

	return b.String()
}
