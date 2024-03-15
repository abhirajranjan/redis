package config

import (
	"os"
	"strconv"
)

type Role string

var (
	RoleMaster  Role = "master"
	RoleSlave   Role = "slave"
	Server      server
	Replication replication
)

type server struct {
	Port int64
}

type replication struct {
	Role             Role `resp:"role"`
	Host             string
	Port             int64
	MasterReplId     string
	MasterReplOffset int
}

func init() {
	Server.Port = 6379
	Replication.Role = RoleMaster
	Replication.MasterReplId = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb"
	Replication.MasterReplOffset = 0

	args := os.Args[1:]
	for idx, v := range args {
		switch v {
		case "--port":
			port, err := strconv.ParseInt(args[idx+1], 10, 64)
			if err != nil {
				panic("port is should be int")
			}
			Server.Port = port

		case "--replicaof":
			host := args[idx+1]
			port, err := strconv.ParseInt(args[idx+2], 10, 64)
			if err != nil {
				panic("port is should be int")
			}

			Replication.Role = RoleSlave
			Replication.Host = host
			Replication.Port = port
		}
	}
}
