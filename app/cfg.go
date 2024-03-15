package main

import (
	"os"
	"strconv"
)

type Role string

var (
	RoleMaster Role = "master"
	RoleSlave  Role = "slave"
	Config     config
)

type config struct {
	Replication replication
	Server      server
}

type server struct {
	Port int
}

type replication struct {
	Role Role `resp:"role"`
	Host string
	Port int
}

func init() {
	Config.Server.Port = 6379
	Config.Replication.Role = RoleMaster

	args := os.Args[1:]
	for idx, v := range args {
		switch v {
		case "--port":
			port, err := strconv.ParseInt(args[idx+1], 10, 64)
			if err != nil {
				panic("port is should be int")
			}
			Config.Server.Port = int(port)

		case "--replicaof":
			host := args[idx+1]
			port, err := strconv.ParseInt(args[idx+2], 10, 64)
			if err != nil {
				panic("port is should be int")
			}

			Config.Replication.Role = RoleSlave
			Config.Replication.Host = host
			Config.Replication.Port = int(port)
		}
	}
}
