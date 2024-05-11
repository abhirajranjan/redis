package config

import (
	"os"
)

type Role string

var (
	RoleMaster Role = "master"
	RoleSlave  Role = "slave"
)

type Config struct {
	Server
	Replication
}

type Server struct {
	Port string
}

type Replication struct {
	Role             Role `resp:"role"`
	Host             string
	Port             string
	MasterReplId     string
	MasterReplOffset int
}

func LoadConfig() *Config {
	config := &Config{
		Server: Server{
			Port: "6379",
		},
		Replication: Replication{
			Role:             RoleMaster,
			MasterReplId:     "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb",
			MasterReplOffset: 0,
		},
	}

	args := os.Args[1:]
	for idx, v := range args {
		switch v {
		case "--port":
			port := args[idx+1]
			config.Server.Port = port

		case "--replicaof":
			host := args[idx+1]
			port := args[idx+2]

			config.Replication.Role = RoleSlave
			config.Replication.Host = host
			config.Replication.Port = port
		}
	}

	return config
}
