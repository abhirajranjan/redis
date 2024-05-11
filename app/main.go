package main

import (
	"github.com/codecrafters-io/redis-starter-go/app/config"
	"github.com/codecrafters-io/redis-starter-go/app/server"
)

func main() {
	cfg := config.LoadConfig()
	s := server.NewServer(cfg)
	s.Run()
}
