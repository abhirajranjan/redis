package server

import (
	"sync/atomic"

	"github.com/codecrafters-io/redis-starter-go/app/config"
)

type serverStateConfig struct {
	*config.Config
	bytesProcessed atomic.Int64
}

func (s *serverStateConfig) ReplicationRole() config.Role {
	return s.Config.Replication.Role
}

func (s *serverStateConfig) MasterReplId() string {
	return s.Config.Replication.MasterReplId
}

func (s *serverStateConfig) MasterReplOffset() int64 {
	return int64(s.Config.Replication.MasterReplOffset)
}

func (s *serverStateConfig) BytesProcessed() int64 {
	return s.bytesProcessed.Load()
}
