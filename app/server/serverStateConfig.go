package server

import (
	"sync/atomic"

	"github.com/codecrafters-io/redis-starter-go/app/config"
)

type serverStateConfig struct {
	bytesProcessed atomic.Int64
	*config.Config
}

func (s *serverStateConfig) ReplicationRole() config.Role {
	return s.Replication.Role
}

func (s *serverStateConfig) MasterReplId() string {
	return s.Replication.MasterReplId
}

func (s *serverStateConfig) MasterReplOffset() int64 {
	return int64(s.Replication.MasterReplOffset)
}

func (s *serverStateConfig) BytesProcessed() int64 {
	return s.bytesProcessed.Load()
}
