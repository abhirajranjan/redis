package server

import (
	"sync/atomic"

	"github.com/codecrafters-io/redis-starter-go/app/config"
	"github.com/codecrafters-io/redis-starter-go/pkg/replication"
)

type serverStateConfig[T ~[]byte] struct {
	*config.Config
	repl           replication.Replication[T]
	bytesProcessed atomic.Int64
}

func (s *serverStateConfig[T]) ReplicationRole() config.Role {
	return s.Replication.Role
}

func (s *serverStateConfig[T]) MasterReplId() string {
	return s.Replication.MasterReplId
}

func (s *serverStateConfig[T]) MasterReplOffset() int64 {
	return int64(s.Replication.MasterReplOffset)
}

func (s *serverStateConfig[T]) BytesProcessed() int64 {
	return s.bytesProcessed.Load()
}

func (s *serverStateConfig[T]) ConnectedReplicas() int64 {
	return s.repl.NumSubscriber()
}
