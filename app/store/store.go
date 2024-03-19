package store

import (
	"fmt"
	"sync"
	"time"

	"github.com/codecrafters-io/redis-starter-go/app/resp"
)

type redisValue struct {
	Val resp.CMD
	Ex  time.Time
}

type Store struct {
	m sync.Map
}

func NewStore() *Store {
	return &Store{
		m: sync.Map{},
	}
}

type SetParam struct {
	Ex time.Time
}

func (s *Store) Set(k resp.CMD, v resp.CMD, param *SetParam) {
	val := redisValue{Val: v}
	if param != nil {
		if !param.Ex.IsZero() {
			fmt.Println(val.Ex)
			val.Ex = param.Ex
		}
	}

	s.m.Store(k, val)
}

func (s *Store) Get(k resp.CMD) (value resp.CMD, ok bool) {
	v, ok := s.m.Load(k)
	if !ok {
		return nil, false
	}

	rv := v.(redisValue)
	fmt.Println(rv.Ex.IsZero(), rv.Ex.Before(time.Now()))
	if !rv.Ex.IsZero() && rv.Ex.Before(time.Now()) {
		go s.m.Delete(k)
		return nil, false
	}

	return rv.Val, true
}
