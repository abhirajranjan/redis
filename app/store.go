package main

import (
	"fmt"
	"sync"
	"time"
)

type store struct {
	m sync.Map
}

type redisValue struct {
	Val CMD
	Ex  time.Time
}

var Store *store = new(store)

type SetParam struct {
	Ex time.Time
}

func (s *store) Set(k CMD, v CMD, param *SetParam) {
	val := redisValue{Val: v}
	if param != nil {
		if !param.Ex.IsZero() {
			fmt.Println(val.Ex)
			val.Ex = param.Ex
		}
	}

	s.m.Store(k, val)
}

func (s *store) Get(k CMD) (value CMD, ok bool) {
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
