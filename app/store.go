package main

import (
	"sync"
)

type store struct {
	m sync.Map
}

var Store *store = new(store)

func (s *store) Set(k CMD, v CMD) {
	s.m.Store(k, v)
}

func (s *store) Get(k CMD) (value CMD, ok bool) {
	v, ok := s.m.Load(k)
	if ok {
		return v.(CMD), ok
	}

	return nil, false
}
