package replication

import (
	"fmt"
	"strconv"
)

type Replication[T ~[]byte] interface {
	Publish(data T)
	Subscribe() chan T
	Unsubscribe(c chan T)
	NumSubscriber() int64
}

type request[T ~[]byte] struct {
	requestType requestType
	param       chan T
}

type requestType int

const (
	numSubRequest requestType = iota
	SubRequest
	UnsubRequest
)

type replication[T ~[]byte] struct {
	subscriberMap map[chan T]struct{}
	pub           chan T
	request       chan request[T]
}

func New[T ~[]byte]() Replication[T] {
	r := &replication[T]{
		subscriberMap: map[chan T]struct{}{},
		pub:           make(chan T),
		request:       make(chan request[T]),
	}

	go r.handleEvent()
	return r
}

func (r *replication[T]) handleEvent() {
	for {
		select {
		case data := <-r.pub:
			for ch := range r.subscriberMap {
				fmt.Printf("sending %v on chan %v\n", data, ch)
				ch <- data
			}

		case req := <-r.request:
			r.handleRequest(req)
		}
	}
}

func (r replication[T]) handleRequest(req request[T]) {
	switch req.requestType {
	case SubRequest:
		r.subscriberMap[req.param] = struct{}{}

	case UnsubRequest:
		close(req.param)
		delete(r.subscriberMap, req.param)

	case numSubRequest:
		req.param <- []byte(string(strconv.FormatInt(int64(len(r.subscriberMap)), 10)))
	}
}

func (r *replication[T]) Publish(data T) {
	r.pub <- data
}

func (r *replication[T]) Subscribe() chan T {
	c := make(chan T, 1024)
	r.request <- request[T]{
		requestType: SubRequest,
		param:       c,
	}
	return c
}

func (r *replication[T]) Unsubscribe(c chan T) {
	r.request <- request[T]{
		requestType: UnsubRequest,
		param:       c,
	}
}

func (r *replication[T]) NumSubscriber() int64 {
	c := make(chan T)
	defer close(c)

	req := request[T]{
		requestType: numSubRequest,
		param:       c,
	}

	r.request <- req
	b := <-c

	v, err := strconv.ParseInt(string(b), 10, 64)
	if err != nil {
		fmt.Println("Replication: NumSubscriber: error parsing numSub to int")
		return 0
	}

	return v
}
