package replication

import "fmt"

type Replication[T any] interface {
	Publish(data T)
	Subscribe() chan T
	Unsubscribe(c chan T)
}

type replication[T any] struct {
	subscriberMap map[chan T]struct{}
	sub           chan chan T
	pub           chan T
	unsub         chan chan T
}

func New[T any]() Replication[T] {
	r := &replication[T]{
		subscriberMap: map[chan T]struct{}{},
		sub:           make(chan chan T),
		pub:           make(chan T),
		unsub:         make(chan chan T),
	}

	go r.handleEvent()
	return r
}

func (r *replication[T]) handleEvent() {
	for {
		select {
		case c := <-r.sub:
			r.subscriberMap[c] = struct{}{}
		case data := <-r.pub:
			for ch := range r.subscriberMap {
				fmt.Printf("sending %v on chan %v\n", data, ch)
				ch <- data
			}

		case ch := <-r.unsub:
			close(ch)
			delete(r.subscriberMap, ch)
		}
	}
}

func (r *replication[T]) Publish(data T) {
	r.pub <- data
}

func (r *replication[T]) Subscribe() chan T {
	c := make(chan T, 1024)
	r.sub <- c
	return c
}

func (r *replication[T]) Unsubscribe(c chan T) {
	r.unsub <- c
}
