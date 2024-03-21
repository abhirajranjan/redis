package replication

import "fmt"

type Replication[T any] struct {
	subscriberMap map[chan T]struct{}
	sub           chan chan T
	pub           chan T
	unsub         chan chan T
}

func (r *Replication[T]) Init() {
	r.subscriberMap = map[chan T]struct{}{}
	r.sub = make(chan chan T)
	r.pub = make(chan T)
	r.unsub = make(chan chan T)

	go func() {
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
	}()
}

func (r *Replication[T]) Publish(data T) {
	r.pub <- data
}

func (r *Replication[T]) Subscribe() chan T {
	c := make(chan T, 1024)
	r.sub <- c
	return c
}

func (r *Replication[T]) Unsubscribe(c chan T) {
	r.unsub <- c
}
