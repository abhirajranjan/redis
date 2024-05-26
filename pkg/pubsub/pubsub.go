package pubsub

type PubSub[T any] interface {
	Publish(data T)
	Subscribe() chan T
	Unsubscribe(c chan T)
	NumSubscriber() int64
}

type request[T any] interface{}

type pubSub[T any] struct {
	subscriberMap map[chan T]struct{}
	pub           chan T
	request       chan request[T]
}

var _ PubSub[any] = (*pubSub[any])(nil)

func New[T any]() PubSub[T] {
	r := &pubSub[T]{
		subscriberMap: map[chan T]struct{}{},
		pub:           make(chan T, 1000),
		request:       make(chan request[T]),
	}

	go r.handleEvent()
	return r
}

func (r *pubSub[T]) handleEvent() {
	for {
		select {
		case data := <-r.pub:
			for ch := range r.subscriberMap {
				ch <- data
			}

		case req := <-r.request:
			r.handleRequest(req)
		}
	}
}

func (repl pubSub[T]) handleRequest(req request[T]) {
	switch r := req.(type) {
	case *subRequest[T]:
		repl.subscriberMap[r.Channel()] = struct{}{}

	case *unSubRequest[T]:
		c := r.Channel()
		close(c)
		delete(repl.subscriberMap, c)

	case *numSubRequest[T]:
		r.SetNumSub(len(repl.subscriberMap))
		r.ack <- struct{}{}
	}
}

func (r *pubSub[T]) Publish(data T) {
	r.pub <- data
}

func (r *pubSub[T]) Subscribe() chan T {
	c := make(chan T, 1024)
	r.request <- &subRequest[T]{
		Chan: c,
	}

	return c
}

func (r *pubSub[T]) Unsubscribe(c chan T) {
	r.request <- &unSubRequest[T]{
		Chan: c,
	}
}

func (r *pubSub[T]) NumSubscriber() int64 {
	req := &numSubRequest[T]{
		ack: make(chan struct{}),
	}
	r.request <- req

	<-req.ack
	return int64(req.numSub)
}
