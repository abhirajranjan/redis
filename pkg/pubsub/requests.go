package pubsub

type subRequest[T any] struct {
	Chan chan T
}

var _ request[any] = (*subRequest[any])(nil)

func (r *subRequest[T]) Channel() chan T {
	return r.Chan
}

type unSubRequest[T any] struct {
	Chan chan T
}

var _ request[any] = (*unSubRequest[any])(nil)

func (r *unSubRequest[T]) Channel() chan T {
	return r.Chan
}

type numSubRequest[T any] struct {
	numSub int
	ack    chan struct{}
}

var _ request[any] = (*numSubRequest[any])(nil)

func (r *numSubRequest[T]) SetNumSub(num int) {
	r.numSub = num
}
