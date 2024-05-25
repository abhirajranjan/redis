package replication

import (
	"bytes"
	"fmt"
	"io"
	"log"
	"math/rand"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/codecrafters-io/redis-starter-go/pkg/pubsub"
	"github.com/codecrafters-io/redis-starter-go/pkg/resp"
)

type cmd interface {
	CmdType() string
}

type pubSub[T any] interface {
	Publish(data T)
	Subscribe() chan T
	Unsubscribe(c chan T)
	NumSubscriber() int64
}

type Replication struct {
	repl            pubSub[cmd]
	NumProcessedMap map[int64]*atomic.Int64
	NumProcessedMu  sync.Mutex
	isFirstCmd      bool
}

func NewReplicaTelemetry() *Replication {
	return &Replication{
		repl:            pubsub.New[cmd](),
		NumProcessedMap: make(map[int64]*atomic.Int64),
		NumProcessedMu:  sync.Mutex{},
		isFirstCmd:      true,
	}
}

func (r *Replication) StartSync(rw io.ReadWriter) {
	ch := r.repl.Subscribe()
	defer r.repl.Unsubscribe(ch)

	for data := range ch {
		switch d := data.(type) {
		case *regularCommand:
			_, err := io.Copy(rw, d)
			if err != nil {
				fmt.Println("WARN", err)
			}

		case *numProcessedCmd:
			if !r.isFirstCmd {
				time.Sleep(time.Second * 1)
				cmd := resp.Array{
					resp.BulkString{Str: "REPLCONF"},
					resp.BulkString{Str: "GETACK"},
					resp.BulkString{Str: "*"},
				}

				if _, err := rw.Write(cmd.Bytes()); err != nil {
					fmt.Println("WARN", err)
					continue
				}

				// get ack
				_, err := resp.Parse(rw)
				if err != nil {
					fmt.Println("WARN", err)
					continue
				}
			}

			r.NumProcessedMu.Lock()
			c, ok := r.NumProcessedMap[d.nonce]
			r.NumProcessedMu.Unlock()

			if ok {
				c.Add(1)
			}
		}
	}
}

func (r *Replication) PublishArray(cmd resp.Array) {
	if !iswriteCMD(cmd) {
		return
	}

	log.Printf("Replication: publish command: %#v\n", cmd)

	r.repl.Publish(&regularCommand{
		Data: bytes.NewReader(cmd.Bytes()),
	})

	if r.isFirstCmd {
		r.isFirstCmd = false
	}
}

func (r *Replication) NumProcessedCmd(atleastAck int64, timeout time.Duration) int64 {
	if atleastAck == 0 || r.repl.NumSubscriber() == 0 {
		return 0
	}

	t := time.NewTimer(timeout)
	nonce := rand.Int63()
	val := &atomic.Int64{}

	r.NumProcessedMu.Lock()
	r.NumProcessedMap[nonce] = val
	r.NumProcessedMu.Unlock()

	r.repl.Publish(&numProcessedCmd{
		nonce: nonce,
	})

	for {
		select {
		case <-t.C:
			r.NumProcessedMu.Lock()
			delete(r.NumProcessedMap, nonce)
			r.NumProcessedMu.Unlock()

			return val.Load()

		default:
			if val.Load() >= atleastAck {
				r.NumProcessedMu.Lock()
				delete(r.NumProcessedMap, nonce)
				r.NumProcessedMu.Unlock()

				return val.Load()
			}
		}
	}

}

func iswriteCMD(cmd resp.Array) bool {
	if len(cmd) == 0 {
		return false
	}

	s, ok := resp.IsString(cmd[0])
	if !ok {
		return false
	}

	switch strings.ToLower(s) {
	case "set":
		return true
	default:
		return false
	}
}
