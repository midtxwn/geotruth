package natssync

import (
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/midtxwn/geotruth/pkg/natsconsumer"
	"github.com/midtxwn/geotruth/pkg/natskeys"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/nats-io/nuid"
)

// Config controls the natssync-owned durable GT_EVENTS consumer. natssync
// intentionally does not expose the underlying natsconsumer lifecycle fields:
// it always uses a durable pull consumer, DeliverLastPerSubjectPolicy, and no
// InactiveThreshold so synchronous waits cannot be invalidated by timed
// server-side cleanup.
type Config struct {
	ConsumerName  string
	AckWait       time.Duration
	MaxDeliver    int
	MemoryStorage bool
	Description   string
}

// DefaultConfig returns the default natssync consumer settings. ConsumerName is
// left empty so New can generate a unique durable name for the client.
func DefaultConfig() Config {
	return Config{
		AckWait:    60 * time.Second,
		MaxDeliver: -1,
	}
}

type Client struct {
	nc   *nats.Conn
	cons *natsconsumer.Consumer

	mu       sync.Mutex
	watchers map[string]*objectWatcher
	closed   bool

	clientOpSeq atomic.Uint64
}

// New creates a synchronous object client. The client owns a durable pull
// consumer and deletes it on Close; if the process exits without Close, a
// generated durable consumer may remain on the server and require operational
// cleanup.
func New(nc *nats.Conn, cfg Config) (*Client, error) {
	js, err := jetstream.New(nc)
	if err != nil {
		return nil, fmt.Errorf("jetstream: %w", err)
	}

	if cfg.AckWait == 0 {
		cfg.AckWait = 60 * time.Second
	}
	if cfg.MaxDeliver == 0 {
		cfg.MaxDeliver = -1
	}
	consumerName := cfg.ConsumerName
	if consumerName == "" {
		consumerName = "syncreq-" + nuid.Next()
	}

	cons, err := natsconsumer.New(js, natskeys.GTStreamName, natsconsumer.Config{
		Name:              consumerName,
		Durable:           true,
		DeliverPolicy:     jetstream.DeliverLastPerSubjectPolicy,
		AckWait:           cfg.AckWait,
		MaxDeliver:        cfg.MaxDeliver,
		InactiveThreshold: 0,
		MemoryStorage:     cfg.MemoryStorage,
		Description:       cfg.Description,
	})
	if err != nil {
		return nil, fmt.Errorf("consumer: %w", err)
	}

	return &Client{nc: nc, cons: cons, watchers: make(map[string]*objectWatcher)}, nil
}

func (c *Client) nextClientOpID() string {
	return strconv.FormatUint(c.clientOpSeq.Add(1), 36)
}

func (c *Client) Close() error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	c.closed = true
	watchers := make([]*objectWatcher, 0, len(c.watchers))
	for _, watcher := range c.watchers {
		watchers = append(watchers, watcher)
	}
	c.watchers = make(map[string]*objectWatcher)
	c.mu.Unlock()

	for _, watcher := range watchers {
		watcher.close()
	}

	return c.cons.Close()
}
