package gtevents

import (
	"context"
	"log"
	"time"

	"github.com/midtxwn/geotruth/internal/streampressure"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

const (
	defaultPublisherWorkers              = 2
	defaultPublisherCommitBuffer         = 4096
	defaultPublisherResultBuffer         = 4096
	defaultPublisherMaxInFlightPerWorker = 64
	defaultPublisherInitialBackoff       = 100 * time.Millisecond
	defaultPublisherMaxBackoff           = 5 * time.Second
	defaultPublisherInProgressInterval   = 15 * time.Second
)

type PublisherConfig struct {
	Workers              int
	CommitBuffer         int
	ResultBuffer         int
	MaxInFlightPerWorker int
	InitialBackoff       time.Duration
	MaxBackoff           time.Duration
	InProgressInterval   time.Duration
}

type MessagePublisher interface {
	PublishMsg(context.Context, *nats.Msg, ...jetstream.PublishOpt) (*jetstream.PubAck, error)
}

// Publisher manages a pool of workers that commit envelopes to GT_EVENTS
// via individual synchronous publishes (no atomic batch). Each message in
// the envelope is published sequentially: geofence events -> position/
// lifecycle event -> state record. The state record (last) is the commit
// marker - once it's confirmed stored, all preceding public events are
// guaranteed to exist.
//
// Per-object ordering is preserved by the dispatcher (only one envelope
// per object is in-flight at a time), so multiple pool workers can safely
// process envelopes for different objects concurrently.
type Publisher struct {
	publishers           []MessagePublisher
	commitCh             chan *CommitEnvelope
	resultCh             chan CommitResult
	workers              int
	maxInFlightPerWorker int
	pressure             *streampressure.Monitor

	initialBackoff     time.Duration
	maxBackoff         time.Duration
	inProgressInterval time.Duration
}

type CommitResult struct {
	ObjectID  string
	SourceSeq uint64
	Err       error
}

// NewPublisher creates a publisher with the given worker count.
// If workers <= 0, defaults to the package default worker count.
// The stream parameter is no longer needed (no atomic batch, no
// verifyCommittedWithBackoff).
func NewPublisher(js jetstream.JetStream, workers int) *Publisher {
	return NewPublisherWithMonitor(js, workers, nil)
}

// NewPublisherWithMonitor creates a publisher that reports best-effort
// GT_EVENTS pressure advisories after successful or failed publishes.
func NewPublisherWithMonitor(js jetstream.JetStream, workers int, pressure *streampressure.Monitor) *Publisher {
	return NewPublisherWithMonitorConfig(js, PublisherConfig{Workers: workers}, pressure)
}

// NewPublisherWithMonitorConfig creates a publisher using explicit runtime
// tuning values. Zero values are normalized to the existing defaults.
func NewPublisherWithMonitorConfig(js jetstream.JetStream, cfg PublisherConfig, pressure *streampressure.Monitor) *Publisher {
	cfg = NormalizePublisherConfig(cfg)
	publishers := make([]MessagePublisher, cfg.Workers)
	for i := range publishers {
		publishers[i] = js
	}
	return NewPublisherPoolWithMonitorConfig(publishers, cfg, pressure)
}

// NewPublisherPoolWithMonitorConfig creates a publisher using one backend per
// worker. Each worker publishes whole envelopes sequentially through its own
// backend, preserving envelope message order while avoiding a shared
// connection flusher.
func NewPublisherPoolWithMonitorConfig(publishers []MessagePublisher, cfg PublisherConfig, pressure *streampressure.Monitor) *Publisher {
	cfg = NormalizePublisherConfig(cfg)
	if len(publishers) == 0 {
		publishers = []MessagePublisher{nil}
	}
	cfg.Workers = len(publishers)
	return &Publisher{
		publishers:           publishers,
		commitCh:             make(chan *CommitEnvelope, cfg.CommitBuffer),
		resultCh:             make(chan CommitResult, cfg.ResultBuffer),
		workers:              cfg.Workers,
		maxInFlightPerWorker: cfg.MaxInFlightPerWorker,
		pressure:             pressure,
		initialBackoff:       cfg.InitialBackoff,
		maxBackoff:           cfg.MaxBackoff,
		inProgressInterval:   cfg.InProgressInterval,
	}
}

func NormalizePublisherConfig(cfg PublisherConfig) PublisherConfig {
	if cfg.Workers <= 0 {
		cfg.Workers = defaultPublisherWorkers
	}
	if cfg.CommitBuffer <= 0 {
		cfg.CommitBuffer = defaultPublisherCommitBuffer
	}
	if cfg.ResultBuffer <= 0 {
		cfg.ResultBuffer = defaultPublisherResultBuffer
	}
	if cfg.MaxInFlightPerWorker <= 0 {
		cfg.MaxInFlightPerWorker = defaultPublisherMaxInFlightPerWorker
	}
	if cfg.InitialBackoff <= 0 {
		cfg.InitialBackoff = defaultPublisherInitialBackoff
	}
	if cfg.MaxBackoff <= 0 {
		cfg.MaxBackoff = defaultPublisherMaxBackoff
	}
	if cfg.InProgressInterval <= 0 {
		cfg.InProgressInterval = defaultPublisherInProgressInterval
	}
	return cfg
}

func (p *Publisher) Submit(env *CommitEnvelope) {
	p.commitCh <- env
}

func (p *Publisher) Results() <-chan CommitResult {
	return p.resultCh
}

// Start spawns the publisher pool. Each worker reads from the shared
// commitCh and processes envelopes independently. Per-object ordering
// is guaranteed by the dispatcher's mailbox model (at most one envelope
// per object at a time).
func (p *Publisher) Start(ctx context.Context) {
	for i := 0; i < p.workers; i++ {
		go p.worker(ctx, p.publishers[i])
	}
}

func (p *Publisher) worker(ctx context.Context, js MessagePublisher) {
	sem := make(chan struct{}, p.maxInFlightPerWorker)
	for {
		select {
		case <-ctx.Done():
			return
		case env := <-p.commitCh:
			select {
			case sem <- struct{}{}:
			case <-ctx.Done():
				return
			}
			go func(env *CommitEnvelope) {
				defer func() { <-sem }()
				err := p.commitOne(ctx, js, env)
				result := CommitResult{
					ObjectID:  env.ObjectID,
					SourceSeq: env.SourceSeq,
					Err:       err,
				}
				select {
				case p.resultCh <- result:
				case <-ctx.Done():
				}
			}(env)
		}
	}
}

// commitOne publishes all messages in the envelope sequentially with
// whole-envelope retry. On any single publish failure, the entire
// envelope is retried from index 0. Nats-Msg-Id dedup on each message
// ensures already-published messages are safely skipped by the server.
//
// On context.Canceled, returns immediately (clean shutdown - SPATIAL
// will redeliver after restart, and ShouldSkipSource handles skip).
// All other errors are retried with exponential backoff and InProgress
// heartbeat to prevent SPATIAL AckWait expiry.
func (p *Publisher) commitOne(ctx context.Context, js MessagePublisher, env *CommitEnvelope) error {
	backoff := p.initialBackoff
	nextProgress := time.Now().Add(p.inProgressInterval)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		allPublished := true
		for i := range env.Messages {
			ack, err := js.PublishMsg(ctx, env.Messages[i])
			if err != nil {
				p.pressure.ObservePublishError(env.Messages[i].Subject, err)
				// Publish failed mid-envelope. On retry, Nats-Msg-Id will
				// dedup already-published messages at the server level, so
				// restarting from index 0 is safe and simpler than tracking
				// which individual messages succeeded.
				log.Printf("[publisher] publish failed object=%s seq=%d msg[%d/%d]: %v - retrying",
					env.ObjectID, env.SourceSeq, i+1, len(env.Messages), err)
				allPublished = false
				break
			}
			p.pressure.ObservePublish(env.Messages[i].Subject, ack.Sequence, uint64(len(env.Messages[i].Data)))
		}

		if allPublished {
			return nil
		}

		// InProgress heartbeat prevents SPATIAL AckWait expiry during
		// prolonged retry cycles. The dispatcher guarantees only one envelope
		// per object, so InProgress on the source message extends its delivery
		// deadline while we work through retries.
		if time.Now().After(nextProgress) && env.SourceMsg != nil {
			_ = env.SourceMsg.InProgress()
			nextProgress = time.Now().Add(p.inProgressInterval)
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
		}

		if backoff < p.maxBackoff {
			backoff *= 2
			if backoff > p.maxBackoff {
				backoff = p.maxBackoff
			}
		}
	}
}
