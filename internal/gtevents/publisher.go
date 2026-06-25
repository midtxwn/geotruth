package gtevents

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/midtxwn/geotruth/internal/streampressure"
	"github.com/midtxwn/geotruth/pkg/natskeys"

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
	GetLastMsgForSubject(context.Context, string) (*jetstream.RawStreamMsg, error)
}

type jetStreamPublisher struct {
	js     jetstream.JetStream
	stream jetstream.Stream
}

func NewJetStreamPublisher(js jetstream.JetStream, stream jetstream.Stream) MessagePublisher {
	return jetStreamPublisher{js: js, stream: stream}
}

func (p jetStreamPublisher) PublishMsg(ctx context.Context, msg *nats.Msg, opts ...jetstream.PublishOpt) (*jetstream.PubAck, error) {
	return p.js.PublishMsg(ctx, msg, opts...)
}

func (p jetStreamPublisher) GetLastMsgForSubject(ctx context.Context, subject string) (*jetstream.RawStreamMsg, error) {
	if p.stream == nil {
		return nil, fmt.Errorf("publisher stream verifier is not configured")
	}
	return p.stream.GetLastMsgForSubject(ctx, subject)
}

// Publisher manages a pool of workers that commit envelopes to GT_EVENTS
// via individual synchronous publishes (no atomic batch). The object commit
// event is published first and acts as the recovery checkpoint; geofence
// projection events are published afterwards and can be repaired from that
// checkpoint if the process crashes.
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
	ObjectID   string
	InstanceID string
	CommitSeq  uint64
	Reply      string
	Err        error
}

// NewPublisher creates a publisher with the given worker count.
// If workers <= 0, defaults to the package default worker count.
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
		publishers[i] = NewJetStreamPublisher(js, nil)
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
					ObjectID:   env.ObjectID,
					InstanceID: env.InstanceID,
					CommitSeq:  env.CommitSeq,
					Reply:      env.Reply,
					Err:        err,
				}
				select {
				case p.resultCh <- result:
				case <-ctx.Done():
				}
			}(env)
		}
	}
}

// commitOne publishes the commit event first and then projection events. Once
// the commit event is confirmed, retries never republish it; a crash between
// commit and projection completion is repaired at startup from StateAfter.
func (p *Publisher) commitOne(ctx context.Context, js MessagePublisher, env *CommitEnvelope) error {
	backoff := p.initialBackoff
	commitPublished := false

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		if !commitPublished {
			ack, err := p.publishOrVerify(ctx, js, env.Commit)
			if err != nil {
				log.Printf("[publisher] commit publish failed object=%s instance=%s commit=%d: %v - retrying",
					env.ObjectID, env.InstanceID, env.CommitSeq, err)
				p.sleepBackoff(ctx, &backoff)
				continue
			}
			p.pressure.ObservePublish(env.Commit.Subject, ack.Sequence, uint64(len(env.Commit.Data)))
			commitPublished = true
		}

		projectionsPublished := true
		for i := range env.Projections {
			ack, err := p.publishOrVerify(ctx, js, env.Projections[i])
			if err != nil {
				log.Printf("[publisher] projection publish failed object=%s instance=%s commit=%d msg[%d/%d]: %v - retrying",
					env.ObjectID, env.InstanceID, env.CommitSeq, i+1, len(env.Projections), err)
				projectionsPublished = false
				break
			}
			p.pressure.ObservePublish(env.Projections[i].Subject, ack.Sequence, uint64(len(env.Projections[i].Data)))
		}
		if projectionsPublished {
			return nil
		}

		p.sleepBackoff(ctx, &backoff)
	}
}

func (p *Publisher) publishOrVerify(ctx context.Context, js MessagePublisher, msg *nats.Msg) (*jetstream.PubAck, error) {
	ack, err := js.PublishMsg(ctx, msg)
	if err == nil {
		return ack, nil
	}
	p.pressure.ObservePublishError(msg.Subject, err)

	expectedID := eventIDFromData(msg.Data)
	raw, getErr := js.GetLastMsgForSubject(ctx, msg.Subject)
	if getErr == nil && raw != nil && eventIDFromData(raw.Data) == expectedID {
		return &jetstream.PubAck{Stream: natskeys.GTStreamName, Sequence: raw.Sequence, Duplicate: true}, nil
	}
	return nil, err
}

func eventIDFromData(data []byte) string {
	var event struct {
		EventID string `json:"event_id"`
	}
	if err := json.Unmarshal(data, &event); err != nil {
		return ""
	}
	return event.EventID
}

func (p *Publisher) sleepBackoff(ctx context.Context, backoff *time.Duration) {
	select {
	case <-ctx.Done():
	case <-time.After(*backoff):
	}
	if *backoff < p.maxBackoff {
		*backoff *= 2
		if *backoff > p.maxBackoff {
			*backoff = p.maxBackoff
		}
	}
}
