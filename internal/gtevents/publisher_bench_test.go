package gtevents

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

type benchmarkPublisher struct {
	seq atomic.Uint64
}

func (p *benchmarkPublisher) PublishMsg(ctx context.Context, _ *nats.Msg, _ ...jetstream.PublishOpt) (*jetstream.PubAck, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return &jetstream.PubAck{Stream: "GT_EVENTS", Sequence: p.seq.Add(1)}, nil
}

func BenchmarkPublisherCommitOne(b *testing.B) {
	for _, messages := range []int{1, 2, 4, 18} {
		env := benchmarkCommitEnvelope(messages)
		pub := benchmarkGTEventsPublisher(1)
		b.Run(fmt.Sprintf("%d_messages", messages), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				if err := pub.commitOne(context.Background(), pub.publishers[0], env); err != nil {
					b.Fatalf("commitOne: %v", err)
				}
			}
		})
	}
}

func BenchmarkPublisherPoolSubmitAndCommit(b *testing.B) {
	for _, workers := range []int{1, 4, 8, 16} {
		b.Run(fmt.Sprintf("%d_workers", workers), func(b *testing.B) {
			ctx, cancel := context.WithCancel(context.Background())
			b.Cleanup(cancel)

			pub := benchmarkGTEventsPublisher(workers)
			pub.Start(ctx)
			env := benchmarkCommitEnvelope(2)

			b.ReportAllocs()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					pub.Submit(env)
					result := <-pub.Results()
					if result.Err != nil {
						b.Fatalf("publisher result: %v", result.Err)
					}
				}
			})
		})
	}
}

func benchmarkGTEventsPublisher(workers int) *Publisher {
	publishers := make([]MessagePublisher, workers)
	for i := range publishers {
		publishers[i] = &benchmarkPublisher{}
	}
	return &Publisher{
		publishers:           publishers,
		commitCh:             make(chan *CommitEnvelope, 65536),
		resultCh:             make(chan CommitResult, 65536),
		workers:              workers,
		maxInFlightPerWorker: defaultPublisherMaxInFlightPerWorker,
		initialBackoff:       time.Microsecond,
		maxBackoff:           time.Microsecond,
		inProgressInterval:   time.Hour,
	}
}

func benchmarkCommitEnvelope(messages int) *CommitEnvelope {
	msgs := make([]*nats.Msg, messages)
	for i := 0; i < messages; i++ {
		msgs[i] = &nats.Msg{
			Subject: fmt.Sprintf("gt.events.v1.object.bench.position.%d", i),
			Data:    []byte(`{"ok":true}`),
			Header:  nats.Header{"Nats-Msg-Id": []string{fmt.Sprintf("bench-%d", i)}},
		}
	}
	return &CommitEnvelope{
		ObjectID:  "bench-object",
		SourceSeq: 42,
		Messages:  msgs,
		Mutation:  PrevInsideMutation{Kind: PrevInsideNoop},
	}
}
