package integration

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	privKeys "github.com/midtxwn/geotruth/internal/natskeys"
	"github.com/midtxwn/geotruth/pkg/domain"
	"github.com/midtxwn/geotruth/pkg/natspublish"

	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func startNATS(tb testing.TB) *nats.Conn {
	tb.Helper()
	opts := &natsserver.Options{
		Port:      -1,
		JetStream: true,
		StoreDir:  tb.TempDir(),
		NoLog:     true,
		NoSigs:    true,
	}
	s, err := natsserver.NewServer(opts)
	if err != nil {
		tb.Fatalf("start nats: %v", err)
	}
	go s.Start()
	if !s.ReadyForConnections(5 * time.Second) {
		tb.Fatal("nats server not ready")
	}
	tb.Cleanup(func() { s.Shutdown() })

	nc, err := nats.Connect(s.ClientURL())
	if err != nil {
		tb.Fatalf("nats connect: %v", err)
	}
	tb.Cleanup(func() { nc.Drain() })
	return nc
}

func setupStream(tb testing.TB, nc *nats.Conn) jetstream.JetStream {
	tb.Helper()
	ctx := context.Background()
	js, err := jetstream.New(nc)
	if err != nil {
		tb.Fatalf("jetstream: %v", err)
	}
	_, err = js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:     privKeys.StreamName,
		Subjects: privKeys.StreamSubjects,
		Storage:  jetstream.MemoryStorage,
	})
	if err != nil {
		tb.Fatalf("create stream: %v", err)
	}
	return js
}

func TestFullPipelineRegisterPositionQuery(t *testing.T) {
	nc := startNATS(t)
	js := setupStream(t, nc)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	cons, err := js.CreateOrUpdateConsumer(ctx, privKeys.StreamName, jetstream.ConsumerConfig{
		Durable:        "geotruth",
		FilterSubjects: []string{"pos.>", "cmd.>"},
		AckPolicy:      jetstream.AckExplicitPolicy,
		AckWait:        60 * time.Second,
		MaxAckPending:  10000,
		MaxDeliver:     -1,
		DeliverPolicy:  jetstream.DeliverAllPolicy,
	})
	if err != nil {
		t.Fatalf("create consumer: %v", err)
	}

	regMsg := natspublish.ObjectRegisterMsg{ID: "r1", Dims: domain.ObjectDimensions{Width: 1, Height: 1}}
	regB, _ := json.Marshal(regMsg)
	if _, err := js.Publish(ctx, privKeys.SubjectCmdObjRegister, regB); err != nil {
		t.Fatalf("publish register: %v", err)
	}

	posMsg := natspublish.PositionMsg{ID: "r1", X: 5, Y: 5, Z: 1.0, RotY: 0}
	posB, _ := json.Marshal(posMsg)
	if _, err := js.Publish(ctx, privKeys.SubjectPosRawObject("r1"), posB); err != nil {
		t.Fatalf("publish position: %v", err)
	}

	iter, err := cons.Messages(jetstream.PullMaxMessages(1))
	if err != nil {
		t.Fatalf("messages iterator: %v", err)
	}
	defer iter.Stop()

	received := 0
	for received < 2 {
		msg, err := iter.Next()
		if err != nil {
			t.Fatalf("next message: %v", err)
		}
		msg.Ack()
		received++
	}
	nc.Flush()
	if received != 2 {
		t.Fatalf("expected 2 messages, got %d", received)
	}

	info, err := cons.Info(ctx)
	if err != nil {
		t.Fatalf("consumer info: %v", err)
	}
	if info.AckFloor.Stream < 2 {
		t.Fatalf("expected ack floor >= 2, got %d", info.AckFloor.Stream)
	}
}

func BenchmarkPositionUpdate(b *testing.B) {
	nc := startNATS(b)
	_ = setupStream(b, nc)
	_ = context.Background()

	js, _ := jetstream.New(nc)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		pos := natspublish.PositionMsg{ID: "bench-r", X: float64(i % 100), Y: float64(i % 50), Z: 1.0, RotY: 0}
		pb, _ := json.Marshal(pos)
		js.Publish(context.Background(), privKeys.SubjectPosRawObject("bench-r"), pb)
	}
}
