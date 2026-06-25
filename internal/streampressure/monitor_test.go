package streampressure

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func TestMonitorPublishesPressureWhenStreamCrossesThreshold(t *testing.T) {
	ctx := context.Background()
	_, nc, js, shutdown := runMonitorNATS(t)
	defer shutdown()

	streamName := "TEST_PRESSURE"
	subject := "test.pressure"
	if _, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:     streamName,
		Subjects: []string{subject},
		Storage:  jetstream.MemoryStorage,
		MaxBytes: 4096,
	}); err != nil {
		t.Fatalf("create stream: %v", err)
	}

	sub, err := nc.SubscribeSync(SubjectStreamPressure(streamName))
	if err != nil {
		t.Fatalf("subscribe pressure: %v", err)
	}
	if err := nc.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}

	ack, err := js.Publish(ctx, subject, make([]byte, 2048))
	if err != nil {
		t.Fatalf("publish test payload: %v", err)
	}

	mon := New(nc, js, streamName, Config{
		Enabled:            true,
		WarnRatio:          0.01,
		CriticalRatio:      0.99,
		MinBytesDelta:      1,
		MinRefreshInterval: time.Hour,
	})
	mon.ObservePublish(subject, ack.Sequence, 2048)

	var event StreamPressureEvent
	nextJSON(t, sub, &event)

	if event.Stream != streamName {
		t.Fatalf("stream = %q, want %q", event.Stream, streamName)
	}
	if event.Level != PressureLevelWarning {
		t.Fatalf("level = %q, want %q", event.Level, PressureLevelWarning)
	}
	if event.Bytes == 0 || event.MaxBytes != 4096 || event.UsageRatio <= 0 {
		t.Fatalf("unexpected usage fields: %+v", event)
	}
	if event.LatestObservedSeq != ack.Sequence {
		t.Fatalf("latest observed seq = %d, want %d", event.LatestObservedSeq, ack.Sequence)
	}
}

func TestMonitorPublishesPublishFailedAdvisory(t *testing.T) {
	_, nc, js, shutdown := runMonitorNATS(t)
	defer shutdown()

	streamName := "TEST_FAILED"
	subject := "test.failed"
	sub, err := nc.SubscribeSync(SubjectStreamPublishFailed(streamName))
	if err != nil {
		t.Fatalf("subscribe publish_failed: %v", err)
	}
	if err := nc.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}

	mon := New(nc, js, streamName, Config{Enabled: true})
	mon.ObservePublish(subject, 42, 10)
	mon.ObservePublishError(subject, errors.New("stream full"))

	var event StreamPublishFailedEvent
	nextJSON(t, sub, &event)

	if event.Stream != streamName {
		t.Fatalf("stream = %q, want %q", event.Stream, streamName)
	}
	if event.Subject != subject {
		t.Fatalf("subject = %q, want %q", event.Subject, subject)
	}
	if event.Error != "stream full" {
		t.Fatalf("error = %q, want stream full", event.Error)
	}
	if event.LatestObservedSeq != 42 {
		t.Fatalf("latest observed seq = %d, want 42", event.LatestObservedSeq)
	}
}

func nextJSON(t *testing.T, sub *nats.Subscription, dst any) {
	t.Helper()
	msg, err := sub.NextMsg(5 * time.Second)
	if err != nil {
		t.Fatalf("next advisory: %v", err)
	}
	if err := json.Unmarshal(msg.Data, dst); err != nil {
		t.Fatalf("unmarshal advisory: %v", err)
	}
}

func runMonitorNATS(t *testing.T) (*natsserver.Server, *nats.Conn, jetstream.JetStream, func()) {
	t.Helper()
	opts := &natsserver.Options{
		Port:      -1,
		JetStream: true,
		StoreDir:  t.TempDir(),
	}
	s, err := natsserver.NewServer(opts)
	if err != nil {
		t.Fatalf("new nats server: %v", err)
	}
	go s.Start()
	if !s.ReadyForConnections(10 * time.Second) {
		t.Fatal("nats server not ready")
	}

	nc, err := nats.Connect(s.ClientURL())
	if err != nil {
		s.Shutdown()
		t.Fatalf("nats connect: %v", err)
	}
	js, err := jetstream.New(nc)
	if err != nil {
		nc.Close()
		s.Shutdown()
		t.Fatalf("jetstream: %v", err)
	}
	return s, nc, js, func() {
		nc.Close()
		s.Shutdown()
	}
}
