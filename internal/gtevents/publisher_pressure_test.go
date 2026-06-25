package gtevents

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/midtxwn/geotruth/internal/streampressure"
	"github.com/midtxwn/geotruth/pkg/domain"
	"github.com/midtxwn/geotruth/pkg/natskeys"

	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func TestPublisherReportsGTEventsPressureAfterCommit(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	nc, js, shutdown := runPublisherPressureNATS(t)
	defer shutdown()

	if _, err := EnsureStream(ctx, js, StreamConfig{
		Storage:  jetstream.FileStorage,
		MaxBytes: 16 * 1024,
		Replicas: 1,
	}); err != nil {
		t.Fatalf("ensure GT_EVENTS: %v", err)
	}

	sub, err := nc.SubscribeSync(streampressure.SubjectStreamPressure(natskeys.GTStreamName))
	if err != nil {
		t.Fatalf("subscribe pressure: %v", err)
	}
	if err := nc.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}

	monitor := streampressure.New(nc, js, natskeys.GTStreamName, streampressure.Config{
		Enabled:            true,
		WarnRatio:          0.001,
		CriticalRatio:      0.99,
		MinBytesDelta:      1,
		MinRefreshInterval: time.Hour,
	})
	publisher := NewPublisherWithMonitor(js, 1, monitor)
	publisher.Start(ctx)

	msgs, err := BuildRegisterCommitMsgs("pressure-object", 7, "", domain.ObjectDimensions{Width: 1, Height: 1})
	if err != nil {
		t.Fatalf("build commit msgs: %v", err)
	}
	publisher.Submit(&CommitEnvelope{
		ObjectID:  "pressure-object",
		SourceSeq: 7,
		Messages:  msgs,
	})

	select {
	case result := <-publisher.Results():
		if result.Err != nil {
			t.Fatalf("commit result: %v", result.Err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for commit result")
	}

	raw, err := sub.NextMsg(5 * time.Second)
	if err != nil {
		t.Fatalf("next pressure advisory: %v", err)
	}
	var event streampressure.StreamPressureEvent
	if err := json.Unmarshal(raw.Data, &event); err != nil {
		t.Fatalf("unmarshal pressure advisory: %v", err)
	}
	if event.Stream != natskeys.GTStreamName {
		t.Fatalf("stream = %q, want %q", event.Stream, natskeys.GTStreamName)
	}
	if event.Level != streampressure.PressureLevelWarning {
		t.Fatalf("level = %q, want %q", event.Level, streampressure.PressureLevelWarning)
	}
	if event.LatestObservedSeq == 0 {
		t.Fatalf("latest observed seq was not recorded: %+v", event)
	}
}

func runPublisherPressureNATS(t *testing.T) (*nats.Conn, jetstream.JetStream, func()) {
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
	return nc, js, func() {
		nc.Close()
		s.Shutdown()
	}
}
