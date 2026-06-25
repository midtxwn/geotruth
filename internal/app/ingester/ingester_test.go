package ingester

import (
	"context"
	"encoding/json"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/midtxwn/geotruth/pkg/domain"
	pkgingester "github.com/midtxwn/geotruth/pkg/ingester"
	"github.com/midtxwn/geotruth/pkg/messages"
	"github.com/midtxwn/geotruth/pkg/natskeys"
	"github.com/midtxwn/geotruth/pkg/natspublish"

	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func TestObjectPublishesDoNotSetMsgIDHeader(t *testing.T) {
	opts := &natsserver.Options{
		Port:      -1,
		JetStream: true,
		StoreDir:  t.TempDir(),
		NoLog:     true,
		NoSigs:    true,
	}
	s, err := natsserver.NewServer(opts)
	if err != nil {
		t.Fatalf("start nats: %v", err)
	}
	go s.Start()
	if !s.ReadyForConnections(5 * time.Second) {
		t.Fatal("nats server not ready")
	}
	t.Cleanup(s.Shutdown)

	connect := func() (*nats.Conn, error) {
		return nats.Connect(s.ClientURL(),
			nats.RetryOnFailedConnect(true),
			nats.MaxReconnects(-1),
			nats.ReconnectWait(2*time.Second),
		)
	}
	nc, err := connect()
	if err != nil {
		t.Fatalf("connect nats: %v", err)
	}
	t.Cleanup(func() { nc.Close() })

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	ing, err := Run(ctx, pkgingester.Config{Storage: jetstream.MemoryStorage, Replicas: 1, ShardCount: 1}, pkgingester.Dependencies{
		NATS: func(role string) (*nats.Conn, error) {
			return connect()
		},
	})
	if err != nil {
		t.Fatalf("run ingester: %v", err)
	}
	select {
	case <-ing.Ready():
	case <-time.After(5 * time.Second):
		t.Fatal("ingester not ready")
	}

	registerReq, _ := json.Marshal(natspublish.RegisterObjectReq{
		ID:   "msgid-test",
		Dims: domain.ObjectDimensions{Width: 1, Height: 1},
	})
	resp, err := nc.Request(natspublish.IngesterRegisterObjectSubject("msgid-test"), registerReq, 5*time.Second)
	if err != nil {
		t.Fatalf("register request: %v", err)
	}
	if _, err := messages.Data[sourceSeqPayload](resp.Data); err != nil {
		t.Fatalf("register response: %v", err)
	}

	updateReq, _ := json.Marshal(natspublish.UpdatePositionReq{
		ID:   "msgid-test",
		X:    1,
		Y:    2,
		Z:    1,
		RotY: 0,
	})
	resp, err = nc.Request(natspublish.IngesterUpdatePositionSubject("msgid-test"), updateReq, 5*time.Second)
	if err != nil {
		t.Fatalf("position request: %v", err)
	}
	if err := messages.Err(resp.Data); err != nil {
		t.Fatalf("position response: %v", err)
	}

	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream: %v", err)
	}
	stream, err := js.Stream(context.Background(), natskeys.StreamName)
	if err != nil {
		t.Fatalf("stream: %v", err)
	}
	cons, err := stream.CreateConsumer(context.Background(), jetstream.ConsumerConfig{
		DeliverPolicy: jetstream.DeliverAllPolicy,
		AckPolicy:     jetstream.AckExplicitPolicy,
	})
	if err != nil {
		t.Fatalf("consumer: %v", err)
	}

	for i := 0; i < 2; i++ {
		msg, err := cons.Next(jetstream.FetchMaxWait(5 * time.Second))
		if err != nil {
			t.Fatalf("next spatial msg %d: %v", i, err)
		}
		if got := msg.Headers().Get(jetstream.MsgIDHeader); got != "" {
			t.Fatalf("SPATIAL message %d Nats-Msg-Id = %q, want empty", i, got)
		}
		_ = msg.Ack()
	}
}

func TestNATSConnectorRoles(t *testing.T) {
	opts := &natsserver.Options{
		Port:      -1,
		JetStream: true,
		StoreDir:  t.TempDir(),
		NoLog:     true,
		NoSigs:    true,
	}
	s, err := natsserver.NewServer(opts)
	if err != nil {
		t.Fatalf("start nats: %v", err)
	}
	go s.Start()
	if !s.ReadyForConnections(5 * time.Second) {
		t.Fatal("nats server not ready")
	}
	t.Cleanup(s.Shutdown)

	var mu sync.Mutex
	var roles []string
	connect := func(role string) (*nats.Conn, error) {
		mu.Lock()
		roles = append(roles, role)
		mu.Unlock()
		return nats.Connect(s.ClientURL(),
			nats.RetryOnFailedConnect(true),
			nats.MaxReconnects(-1),
			nats.ReconnectWait(2*time.Second),
		)
	}

	ing, err := newIngester(pkgingester.Config{Storage: jetstream.MemoryStorage, Replicas: 1, ShardCount: 2}, pkgingester.Dependencies{
		NATS: connect,
	})
	if err != nil {
		t.Fatalf("new ingester: %v", err)
	}
	t.Cleanup(ing.drainMainConn)

	if err := ing.startShards(); err != nil {
		t.Fatalf("start shards: %v", err)
	}
	ing.closeShardInputs()
	ing.shardWG.Wait()
	t.Cleanup(ing.closeShardConns)

	mu.Lock()
	got := append([]string(nil), roles...)
	mu.Unlock()
	want := []string{"main", "shard-0", "shard-1"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("connector roles = %v, want %v", got, want)
	}
}
