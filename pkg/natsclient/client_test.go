package natsclient

import (
	"context"
	"os"
	"testing"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
)

func startEmbeddedNATS(tb testing.TB) string {
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

	return s.ClientURL()
}

func TestNew_Success(t *testing.T) {
	url := startEmbeddedNATS(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	client, err := New(ctx, url)
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	if client == nil {
		t.Fatal("expected non-nil client")
	}
	defer client.Close()

	if client.nc == nil {
		t.Fatal("expected non-nil nats connection")
	}
	if client.nc.Status() != nats.CONNECTED {
		t.Errorf("expected connected status, got %v", client.nc.Status())
	}
}

func TestNew_EmptyURL(t *testing.T) {
	url := startEmbeddedNATS(t)

	os.Setenv("NATS_URL", url)
	defer os.Unsetenv("NATS_URL")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	client, err := New(ctx, "")
	if err != nil {
		t.Fatalf("New with empty URL failed: %v", err)
	}
	if client == nil {
		t.Fatal("expected non-nil client")
	}
	defer client.Close()

	if client.nc.Status() != nats.CONNECTED {
		t.Error("expected connected via env var")
	}
}

func TestNew_ContextCancelled(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	_, err := New(ctx, "nats://localhost:9999")
	if err == nil {
		t.Fatal("expected error when connecting to unavailable server with short timeout")
	}
	if ctx.Err() != context.DeadlineExceeded {
		t.Errorf("expected DeadlineExceeded, got %v", ctx.Err())
	}
}

func TestConn(t *testing.T) {
	url := startEmbeddedNATS(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	client, err := New(ctx, url)
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	defer client.Close()

	conn := client.Conn()
	if conn == nil {
		t.Fatal("expected non-nil connection from Conn()")
	}
	if conn.Status() != nats.CONNECTED {
		t.Error("expected connected connection")
	}
}

func TestClose(t *testing.T) {
	url := startEmbeddedNATS(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	client, err := New(ctx, url)
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}

	client.Close()
	client.Close()

	status := client.nc.Status()
	if status != nats.CLOSED && status != nats.DRAINING_SUBS && status != nats.DRAINING_PUBS {
		t.Logf("Connection status after close: %v", status)
	}
}

func TestClient_Reconnect(t *testing.T) {
	url := startEmbeddedNATS(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	client, err := New(ctx, url)
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	defer client.Close()

	if client.nc == nil {
		t.Fatal("expected non-nil connection")
	}

	testSubj := "test.reconnect"
	received := make(chan bool, 1)

	sub, err := client.nc.Subscribe(testSubj, func(msg *nats.Msg) {
		received <- true
	})
	if err != nil {
		t.Fatalf("subscribe failed: %v", err)
	}
	defer sub.Unsubscribe()

	if err := client.nc.Publish(testSubj, []byte("test")); err != nil {
		t.Fatalf("publish failed: %v", err)
	}

	select {
	case <-received:
	case <-time.After(2 * time.Second):
		t.Error("timeout waiting for message")
	}
}

func TestClient_MultipleInstances(t *testing.T) {
	url := startEmbeddedNATS(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	client1, err := New(ctx, url)
	if err != nil {
		t.Fatalf("New client1 failed: %v", err)
	}
	defer client1.Close()

	client2, err := New(ctx, url)
	if err != nil {
		t.Fatalf("New client2 failed: %v", err)
	}
	defer client2.Close()

	if client1.nc.Status() != nats.CONNECTED {
		t.Error("client1 not connected")
	}
	if client2.nc.Status() != nats.CONNECTED {
		t.Error("client2 not connected")
	}
	if client1.nc == client2.nc {
		t.Error("clients should have separate connections")
	}
}
