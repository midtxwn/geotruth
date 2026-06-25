package gtevents

import (
	"context"
	"testing"

	"github.com/nats-io/nats.go/jetstream"
)

func TestEnsureStreamHonorsStorageConfig(t *testing.T) {
	ctx := context.Background()
	_, js, shutdown := runPublisherPressureNATS(t)
	defer shutdown()

	stream, err := EnsureStream(ctx, js, StreamConfig{
		Storage:  jetstream.MemoryStorage,
		MaxBytes: 1024 * 1024,
		Replicas: 1,
	})
	if err != nil {
		t.Fatalf("ensure stream: %v", err)
	}

	info, err := stream.Info(ctx)
	if err != nil {
		t.Fatalf("stream info: %v", err)
	}
	if info.Config.Storage != jetstream.MemoryStorage {
		t.Fatalf("storage = %v, want %v", info.Config.Storage, jetstream.MemoryStorage)
	}
}
