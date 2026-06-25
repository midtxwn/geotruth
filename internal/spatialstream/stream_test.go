package spatialstream

import (
	"testing"

	privKeys "github.com/midtxwn/geotruth/internal/natskeys"

	"github.com/nats-io/nats.go/jetstream"
)

func TestStreamConfigAppliesSafeSpatialSettings(t *testing.T) {
	maxBytes := int64(4096)
	replicas := 3

	cfg := StreamConfig(Config{
		Storage:  jetstream.MemoryStorage,
		MaxBytes: &maxBytes,
		Replicas: &replicas,
	}, nil)

	if cfg.Name != privKeys.StreamName {
		t.Fatalf("Name = %q, want %q", cfg.Name, privKeys.StreamName)
	}
	if cfg.Retention != jetstream.LimitsPolicy {
		t.Fatalf("Retention = %v, want LimitsPolicy", cfg.Retention)
	}
	if cfg.Discard != jetstream.DiscardNew {
		t.Fatalf("Discard = %v, want DiscardNew", cfg.Discard)
	}
	if cfg.AllowRollup {
		t.Fatal("AllowRollup = true, want false")
	}
	if cfg.MaxBytes != maxBytes {
		t.Fatalf("MaxBytes = %d, want %d", cfg.MaxBytes, maxBytes)
	}
	if cfg.Replicas != replicas {
		t.Fatalf("Replicas = %d, want %d", cfg.Replicas, replicas)
	}
}

func TestStreamConfigPreservesExistingOwnerValues(t *testing.T) {
	existing := jetstream.StreamConfig{
		Storage:  jetstream.MemoryStorage,
		MaxBytes: 8192,
		Replicas: 5,
	}

	cfg := StreamConfig(Config{}, &existing)

	if cfg.Storage != existing.Storage {
		t.Fatalf("Storage = %v, want %v", cfg.Storage, existing.Storage)
	}
	if cfg.MaxBytes != existing.MaxBytes {
		t.Fatalf("MaxBytes = %d, want %d", cfg.MaxBytes, existing.MaxBytes)
	}
	if cfg.Replicas != existing.Replicas {
		t.Fatalf("Replicas = %d, want %d", cfg.Replicas, existing.Replicas)
	}
}

func TestStreamConfigDefaults(t *testing.T) {
	cfg := StreamConfig(Config{}, nil)

	if cfg.Storage != jetstream.FileStorage {
		t.Fatalf("Storage = %v, want FileStorage", cfg.Storage)
	}
	if cfg.MaxBytes != 0 {
		t.Fatalf("MaxBytes = %d, want 0", cfg.MaxBytes)
	}
	if cfg.Replicas != 1 {
		t.Fatalf("Replicas = %d, want 1", cfg.Replicas)
	}
}
