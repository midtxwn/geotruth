package spatialstream

import (
	"context"

	privKeys "github.com/midtxwn/geotruth/internal/natskeys"

	"github.com/nats-io/nats.go/jetstream"
)

// Config contains the invariant-safe knobs for the SPATIAL source stream.
type Config struct {
	Storage  jetstream.StorageType
	MaxBytes *int64
	Replicas *int
}

// EnsureStream creates or updates the SPATIAL stream with invariant-preserving
// source-log semantics. If MaxBytes or Replicas are nil and the stream already
// exists, the existing values are preserved so a non-owning service does not
// accidentally clear the ingester's configured retention or durability.
func EnsureStream(ctx context.Context, js jetstream.JetStream, cfg Config) (jetstream.Stream, error) {
	var existing *jetstream.StreamConfig
	if stream, err := js.Stream(ctx, privKeys.StreamName); err == nil {
		if info := stream.CachedInfo(); info != nil {
			existing = &info.Config
		}
	}

	return js.CreateOrUpdateStream(ctx, StreamConfig(cfg, existing))
}

// StreamConfig builds the SPATIAL stream config. Retention, discard, and rollup
// are intentionally not configurable: SPATIAL is the accepted-command source
// log, so old accepted messages must not be silently discarded or compacted.
func StreamConfig(cfg Config, existing *jetstream.StreamConfig) jetstream.StreamConfig {
	storage := cfg.Storage
	if storage == 0 {
		if existing != nil && existing.Storage != 0 {
			storage = existing.Storage
		} else {
			storage = jetstream.FileStorage
		}
	}

	maxBytes := int64(0)
	if cfg.MaxBytes != nil {
		maxBytes = *cfg.MaxBytes
	} else if existing != nil {
		maxBytes = existing.MaxBytes
	}

	replicas := 1
	if cfg.Replicas != nil {
		if *cfg.Replicas > 0 {
			replicas = *cfg.Replicas
		}
	} else if existing != nil && existing.Replicas > 0 {
		replicas = existing.Replicas
	}

	return jetstream.StreamConfig{
		Name:      privKeys.StreamName,
		Subjects:  privKeys.StreamSubjects,
		Storage:   storage,
		Retention: jetstream.LimitsPolicy,
		Discard:   jetstream.DiscardNew,
		MaxBytes:  maxBytes,
		Replicas:  replicas,
	}
}
