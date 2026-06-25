package geotruth

import (
	"strings"
	"testing"
	"time"

	"github.com/midtxwn/geotruth/internal/gtevents"
	pkggeotruth "github.com/midtxwn/geotruth/pkg/geotruth"
)

func TestNormalizeRuntimeConfigDefaults(t *testing.T) {
	publisherCfg, consumerCfg, err := normalizeRuntimeConfig(pkggeotruth.Config{})
	if err != nil {
		t.Fatalf("normalizeRuntimeConfig: %v", err)
	}

	wantWorkers := gtevents.NormalizePublisherConfig(gtevents.PublisherConfig{}).Workers
	if publisherCfg.Workers != wantWorkers {
		t.Fatalf("publisher workers = %d, want %d", publisherCfg.Workers, wantWorkers)
	}
	if publisherCfg.CommitBuffer != 4096 {
		t.Fatalf("publisher commit buffer = %d, want 4096", publisherCfg.CommitBuffer)
	}
	if publisherCfg.ResultBuffer != 4096 {
		t.Fatalf("publisher result buffer = %d, want 4096", publisherCfg.ResultBuffer)
	}
	if publisherCfg.MaxInFlightPerWorker != 64 {
		t.Fatalf("publisher max in-flight per worker = %d, want 64", publisherCfg.MaxInFlightPerWorker)
	}
	if publisherCfg.InitialBackoff != 100*time.Millisecond {
		t.Fatalf("publisher initial backoff = %s, want 100ms", publisherCfg.InitialBackoff)
	}
	if publisherCfg.MaxBackoff != 5*time.Second {
		t.Fatalf("publisher max backoff = %s, want 5s", publisherCfg.MaxBackoff)
	}
	if publisherCfg.InProgressInterval != 15*time.Second {
		t.Fatalf("publisher in-progress interval = %s, want 15s", publisherCfg.InProgressInterval)
	}

	if consumerCfg.AckWait != 60*time.Second {
		t.Fatalf("consumer ack wait = %s, want 60s", consumerCfg.AckWait)
	}
	if consumerCfg.MaxAckPending != 10000 {
		t.Fatalf("consumer max ack pending = %d, want 10000", consumerCfg.MaxAckPending)
	}
	if consumerCfg.MaxDeliver != -1 {
		t.Fatalf("consumer max deliver = %d, want -1", consumerCfg.MaxDeliver)
	}
	if consumerCfg.ReaderBuffer != 256 {
		t.Fatalf("consumer reader buffer = %d, want 256", consumerCfg.ReaderBuffer)
	}
	if consumerCfg.PullBatchSize != 128 {
		t.Fatalf("consumer pull batch size = %d, want 128", consumerCfg.PullBatchSize)
	}
}

func TestNormalizeRuntimeConfigHonorsExplicitValues(t *testing.T) {
	publisherCfg, consumerCfg, err := normalizeRuntimeConfig(pkggeotruth.Config{
		Publisher: pkggeotruth.PublisherConfig{
			Workers:              2,
			CommitBuffer:         10,
			ResultBuffer:         12,
			MaxInFlightPerWorker: 9,
			InitialBackoff:       50 * time.Millisecond,
			MaxBackoff:           time.Second,
			InProgressInterval:   2 * time.Second,
		},
		Consumer: pkggeotruth.ConsumerConfig{
			AckWait:       30 * time.Second,
			MaxAckPending: 123,
			MaxDeliver:    5,
			ReaderBuffer:  17,
			PullBatchSize: 4,
		},
	})
	if err != nil {
		t.Fatalf("normalizeRuntimeConfig: %v", err)
	}

	if publisherCfg.Workers != 2 || publisherCfg.CommitBuffer != 10 || publisherCfg.ResultBuffer != 12 || publisherCfg.MaxInFlightPerWorker != 9 {
		t.Fatalf("publisher cfg = %+v", publisherCfg)
	}
	if publisherCfg.InitialBackoff != 50*time.Millisecond || publisherCfg.MaxBackoff != time.Second || publisherCfg.InProgressInterval != 2*time.Second {
		t.Fatalf("publisher timing cfg = %+v", publisherCfg)
	}
	if consumerCfg.AckWait != 30*time.Second || consumerCfg.MaxAckPending != 123 || consumerCfg.MaxDeliver != 5 ||
		consumerCfg.ReaderBuffer != 17 || consumerCfg.PullBatchSize != 4 {
		t.Fatalf("consumer cfg = %+v", consumerCfg)
	}
}

func TestNormalizeRuntimeConfigRejectsUnsafeInProgressInterval(t *testing.T) {
	_, _, err := normalizeRuntimeConfig(pkggeotruth.Config{
		Publisher: pkggeotruth.PublisherConfig{InProgressInterval: 30 * time.Second},
		Consumer:  pkggeotruth.ConsumerConfig{AckWait: 30 * time.Second},
	})
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "must be less than consumer ack wait") {
		t.Fatalf("error = %q", err)
	}
}

func TestNormalizeRuntimeConfigRejectsNegativeValues(t *testing.T) {
	cases := []struct {
		name string
		cfg  pkggeotruth.Config
	}{
		{name: "publisher workers", cfg: pkggeotruth.Config{Publisher: pkggeotruth.PublisherConfig{Workers: -1}}},
		{name: "publisher commit buffer", cfg: pkggeotruth.Config{Publisher: pkggeotruth.PublisherConfig{CommitBuffer: -1}}},
		{name: "publisher result buffer", cfg: pkggeotruth.Config{Publisher: pkggeotruth.PublisherConfig{ResultBuffer: -1}}},
		{name: "publisher max in-flight", cfg: pkggeotruth.Config{Publisher: pkggeotruth.PublisherConfig{MaxInFlightPerWorker: -1}}},
		{name: "publisher initial backoff", cfg: pkggeotruth.Config{Publisher: pkggeotruth.PublisherConfig{InitialBackoff: -1}}},
		{name: "publisher max backoff", cfg: pkggeotruth.Config{Publisher: pkggeotruth.PublisherConfig{MaxBackoff: -1}}},
		{name: "publisher in-progress", cfg: pkggeotruth.Config{Publisher: pkggeotruth.PublisherConfig{InProgressInterval: -1}}},
		{name: "consumer ack wait", cfg: pkggeotruth.Config{Consumer: pkggeotruth.ConsumerConfig{AckWait: -1}}},
		{name: "consumer max ack pending", cfg: pkggeotruth.Config{Consumer: pkggeotruth.ConsumerConfig{MaxAckPending: -1}}},
		{name: "consumer max deliver", cfg: pkggeotruth.Config{Consumer: pkggeotruth.ConsumerConfig{MaxDeliver: -2}}},
		{name: "consumer reader buffer", cfg: pkggeotruth.Config{Consumer: pkggeotruth.ConsumerConfig{ReaderBuffer: -1}}},
		{name: "consumer pull batch size", cfg: pkggeotruth.Config{Consumer: pkggeotruth.ConsumerConfig{PullBatchSize: -1}}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, _, err := normalizeRuntimeConfig(tc.cfg); err == nil {
				t.Fatal("expected error")
			}
		})
	}
}
