package gtevents

import (
	"testing"
	"time"
)

func TestNormalizePublisherConfigDefaults(t *testing.T) {
	cfg := NormalizePublisherConfig(PublisherConfig{})

	if cfg.Workers != defaultPublisherWorkers {
		t.Fatalf("Workers = %d, want %d", cfg.Workers, defaultPublisherWorkers)
	}
	if cfg.CommitBuffer != defaultPublisherCommitBuffer {
		t.Fatalf("CommitBuffer = %d, want %d", cfg.CommitBuffer, defaultPublisherCommitBuffer)
	}
	if cfg.ResultBuffer != defaultPublisherResultBuffer {
		t.Fatalf("ResultBuffer = %d, want %d", cfg.ResultBuffer, defaultPublisherResultBuffer)
	}
	if cfg.MaxInFlightPerWorker != defaultPublisherMaxInFlightPerWorker {
		t.Fatalf("MaxInFlightPerWorker = %d, want %d", cfg.MaxInFlightPerWorker, defaultPublisherMaxInFlightPerWorker)
	}
	if cfg.InitialBackoff != defaultPublisherInitialBackoff {
		t.Fatalf("InitialBackoff = %s, want %s", cfg.InitialBackoff, defaultPublisherInitialBackoff)
	}
	if cfg.MaxBackoff != defaultPublisherMaxBackoff {
		t.Fatalf("MaxBackoff = %s, want %s", cfg.MaxBackoff, defaultPublisherMaxBackoff)
	}
	if cfg.InProgressInterval != defaultPublisherInProgressInterval {
		t.Fatalf("InProgressInterval = %s, want %s", cfg.InProgressInterval, defaultPublisherInProgressInterval)
	}
}

func TestNewPublisherWithMonitorConfigHonorsRuntimeConfig(t *testing.T) {
	p := NewPublisherWithMonitorConfig(nil, PublisherConfig{
		Workers:              3,
		CommitBuffer:         11,
		ResultBuffer:         13,
		MaxInFlightPerWorker: 17,
		InitialBackoff:       20 * time.Millisecond,
		MaxBackoff:           2 * time.Second,
		InProgressInterval:   7 * time.Second,
	}, nil)

	if p.workers != 3 {
		t.Fatalf("workers = %d, want 3", p.workers)
	}
	if cap(p.commitCh) != 11 {
		t.Fatalf("commitCh cap = %d, want 11", cap(p.commitCh))
	}
	if cap(p.resultCh) != 13 {
		t.Fatalf("resultCh cap = %d, want 13", cap(p.resultCh))
	}
	if p.maxInFlightPerWorker != 17 {
		t.Fatalf("maxInFlightPerWorker = %d, want 17", p.maxInFlightPerWorker)
	}
	if p.initialBackoff != 20*time.Millisecond {
		t.Fatalf("initialBackoff = %s, want 20ms", p.initialBackoff)
	}
	if p.maxBackoff != 2*time.Second {
		t.Fatalf("maxBackoff = %s, want 2s", p.maxBackoff)
	}
	if p.inProgressInterval != 7*time.Second {
		t.Fatalf("inProgressInterval = %s, want 7s", p.inProgressInterval)
	}
}
