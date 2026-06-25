package streampressure

import (
	"context"
	"encoding/json"
	"log"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

const (
	defaultWarnRatio          = 0.75
	defaultCriticalRatio      = 0.90
	defaultMinRefreshInterval = 5 * time.Second
	defaultMinBytesDelta      = 1 << 20
	publishOverheadBytes      = 128
	refreshTimeout            = 5 * time.Second

	SubjectPrefix = "ops.geotruth.v1.stream."

	PressureLevelUnbounded = "unbounded"
	PressureLevelOK        = "ok"
	PressureLevelWarning   = "warning"
	PressureLevelCritical  = "critical"
)

type Config struct {
	Enabled            bool          `mapstructure:"enabled"`
	WarnRatio          float64       `mapstructure:"warnRatio"`
	CriticalRatio      float64       `mapstructure:"criticalRatio"`
	MinRefreshInterval time.Duration `mapstructure:"minRefreshInterval"`
	MinBytesDelta      uint64        `mapstructure:"minBytesDelta"`
}

type StreamPressureEvent struct {
	Stream            string    `json:"stream"`
	Level             string    `json:"level"`
	Bytes             uint64    `json:"bytes"`
	MaxBytes          int64     `json:"max_bytes"`
	UsageRatio        float64   `json:"usage_ratio"`
	Messages          uint64    `json:"messages"`
	FirstSeq          uint64    `json:"first_seq"`
	LastSeq           uint64    `json:"last_seq"`
	LatestObservedSeq uint64    `json:"latest_observed_seq"`
	OccurredAt        time.Time `json:"occurred_at"`
}

type StreamPublishFailedEvent struct {
	Stream            string    `json:"stream"`
	Subject           string    `json:"subject,omitempty"`
	Error             string    `json:"error"`
	LatestObservedSeq uint64    `json:"latest_observed_seq"`
	OccurredAt        time.Time `json:"occurred_at"`
}

func SubjectStreamPressure(stream string) string {
	return SubjectPrefix + stream + ".pressure"
}

func SubjectStreamPublishFailed(stream string) string {
	return SubjectPrefix + stream + ".publish_failed"
}

// Monitor observes successful and failed publishes to one JetStream stream and
// emits best-effort core NATS advisories. It is intentionally approximate on
// the hot path: publishes only increment counters and occasionally schedule an
// async Stream.Info refresh.
type Monitor struct {
	nc     *nats.Conn
	js     jetstream.JetStream
	stream string

	warnRatio          float64
	criticalRatio      float64
	minRefreshInterval time.Duration
	minBytesDelta      uint64

	mu                sync.Mutex
	lastRefreshAt     time.Time
	bytesSinceRefresh uint64
	refreshRunning    bool
	lastLevel         string
	latestObservedSeq uint64
}

// New returns nil when monitoring is disabled. Callers may still invoke methods
// on a nil *Monitor; all methods are nil-safe.
func New(nc *nats.Conn, js jetstream.JetStream, stream string, cfg Config) *Monitor {
	if !cfg.Enabled || nc == nil || js == nil || stream == "" {
		return nil
	}

	warnRatio := cfg.WarnRatio
	if warnRatio <= 0 {
		warnRatio = defaultWarnRatio
	}
	criticalRatio := cfg.CriticalRatio
	if criticalRatio <= 0 {
		criticalRatio = defaultCriticalRatio
	}
	if criticalRatio < warnRatio {
		criticalRatio = warnRatio
	}

	minRefreshInterval := cfg.MinRefreshInterval
	if minRefreshInterval <= 0 {
		minRefreshInterval = defaultMinRefreshInterval
	}
	minBytesDelta := cfg.MinBytesDelta
	if minBytesDelta == 0 {
		minBytesDelta = defaultMinBytesDelta
	}

	return &Monitor{
		nc:                 nc,
		js:                 js,
		stream:             stream,
		warnRatio:          warnRatio,
		criticalRatio:      criticalRatio,
		minRefreshInterval: minRefreshInterval,
		minBytesDelta:      minBytesDelta,
		lastLevel:          PressureLevelOK,
	}
}

// ObservePublish records a successful publish and schedules a bounded async
// Stream.Info refresh when local byte estimates cross the configured threshold.
func (m *Monitor) ObservePublish(subject string, streamSeq uint64, payloadBytes uint64) {
	if m == nil {
		return
	}

	estimatedBytes := payloadBytes + uint64(len(subject)) + publishOverheadBytes

	m.mu.Lock()
	m.bytesSinceRefresh += estimatedBytes
	if streamSeq > m.latestObservedSeq {
		m.latestObservedSeq = streamSeq
	}

	shouldRefresh := m.bytesSinceRefresh >= m.minBytesDelta
	if !shouldRefresh && !m.lastRefreshAt.IsZero() {
		shouldRefresh = time.Since(m.lastRefreshAt) >= m.minRefreshInterval
	}
	if !shouldRefresh && m.lastRefreshAt.IsZero() {
		shouldRefresh = m.bytesSinceRefresh > 0
	}

	if !shouldRefresh || m.refreshRunning {
		m.mu.Unlock()
		return
	}
	m.refreshRunning = true
	m.mu.Unlock()

	go m.refresh()
}

// ObservePublishError emits a publish_failed advisory and schedules a refresh.
// Advisory failures are logged but never returned to the publishing path.
func (m *Monitor) ObservePublishError(subject string, err error) {
	if m == nil {
		return
	}

	errText := ""
	if err != nil {
		errText = err.Error()
	}

	m.mu.Lock()
	latestSeq := m.latestObservedSeq
	m.mu.Unlock()

	event := StreamPublishFailedEvent{
		Stream:            m.stream,
		Subject:           subject,
		Error:             errText,
		LatestObservedSeq: latestSeq,
		OccurredAt:        time.Now().UTC(),
	}
	m.publishJSON(SubjectStreamPublishFailed(m.stream), event)
	m.scheduleRefresh()
}

func (m *Monitor) scheduleRefresh() {
	m.mu.Lock()
	if m.refreshRunning {
		m.mu.Unlock()
		return
	}
	m.refreshRunning = true
	m.mu.Unlock()

	go m.refresh()
}

func (m *Monitor) refresh() {
	ctx, cancel := context.WithTimeout(context.Background(), refreshTimeout)
	defer cancel()

	stream, err := m.js.Stream(ctx, m.stream)
	if err != nil {
		log.Printf("[stream-pressure] stream info %s: %v", m.stream, err)
		m.finishRefresh()
		return
	}
	info, err := stream.Info(ctx)
	if err != nil {
		log.Printf("[stream-pressure] stream info %s: %v", m.stream, err)
		m.finishRefresh()
		return
	}

	level, ratio := pressureLevel(info.Config.MaxBytes, info.State.Bytes, m.warnRatio, m.criticalRatio)

	m.mu.Lock()
	latestSeq := m.latestObservedSeq
	previousLevel := m.lastLevel
	m.lastLevel = level
	m.lastRefreshAt = time.Now()
	m.bytesSinceRefresh = 0
	m.refreshRunning = false
	m.mu.Unlock()

	if level == previousLevel && level != PressureLevelWarning && level != PressureLevelCritical {
		return
	}

	event := StreamPressureEvent{
		Stream:            m.stream,
		Level:             level,
		Bytes:             info.State.Bytes,
		MaxBytes:          info.Config.MaxBytes,
		UsageRatio:        ratio,
		Messages:          info.State.Msgs,
		FirstSeq:          info.State.FirstSeq,
		LastSeq:           info.State.LastSeq,
		LatestObservedSeq: latestSeq,
		OccurredAt:        time.Now().UTC(),
	}
	m.publishJSON(SubjectStreamPressure(m.stream), event)
}

func (m *Monitor) finishRefresh() {
	m.mu.Lock()
	m.refreshRunning = false
	m.mu.Unlock()
}

func (m *Monitor) publishJSON(subject string, event any) {
	data, err := json.Marshal(event)
	if err != nil {
		log.Printf("[stream-pressure] marshal advisory %s: %v", subject, err)
		return
	}
	if err := m.nc.Publish(subject, data); err != nil {
		log.Printf("[stream-pressure] publish advisory %s: %v", subject, err)
	}
}

func pressureLevel(maxBytes int64, bytes uint64, warnRatio, criticalRatio float64) (string, float64) {
	if maxBytes <= 0 {
		return PressureLevelUnbounded, 0
	}

	ratio := float64(bytes) / float64(maxBytes)
	switch {
	case ratio >= criticalRatio:
		return PressureLevelCritical, ratio
	case ratio >= warnRatio:
		return PressureLevelWarning, ratio
	default:
		return PressureLevelOK, ratio
	}
}
