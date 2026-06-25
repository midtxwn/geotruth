package integration

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math"
	"os"
	"runtime"
	"runtime/pprof"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/midtxwn/geotruth/embedded"
	internalnatskeys "github.com/midtxwn/geotruth/internal/natskeys"
	"github.com/midtxwn/geotruth/pkg/domain"
	"github.com/midtxwn/geotruth/pkg/geotruthops"
	pkgingester "github.com/midtxwn/geotruth/pkg/ingester"
	"github.com/midtxwn/geotruth/pkg/messages"
	"github.com/midtxwn/geotruth/pkg/natskeys"
	"github.com/midtxwn/geotruth/pkg/natspublish"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

const throughputPollInterval = 20 * time.Millisecond

type natsConnPool struct {
	conns []*nats.Conn
	mu    sync.Mutex
	next  int
}

func newConnPool(url string, size int) (*natsConnPool, error) {
	if size <= 0 {
		return nil, nil
	}
	conns := make([]*nats.Conn, size)
	for i := 0; i < size; i++ {
		nc, err := nats.Connect(url,
			nats.RetryOnFailedConnect(true),
			nats.MaxReconnects(-1),
			nats.ReconnectWait(2*time.Second),
		)
		if err != nil {
			for j := 0; j < i; j++ {
				conns[j].Close()
			}
			return nil, fmt.Errorf("pool conn %d: %w", i, err)
		}
		conns[i] = nc
	}
	return &natsConnPool{conns: conns}, nil
}

func (p *natsConnPool) get() *nats.Conn {
	if p == nil || len(p.conns) == 0 {
		return nil
	}
	p.mu.Lock()
	conn := p.conns[p.next]
	p.next = (p.next + 1) % len(p.conns)
	p.mu.Unlock()
	return conn
}

func (p *natsConnPool) close() {
	if p == nil {
		return
	}
	for _, nc := range p.conns {
		nc.Close()
	}
}

type sourceSeqResp struct {
	SourceSeq uint64 `json:"source_seq"`
}

type acceptedUpdate struct {
	objectID   string
	clientOpID string
	sentAt     time.Time
	acceptedAt time.Time
}

type commitKey struct {
	objectID   string
	clientOpID string
}

type statsSample struct {
	at       time.Time
	ackFloor uint64
	lag      uint64
	pending  uint64
	inFlight int
}

type throughputSummary struct {
	sentUpdates      int
	acceptedUpdates  int
	processedUpdates int
	publishErrors    int64
	maxLag           uint64
	endLag           uint64
	meanLagRate      float64
	maxPending       uint64
	maxInFlight      int
	drain            time.Duration
	ingesterP50      time.Duration
	ingesterP95      time.Duration
	ingesterP99      time.Duration
	geotruthP50      time.Duration
	geotruthP95      time.Duration
	geotruthP99      time.Duration
	e2eP50           time.Duration
	e2eP95           time.Duration
	e2eP99           time.Duration
	finalPending     uint64
	finalAckPending  int
	finalRedelivered int
	finalAckFloorSeq uint64
	finalSpatialLast uint64
}

type fullPathMaxThroughputResult struct {
	sent          int64
	accepted      int64
	errors        int64
	elapsed       time.Duration
	commitLatency []time.Duration
}
type capacityConfig struct {
	ratePerObject       int
	warmup              time.Duration
	duration            time.Duration
	p95Limit            time.Duration
	drainTimeout        time.Duration
	objectCount         int
	pullBatchSize       int
	readerBuffer        int
	pubWorkers          int
	pubInFlight         int
	setupTimeout        time.Duration
	registerConcurrency int
	harnessPoolSize     int
	senderWorkers       int
	storage             jetstream.StorageType
	storageName         string
	trialCPUProfile     string
	trialMutexProfile   string
}

type capacityTrialResult struct {
	objectCount   int
	ratePerObject int
	senderWorkers int
	pullBatchSize int
	pubWorkers    int
	pubInFlight   int
	duration      time.Duration
	repeat        int
	storageName   string
	summary       throughputSummary
	pass          bool
	reason        string
}

type sourceSeqPayload struct {
	SourceSeq uint64 `json:"source_seq"`
}

func scanGTEvents(ctx context.Context, js jetstream.JetStream) (map[commitKey]time.Time, error) {
	stream, err := js.Stream(ctx, natskeys.GTStreamName)
	if err != nil {
		return nil, fmt.Errorf("get GT_EVENTS stream: %w", err)
	}

	consName := fmt.Sprintf("bench-scan-%d", time.Now().UnixNano())
	cons, err := stream.CreateConsumer(ctx, jetstream.ConsumerConfig{
		Name:           consName,
		FilterSubjects: []string{"gt.events.v1.>"},
		DeliverPolicy:  jetstream.DeliverAllPolicy,
		AckPolicy:      jetstream.AckExplicitPolicy,
		AckWait:        60 * time.Second,
		MaxDeliver:     1,
	})
	if err != nil {
		return nil, fmt.Errorf("create scan consumer: %w", err)
	}
	defer func() {
		_ = stream.DeleteConsumer(ctx, consName)
	}()

	commits := make(map[commitKey]time.Time)
	for {
		msg, err := cons.Next(jetstream.FetchMaxWait(200 * time.Millisecond))
		if err != nil {
			if errors.Is(err, nats.ErrTimeout) {
				break
			}
			return nil, fmt.Errorf("next: %w", err)
		}
		var payload struct {
			ObjectID   string `json:"object_id"`
			ClientOpID string `json:"client_op_id"`
		}
		if err := json.Unmarshal(msg.Data(), &payload); err != nil || payload.ObjectID == "" || payload.ClientOpID == "" {
			_ = msg.Ack()
			continue
		}
		meta, err := msg.Metadata()
		if err != nil {
			_ = msg.Ack()
			continue
		}
		key := commitKey{objectID: payload.ObjectID, clientOpID: payload.ClientOpID}
		if _, exists := commits[key]; !exists {
			commits[key] = meta.Timestamp
		}
		_ = msg.Ack()
	}
	return commits, nil
}

type benchmarkTimer interface {
	StartTimer()
	StopTimer()
}

func stopBenchmarkTimer(t testing.TB) {
	if timer, ok := t.(benchmarkTimer); ok {
		timer.StopTimer()
	}
}

func startBenchmarkTimer(t testing.TB) {
	if timer, ok := t.(benchmarkTimer); ok {
		timer.StartTimer()
	}
}

func requestSourceSeq(ctx context.Context, nc *nats.Conn, subject string, payload any) (sentAt time.Time, seq uint64, err error) {
	data, err := json.Marshal(payload)
	if err != nil {
		return time.Time{}, 0, err
	}
	sentAt = time.Now()
	resp, err := nc.RequestWithContext(ctx, subject, data)
	if err != nil {
		return sentAt, 0, err
	}
	parsed, err := messages.Data[sourceSeqResp](resp.Data)
	if err != nil {
		return sentAt, 0, err
	}
	if parsed.SourceSeq == 0 {
		return sentAt, 0, fmt.Errorf("response missing source_seq")
	}
	return sentAt, parsed.SourceSeq, nil
}

func requestOK(ctx context.Context, nc *nats.Conn, subject string, payload any) (sentAt time.Time, err error) {
	data, err := json.Marshal(payload)
	if err != nil {
		return time.Time{}, err
	}
	sentAt = time.Now()
	resp, err := nc.RequestWithContext(ctx, subject, data)
	if err != nil {
		return sentAt, err
	}
	if err := messages.Err(resp.Data); err != nil {
		return sentAt, err
	}
	return sentAt, nil
}

func registerBenchObject(ctx context.Context, tb testing.TB, nc *nats.Conn, objectID string) uint64 {
	tb.Helper()
	_, seq, err := requestSourceSeq(ctx, nc, natspublish.IngesterRegisterObjectSubject(objectID), natspublish.RegisterObjectReq{
		ID:   objectID,
		Dims: domain.ObjectDimensions{Width: 1, Height: 1},
	})
	if err != nil {
		tb.Fatalf("register %s: %v", objectID, err)
	}
	return seq
}

func updateBenchObject(ctx context.Context, nc *nats.Conn, objectID string, step int, clientOpSeq *atomic.Uint64) (sentAt time.Time, clientOpID string, err error) {
	clientOpID = strconv.FormatUint(clientOpSeq.Add(1), 36)
	sentAt, err = requestOK(ctx, nc, natspublish.IngesterUpdatePositionSubject(objectID), natspublish.UpdatePositionReq{
		ID:         objectID,
		X:          float64(step % 1000),
		Y:          float64((step * 7) % 1000),
		Z:          1.0,
		RotY:       0,
		ClientOpID: clientOpID,
	})
	if err != nil {
		return sentAt, "", err
	}
	return sentAt, clientOpID, nil
}

func TestThroughputHarnessDrains(t *testing.T) {
	svc, _, _ := startServices(t, 1)
	nc := svc.NATSConn()
	ops, err := geotruthops.New(nc)
	if err != nil {
		t.Fatalf("ops: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	maxRegSeq := registerBenchObject(ctx, t, nc, "throughput-smoke-obj")
	waitForAckFloor(t, ops, maxRegSeq, 5*time.Second)

	accepted := 0
	var clientOpSeq atomic.Uint64
	for i := 0; i < 20; i++ {
		_, _, err := updateBenchObject(ctx, nc, "throughput-smoke-obj", i, &clientOpSeq)
		if err != nil {
			t.Fatalf("update %d: %v", i, err)
		}
		accepted++
	}
	waitForAckFloor(t, ops, maxRegSeq+uint64(accepted), 5*time.Second)

	stats, err := ops.Stats(ctx)
	if err != nil {
		t.Fatalf("stats: %v", err)
	}
	if stats.GeoTruthConsumer.NumRedelivered != 0 {
		t.Fatalf("redelivered = %d, want 0", stats.GeoTruthConsumer.NumRedelivered)
	}
	if stats.GeoTruthConsumer.NumPending != 0 || stats.GeoTruthConsumer.NumAckPending != 0 {
		t.Fatalf("consumer not drained: %+v", stats.GeoTruthConsumer)
	}
}

type benchmarkCommitWaiter struct {
	stream jetstream.Stream
	name   string
	cancel context.CancelFunc
	done   chan struct{}

	mu      sync.Mutex
	waiters map[commitKey]chan time.Time
}

func newBenchmarkCommitWaiter(ctx context.Context, js jetstream.JetStream) (*benchmarkCommitWaiter, error) {
	stream, err := js.Stream(ctx, natskeys.GTStreamName)
	if err != nil {
		return nil, fmt.Errorf("get GT_EVENTS stream: %w", err)
	}
	name := fmt.Sprintf("bench-fullpath-max-%d", time.Now().UnixNano())
	cons, err := stream.CreateConsumer(ctx, jetstream.ConsumerConfig{
		Name:           name,
		FilterSubjects: []string{"gt.events.v1.>"},
		DeliverPolicy:  jetstream.DeliverNewPolicy,
		AckPolicy:      jetstream.AckExplicitPolicy,
		AckWait:        60 * time.Second,
		MaxDeliver:     1,
	})
	if err != nil {
		return nil, fmt.Errorf("create benchmark commit consumer: %w", err)
	}

	watchCtx, cancel := context.WithCancel(ctx)
	w := &benchmarkCommitWaiter{
		stream:  stream,
		name:    name,
		cancel:  cancel,
		done:    make(chan struct{}),
		waiters: make(map[commitKey]chan time.Time),
	}
	go w.run(watchCtx, cons)
	return w, nil
}

func (w *benchmarkCommitWaiter) run(ctx context.Context, cons jetstream.Consumer) {
	defer close(w.done)
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		msg, err := cons.Next(jetstream.FetchMaxWait(100 * time.Millisecond))
		if err != nil {
			if errors.Is(err, nats.ErrTimeout) {
				continue
			}
			select {
			case <-ctx.Done():
				return
			default:
				continue
			}
		}

		var payload struct {
			ObjectID   string `json:"object_id"`
			ClientOpID string `json:"client_op_id"`
		}
		if err := json.Unmarshal(msg.Data(), &payload); err == nil && payload.ObjectID != "" && payload.ClientOpID != "" {
			w.complete(commitKey{objectID: payload.ObjectID, clientOpID: payload.ClientOpID}, time.Now())
		}
		_ = msg.Ack()
	}
}

func (w *benchmarkCommitWaiter) add(key commitKey) chan time.Time {
	ch := make(chan time.Time, 1)
	w.mu.Lock()
	w.waiters[key] = ch
	w.mu.Unlock()
	return ch
}

func (w *benchmarkCommitWaiter) remove(key commitKey, ch chan time.Time) {
	w.mu.Lock()
	if current := w.waiters[key]; current == ch {
		delete(w.waiters, key)
	}
	w.mu.Unlock()
}

func (w *benchmarkCommitWaiter) complete(key commitKey, at time.Time) {
	w.mu.Lock()
	ch := w.waiters[key]
	if ch != nil {
		delete(w.waiters, key)
	}
	w.mu.Unlock()
	if ch != nil {
		ch <- at
	}
}

func (w *benchmarkCommitWaiter) close(ctx context.Context) {
	w.cancel()
	<-w.done
	_ = w.stream.DeleteConsumer(ctx, w.name)
}

func runFullPathMaxThroughputTrial(t testing.TB, cfg capacityConfig, repeat int) fullPathMaxThroughputResult {
	t.Helper()
	stopBenchmarkTimer(t)

	svc := startCapacityServices(t, cfg)
	defer func() {
		svc.Shutdown()
		startBenchmarkTimer(t)
	}()
	nc := svc.NATSConn()
	pool, err := newConnPool(nc.ConnectedUrl(), cfg.harnessPoolSize)
	if err != nil {
		t.Fatalf("create harness pool: %v", err)
	}
	defer pool.close()

	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream: %v", err)
	}
	ops, err := geotruthops.New(nc)
	if err != nil {
		t.Fatalf("ops: %v", err)
	}

	setupCtx, cancelSetup := context.WithTimeout(context.Background(), cfg.setupTimeout)
	objectIDs, maxRegSeq := registerCapacityObjects(setupCtx, t, nc, cfg.objectCount, repeat, cfg.registerConcurrency)
	waitForAckFloor(t, ops, maxRegSeq, cfg.setupTimeout)
	cancelSetup()

	ctx, cancel := context.WithTimeout(context.Background(), cfg.warmup+cfg.duration+cfg.drainTimeout+60*time.Second)
	defer cancel()

	if cfg.warmup > 0 {
		warmup := runClosedLoopFullPathLoad(t, ctx, pool, js, objectIDs, cfg, cfg.warmup)
		if warmup.errors != 0 {
			t.Fatalf("warmup errors: %d", warmup.errors)
		}
	}

	if cfg.trialCPUProfile != "" {
		f, err := os.Create(cfg.trialCPUProfile)
		if err != nil {
			t.Fatalf("create cpu profile: %v", err)
		}
		if err := pprof.StartCPUProfile(f); err != nil {
			f.Close()
			t.Fatalf("start cpu profile: %v", err)
		}
		defer f.Close()
	}
	if cfg.trialMutexProfile != "" {
		runtime.SetMutexProfileFraction(1)
	}
	startBenchmarkTimer(t)
	result := runClosedLoopFullPathLoad(t, ctx, pool, js, objectIDs, cfg, cfg.duration)
	stopBenchmarkTimer(t)

	if cfg.trialCPUProfile != "" {
		pprof.StopCPUProfile()
	}
	if cfg.trialMutexProfile != "" {
		runtime.SetMutexProfileFraction(0)
		f, err := os.Create(cfg.trialMutexProfile)
		if err != nil {
			t.Fatalf("create mutex profile: %v", err)
		}
		if err := pprof.Lookup("mutex").WriteTo(f, 0); err != nil {
			f.Close()
			t.Fatalf("write mutex profile: %v", err)
		}
		f.Close()
	}

	return result
}
func runCapacityTrial(t testing.TB, cfg capacityConfig, objectCount, repeat int) capacityTrialResult {
	t.Helper()
	stopBenchmarkTimer(t)

	svc := startCapacityServices(t, cfg)
	shutdownDone := false
	defer func() {
		if !shutdownDone {
			svc.Shutdown()
			startBenchmarkTimer(t)
		}
	}()
	nc := svc.NATSConn()
	ops, err := geotruthops.New(nc)
	if err != nil {
		t.Fatalf("ops: %v", err)
	}

	pool, err := newConnPool(nc.ConnectedUrl(), cfg.harnessPoolSize)
	if err != nil {
		t.Fatalf("create harness pool: %v", err)
	}
	defer pool.close()

	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream: %v", err)
	}

	setupCtx, cancelSetup := context.WithTimeout(context.Background(), cfg.setupTimeout)
	objectIDs, maxRegSeq := registerCapacityObjects(setupCtx, t, nc, objectCount, repeat, cfg.registerConcurrency)
	waitForAckFloor(t, ops, maxRegSeq, cfg.setupTimeout)
	cancelSetup()

	ctx, cancel := context.WithTimeout(context.Background(), cfg.warmup+cfg.duration+cfg.drainTimeout+60*time.Second)
	defer cancel()

	queuedAccepted := 0
	var clientOpSeq atomic.Uint64
	if cfg.warmup > 0 {
		warmupAccepted, warmupPublishErrors := sendMultiObjectScheduled(ctx, pool, objectIDs, cfg.ratePerObject, cfg.warmup, &clientOpSeq, cfg.senderWorkers)
		warmupSent := plannedUpdates(objectCount, cfg.ratePerObject, cfg.warmup)
		if warmupPublishErrors != 0 {
			t.Fatalf("warmup publish errors: %d", warmupPublishErrors)
		}
		if len(warmupAccepted) != warmupSent {
			t.Fatalf("warmup accepted %d of %d sent updates", len(warmupAccepted), warmupSent)
		}
		queuedAccepted += len(warmupAccepted)
	}

	sampleCount := int((cfg.duration+cfg.drainTimeout)/throughputPollInterval) + 2
	samples := make([]statsSample, 0, maxInt(16, sampleCount))
	if sample, ok := currentStatsSample(ctx, ops); ok {
		samples = append(samples, sample)
	}

	sampleCtx, cancelSampling := context.WithCancel(ctx)
	sampleCh, sampleDone := startStatsSampler(sampleCtx, ops, throughputPollInterval, maxInt(8192, sampleCount))

	if cfg.trialCPUProfile != "" {
		f, err2 := os.Create(cfg.trialCPUProfile)
		if err2 != nil {
			t.Fatalf("create cpu profile: %v", err2)
		}
		if err2 := pprof.StartCPUProfile(f); err2 != nil {
			f.Close()
			t.Fatalf("start cpu profile: %v", err2)
		}
		defer f.Close()
	}
	if cfg.trialMutexProfile != "" {
		runtime.SetMutexProfileFraction(1)
	}
	startBenchmarkTimer(t)
	accepted, publishErrors := sendMultiObjectScheduled(ctx, pool, objectIDs, cfg.ratePerObject, cfg.duration, &clientOpSeq, cfg.senderWorkers)
	trialEnd := time.Now()
	if sample, ok := currentStatsSample(ctx, ops); ok {
		samples = append(samples, sample)
	}
	queuedAccepted += len(accepted)
	drain, _ := drainUntilAckedOK(ctx, ops, maxRegSeq, queuedAccepted, cfg.drainTimeout)
	stopBenchmarkTimer(t)

	if cfg.trialCPUProfile != "" {
		pprof.StopCPUProfile()
	}
	if cfg.trialMutexProfile != "" {
		runtime.SetMutexProfileFraction(0)
		f, err2 := os.Create(cfg.trialMutexProfile)
		if err2 != nil {
			t.Fatalf("create mutex profile: %v", err2)
		}
		if err2 := pprof.Lookup("mutex").WriteTo(f, 0); err2 != nil {
			f.Close()
			t.Fatalf("write mutex profile: %v", err2)
		}
		f.Close()
	}

	cancelSampling()
	<-sampleDone
	samples = append(samples, collectStatsSamples(sampleCh)...)
	if sample, ok := currentStatsSample(ctx, ops); ok {
		samples = append(samples, sample)
	}
	sort.Slice(samples, func(i, j int) bool { return samples[i].at.Before(samples[j].at) })
	trialSamples := samplesUntil(samples, trialEnd)

	commits, err := scanGTEvents(ctx, js)
	if err != nil {
		log.Printf("[capacity] scan GT_EVENTS: %v", err)
		commits = make(map[commitKey]time.Time)
	}

	finalStats, err := ops.Stats(context.Background())
	var summary throughputSummary
	if err != nil {
		summary = summarizeThroughput(plannedUpdates(objectCount, cfg.ratePerObject, cfg.duration), accepted, trialSamples, drain, publishErrors, commits, geotruthops.Stats{})
		pass, reason := evaluateCapacitySummary(summary, cfg)
		t.Logf("final stats: %v (trial context expired); reporting best-effort summary: pass=%v reason=%s", err, pass, reason)
		result := capacityTrialResult{objectCount: objectCount, ratePerObject: cfg.ratePerObject, senderWorkers: cfg.senderWorkers, pullBatchSize: cfg.pullBatchSize, pubWorkers: cfg.pubWorkers, pubInFlight: cfg.pubInFlight, duration: cfg.duration, repeat: repeat, summary: summary, pass: pass, reason: reason}
		svc.Shutdown()
		shutdownDone = true
		startBenchmarkTimer(t)
		return result
	}

	summary = summarizeThroughput(plannedUpdates(objectCount, cfg.ratePerObject, cfg.duration), accepted, trialSamples, drain, publishErrors, commits, finalStats)
	pass, reason := evaluateCapacitySummary(summary, cfg)
	result := capacityTrialResult{
		objectCount:   objectCount,
		ratePerObject: cfg.ratePerObject,
		senderWorkers: cfg.senderWorkers,
		pullBatchSize: cfg.pullBatchSize,
		pubWorkers:    cfg.pubWorkers,
		pubInFlight:   cfg.pubInFlight,
		duration:      cfg.duration,
		repeat:        repeat,
		storageName:   cfg.storageName,
		summary:       summary,
		pass:          pass,
		reason:        reason,
	}
	svc.Shutdown()
	shutdownDone = true
	startBenchmarkTimer(t)
	return result
}

func reportCapacityMetrics(b *testing.B, trial capacityTrialResult) {
	b.Helper()
	summary := trial.summary
	if trial.pass {
		b.ReportMetric(1, "capacity_pass")
	} else {
		b.ReportMetric(0, "capacity_pass")
	}
	b.ReportMetric(float64(trial.objectCount), "objects")
	b.ReportMetric(float64(trial.ratePerObject), "per_object_updates_per_sec")
	b.ReportMetric(float64(trial.senderWorkers), "sender_workers")
	if trial.pullBatchSize > 0 {
		b.ReportMetric(float64(trial.pullBatchSize), "gt_pull_batch")
	}
	if trial.pubWorkers > 0 {
		b.ReportMetric(float64(trial.pubWorkers), "gt_pub_workers")
	}
	if trial.pubInFlight > 0 {
		b.ReportMetric(float64(trial.pubInFlight), "gt_pub_inflight")
	}
	b.ReportMetric(float64(trial.objectCount*trial.ratePerObject), "target_updates_per_sec")
	b.ReportMetric(float64(summary.sentUpdates), "sent_updates")
	b.ReportMetric(float64(summary.acceptedUpdates), "accepted_updates")
	b.ReportMetric(float64(summary.processedUpdates), "processed_updates")
	b.ReportMetric(float64(summary.maxLag), "max_lag_msgs")
	b.ReportMetric(float64(summary.endLag), "end_lag_msgs")
	b.ReportMetric(summary.meanLagRate, "mean_lag_rate_msgs_per_sec")
	b.ReportMetric(float64(summary.maxPending), "max_pending_msgs")
	b.ReportMetric(float64(summary.maxInFlight), "max_ack_pending_msgs")
	b.ReportMetric(float64(summary.drain.Milliseconds()), "drain_ms")
	b.ReportMetric(float64(summary.publishErrors), "publish_error_count")
	b.ReportMetric(float64(summary.ingesterP50.Milliseconds()), "ingester_p50_latency_ms")
	b.ReportMetric(float64(summary.ingesterP95.Milliseconds()), "ingester_p95_latency_ms")
	b.ReportMetric(float64(summary.ingesterP99.Milliseconds()), "ingester_p99_latency_ms")
	b.ReportMetric(float64(summary.geotruthP50.Milliseconds()), "geotruth_p50_latency_ms")
	b.ReportMetric(float64(summary.geotruthP95.Milliseconds()), "geotruth_p95_latency_ms")
	b.ReportMetric(float64(summary.geotruthP99.Milliseconds()), "geotruth_p99_latency_ms")
	b.ReportMetric(float64(summary.e2eP50.Milliseconds()), "e2e_p50_latency_ms")
	b.ReportMetric(float64(summary.e2eP95.Milliseconds()), "e2e_p95_latency_ms")
	b.ReportMetric(float64(summary.e2eP99.Milliseconds()), "e2e_p99_latency_ms")
	b.ReportMetric(float64(summary.finalPending), "final_pending_msgs")
	b.ReportMetric(float64(summary.finalAckPending), "final_ack_pending_msgs")
	b.ReportMetric(float64(summary.finalRedelivered), "final_redelivered_msgs")
}

func logCapacitySummary(tb testing.TB, trial capacityTrialResult) {
	tb.Helper()
	s := trial.summary
	tb.Logf(
		"capacity summary pass=%v reason=%q storage=%s objects=%d hz=%d sender_workers=%d gt_pull_batch=%d gt_pub_workers=%d gt_pub_inflight=%d sent=%d accepted=%d processed=%d publish_errors=%d mean_lag_rate=%.1f/s max_lag=%d end_lag=%d max_pending=%d max_ack_pending=%d drain_ms=%d ingester_p50/p95/p99_ms=%d/%d/%d geotruth_p50/p95/p99_ms=%d/%d/%d e2e_p50/p95/p99_ms=%d/%d/%d final_pending=%d final_ack_pending=%d final_redelivered=%d final_ack_floor=%d final_spatial_last=%d",
		trial.pass,
		trial.reason,
		trial.storageName,
		trial.objectCount,
		trial.ratePerObject,
		trial.senderWorkers,
		trial.pullBatchSize,
		trial.pubWorkers,
		trial.pubInFlight,
		s.sentUpdates,
		s.acceptedUpdates,
		s.processedUpdates,
		s.publishErrors,
		s.meanLagRate,
		s.maxLag,
		s.endLag,
		s.maxPending,
		s.maxInFlight,
		s.drain.Milliseconds(),
		s.ingesterP50.Milliseconds(),
		s.ingesterP95.Milliseconds(),
		s.ingesterP99.Milliseconds(),
		s.geotruthP50.Milliseconds(),
		s.geotruthP95.Milliseconds(),
		s.geotruthP99.Milliseconds(),
		s.e2eP50.Milliseconds(),
		s.e2eP95.Milliseconds(),
		s.e2eP99.Milliseconds(),
		s.finalPending,
		s.finalAckPending,
		s.finalRedelivered,
		s.finalAckFloorSeq,
		s.finalSpatialLast,
	)
}

func summarizeThroughput(sentUpdates int, accepted []acceptedUpdate, trialSamples []statsSample, drain time.Duration, publishErrors int64, commits map[commitKey]time.Time, finalStats geotruthops.Stats) throughputSummary {
	var maxLag, endLag, maxPending uint64
	var maxInFlight int
	if len(trialSamples) > 0 {
		endLag = trialSamples[len(trialSamples)-1].lag
	}
	for _, sample := range trialSamples {
		if sample.lag > maxLag {
			maxLag = sample.lag
		}
		if sample.pending > maxPending {
			maxPending = sample.pending
		}
		if sample.inFlight > maxInFlight {
			maxInFlight = sample.inFlight
		}
	}

	lvs := computeLatencies(accepted, commits)
	return throughputSummary{
		sentUpdates:      sentUpdates,
		acceptedUpdates:  len(accepted),
		processedUpdates: len(lvs.geotruth),
		publishErrors:    publishErrors,
		maxLag:           maxLag,
		endLag:           endLag,
		meanLagRate:      meanLagRate(trialSamples),
		maxPending:       maxPending,
		maxInFlight:      maxInFlight,
		drain:            drain,
		ingesterP50:      percentileDuration(lvs.ingester, 50),
		ingesterP95:      percentileDuration(lvs.ingester, 95),
		ingesterP99:      percentileDuration(lvs.ingester, 99),
		geotruthP50:      percentileDuration(lvs.geotruth, 50),
		geotruthP95:      percentileDuration(lvs.geotruth, 95),
		geotruthP99:      percentileDuration(lvs.geotruth, 99),
		e2eP50:           percentileDuration(lvs.e2e, 50),
		e2eP95:           percentileDuration(lvs.e2e, 95),
		e2eP99:           percentileDuration(lvs.e2e, 99),
		finalPending:     finalStats.GeoTruthConsumer.NumPending,
		finalAckPending:  finalStats.GeoTruthConsumer.NumAckPending,
		finalRedelivered: finalStats.GeoTruthConsumer.NumRedelivered,
		finalAckFloorSeq: finalStats.GeoTruthConsumer.AckFloorSeq,
		finalSpatialLast: finalStats.Spatial.LastSeq,
	}
}

func evaluateCapacitySummary(summary throughputSummary, cfg capacityConfig) (bool, string) {
	switch {
	case summary.publishErrors != 0:
		return false, fmt.Sprintf("publish errors: %d", summary.publishErrors)
	case summary.acceptedUpdates != summary.sentUpdates:
		return false, fmt.Sprintf("accepted %d of %d sent updates", summary.acceptedUpdates, summary.sentUpdates)
	case summary.processedUpdates != summary.acceptedUpdates:
		return false, fmt.Sprintf("processed %d of %d accepted updates", summary.processedUpdates, summary.acceptedUpdates)
	case summary.meanLagRate > 0:
		return false, fmt.Sprintf("lag trended positive: %+.1f msgs/sec over the trial", summary.meanLagRate)
	case summary.finalSpatialLast > summary.finalAckFloorSeq:
		return false, fmt.Sprintf("final ack floor %d below SPATIAL last seq %d", summary.finalAckFloorSeq, summary.finalSpatialLast)
	case summary.finalPending != 0:
		return false, fmt.Sprintf("final pending: %d", summary.finalPending)
	case summary.finalAckPending != 0:
		return false, fmt.Sprintf("final ack pending: %d", summary.finalAckPending)
	case summary.finalRedelivered != 0:
		return false, fmt.Sprintf("redelivered: %d", summary.finalRedelivered)
	case summary.geotruthP95 > cfg.p95Limit:
		return false, fmt.Sprintf("geotruth p95 latency %s above %s", summary.geotruthP95, cfg.p95Limit)
	default:
		return true, ""
	}
}

func waitForAckFloor(tb testing.TB, ops *geotruthops.Ops, wantSeq uint64, timeout time.Duration) {
	tb.Helper()
	ctx := context.Background()
	deadline := time.Now().Add(timeout)
	var last geotruthops.Stats
	for time.Now().Before(deadline) {
		stats, err := ops.Stats(ctx)
		if err == nil {
			last = stats
			if stats.GeoTruthConsumer.AckFloorSeq >= wantSeq {
				return
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	tb.Fatalf("ack floor did not reach %d within %s; last=%+v", wantSeq, timeout, last.GeoTruthConsumer)
}

func waitForSpatialLastSeq(tb testing.TB, js jetstream.JetStream, wantSeq uint64, timeout time.Duration) {
	tb.Helper()
	ctx := context.Background()
	stream, err := js.Stream(ctx, internalnatskeys.StreamName)
	if err != nil {
		tb.Fatalf("get SPATIAL stream: %v", err)
	}
	deadline := time.Now().Add(timeout)
	var last uint64
	for time.Now().Before(deadline) {
		info, err := stream.Info(ctx)
		if err == nil {
			last = info.State.LastSeq
			if last >= wantSeq {
				return
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	tb.Fatalf("SPATIAL last seq did not reach %d within %s; last=%d", wantSeq, timeout, last)
}

func sampleThroughputStats(ctx context.Context, ops *geotruthops.Ops, interval time.Duration, out chan<- statsSample) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			stats, err := ops.Stats(ctx)
			if err != nil {
				continue
			}
			var lag uint64
			if stats.Spatial.LastSeq > stats.GeoTruthConsumer.AckFloorSeq {
				lag = stats.Spatial.LastSeq - stats.GeoTruthConsumer.AckFloorSeq
			}
			select {
			case out <- statsSample{
				at:       now,
				ackFloor: stats.GeoTruthConsumer.AckFloorSeq,
				lag:      lag,
				pending:  stats.GeoTruthConsumer.NumPending,
				inFlight: stats.GeoTruthConsumer.NumAckPending,
			}:
			case <-ctx.Done():
				return
			}
		}
	}
}

func startStatsSampler(ctx context.Context, ops *geotruthops.Ops, interval time.Duration, buffer int) (<-chan statsSample, <-chan struct{}) {
	out := make(chan statsSample, buffer)
	done := make(chan struct{})
	go func() {
		defer close(done)
		defer close(out)
		sampleThroughputStats(ctx, ops, interval, out)
	}()
	return out, done
}

func collectStatsSamples(ch <-chan statsSample) []statsSample {
	var samples []statsSample
	for sample := range ch {
		samples = append(samples, sample)
	}
	return samples
}

func currentStatsSample(ctx context.Context, ops *geotruthops.Ops) (statsSample, bool) {
	stats, err := ops.Stats(ctx)
	if err != nil {
		return statsSample{}, false
	}
	var lag uint64
	if stats.Spatial.LastSeq > stats.GeoTruthConsumer.AckFloorSeq {
		lag = stats.Spatial.LastSeq - stats.GeoTruthConsumer.AckFloorSeq
	}
	return statsSample{
		at:       time.Now(),
		ackFloor: stats.GeoTruthConsumer.AckFloorSeq,
		lag:      lag,
		pending:  stats.GeoTruthConsumer.NumPending,
		inFlight: stats.GeoTruthConsumer.NumAckPending,
	}, true
}

func samplesUntil(samples []statsSample, at time.Time) []statsSample {
	idx := sort.Search(len(samples), func(i int) bool {
		return samples[i].at.After(at)
	})
	return samples[:idx]
}

func meanLagRate(samples []statsSample) float64 {
	if len(samples) < 2 {
		return 0
	}
	dt := samples[len(samples)-1].at.Sub(samples[0].at).Seconds()
	if dt <= 0 {
		return 0
	}
	return (float64(samples[len(samples)-1].lag) - float64(samples[0].lag)) / dt
}

func percentileDuration(values []time.Duration, p float64) time.Duration {
	if len(values) == 0 {
		return 0
	}
	sort.Slice(values, func(i, j int) bool { return values[i] < values[j] })
	idx := int(math.Ceil((p/100)*float64(len(values)))) - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= len(values) {
		idx = len(values) - 1
	}
	return values[idx]
}

type latencyVectors struct {
	ingester []time.Duration
	geotruth []time.Duration
	e2e      []time.Duration
}

func computeLatencies(accepted []acceptedUpdate, commits map[commitKey]time.Time) latencyVectors {
	if len(accepted) == 0 {
		return latencyVectors{}
	}
	sort.Slice(accepted, func(i, j int) bool {
		if accepted[i].objectID != accepted[j].objectID {
			return accepted[i].objectID < accepted[j].objectID
		}
		return accepted[i].clientOpID < accepted[j].clientOpID
	})
	ingester := make([]time.Duration, 0, len(accepted))
	geotruth := make([]time.Duration, 0, len(accepted))
	e2e := make([]time.Duration, 0, len(accepted))
	for _, update := range accepted {
		commitTime, ok := commits[commitKey{objectID: update.objectID, clientOpID: update.clientOpID}]
		if !ok {
			continue
		}
		ingester = append(ingester, update.acceptedAt.Sub(update.sentAt))
		d := commitTime.Sub(update.acceptedAt)
		if d < 0 {
			d = 0
		}
		geotruth = append(geotruth, d)
		d = commitTime.Sub(update.sentAt)
		if d < 0 {
			d = 0
		}
		e2e = append(e2e, d)
	}
	return latencyVectors{ingester: ingester, geotruth: geotruth, e2e: e2e}
}

func registerCapacityObjects(ctx context.Context, t testing.TB, nc *nats.Conn, objectCount, repeat, concurrency int) ([]string, uint64) {
	t.Helper()
	objectIDs := make([]string, objectCount)
	jobs := make(chan int)
	errCh := make(chan error, 1)
	var maxSeq atomic.Uint64
	var wg sync.WaitGroup

	workers := minInt(concurrency, objectCount)
	for worker := 0; worker < workers; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range jobs {
				objectID := fmt.Sprintf("capacity-%d-r%d-of-%d", i, repeat, objectCount)
				_, seq, err := requestSourceSeq(ctx, nc, natspublish.IngesterRegisterObjectSubject(objectID), natspublish.RegisterObjectReq{
					ID:   objectID,
					Dims: domain.ObjectDimensions{Width: 1, Height: 1},
				})
				if err != nil {
					select {
					case errCh <- fmt.Errorf("register %s: %w", objectID, err):
					default:
					}
					return
				}
				objectIDs[i] = objectID
				for {
					current := maxSeq.Load()
					if seq <= current || maxSeq.CompareAndSwap(current, seq) {
						break
					}
				}
			}
		}()
	}

	for i := 0; i < objectCount; i++ {
		select {
		case err := <-errCh:
			close(jobs)
			wg.Wait()
			t.Fatalf("%v", err)
		case jobs <- i:
		case <-ctx.Done():
			close(jobs)
			wg.Wait()
			t.Fatalf("register objects: %v", ctx.Err())
		}
	}
	close(jobs)
	wg.Wait()
	select {
	case err := <-errCh:
		t.Fatalf("%v", err)
	default:
	}
	return objectIDs, maxSeq.Load()
}

func startCapacityServices(t testing.TB, capacityCfg capacityConfig) *embedded.Services {
	t.Helper()
	cfg := embedded.DefaultConfig
	cfg.Ingester.Storage = capacityCfg.storage
	cfg.GeoTruth.Storage = capacityCfg.storage
	if capacityCfg.pullBatchSize > 0 {
		cfg.GeoTruth.Consumer.PullBatchSize = capacityCfg.pullBatchSize
	}
	if capacityCfg.readerBuffer > 0 {
		cfg.GeoTruth.Consumer.ReaderBuffer = capacityCfg.readerBuffer
	}
	if capacityCfg.pubWorkers > 0 {
		cfg.GeoTruth.Publisher.Workers = capacityCfg.pubWorkers
	}
	if capacityCfg.pubInFlight > 0 {
		cfg.GeoTruth.Publisher.MaxInFlightPerWorker = capacityCfg.pubInFlight
	}
	deps := embedded.DefaultDependencies
	deps.Resolver = embedded.NewFlatResolver(1)
	svc, err := embedded.Run(context.Background(), cfg, deps)
	if err != nil {
		t.Fatalf("start capacity services: %v", err)
	}
	return svc
}

func runClosedLoopFullPathLoad(t testing.TB, ctx context.Context, pool *natsConnPool, js jetstream.JetStream, objectIDs []string, cfg capacityConfig, duration time.Duration) fullPathMaxThroughputResult {
	t.Helper()
	if duration <= 0 || len(objectIDs) == 0 {
		return fullPathMaxThroughputResult{}
	}
	workers := cfg.senderWorkers
	if workers <= 0 {
		workers = 1
	}
	if workers > len(objectIDs) {
		workers = len(objectIDs)
	}

	waiter, err := newBenchmarkCommitWaiter(ctx, js)
	if err != nil {
		t.Fatalf("commit waiter: %v", err)
	}
	defer waiter.close(context.Background())

	var sent atomic.Int64
	var accepted atomic.Int64
	var errorsCount atomic.Int64
	var clientOpSeq atomic.Uint64
	objectSteps := make([]atomic.Int64, len(objectIDs))
	latCh := make(chan time.Duration, 8192)
	start := time.Now()
	stopAt := start.Add(duration)

	var wg sync.WaitGroup
	for worker := 0; worker < workers; worker++ {
		worker := worker
		conn := pool.get()
		wg.Add(1)
		go func() {
			defer wg.Done()
			for time.Now().Before(stopAt) {
				for idx := worker; idx < len(objectIDs); idx += workers {
					if !time.Now().Before(stopAt) {
						return
					}
					objectID := objectIDs[idx]
					step := int(objectSteps[idx].Add(1)) - 1
					clientOpID := strconv.FormatUint(clientOpSeq.Add(1), 36)
					key := commitKey{objectID: objectID, clientOpID: clientOpID}
					commitCh := waiter.add(key)

					sentAt, err := updateBenchObjectWithClientOpID(ctx, conn, objectID, step, clientOpID)
					sent.Add(1)
					if err != nil {
						waiter.remove(key, commitCh)
						errorsCount.Add(1)
						if ctx.Err() != nil {
							return
						}
						continue
					}

					select {
					case committedAt := <-commitCh:
						accepted.Add(1)
						latency := committedAt.Sub(sentAt)
						if latency < 0 {
							latency = 0
						}
						select {
						case latCh <- latency:
						default:
						}
					case <-ctx.Done():
						waiter.remove(key, commitCh)
						errorsCount.Add(1)
						return
					}
				}
			}
		}()
	}

	wg.Wait()
	close(latCh)
	done := time.Now()
	latencies := make([]time.Duration, 0, len(latCh))
	for latency := range latCh {
		latencies = append(latencies, latency)
	}
	return fullPathMaxThroughputResult{
		sent:          sent.Load(),
		accepted:      accepted.Load(),
		errors:        errorsCount.Load(),
		elapsed:       done.Sub(start),
		commitLatency: latencies,
	}
}

func updateBenchObjectWithClientOpID(ctx context.Context, nc *nats.Conn, objectID string, step int, clientOpID string) (sentAt time.Time, err error) {
	return requestOK(ctx, nc, natspublish.IngesterUpdatePositionSubject(objectID), natspublish.UpdatePositionReq{
		ID:         objectID,
		X:          float64(step % 1000),
		Y:          float64((step * 7) % 1000),
		Z:          1.0,
		RotY:       0,
		ClientOpID: clientOpID,
	})
}
func sendMultiObjectScheduled(ctx context.Context, pool *natsConnPool, objectIDs []string, perObjectRate int, duration time.Duration, clientOpSeq *atomic.Uint64, workers int) ([]acceptedUpdate, int64) {
	steps := int(duration.Seconds() * float64(perObjectRate))
	if steps <= 0 || len(objectIDs) == 0 {
		return nil, 0
	}
	if workers <= 0 {
		workers = 1
	}
	if workers > len(objectIDs) {
		workers = len(objectIDs)
	}

	results := make(chan acceptedUpdate, len(objectIDs)*steps)
	var publishErrors int64
	var firstErr sync.Once
	var wg sync.WaitGroup
	start := time.Now()
	interval := time.Second / time.Duration(perObjectRate)
	stagger := time.Duration(0)
	if len(objectIDs) > 1 {
		stagger = interval / time.Duration(len(objectIDs))
	}

	for worker := 0; worker < workers; worker++ {
		conn := pool.get()
		wg.Add(1)
		go func(worker int, conn *nats.Conn) {
			defer wg.Done()
			for step := 0; step < steps; step++ {
				for idx := worker; idx < len(objectIDs); idx += workers {
					objectID := objectIDs[idx]
					due := start.Add(time.Duration(idx) * stagger).Add(time.Duration(step) * interval)
					if sleep := time.Until(due); sleep > 0 {
						select {
						case <-ctx.Done():
							return
						case <-time.After(sleep):
						}
					}
					sentAt, clientOpID, err := updateBenchObject(ctx, conn, objectID, idx*steps+step, clientOpSeq)
					if err != nil {
						firstErr.Do(func() { log.Printf("sendMultiObjectScheduled: first publish error: %v", err) })
						atomic.AddInt64(&publishErrors, 1)
						continue
					}
					results <- acceptedUpdate{objectID: objectID, clientOpID: clientOpID, sentAt: sentAt, acceptedAt: time.Now()}
				}
			}
		}(worker, conn)
	}

	wg.Wait()
	close(results)
	return acceptedResults(results, cap(results)), publishErrors
}

func acceptedResults(results <-chan acceptedUpdate, capacity int) []acceptedUpdate {
	accepted := make([]acceptedUpdate, 0, capacity)
	for result := range results {
		accepted = append(accepted, result)
	}
	return accepted
}

func drainUntilAckedOK(ctx context.Context, ops *geotruthops.Ops, maxRegSeq uint64, acceptedCount int, timeout time.Duration) (time.Duration, bool) {
	start := time.Now()
	deadline := time.Now().Add(timeout)
	expectedSpatialSeq := maxRegSeq + uint64(acceptedCount)
	for time.Now().Before(deadline) {
		stats, err := ops.Stats(ctx)
		if err == nil &&
			stats.Spatial.LastSeq >= expectedSpatialSeq &&
			stats.GeoTruthConsumer.AckFloorSeq >= expectedSpatialSeq &&
			stats.GeoTruthConsumer.NumPending == 0 &&
			stats.GeoTruthConsumer.NumAckPending == 0 {
			return time.Since(start), true
		}
		select {
		case <-ctx.Done():
			return time.Since(start), false
		case <-time.After(10 * time.Millisecond):
		}
	}
	return time.Since(start), false
}

func plannedUpdates(objectCount, perObjectRate int, duration time.Duration) int {
	return objectCount * int(duration.Seconds()*float64(perObjectRate))
}

func startNoopAckerServices(t testing.TB, storage jetstream.StorageType) (*embedded.NATSServer, *nats.Conn) {
	t.Helper()
	ctx := context.Background()
	natsSvc, err := embedded.RunNATSServer(ctx, embedded.NATSServerConfig{Port: -1})
	if err != nil {
		t.Fatalf("start nats server: %v", err)
	}
	nc := natsSvc.NATSConn()
	js, err := jetstream.New(nc)
	if err != nil {
		natsSvc.Shutdown()
		t.Fatalf("create jetstream: %v", err)
	}

	ingesterCfg := embedded.DefaultConfig.Ingester
	ingesterCfg.Storage = storage
	_, err = embedded.RunIngester(ctx, ingesterCfg, pkgingester.Dependencies{
		NATS: func(role string) (*nats.Conn, error) {
			return nats.Connect(natsSvc.NATSURL(),
				nats.RetryOnFailedConnect(true),
				nats.MaxReconnects(-1),
				nats.ReconnectWait(2*time.Second),
			)
		},
	})
	if err != nil {
		natsSvc.Shutdown()
		t.Fatalf("start ingester: %v", err)
	}

	ackCtx, cancelAck := context.WithCancel(ctx)
	cons, err := js.CreateConsumer(ackCtx, internalnatskeys.StreamName, jetstream.ConsumerConfig{
		FilterSubjects: internalnatskeys.StreamSubjects,
		AckPolicy:      jetstream.AckExplicitPolicy,
		AckWait:        60 * time.Second,
		MaxAckPending:  10000,
		MaxDeliver:     -1,
		DeliverPolicy:  jetstream.DeliverNewPolicy,
	})
	if err != nil {
		cancelAck()
		natsSvc.Shutdown()
		t.Fatalf("create noop consumer: %v", err)
	}

	iter, err := cons.Messages(jetstream.PullMaxMessages(256))
	if err != nil {
		cancelAck()
		natsSvc.Shutdown()
		t.Fatalf("create noop iterator: %v", err)
	}

	go func() {
		for {
			msg, err := iter.Next()
			if err != nil {
				return
			}
			msg.Ack()
		}
	}()

	t.Cleanup(func() {
		cancelAck()
		natsSvc.Shutdown()
	})

	return natsSvc, nc
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func effectiveCapacityPubWorkers(configured int) int {
	if configured > 0 {
		return configured
	}
	return 2
}

func effectiveCapacityPubInFlight(configured int) int {
	if configured > 0 {
		return configured
	}
	return 64
}
