package integration

import (
	"context"
	"flag"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/midtxwn/geotruth/embedded"
	"github.com/midtxwn/geotruth/pkg/domain"
	"github.com/midtxwn/geotruth/pkg/geotruth"
	"github.com/midtxwn/geotruth/pkg/geotruthops"
	"github.com/midtxwn/geotruth/pkg/natspublish"
	"github.com/midtxwn/geotruth/pkg/natsquery"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

var (
	gtCapacityObjects           = flag.Int("objects", 10000, "number of registered objects")
	gtCapacityHz                = flag.Int("hz", 1, "position updates per object per second")
	gtCapacityPool              = flag.Int("pool", 512, "client NATS connection pool size")
	gtCapacitySenderWorkers     = flag.Int("sender-workers", 512, "scheduled sender worker count")
	gtCapacityWarmup            = flag.Duration("warmup", 2*time.Second, "warmup duration before measured trial")
	gtCapacityTrialWindow       = flag.Duration("trial-window", 5*time.Second, "measured scheduled-load window")
	gtCapacityDrainTimeout      = flag.Duration("drain-timeout", 30*time.Second, "time allowed after trial window for scheduled requests to complete")
	gtCapacityP95Limit          = flag.Duration("p95-limit", 250*time.Millisecond, "commit p95 latency limit; <=0 disables this pass check")
	gtCapacitySchedulerP95Limit = flag.Duration("scheduler-p95-limit", 50*time.Millisecond, "sender scheduler p95 lag limit; <=0 disables this pass check")
	gtCapacityScenario          = flag.String("scenario", "position", "benchmark scenario: position, geofence-periodic, geofence-toggle, or region-transition")
	gtCapacityGeofencePeriod    = flag.Int("geofence-period", 16, "updates between geofence crossings for geofence-periodic scenario")
	gtCapacityRegions           = flag.Int("regions", 1, "number of flat resolver regions/floors")
	gtCapacityStorage           = flag.String("storage", "memory", "GT_EVENTS stream storage: memory or file")
	gtCapacityGTPubWorkers      = flag.Int("gt-pub-workers", 0, "GeoTruth GT_EVENTS publisher workers; 0 uses service default")
	gtCapacityGTPubInflight     = flag.Int("gt-pub-inflight", 0, "GeoTruth max in-flight envelopes per publisher worker; 0 uses service default")
	gtCapacityCPUProfile        = flag.Bool("trial-cpuprofile", false, "write CPU profile for measured trial only")
	gtCapacityMutexProfile      = flag.Bool("trial-mutexprofile", false, "write mutex profile for measured trial only")
	gtCapacityProfileDir        = flag.String("profile-dir", "profiles", "directory for trial profile output")
	gtCapacityFailOnCapacity    = flag.Bool("fail-on-capacity", false, "fail benchmark when capacity pass criteria are not met")
)

const (
	capacityScenarioPosition         = "position"
	capacityScenarioGeofencePeriodic = "geofence-periodic"
	capacityScenarioGeofenceToggle   = "geofence-toggle"
	capacityScenarioRegionTransition = "region-transition"

	capacityDefaultGTPubWorkers  = 2
	capacityDefaultGTPubInflight = 64
)

type geotruthCapacityConfig struct {
	objects           int
	hz                int
	pool              int
	senderWorkers     int
	warmup            time.Duration
	trialWindow       time.Duration
	drainTimeout      time.Duration
	p95Limit          time.Duration
	schedulerP95Limit time.Duration
	scenario          string
	geofencePeriod    int
	regions           int
	storage           jetstream.StorageType
	storageName       string
	gtPubWorkers      int
	gtPubInflight     int
	cpuProfile        bool
	mutexProfile      bool
	profileDir        string
	failOnCapacity    bool
}

type benchNATSPool struct {
	conns []*nats.Conn
}

type updateResult struct {
	dueAt  time.Time
	sentAt time.Time
	doneAt time.Time
	err    error
}

type lagSample struct {
	at            time.Time
	completionLag int64
	driverLag     int64
	inflight      int64
}

type durationSummary struct {
	p50 time.Duration
	p95 time.Duration
	p99 time.Duration
}

type scheduledLoadResult struct {
	planned               int64
	sent                  int64
	accepted              int64
	errors                int64
	elapsed               time.Duration
	drain                 time.Duration
	commitLatency         durationSummary
	scheduleLag           durationSummary
	e2eLatency            durationSummary
	meanCompletionLagRate float64
	maxCompletionLag      int64
	endCompletionLag      int64
	maxDriverLag          int64
	maxInflight           int64
}

type maxThroughputResult struct {
	sent          int64
	accepted      int64
	errors        int64
	elapsed       time.Duration
	drain         time.Duration
	commitLatency durationSummary
}

func parseGeoTruthBenchmarkStorage(raw string) (jetstream.StorageType, string, error) {
	switch raw {
	case "", "memory":
		return jetstream.MemoryStorage, "memory", nil
	case "file":
		return jetstream.FileStorage, "file", nil
	default:
		return 0, "", fmt.Errorf("unknown -storage %q; expected memory or file", raw)
	}
}

func BenchmarkGeoTruthCapacity(b *testing.B) {
	cfg := readGeoTruthCapacityConfig(b)
	for i := 0; i < b.N; i++ {
		runGeoTruthCapacityTrial(b, cfg)
	}
}

func BenchmarkGeoTruthMaxThroughput(b *testing.B) {
	cfg := readGeoTruthCapacityConfig(b)
	for i := 0; i < b.N; i++ {
		runGeoTruthMaxThroughputTrial(b, cfg)
	}
}

func readGeoTruthCapacityConfig(b *testing.B) geotruthCapacityConfig {
	b.Helper()
	storage, storageName, err := parseGeoTruthBenchmarkStorage(*gtCapacityStorage)
	if err != nil {
		b.Fatalf("%v", err)
	}
	cfg := geotruthCapacityConfig{
		objects:           *gtCapacityObjects,
		hz:                *gtCapacityHz,
		pool:              *gtCapacityPool,
		senderWorkers:     *gtCapacitySenderWorkers,
		warmup:            *gtCapacityWarmup,
		trialWindow:       *gtCapacityTrialWindow,
		drainTimeout:      *gtCapacityDrainTimeout,
		p95Limit:          *gtCapacityP95Limit,
		schedulerP95Limit: *gtCapacitySchedulerP95Limit,
		scenario:          *gtCapacityScenario,
		geofencePeriod:    *gtCapacityGeofencePeriod,
		regions:           *gtCapacityRegions,
		storage:           storage,
		storageName:       storageName,
		gtPubWorkers:      *gtCapacityGTPubWorkers,
		gtPubInflight:     *gtCapacityGTPubInflight,
		cpuProfile:        *gtCapacityCPUProfile,
		mutexProfile:      *gtCapacityMutexProfile,
		profileDir:        *gtCapacityProfileDir,
		failOnCapacity:    *gtCapacityFailOnCapacity,
	}
	if cfg.objects <= 0 {
		b.Fatalf("-objects must be > 0, got %d", cfg.objects)
	}
	if cfg.hz <= 0 {
		b.Fatalf("-hz must be > 0, got %d", cfg.hz)
	}
	if cfg.pool <= 0 {
		b.Fatalf("-pool must be > 0, got %d", cfg.pool)
	}
	if cfg.senderWorkers <= 0 {
		b.Fatalf("-sender-workers must be > 0, got %d", cfg.senderWorkers)
	}
	if cfg.regions <= 0 {
		b.Fatalf("-regions must be > 0, got %d", cfg.regions)
	}
	if cfg.geofencePeriod <= 0 {
		b.Fatalf("-geofence-period must be > 0, got %d", cfg.geofencePeriod)
	}
	if cfg.warmup < 0 || cfg.trialWindow <= 0 || cfg.drainTimeout < 0 {
		b.Fatalf("durations must satisfy warmup>=0, trial-window>0, drain-timeout>=0")
	}
	if cfg.gtPubWorkers < 0 || cfg.gtPubInflight < 0 {
		b.Fatalf("GeoTruth publisher tuning flags must be >= 0")
	}
	if cfg.cpuProfile && cfg.mutexProfile {
		b.Fatalf("-trial-cpuprofile and -trial-mutexprofile cannot be used together")
	}
	switch cfg.scenario {
	case capacityScenarioPosition, capacityScenarioGeofencePeriodic, capacityScenarioGeofenceToggle, capacityScenarioRegionTransition:
	default:
		b.Fatalf("unknown -scenario %q", cfg.scenario)
	}
	return cfg
}

func runGeoTruthCapacityTrial(b *testing.B, cfg geotruthCapacityConfig) {
	b.Helper()

	embeddedCfg := embedded.DefaultConfig
	embeddedCfg.GeoTruth.Storage = cfg.storage
	embeddedCfg.GeoTruth.Publisher = geotruth.PublisherConfig{
		Workers:              cfg.gtPubWorkers,
		MaxInFlightPerWorker: cfg.gtPubInflight,
	}
	svc, err := embedded.RunLocalStack(context.Background(), embeddedCfg, benchmarkDependencies(cfg))
	if err != nil {
		b.Fatalf("start embedded stack: %v", err)
	}
	defer svc.Shutdown()

	pool, err := newBenchNATSPool(svc.NATSURL(), cfg.pool)
	if err != nil {
		b.Fatalf("create client pool: %v", err)
	}
	defer pool.Close()

	pub := natspublish.New(pool.Get(0))
	query := natsquery.New(pool.Get(0))
	if err := setupCapacityScenario(context.Background(), pub, query, cfg.scenario, cfg.regions); err != nil {
		b.Fatalf("setup scenario: %v", err)
	}

	objectIDs := makeCapacityObjectIDs(cfg.objects)
	if err := registerCapacityObjects(context.Background(), pool, objectIDs, cfg.senderWorkers); err != nil {
		b.Fatalf("register objects: %v", err)
	}

	warmupSteps := stepsForDuration(cfg.warmup, cfg.hz)
	if warmupSteps > 0 {
		warmupCtx, cancel := context.WithTimeout(context.Background(), cfg.warmup+cfg.drainTimeout)
		warmup, err := runScheduledPositionLoad(warmupCtx, pool, objectIDs, cfg, cfg.warmup, 0)
		cancel()
		if err != nil {
			b.Fatalf("warmup load: %v", err)
		}
		if warmup.accepted != warmup.planned || warmup.errors != 0 {
			b.Fatalf("warmup failed: accepted=%d planned=%d errors=%d", warmup.accepted, warmup.planned, warmup.errors)
		}
	}

	ops, err := geotruthops.New(svc.NATSConn())
	if err != nil {
		b.Fatalf("ops client: %v", err)
	}
	beforeStats, err := ops.Stats(context.Background())
	if err != nil {
		b.Fatalf("stats before trial: %v", err)
	}

	stopProfile, err := startCapacityTrialProfile(cfg)
	if err != nil {
		b.Fatalf("start trial profile: %v", err)
	}

	loadCtx, cancel := context.WithTimeout(context.Background(), cfg.trialWindow+cfg.drainTimeout)
	b.ResetTimer()
	result, loadErr := runScheduledPositionLoad(loadCtx, pool, objectIDs, cfg, cfg.trialWindow, warmupSteps)
	b.StopTimer()
	cancel()
	if stopErr := stopProfile(); stopErr != nil {
		b.Fatalf("stop trial profile: %v", stopErr)
	}
	if loadErr != nil {
		b.Fatalf("trial load: %v", loadErr)
	}

	afterStats, err := ops.Stats(context.Background())
	if err != nil {
		b.Fatalf("stats after trial: %v", err)
	}

	gtEventsDeltaMsgs := deltaUint64(afterStats.GTEvents.Messages, beforeStats.GTEvents.Messages)
	gtEventsDeltaBytes := deltaUint64(afterStats.GTEvents.Bytes, beforeStats.GTEvents.Bytes)
	pass, reason := evaluateCapacityResult(cfg, result)
	reportCapacityMetrics(b, cfg, result, pass, gtEventsDeltaMsgs, gtEventsDeltaBytes)

	b.Logf("capacity summary pass=%v reason=%q scenario=%s storage=%s objects=%d hz=%d regions=%d sender_workers=%d client_conns=%d gt_pub_workers=%d gt_pub_inflight=%d planned=%d sent=%d accepted=%d errors=%d mean_completion_lag_rate=%.1f/s max_completion_lag=%d end_completion_lag=%d max_driver_lag=%d max_inflight=%d drain_ms=%d commit_p50/p95/p99_ms=%.3f/%.3f/%.3f schedule_p50/p95/p99_ms=%.3f/%.3f/%.3f e2e_p50/p95/p99_ms=%.3f/%.3f/%.3f gt_events_delta_msgs=%d gt_events_delta_bytes=%d",
		pass, reason, cfg.scenario, cfg.storageName, cfg.objects, cfg.hz, cfg.regions, cfg.senderWorkers, cfg.pool,
		effectiveGTPubWorkers(cfg.gtPubWorkers), effectiveGTPubInflight(cfg.gtPubInflight),
		result.planned, result.sent, result.accepted, result.errors,
		result.meanCompletionLagRate, result.maxCompletionLag, result.endCompletionLag, result.maxDriverLag, result.maxInflight,
		result.drain.Milliseconds(),
		durationMillis(result.commitLatency.p50), durationMillis(result.commitLatency.p95), durationMillis(result.commitLatency.p99),
		durationMillis(result.scheduleLag.p50), durationMillis(result.scheduleLag.p95), durationMillis(result.scheduleLag.p99),
		durationMillis(result.e2eLatency.p50), durationMillis(result.e2eLatency.p95), durationMillis(result.e2eLatency.p99),
		gtEventsDeltaMsgs, gtEventsDeltaBytes)

	if cfg.failOnCapacity && !pass {
		b.Fatalf("capacity target failed: %s", reason)
	}
}

func runGeoTruthMaxThroughputTrial(b *testing.B, cfg geotruthCapacityConfig) {
	b.Helper()

	embeddedCfg := embedded.DefaultConfig
	embeddedCfg.GeoTruth.Storage = cfg.storage
	embeddedCfg.GeoTruth.Publisher = geotruth.PublisherConfig{
		Workers:              cfg.gtPubWorkers,
		MaxInFlightPerWorker: cfg.gtPubInflight,
	}
	svc, err := embedded.RunLocalStack(context.Background(), embeddedCfg, benchmarkDependencies(cfg))
	if err != nil {
		b.Fatalf("start embedded stack: %v", err)
	}
	defer svc.Shutdown()

	pool, err := newBenchNATSPool(svc.NATSURL(), cfg.pool)
	if err != nil {
		b.Fatalf("create client pool: %v", err)
	}
	defer pool.Close()

	pub := natspublish.New(pool.Get(0))
	query := natsquery.New(pool.Get(0))
	if err := setupCapacityScenario(context.Background(), pub, query, cfg.scenario, cfg.regions); err != nil {
		b.Fatalf("setup scenario: %v", err)
	}

	objectIDs := makeCapacityObjectIDs(cfg.objects)
	if err := registerCapacityObjects(context.Background(), pool, objectIDs, cfg.senderWorkers); err != nil {
		b.Fatalf("register objects: %v", err)
	}

	if cfg.warmup > 0 {
		warmupCtx, cancel := context.WithTimeout(context.Background(), cfg.warmup+cfg.drainTimeout)
		warmup, err := runClosedLoopPositionLoad(warmupCtx, pool, objectIDs, cfg, cfg.warmup, 0)
		cancel()
		if err != nil {
			b.Fatalf("warmup load: %v", err)
		}
		if warmup.errors != 0 {
			b.Fatalf("warmup failed: accepted=%d errors=%d", warmup.accepted, warmup.errors)
		}
	}

	ops, err := geotruthops.New(svc.NATSConn())
	if err != nil {
		b.Fatalf("ops client: %v", err)
	}
	beforeStats, err := ops.Stats(context.Background())
	if err != nil {
		b.Fatalf("stats before trial: %v", err)
	}

	stopProfile, err := startCapacityTrialProfile(cfg)
	if err != nil {
		b.Fatalf("start trial profile: %v", err)
	}

	loadCtx, cancel := context.WithTimeout(context.Background(), cfg.trialWindow+cfg.drainTimeout)
	b.ResetTimer()
	result, loadErr := runClosedLoopPositionLoad(loadCtx, pool, objectIDs, cfg, cfg.trialWindow, 0)
	b.StopTimer()
	cancel()
	if stopErr := stopProfile(); stopErr != nil {
		b.Fatalf("stop trial profile: %v", stopErr)
	}
	if loadErr != nil {
		b.Fatalf("trial load: %v", loadErr)
	}

	afterStats, err := ops.Stats(context.Background())
	if err != nil {
		b.Fatalf("stats after trial: %v", err)
	}

	gtEventsDeltaMsgs := deltaUint64(afterStats.GTEvents.Messages, beforeStats.GTEvents.Messages)
	gtEventsDeltaBytes := deltaUint64(afterStats.GTEvents.Bytes, beforeStats.GTEvents.Bytes)
	reportMaxThroughputMetrics(b, cfg, result, gtEventsDeltaMsgs, gtEventsDeltaBytes)

	b.Logf("max throughput summary scenario=%s storage=%s objects=%d regions=%d sender_workers=%d client_conns=%d gt_pub_workers=%d gt_pub_inflight=%d sent=%d accepted=%d errors=%d elapsed_ms=%.3f drain_ms=%.3f completed_updates_per_sec=%.1f accepted_per_trial_sec=%.1f commit_p50/p95/p99_ms=%.3f/%.3f/%.3f gt_events_delta_msgs=%d gt_events_delta_bytes=%d",
		cfg.scenario, cfg.storageName, cfg.objects, cfg.regions, cfg.senderWorkers, cfg.pool,
		effectiveGTPubWorkers(cfg.gtPubWorkers), effectiveGTPubInflight(cfg.gtPubInflight),
		result.sent, result.accepted, result.errors,
		durationMillis(result.elapsed), durationMillis(result.drain),
		completedThroughput(result), trialWindowThroughput(cfg, result),
		durationMillis(result.commitLatency.p50), durationMillis(result.commitLatency.p95), durationMillis(result.commitLatency.p99),
		gtEventsDeltaMsgs, gtEventsDeltaBytes)
}

func newBenchNATSPool(url string, size int) (*benchNATSPool, error) {
	pool := &benchNATSPool{conns: make([]*nats.Conn, size)}
	for i := range pool.conns {
		nc, err := nats.Connect(url,
			nats.Name(fmt.Sprintf("geotruth-capacity-client-%d", i)),
			nats.RetryOnFailedConnect(true),
			nats.MaxReconnects(-1),
			nats.ReconnectWait(2*time.Second),
		)
		if err != nil {
			pool.Close()
			return nil, err
		}
		pool.conns[i] = nc
	}
	return pool, nil
}

func (p *benchNATSPool) Get(idx int) *nats.Conn {
	return p.conns[idx%len(p.conns)]
}

func (p *benchNATSPool) Close() {
	for _, nc := range p.conns {
		if nc != nil {
			nc.Close()
		}
	}
}

func benchmarkDependencies(cfg geotruthCapacityConfig) embedded.Dependencies {
	return embedded.Dependencies{
		Resolver: embedded.NewFlatResolver(cfg.regions),
	}
}

func setupCapacityScenario(ctx context.Context, pub natspublish.Publish, query natsquery.Query, scenario string, regions int) error {
	if scenario != capacityScenarioGeofencePeriodic && scenario != capacityScenarioGeofenceToggle && scenario != capacityScenarioRegionTransition {
		return nil
	}
	points := []domain.Point{{X: 0, Y: 0}, {X: 10, Y: 0}, {X: 10, Y: 10}, {X: 0, Y: 10}}
	for region := 0; region < regions; region++ {
		areaID := benchmarkAreaID(region)
		if err := pub.RegisterArea(ctx, areaID, fmt.Sprintf("%d", region), points); err != nil {
			return err
		}
	}
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		ready := true
		for region := 0; region < regions; region++ {
			if _, err := query.Area(ctx, benchmarkAreaID(region)); err != nil {
				ready = false
				break
			}
		}
		if ready {
			return nil
		}
		time.Sleep(10 * time.Millisecond)
	}
	return fmt.Errorf("benchmark areas were not visible before timeout")
}

func benchmarkAreaID(region int) string {
	return fmt.Sprintf("bench-zone-%d", region)
}

func makeCapacityObjectIDs(n int) []string {
	ids := make([]string, n)
	for i := range ids {
		ids[i] = fmt.Sprintf("capacity-%d", i)
	}
	return ids
}

func registerCapacityObjects(ctx context.Context, pool *benchNATSPool, objectIDs []string, workers int) error {
	if workers > len(objectIDs) {
		workers = len(objectIDs)
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var once sync.Once
	var firstErr error
	setErr := func(err error) {
		if err == nil {
			return
		}
		once.Do(func() {
			firstErr = err
			cancel()
		})
	}

	dims := domain.ObjectDimensions{Width: 1, Height: 1}
	var wg sync.WaitGroup
	for worker := 0; worker < workers; worker++ {
		worker := worker
		wg.Add(1)
		go func() {
			defer wg.Done()
			pub := natspublish.New(pool.Get(worker))
			for idx := worker; idx < len(objectIDs); idx += workers {
				select {
				case <-ctx.Done():
					return
				default:
				}
				if _, err := pub.RegisterObject(ctx, objectIDs[idx], dims); err != nil {
					setErr(fmt.Errorf("register %s: %w", objectIDs[idx], err))
					return
				}
			}
		}()
	}
	wg.Wait()
	return firstErr
}

func runScheduledPositionLoad(ctx context.Context, pool *benchNATSPool, objectIDs []string, cfg geotruthCapacityConfig, duration time.Duration, baseStep int) (scheduledLoadResult, error) {
	steps := stepsForDuration(duration, cfg.hz)
	planned := int64(len(objectIDs) * steps)
	if planned == 0 {
		return scheduledLoadResult{}, nil
	}

	workers := cfg.senderWorkers
	if workers > len(objectIDs) {
		workers = len(objectIDs)
	}

	var sent atomic.Int64
	var accepted atomic.Int64
	var errors atomic.Int64

	resultCh := make(chan updateResult, minInt64(planned, 8192))
	startAt := time.Now().Add(50 * time.Millisecond)
	trialEnd := startAt.Add(duration)
	interval := time.Duration(float64(time.Second) / float64(cfg.hz))
	stagger := time.Duration(float64(interval) / float64(len(objectIDs)))
	if stagger <= 0 {
		stagger = time.Nanosecond
	}

	var wg sync.WaitGroup
	for worker := 0; worker < workers; worker++ {
		worker := worker
		wg.Add(1)
		go func() {
			defer wg.Done()
			pub := natspublish.New(pool.Get(worker))
			for step := 0; step < steps; step++ {
				for objIdx := worker; objIdx < len(objectIDs); objIdx += workers {
					dueAt := startAt.Add(time.Duration(step) * interval).Add(time.Duration(objIdx) * stagger)
					if err := waitUntil(ctx, dueAt); err != nil {
						return
					}

					x, y, z, rotY := positionForScenario(cfg, objIdx, baseStep+step)
					sentAt := time.Now()
					sent.Add(1)
					_, err := pub.UpdateObjectPosition(ctx, objectIDs[objIdx], x, y, z, rotY)
					doneAt := time.Now()
					if err != nil {
						errors.Add(1)
					} else {
						accepted.Add(1)
					}

					select {
					case resultCh <- updateResult{dueAt: dueAt, sentAt: sentAt, doneAt: doneAt, err: err}:
					case <-ctx.Done():
						return
					}
					if err != nil && ctx.Err() != nil {
						return
					}
				}
			}
		}()
	}

	doneCh := make(chan struct{})
	go func() {
		wg.Wait()
		close(resultCh)
		close(doneCh)
	}()

	var commitLatencies []int64
	var scheduleLags []int64
	var e2eLatencies []int64
	samples := make([]lagSample, 0, int(duration/(100*time.Millisecond))+8)

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	trialEndTimer := time.NewTimer(time.Until(trialEnd))
	defer trialEndTimer.Stop()
	ctxDone := ctx.Done()
	endRecorded := false
	resultClosed := false
	doneAt := time.Now()

	recordSample := func(now time.Time) lagSample {
		plannedDue := plannedDueAt(now, startAt, duration, len(objectIDs), steps, interval, stagger)
		completed := accepted.Load()
		sample := lagSample{
			at:            now,
			completionLag: maxInt64(0, plannedDue-completed),
			driverLag:     maxInt64(0, plannedDue-sent.Load()),
			inflight:      maxInt64(0, sent.Load()-completed-errors.Load()),
		}
		samples = append(samples, sample)
		return sample
	}

	for !resultClosed || !endRecorded {
		select {
		case res, ok := <-resultCh:
			if !ok {
				resultClosed = true
				doneAt = time.Now()
				continue
			}
			scheduleLags = append(scheduleLags, maxDuration(0, res.sentAt.Sub(res.dueAt)).Nanoseconds())
			if res.err == nil {
				commitLatencies = append(commitLatencies, res.doneAt.Sub(res.sentAt).Nanoseconds())
				e2eLatencies = append(e2eLatencies, res.doneAt.Sub(res.dueAt).Nanoseconds())
			}

		case now := <-ticker.C:
			recordSample(now)

		case now := <-trialEndTimer.C:
			sample := recordSample(now)
			_ = sample
			endRecorded = true

		case <-ctxDone:
			<-doneCh
			ctxDone = nil
		}
	}

	if !resultClosed {
		for range resultCh {
		}
		doneAt = time.Now()
	}
	if doneAt.Before(trialEnd) {
		if err := waitUntil(context.Background(), trialEnd); err != nil {
			return scheduledLoadResult{}, err
		}
		doneAt = trialEnd
	}

	maxCompletionLag, endCompletionLag, maxDriverLag, maxInflight := summarizeLagSamples(samples, trialEnd)
	return scheduledLoadResult{
		planned:               planned,
		sent:                  sent.Load(),
		accepted:              accepted.Load(),
		errors:                errors.Load(),
		elapsed:               doneAt.Sub(startAt),
		drain:                 maxDuration(0, doneAt.Sub(trialEnd)),
		commitLatency:         summarizeDurations(commitLatencies),
		scheduleLag:           summarizeDurations(scheduleLags),
		e2eLatency:            summarizeDurations(e2eLatencies),
		meanCompletionLagRate: lagSlope(samples, startAt, trialEnd),
		maxCompletionLag:      maxCompletionLag,
		endCompletionLag:      endCompletionLag,
		maxDriverLag:          maxDriverLag,
		maxInflight:           maxInflight,
	}, nil
}

func runClosedLoopPositionLoad(ctx context.Context, pool *benchNATSPool, objectIDs []string, cfg geotruthCapacityConfig, duration time.Duration, baseStep int) (maxThroughputResult, error) {
	if duration <= 0 {
		return maxThroughputResult{}, nil
	}

	workers := cfg.senderWorkers
	if workers > len(objectIDs) {
		workers = len(objectIDs)
	}
	if workers <= 0 {
		return maxThroughputResult{}, nil
	}

	var sent atomic.Int64
	var accepted atomic.Int64
	var errors atomic.Int64
	objectSteps := make([]atomic.Int64, len(objectIDs))

	resultCh := make(chan updateResult, 8192)
	startAt := time.Now()
	stopIssuingAt := startAt.Add(duration)

	var wg sync.WaitGroup
	for worker := 0; worker < workers; worker++ {
		worker := worker
		wg.Add(1)
		go func() {
			defer wg.Done()
			pub := natspublish.New(pool.Get(worker))
			for time.Now().Before(stopIssuingAt) {
				for objIdx := worker; objIdx < len(objectIDs); objIdx += workers {
					if !time.Now().Before(stopIssuingAt) {
						return
					}

					step := baseStep + int(objectSteps[objIdx].Add(1)) - 1
					x, y, z, rotY := positionForScenario(cfg, objIdx, step)
					sentAt := time.Now()
					sent.Add(1)
					_, err := pub.UpdateObjectPosition(ctx, objectIDs[objIdx], x, y, z, rotY)
					doneAt := time.Now()
					if err != nil {
						errors.Add(1)
					} else {
						accepted.Add(1)
					}

					select {
					case resultCh <- updateResult{sentAt: sentAt, doneAt: doneAt, err: err}:
					case <-ctx.Done():
						return
					}
					if err != nil && ctx.Err() != nil {
						return
					}
				}
			}
		}()
	}

	doneCh := make(chan struct{})
	go func() {
		wg.Wait()
		close(resultCh)
		close(doneCh)
	}()

	var commitLatencies []int64
	doneAt := startAt
	for res := range resultCh {
		doneAt = res.doneAt
		if res.err == nil {
			commitLatencies = append(commitLatencies, res.doneAt.Sub(res.sentAt).Nanoseconds())
		}
	}
	<-doneCh
	if doneAt.Before(startAt) {
		doneAt = time.Now()
	}

	return maxThroughputResult{
		sent:          sent.Load(),
		accepted:      accepted.Load(),
		errors:        errors.Load(),
		elapsed:       doneAt.Sub(startAt),
		drain:         maxDuration(0, doneAt.Sub(stopIssuingAt)),
		commitLatency: summarizeDurations(commitLatencies),
	}, nil
}

func stepsForDuration(duration time.Duration, hz int) int {
	if duration <= 0 {
		return 0
	}
	steps := int(math.Round(duration.Seconds() * float64(hz)))
	if steps < 1 {
		return 1
	}
	return steps
}

func waitUntil(ctx context.Context, due time.Time) error {
	delay := time.Until(due)
	if delay <= 0 {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			return nil
		}
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func positionForScenario(cfg geotruthCapacityConfig, objIdx int, step int) (x, y, z, rotY float64) {
	z = zForObjectRegion(objIdx, cfg.regions)
	switch cfg.scenario {
	case capacityScenarioGeofencePeriodic:
		phase := step + objIdx%(2*cfg.geofencePeriod)
		if (phase/cfg.geofencePeriod)%2 == 0 {
			return 50, 50, z, 0
		}
		return 5, 5, z, 0
	case capacityScenarioGeofenceToggle:
		if step%2 == 0 {
			return 5, 5, z, 0
		}
		return 50, 50, z, 0
	case capacityScenarioRegionTransition:
		return 5, 5, zForObjectStepRegion(objIdx, step+1, cfg.regions), 0
	default:
		return float64((objIdx+step)%1000) + 0.25, float64((objIdx*7+step)%1000) + 0.25, z, 0
	}
}

func zForObjectRegion(objIdx, regions int) float64 {
	if regions <= 1 {
		return 1
	}
	region := objIdx % regions
	return float64(region*4) + 1
}

func zForObjectStepRegion(objIdx, step, regions int) float64 {
	if regions <= 1 {
		return 1
	}
	region := (objIdx + step) % regions
	return float64(region*4) + 1
}

func plannedDueAt(now, start time.Time, duration time.Duration, objects, steps int, interval, stagger time.Duration) int64 {
	if now.Before(start) {
		return 0
	}
	if !now.Before(start.Add(duration)) {
		return int64(objects * steps)
	}
	elapsed := now.Sub(start)
	fullSteps := int(elapsed / interval)
	if fullSteps >= steps {
		return int64(objects * steps)
	}
	count := fullSteps * objects
	remainder := elapsed - (time.Duration(fullSteps) * interval)
	currentObjects := int(remainder/stagger) + 1
	if currentObjects > objects {
		currentObjects = objects
	}
	if currentObjects < 0 {
		currentObjects = 0
	}
	return int64(count + currentObjects)
}

func summarizeLagSamples(samples []lagSample, trialEnd time.Time) (maxCompletionLag, endCompletionLag, maxDriverLag, maxInflight int64) {
	for _, sample := range samples {
		if sample.at.After(trialEnd) {
			continue
		}
		if sample.completionLag > maxCompletionLag {
			maxCompletionLag = sample.completionLag
		}
		if sample.driverLag > maxDriverLag {
			maxDriverLag = sample.driverLag
		}
		if sample.inflight > maxInflight {
			maxInflight = sample.inflight
		}
		endCompletionLag = sample.completionLag
	}
	return maxCompletionLag, endCompletionLag, maxDriverLag, maxInflight
}

func lagSlope(samples []lagSample, start, trialEnd time.Time) float64 {
	var n float64
	var sumX float64
	var sumY float64
	var sumXX float64
	var sumXY float64
	for _, sample := range samples {
		if sample.at.Before(start) || sample.at.After(trialEnd) {
			continue
		}
		x := sample.at.Sub(start).Seconds()
		y := float64(sample.completionLag)
		n++
		sumX += x
		sumY += y
		sumXX += x * x
		sumXY += x * y
	}
	if n < 2 {
		return 0
	}
	denom := n*sumXX - sumX*sumX
	if denom == 0 {
		return 0
	}
	return (n*sumXY - sumX*sumY) / denom
}

func summarizeDurations(values []int64) durationSummary {
	if len(values) == 0 {
		return durationSummary{}
	}
	sort.Slice(values, func(i, j int) bool { return values[i] < values[j] })
	return durationSummary{
		p50: percentileDuration(values, 0.50),
		p95: percentileDuration(values, 0.95),
		p99: percentileDuration(values, 0.99),
	}
}

func percentileDuration(sorted []int64, p float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	idx := int(math.Ceil(float64(len(sorted))*p)) - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return time.Duration(sorted[idx])
}

func evaluateCapacityResult(cfg geotruthCapacityConfig, result scheduledLoadResult) (bool, string) {
	if result.accepted != result.planned {
		return false, fmt.Sprintf("accepted %d of %d planned updates", result.accepted, result.planned)
	}
	if result.errors != 0 {
		return false, fmt.Sprintf("saw %d request errors", result.errors)
	}
	if result.meanCompletionLagRate > 0 {
		return false, fmt.Sprintf("completion lag trended positive: %.1f msgs/sec", result.meanCompletionLagRate)
	}
	if cfg.p95Limit > 0 && result.commitLatency.p95 > cfg.p95Limit {
		return false, fmt.Sprintf("commit p95 %s exceeded limit %s", result.commitLatency.p95, cfg.p95Limit)
	}
	if cfg.schedulerP95Limit > 0 && result.scheduleLag.p95 > cfg.schedulerP95Limit {
		return false, fmt.Sprintf("scheduler p95 %s exceeded limit %s", result.scheduleLag.p95, cfg.schedulerP95Limit)
	}
	return true, "ok"
}

func reportCapacityMetrics(b *testing.B, cfg geotruthCapacityConfig, result scheduledLoadResult, pass bool, gtEventsDeltaMsgs, gtEventsDeltaBytes uint64) {
	b.Helper()
	b.ReportMetric(boolMetric(pass), "capacity_pass")
	b.ReportMetric(float64(result.planned), "planned_updates")
	b.ReportMetric(float64(result.sent), "sent_updates")
	b.ReportMetric(float64(result.accepted), "accepted_updates")
	b.ReportMetric(float64(result.errors), "error_count")
	b.ReportMetric(float64(cfg.objects), "objects")
	b.ReportMetric(float64(cfg.regions), "regions")
	b.ReportMetric(float64(cfg.hz), "per_object_updates_per_sec")
	b.ReportMetric(float64(cfg.objects*cfg.hz), "target_updates_per_sec")
	b.ReportMetric(float64(cfg.senderWorkers), "sender_workers")
	b.ReportMetric(float64(cfg.pool), "client_conns")
	b.ReportMetric(float64(effectiveGTPubWorkers(cfg.gtPubWorkers)), "gt_pub_workers")
	b.ReportMetric(float64(effectiveGTPubInflight(cfg.gtPubInflight)), "gt_pub_inflight")
	b.ReportMetric(durationMillis(result.commitLatency.p50), "commit_p50_latency_ms")
	b.ReportMetric(durationMillis(result.commitLatency.p95), "commit_p95_latency_ms")
	b.ReportMetric(durationMillis(result.commitLatency.p99), "commit_p99_latency_ms")
	b.ReportMetric(durationMillis(result.scheduleLag.p50), "schedule_p50_lag_ms")
	b.ReportMetric(durationMillis(result.scheduleLag.p95), "schedule_p95_lag_ms")
	b.ReportMetric(durationMillis(result.scheduleLag.p99), "schedule_p99_lag_ms")
	b.ReportMetric(durationMillis(result.e2eLatency.p50), "e2e_p50_latency_ms")
	b.ReportMetric(durationMillis(result.e2eLatency.p95), "e2e_p95_latency_ms")
	b.ReportMetric(durationMillis(result.e2eLatency.p99), "e2e_p99_latency_ms")
	b.ReportMetric(result.meanCompletionLagRate, "mean_completion_lag_rate_msgs_per_sec")
	b.ReportMetric(float64(result.maxCompletionLag), "max_completion_lag_msgs")
	b.ReportMetric(float64(result.endCompletionLag), "end_completion_lag_msgs")
	b.ReportMetric(float64(result.maxDriverLag), "max_driver_lag_msgs")
	b.ReportMetric(float64(result.maxInflight), "max_inflight_requests")
	b.ReportMetric(durationMillis(result.drain), "drain_ms")
	b.ReportMetric(float64(gtEventsDeltaMsgs), "gt_events_delta_msgs")
	b.ReportMetric(float64(gtEventsDeltaBytes), "gt_events_delta_bytes")
}

func reportMaxThroughputMetrics(b *testing.B, cfg geotruthCapacityConfig, result maxThroughputResult, gtEventsDeltaMsgs, gtEventsDeltaBytes uint64) {
	b.Helper()
	b.ReportMetric(float64(result.sent), "sent_updates")
	b.ReportMetric(float64(result.accepted), "accepted_updates")
	b.ReportMetric(float64(result.errors), "error_count")
	b.ReportMetric(float64(cfg.objects), "objects")
	b.ReportMetric(float64(cfg.regions), "regions")
	b.ReportMetric(float64(cfg.senderWorkers), "sender_workers")
	b.ReportMetric(float64(cfg.pool), "client_conns")
	b.ReportMetric(float64(effectiveGTPubWorkers(cfg.gtPubWorkers)), "gt_pub_workers")
	b.ReportMetric(float64(effectiveGTPubInflight(cfg.gtPubInflight)), "gt_pub_inflight")
	b.ReportMetric(durationMillis(result.elapsed), "elapsed_ms")
	b.ReportMetric(durationMillis(result.drain), "drain_ms")
	b.ReportMetric(completedThroughput(result), "completed_updates_per_sec")
	b.ReportMetric(trialWindowThroughput(cfg, result), "accepted_per_trial_sec")
	b.ReportMetric(durationMillis(result.commitLatency.p50), "commit_p50_latency_ms")
	b.ReportMetric(durationMillis(result.commitLatency.p95), "commit_p95_latency_ms")
	b.ReportMetric(durationMillis(result.commitLatency.p99), "commit_p99_latency_ms")
	b.ReportMetric(float64(gtEventsDeltaMsgs), "gt_events_delta_msgs")
	b.ReportMetric(float64(gtEventsDeltaBytes), "gt_events_delta_bytes")
}

func completedThroughput(result maxThroughputResult) float64 {
	if result.elapsed <= 0 {
		return 0
	}
	return float64(result.accepted) / result.elapsed.Seconds()
}

func trialWindowThroughput(cfg geotruthCapacityConfig, result maxThroughputResult) float64 {
	if cfg.trialWindow <= 0 {
		return 0
	}
	return float64(result.accepted) / cfg.trialWindow.Seconds()
}

func startCapacityTrialProfile(cfg geotruthCapacityConfig) (func() error, error) {
	if !cfg.cpuProfile && !cfg.mutexProfile {
		return func() error { return nil }, nil
	}
	if err := os.MkdirAll(cfg.profileDir, 0o755); err != nil {
		return nil, err
	}
	kind := "cpu"
	if cfg.mutexProfile {
		kind = "mutex"
	}
	name := fmt.Sprintf("GeoTruth_o%d_hz%d_pool%d_%s_%s.out",
		cfg.objects, cfg.hz, cfg.pool, time.Now().Format("20060102_150405"), kind)
	path := filepath.Join(cfg.profileDir, name)
	f, err := os.Create(path)
	if err != nil {
		return nil, err
	}

	if cfg.cpuProfile {
		if err := pprof.StartCPUProfile(f); err != nil {
			_ = f.Close()
			return nil, err
		}
		return func() error {
			pprof.StopCPUProfile()
			return f.Close()
		}, nil
	}

	oldFraction := runtime.SetMutexProfileFraction(1)
	return func() error {
		runtime.SetMutexProfileFraction(oldFraction)
		if err := pprof.Lookup("mutex").WriteTo(f, 0); err != nil {
			_ = f.Close()
			return err
		}
		return f.Close()
	}, nil
}

func boolMetric(v bool) float64 {
	if v {
		return 1
	}
	return 0
}

func effectiveGTPubWorkers(v int) int {
	if v > 0 {
		return v
	}
	return capacityDefaultGTPubWorkers
}

func effectiveGTPubInflight(v int) int {
	if v > 0 {
		return v
	}
	return capacityDefaultGTPubInflight
}

func durationMillis(d time.Duration) float64 {
	return float64(d) / float64(time.Millisecond)
}

func deltaUint64(after, before uint64) uint64 {
	if after < before {
		return 0
	}
	return after - before
}

func minInt64(a int64, b int64) int {
	if a < b {
		return int(a)
	}
	return int(b)
}

func maxInt64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

func maxDuration(a, b time.Duration) time.Duration {
	if a > b {
		return a
	}
	return b
}
