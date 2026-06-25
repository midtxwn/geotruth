package integration

import (
	"context"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

var (
	capacityObjects           = flag.Int("objects", 1000, "number of objects to register")
	capacityHz                = flag.Int("hz", 10, "updates per object per second")
	capacityWarmup            = flag.Duration("warmup", 2*time.Second, "warmup load before sampling")
	capacityTrialWindow       = flag.Duration("trial-window", 5*time.Second, "measured send window")
	capacityP95Limit          = flag.Duration("p95-limit", 250*time.Millisecond, "p95 event latency threshold")
	capacityDrainTimeout      = flag.Duration("drain-timeout", 30*time.Second, "maximum time to wait for final catch-up")
	capacityHarnessPool       = flag.Int("pool", 2048, "harness publish connection pool size")
	capacityStorage           = flag.String("storage", "memory", "SPATIAL and GT_EVENTS stream storage: memory or file")
	capacitySenderWorkers     = flag.Int("sender-workers", 512, "benchmark sender worker count")
	capacityGTPullBatch       = flag.Int("gt-pull-batch", 128, "GeoTruth consumer PullMaxMessages for capacity benchmarks")
	capacityGTPubWorkers      = flag.Int("gt-pub-workers", 0, "GeoTruth publisher worker override; 0 uses service default")
	capacityGTPubInFlight     = flag.Int("gt-pub-inflight", 0, "GeoTruth publisher max in-flight envelopes per worker; 0 uses service default")
	capacityAllowFail         = flag.Bool("allow-capacity-fail", false, "report capacity-failure benchmark rows instead of failing the run")
	capacityTrialCPUProfile   = flag.Bool("trial-cpuprofile", false, "write scoped CPU profile (setup excluded)")
	capacityTrialMutexProfile = flag.Bool("trial-mutexprofile", false, "write scoped mutex profile (setup excluded)")
	capacityProfileDir        = flag.String("profile-dir", "profiles", "output directory for profile files")
)

func parseCapacityBenchmarkStorage(raw string) (jetstream.StorageType, string, error) {
	switch raw {
	case "", "memory":
		return jetstream.MemoryStorage, "memory", nil
	case "file":
		return jetstream.FileStorage, "file", nil
	default:
		return 0, "", fmt.Errorf("unknown -storage %q; expected memory or file", raw)
	}
}

func makeProfilePath(bench string, objects, hz, pool int, suffix string) string {
	os.MkdirAll(*capacityProfileDir, 0755)
	ts := time.Now().Format("20060102_150405")
	return filepath.Join(*capacityProfileDir,
		fmt.Sprintf("%s_o%d_hz%d_pool%d_%s_%s.out", bench, objects, hz, pool, ts, suffix))
}

func BenchmarkFullPathCapacity(b *testing.B) {
	if enabledProfileCount() > 1 {
		b.Fatalf("use only one scoped profile flag per run")
	}
	if *capacityObjects <= 0 || *capacityHz <= 0 || *capacityWarmup < 0 || *capacityTrialWindow <= 0 || *capacityP95Limit <= 0 || *capacityDrainTimeout <= 0 {
		b.Fatalf("invalid capacity config: objects=%d hz=%d warmup=%s trial=%s p95=%s drain=%s",
			*capacityObjects, *capacityHz, *capacityWarmup, *capacityTrialWindow, *capacityP95Limit, *capacityDrainTimeout)
	}
	if *capacitySenderWorkers <= 0 {
		b.Fatalf("sender-workers must be > 0")
	}
	storage, storageName, err := parseCapacityBenchmarkStorage(*capacityStorage)
	if err != nil {
		b.Fatalf("%v", err)
	}
	if *capacityGTPullBatch <= 0 {
		b.Fatalf("gt-pull-batch must be > 0")
	}

	cfg := capacityConfig{
		objectCount:         *capacityObjects,
		ratePerObject:       *capacityHz,
		warmup:              *capacityWarmup,
		duration:            *capacityTrialWindow,
		p95Limit:            *capacityP95Limit,
		drainTimeout:        *capacityDrainTimeout,
		pullBatchSize:       *capacityGTPullBatch,
		registerConcurrency: 512,
		setupTimeout:        20 * time.Minute,
		harnessPoolSize:     *capacityHarnessPool,
		storage:             storage,
		storageName:         storageName,
		senderWorkers:       *capacitySenderWorkers,
		pubWorkers:          *capacityGTPubWorkers,
		pubInFlight:         *capacityGTPubInFlight,
	}
	if *capacityTrialCPUProfile {
		cfg.trialCPUProfile = makeProfilePath("FullPath", *capacityObjects, *capacityHz, *capacityHarnessPool, "cpu")
	}
	if *capacityTrialMutexProfile {
		cfg.trialMutexProfile = makeProfilePath("FullPath", *capacityObjects, *capacityHz, *capacityHarnessPool, "mutex")
	}

	repeat := 0
	for b.Loop() {
		repeat++
		trial := runCapacityTrial(b, cfg, cfg.objectCount, repeat)
		reportCapacityMetrics(b, trial)
		logCapacitySummary(b, trial)
		if !trial.pass && !*capacityAllowFail {
			b.Fatalf("capacity target failed: %s", trial.reason)
		}
	}
}

func BenchmarkFullPathMaxThroughput(b *testing.B) {
	if enabledProfileCount() > 1 {
		b.Fatalf("use only one scoped profile flag per run")
	}
	if *capacityObjects <= 0 || *capacityWarmup < 0 || *capacityTrialWindow <= 0 || *capacityDrainTimeout <= 0 {
		b.Fatalf("invalid config: objects=%d warmup=%s trial=%s drain=%s",
			*capacityObjects, *capacityWarmup, *capacityTrialWindow, *capacityDrainTimeout)
	}
	if *capacitySenderWorkers <= 0 {
		b.Fatalf("sender-workers must be > 0")
	}
	storage, storageName, err := parseCapacityBenchmarkStorage(*capacityStorage)
	if err != nil {
		b.Fatalf("%v", err)
	}
	if *capacityGTPullBatch <= 0 {
		b.Fatalf("gt-pull-batch must be > 0")
	}

	cfg := capacityConfig{
		objectCount:         *capacityObjects,
		warmup:              *capacityWarmup,
		duration:            *capacityTrialWindow,
		drainTimeout:        *capacityDrainTimeout,
		pullBatchSize:       *capacityGTPullBatch,
		registerConcurrency: 512,
		setupTimeout:        20 * time.Minute,
		harnessPoolSize:     *capacityHarnessPool,
		storage:             storage,
		storageName:         storageName,
		senderWorkers:       *capacitySenderWorkers,
		pubWorkers:          *capacityGTPubWorkers,
		pubInFlight:         *capacityGTPubInFlight,
	}
	if *capacityTrialCPUProfile {
		cfg.trialCPUProfile = makeProfilePath("FullPathMax", *capacityObjects, 0, *capacityHarnessPool, "cpu")
	}
	if *capacityTrialMutexProfile {
		cfg.trialMutexProfile = makeProfilePath("FullPathMax", *capacityObjects, 0, *capacityHarnessPool, "mutex")
	}

	repeat := 0
	for b.Loop() {
		repeat++
		trial := runFullPathMaxThroughputTrial(b, cfg, repeat)
		lat := trial.commitLatency
		throughput := 0.0
		if trial.elapsed > 0 {
			throughput = float64(trial.accepted) / trial.elapsed.Seconds()
		}
		trialWindowThroughput := 0.0
		if cfg.duration > 0 {
			trialWindowThroughput = float64(trial.accepted) / cfg.duration.Seconds()
		}

		b.ReportMetric(float64(cfg.objectCount), "objects")
		b.ReportMetric(float64(cfg.senderWorkers), "sender_workers")
		b.ReportMetric(float64(cfg.harnessPoolSize), "client_conns")
		b.ReportMetric(float64(effectiveCapacityPubWorkers(cfg.pubWorkers)), "gt_pub_workers")
		b.ReportMetric(float64(effectiveCapacityPubInFlight(cfg.pubInFlight)), "gt_pub_inflight")
		b.ReportMetric(float64(trial.sent), "sent_updates")
		b.ReportMetric(float64(trial.accepted), "accepted_updates")
		b.ReportMetric(float64(trial.errors), "error_count")
		b.ReportMetric(throughput, "completed_updates_per_sec")
		b.ReportMetric(trialWindowThroughput, "accepted_per_trial_sec")
		b.ReportMetric(float64(percentileDuration(lat, 50).Microseconds())/1000, "commit_p50_latency_ms")
		b.ReportMetric(float64(percentileDuration(lat, 95).Microseconds())/1000, "commit_p95_latency_ms")
		b.ReportMetric(float64(percentileDuration(lat, 99).Microseconds())/1000, "commit_p99_latency_ms")

		b.Logf("max throughput summary architecture=ingester_spatial storage=%s objects=%d sender_workers=%d client_conns=%d gt_pub_workers=%d gt_pub_inflight=%d sent=%d accepted=%d errors=%d elapsed_ms=%.3f completed_updates_per_sec=%.1f accepted_per_trial_sec=%.1f commit_p50/p95/p99_ms=%.3f/%.3f/%.3f",
			cfg.storageName, cfg.objectCount, cfg.senderWorkers, cfg.harnessPoolSize,
			effectiveCapacityPubWorkers(cfg.pubWorkers), effectiveCapacityPubInFlight(cfg.pubInFlight),
			trial.sent, trial.accepted, trial.errors, float64(trial.elapsed.Nanoseconds())/1e6,
			throughput, trialWindowThroughput,
			float64(percentileDuration(lat, 50).Microseconds())/1000,
			float64(percentileDuration(lat, 95).Microseconds())/1000,
			float64(percentileDuration(lat, 99).Microseconds())/1000)
	}
}
func BenchmarkIngesterThroughput(b *testing.B) {
	if enabledProfileCount() > 1 {
		b.Fatalf("use only one scoped profile flag per run")
	}
	if *capacityObjects <= 0 || *capacityHz <= 0 || *capacityWarmup < 0 || *capacityTrialWindow <= 0 {
		b.Fatalf("invalid config: objects=%d hz=%d warmup=%s trial=%s",
			*capacityObjects, *capacityHz, *capacityWarmup, *capacityTrialWindow)
	}
	if *capacitySenderWorkers <= 0 {
		b.Fatalf("sender-workers must be > 0")
	}
	storage, storageName, err := parseCapacityBenchmarkStorage(*capacityStorage)
	if err != nil {
		b.Fatalf("%v", err)
	}
	if *capacityGTPullBatch <= 0 {
		b.Fatalf("gt-pull-batch must be > 0")
	}

	cfg := capacityConfig{
		objectCount:         *capacityObjects,
		ratePerObject:       *capacityHz,
		warmup:              *capacityWarmup,
		duration:            *capacityTrialWindow,
		pullBatchSize:       *capacityGTPullBatch,
		registerConcurrency: 512,
		setupTimeout:        20 * time.Minute,
		harnessPoolSize:     *capacityHarnessPool,
		storage:             storage,
		storageName:         storageName,
		drainTimeout:        *capacityDrainTimeout,
		senderWorkers:       *capacitySenderWorkers,
	}
	if *capacityTrialCPUProfile {
		cfg.trialCPUProfile = makeProfilePath("Ingester", *capacityObjects, *capacityHz, *capacityHarnessPool, "cpu")
	}
	if *capacityTrialMutexProfile {
		cfg.trialMutexProfile = makeProfilePath("Ingester", *capacityObjects, *capacityHz, *capacityHarnessPool, "mutex")
	}

	_, nc := startNoopAckerServices(b, cfg.storage)

	pool, err := newConnPool(nc.ConnectedUrl(), cfg.harnessPoolSize)
	if err != nil {
		b.Fatalf("create harness pool: %v", err)
	}
	defer pool.close()

	setupCtx, cancelSetup := context.WithTimeout(context.Background(), cfg.setupTimeout)
	objectIDs, maxRegSeq := registerCapacityObjects(setupCtx, b, nc, cfg.objectCount, 0, cfg.registerConcurrency)
	cancelSetup()

	js, err := jetstream.New(nc)
	if err != nil {
		b.Fatalf("jetstream: %v", err)
	}

	loadCtx := context.Background()

	queuedAccepted := 0
	var clientOpSeq atomic.Uint64
	if cfg.warmup > 0 {
		warmupAccepted, warmupPublishErrors := sendMultiObjectScheduled(loadCtx, pool, objectIDs, cfg.ratePerObject, cfg.warmup, &clientOpSeq, cfg.senderWorkers)
		warmupSent := plannedUpdates(cfg.objectCount, cfg.ratePerObject, cfg.warmup)
		if warmupPublishErrors != 0 {
			b.Fatalf("ingester-only warmup publish errors: %d of %d sent", warmupPublishErrors, warmupSent)
		}
		if len(warmupAccepted) != warmupSent {
			b.Fatalf("ingester-only warmup accepted %d of %d sent updates", len(warmupAccepted), warmupSent)
		}
		queuedAccepted += len(warmupAccepted)
	}

	if cfg.trialCPUProfile != "" {
		f, err := os.Create(cfg.trialCPUProfile)
		if err != nil {
			b.Fatalf("create cpu profile: %v", err)
		}
		if err := pprof.StartCPUProfile(f); err != nil {
			f.Close()
			b.Fatalf("start cpu profile: %v", err)
		}
		defer f.Close()
	}
	if cfg.trialMutexProfile != "" {
		runtime.SetMutexProfileFraction(1)
	}
	b.ResetTimer()
	accepted, publishErrors := sendMultiObjectScheduled(loadCtx, pool, objectIDs, cfg.ratePerObject, cfg.duration, &clientOpSeq, cfg.senderWorkers)
	b.StopTimer()

	if cfg.trialCPUProfile != "" {
		pprof.StopCPUProfile()
	}
	if cfg.trialMutexProfile != "" {
		runtime.SetMutexProfileFraction(0)
		f, err := os.Create(cfg.trialMutexProfile)
		if err != nil {
			b.Fatalf("create mutex profile: %v", err)
		}
		if err := pprof.Lookup("mutex").WriteTo(f, 0); err != nil {
			f.Close()
			b.Fatalf("write mutex profile: %v", err)
		}
		f.Close()
	}

	sent := plannedUpdates(cfg.objectCount, cfg.ratePerObject, cfg.duration)
	if publishErrors != 0 {
		b.Fatalf("ingester-only: %d publish errors out of %d sent", publishErrors, sent)
	}
	if len(accepted) != sent {
		b.Fatalf("ingester-only: accepted %d of %d sent updates", len(accepted), sent)
	}
	queuedAccepted += len(accepted)
	waitForSpatialLastSeq(b, js, maxRegSeq+uint64(queuedAccepted), cfg.drainTimeout)

	ingesterLatencies := make([]time.Duration, len(accepted))
	for i, update := range accepted {
		ingesterLatencies[i] = update.acceptedAt.Sub(update.sentAt)
	}

	b.ReportMetric(float64(cfg.objectCount), "objects")
	b.ReportMetric(float64(cfg.ratePerObject), "per_object_updates_per_sec")
	b.ReportMetric(float64(cfg.senderWorkers), "sender_workers")
	b.ReportMetric(float64(cfg.objectCount*cfg.ratePerObject), "target_updates_per_sec")
	b.ReportMetric(float64(sent), "sent_updates")
	b.ReportMetric(float64(len(accepted)), "accepted_updates")
	b.ReportMetric(float64(publishErrors), "publish_error_count")
	b.ReportMetric(float64(percentileDuration(ingesterLatencies, 50).Milliseconds()), "ingester_p50_latency_ms")
	b.ReportMetric(float64(percentileDuration(ingesterLatencies, 95).Milliseconds()), "ingester_p95_latency_ms")
	b.ReportMetric(float64(percentileDuration(ingesterLatencies, 99).Milliseconds()), "ingester_p99_latency_ms")
}

func enabledProfileCount() int {
	n := 0
	for _, enabled := range []bool{*capacityTrialCPUProfile, *capacityTrialMutexProfile} {
		if enabled {
			n++
		}
	}
	return n
}
