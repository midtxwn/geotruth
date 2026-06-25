package geotruth

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"regexp"
	"sync"
	"sync/atomic"
	"time"

	"github.com/midtxwn/geotruth/internal/engine"
	"github.com/midtxwn/geotruth/internal/gtevents"
	privKeys "github.com/midtxwn/geotruth/internal/natskeys"
	"github.com/midtxwn/geotruth/internal/spatialstream"
	"github.com/midtxwn/geotruth/internal/streampressure"
	pkggeotruth "github.com/midtxwn/geotruth/pkg/geotruth"
	"github.com/midtxwn/geotruth/pkg/messages"
	pkgKeys "github.com/midtxwn/geotruth/pkg/natskeys"
	"github.com/midtxwn/geotruth/pkg/natspublish"
	"github.com/midtxwn/geotruth/pkg/natsquery"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

type GeoTruth struct {
	js       jetstream.JetStream
	nc       *nats.Conn
	eng      *engine.Engine
	disp     *engine.Dispatcher
	gtPub    *gtevents.Publisher
	gtStream jetstream.Stream
	kvAreas  jetstream.KeyValue
	pressure *streampressure.Monitor
	ready    atomic.Bool
	chReady  chan struct{}
	chDone   chan struct{}
	errMu    sync.Mutex
	err      error

	publisherCfg gtevents.PublisherConfig
	consumerCfg  consumerRuntimeConfig
	connectNATS  pkggeotruth.NATSConnector
	publisherNCs []*nats.Conn

	booted               bool
	committedSeqByObject map[string]uint64
}

type consumerRuntimeConfig struct {
	AckWait       time.Duration
	MaxAckPending int
	MaxDeliver    int
	ReaderBuffer  int
	PullBatchSize int
}

const (
	defaultConsumerAckWait       = 60 * time.Second
	defaultConsumerMaxAckPending = 10000
	defaultConsumerMaxDeliver    = -1
	defaultConsumerReaderBuffer  = 256
	defaultConsumerPullBatchSize = 128
)

func newGeoTruth(cfg pkggeotruth.Config, deps pkggeotruth.Dependencies) (*GeoTruth, error) {
	if deps.NATS == nil {
		return nil, fmt.Errorf("geotruth: NATS dependency is required")
	}
	if deps.Resolver == nil {
		return nil, fmt.Errorf("geotruth: resolver is required")
	}

	publisherCfg, consumerCfg, err := normalizeRuntimeConfig(cfg)
	if err != nil {
		return nil, err
	}

	nc, err := deps.NATS("main")
	if err != nil {
		return nil, fmt.Errorf("connect main: %w", err)
	}
	cleanupMain := true
	defer func() {
		if cleanupMain {
			nc.Close()
		}
	}()

	js, err := jetstream.New(nc)
	if err != nil {
		return nil, fmt.Errorf("jetstream: %w", err)
	}

	storage := cfg.Storage
	if storage == 0 {
		storage = jetstream.FileStorage
	}

	replicas := cfg.Replicas
	if replicas <= 0 {
		replicas = 1
	}

	ctx := context.Background()

	_, err = spatialstream.EnsureStream(ctx, js, spatialstream.Config{Storage: storage})
	if err != nil {
		return nil, fmt.Errorf("create stream: %w", err)
	}

	kvAreas, err := js.CreateOrUpdateKeyValue(ctx, jetstream.KeyValueConfig{
		Bucket:  natspublish.KVAreas,
		Storage: storage,
	})
	if err != nil {
		return nil, fmt.Errorf("create areas KV: %w", err)
	}

	gtStream, err := gtevents.EnsureStream(ctx, js, gtevents.StreamConfig{
		Storage:  storage,
		MaxBytes: cfg.GTEventsMaxBytes,
		Replicas: replicas,
	})
	if err != nil {
		return nil, fmt.Errorf("create GT_EVENTS stream: %w", err)
	}

	gt := &GeoTruth{
		js:       js,
		nc:       nc,
		eng:      engine.NewEngine(deps.Resolver),
		gtStream: gtStream,
		kvAreas:  kvAreas,
		pressure: streampressure.New(nc, js, pkgKeys.GTStreamName, streampressure.Config{
			Enabled:            cfg.PressureMonitor.Enabled,
			WarnRatio:          cfg.PressureMonitor.WarnRatio,
			CriticalRatio:      cfg.PressureMonitor.CriticalRatio,
			MinRefreshInterval: cfg.PressureMonitor.MinRefreshInterval,
			MinBytesDelta:      cfg.PressureMonitor.MinBytesDelta,
		}),
		chReady:              make(chan struct{}),
		chDone:               make(chan struct{}),
		publisherCfg:         publisherCfg,
		consumerCfg:          consumerCfg,
		connectNATS:          deps.NATS,
		committedSeqByObject: make(map[string]uint64),
	}
	cleanupMain = false
	return gt, nil
}

func normalizeRuntimeConfig(cfg pkggeotruth.Config) (gtevents.PublisherConfig, consumerRuntimeConfig, error) {
	publisherCfg, err := normalizePublisherConfig(cfg.Publisher)
	if err != nil {
		return gtevents.PublisherConfig{}, consumerRuntimeConfig{}, err
	}
	consumerCfg, err := normalizeConsumerConfig(cfg.Consumer)
	if err != nil {
		return gtevents.PublisherConfig{}, consumerRuntimeConfig{}, err
	}
	if publisherCfg.InProgressInterval >= consumerCfg.AckWait {
		return gtevents.PublisherConfig{}, consumerRuntimeConfig{}, fmt.Errorf(
			"geotruth config: publisher in-progress interval %s must be less than consumer ack wait %s",
			publisherCfg.InProgressInterval, consumerCfg.AckWait)
	}
	return publisherCfg, consumerCfg, nil
}

func normalizePublisherConfig(cfg pkggeotruth.PublisherConfig) (gtevents.PublisherConfig, error) {
	if cfg.Workers < 0 {
		return gtevents.PublisherConfig{}, fmt.Errorf("geotruth config: publisher workers must be >= 0")
	}
	if cfg.CommitBuffer < 0 {
		return gtevents.PublisherConfig{}, fmt.Errorf("geotruth config: publisher commit buffer must be >= 0")
	}
	if cfg.ResultBuffer < 0 {
		return gtevents.PublisherConfig{}, fmt.Errorf("geotruth config: publisher result buffer must be >= 0")
	}
	if cfg.MaxInFlightPerWorker < 0 {
		return gtevents.PublisherConfig{}, fmt.Errorf("geotruth config: publisher max in-flight per worker must be >= 0")
	}
	if cfg.InitialBackoff < 0 {
		return gtevents.PublisherConfig{}, fmt.Errorf("geotruth config: publisher initial backoff must be >= 0")
	}
	if cfg.MaxBackoff < 0 {
		return gtevents.PublisherConfig{}, fmt.Errorf("geotruth config: publisher max backoff must be >= 0")
	}
	if cfg.InProgressInterval < 0 {
		return gtevents.PublisherConfig{}, fmt.Errorf("geotruth config: publisher in-progress interval must be >= 0")
	}

	publisherCfg := gtevents.NormalizePublisherConfig(gtevents.PublisherConfig{
		Workers:              cfg.Workers,
		CommitBuffer:         cfg.CommitBuffer,
		ResultBuffer:         cfg.ResultBuffer,
		MaxInFlightPerWorker: cfg.MaxInFlightPerWorker,
		InitialBackoff:       cfg.InitialBackoff,
		MaxBackoff:           cfg.MaxBackoff,
		InProgressInterval:   cfg.InProgressInterval,
	})
	if publisherCfg.MaxBackoff < publisherCfg.InitialBackoff {
		return gtevents.PublisherConfig{}, fmt.Errorf("geotruth config: publisher max backoff must be >= initial backoff")
	}
	return publisherCfg, nil
}

func normalizeConsumerConfig(cfg pkggeotruth.ConsumerConfig) (consumerRuntimeConfig, error) {
	if cfg.AckWait < 0 {
		return consumerRuntimeConfig{}, fmt.Errorf("geotruth config: consumer ack wait must be >= 0")
	}
	if cfg.MaxAckPending < 0 {
		return consumerRuntimeConfig{}, fmt.Errorf("geotruth config: consumer max ack pending must be >= 0")
	}
	if cfg.MaxDeliver < -1 {
		return consumerRuntimeConfig{}, fmt.Errorf("geotruth config: consumer max deliver must be >= -1")
	}
	if cfg.ReaderBuffer < 0 {
		return consumerRuntimeConfig{}, fmt.Errorf("geotruth config: consumer reader buffer must be >= 0")
	}
	if cfg.PullBatchSize < 0 {
		return consumerRuntimeConfig{}, fmt.Errorf("geotruth config: consumer pull batch size must be >= 0")
	}

	consumerCfg := consumerRuntimeConfig{
		AckWait:       cfg.AckWait,
		MaxAckPending: cfg.MaxAckPending,
		MaxDeliver:    cfg.MaxDeliver,
		ReaderBuffer:  cfg.ReaderBuffer,
		PullBatchSize: cfg.PullBatchSize,
	}
	if consumerCfg.AckWait == 0 {
		consumerCfg.AckWait = defaultConsumerAckWait
	}
	if consumerCfg.MaxAckPending == 0 {
		consumerCfg.MaxAckPending = defaultConsumerMaxAckPending
	}
	if consumerCfg.MaxDeliver == 0 {
		consumerCfg.MaxDeliver = defaultConsumerMaxDeliver
	}
	if consumerCfg.ReaderBuffer == 0 {
		consumerCfg.ReaderBuffer = defaultConsumerReaderBuffer
	}
	if consumerCfg.PullBatchSize == 0 {
		consumerCfg.PullBatchSize = defaultConsumerPullBatchSize
	}
	return consumerCfg, nil
}

func (gt *GeoTruth) Ready() <-chan struct{} {
	return gt.chReady
}

func (gt *GeoTruth) Done() <-chan struct{} {
	return gt.chDone
}

func (gt *GeoTruth) Err() error {
	gt.errMu.Lock()
	defer gt.errMu.Unlock()
	return gt.err
}

func (gt *GeoTruth) finish(err error) {
	gt.errMu.Lock()
	gt.err = err
	gt.errMu.Unlock()
	close(gt.chDone)
}

func Run(ctx context.Context, cfg pkggeotruth.Config, deps pkggeotruth.Dependencies) (*GeoTruth, error) {
	gt, err := newGeoTruth(cfg, deps)
	if err != nil {
		return nil, fmt.Errorf("geotruth init: %w", err)
	}
	go func() {
		gt.finish(gt.run(ctx))
	}()
	return gt, nil
}

// Boot recovers committed object state from GT_EVENTS, seeds areas from KV,
// and prepares the SPATIAL durable consumer. The old replayCommands +
// drainSnapshot boot path is replaced by DeliverLastPerSubject on the
// internal state subject wildcard (gtevents.SubjectObjectStateWildcard).
func (gt *GeoTruth) Boot(ctx context.Context) error {
	if gt.booted {
		return nil
	}

	if err := gt.seedAreas(ctx); err != nil {
		return fmt.Errorf("seed areas: %w", err)
	}

	bootID := fmt.Sprintf("%d", time.Now().UnixNano())

	states, committedSeqByObject, err := gtevents.RecoverObjectState(ctx, gt.js, bootID)
	if err != nil {
		return fmt.Errorf("recover object state: %w", err)
	}
	gt.committedSeqByObject = committedSeqByObject

	for _, st := range states {
		if st.Lifecycle == gtevents.LifecycleActive {
			gt.eng.BootstrapFromState(st)
		}
		// LifecycleRemoved objects are not placed in the R-tree.
		// Their detectorStateSeq is recorded in committedSeqByObject
		// so that redelivered SPATIAL messages are skipped.
	}

	// Ensure the SPATIAL durable consumer exists.
	_, err = gt.js.CreateOrUpdateConsumer(ctx, privKeys.StreamName, jetstream.ConsumerConfig{
		Durable:        privKeys.DurableGeoTruth,
		FilterSubjects: privKeys.StreamSubjects,
		AckPolicy:      jetstream.AckExplicitPolicy,
		AckWait:        gt.consumerCfg.AckWait,
		MaxAckPending:  gt.consumerCfg.MaxAckPending,
		MaxDeliver:     gt.consumerCfg.MaxDeliver,
		DeliverPolicy:  jetstream.DeliverAllPolicy,
	})
	if err != nil {
		return fmt.Errorf("create durable consumer: %w", err)
	}

	go gt.watchAreas(ctx)

	gt.ready.Store(true)
	gt.booted = true
	log.Printf("[geotruth] boot complete: %d objects, %d regions", gt.eng.ObjectCount(), gt.eng.NumRegions())
	return nil
}

func (gt *GeoTruth) run(ctx context.Context) error {
	defer gt.closeOwnedConnections()

	if !gt.booted {
		if err := gt.Boot(ctx); err != nil {
			return fmt.Errorf("boot: %w", err)
		}
	}

	cons, err := gt.js.Consumer(ctx, privKeys.StreamName, privKeys.DurableGeoTruth)
	if err != nil {
		return fmt.Errorf("get consumer: %w", err)
	}

	msgCh := make(chan jetstream.Msg, gt.consumerCfg.ReaderBuffer)

	publishers, err := gt.openPublisherBackends()
	if err != nil {
		return err
	}
	gt.gtPub = gtevents.NewPublisherPoolWithMonitorConfig(publishers, gt.publisherCfg, gt.pressure)
	go gt.gtPub.Start(ctx)

	gt.disp = engine.NewDispatcher(gt.eng, msgCh, gt.gtPub, gt.eng.Resolver(), gt.committedSeqByObject)
	gt.disp.Start(ctx)

	if err := gt.startQueryHandlers(); err != nil {
		return err
	}

	readerReady := make(chan error, 1)
	go gt.reader(ctx, cons, msgCh, readerReady)

	select {
	case err := <-readerReady:
		if err != nil {
			return fmt.Errorf("reader init: %w", err)
		}
	case <-ctx.Done():
		return ctx.Err()
	}

	close(gt.chReady)

	log.Printf("[geotruth] consuming stream via dispatcher")
	gt.disp.Run(ctx)
	return nil
}

func (gt *GeoTruth) openPublisherBackends() ([]gtevents.MessagePublisher, error) {
	publishers := make([]gtevents.MessagePublisher, gt.publisherCfg.Workers)
	for i := range publishers {
		role := fmt.Sprintf("publisher-%d", i)
		nc, err := gt.connectNATS(role)
		if err != nil {
			for _, opened := range gt.publisherNCs {
				opened.Close()
			}
			gt.publisherNCs = nil
			return nil, fmt.Errorf("connect %s: %w", role, err)
		}
		js, err := jetstream.New(nc)
		if err != nil {
			nc.Close()
			for _, opened := range gt.publisherNCs {
				opened.Close()
			}
			gt.publisherNCs = nil
			return nil, fmt.Errorf("jetstream %s: %w", role, err)
		}
		gt.publisherNCs = append(gt.publisherNCs, nc)
		publishers[i] = js
	}
	log.Printf("[geotruth] started %d GT_EVENTS publisher workers (inflight_per_worker=%d, one NATS connection each)",
		len(publishers), gt.publisherCfg.MaxInFlightPerWorker)
	return publishers, nil
}

func (gt *GeoTruth) closeOwnedConnections() {
	for _, nc := range gt.publisherNCs {
		_ = nc.Drain()
	}
	gt.publisherNCs = nil
	if gt.nc != nil {
		_ = gt.nc.Drain()
	}
}

func (gt *GeoTruth) reader(ctx context.Context, cons jetstream.Consumer, msgCh chan<- jetstream.Msg, readerReady chan<- error) {
	iter, err := cons.Messages(jetstream.PullMaxMessages(gt.consumerCfg.PullBatchSize))
	if err != nil {
		readerReady <- fmt.Errorf("messages iterator: %w", err)
		return
	}
	readerReady <- nil
	defer iter.Stop()

	for {
		msg, err := iter.Next()
		if err != nil {
			return
		}
		select {
		case msgCh <- msg:
		case <-ctx.Done():
			return
		}
	}
}

func (gt *GeoTruth) seedAreas(ctx context.Context) error {
	watcher, err := gt.kvAreas.WatchAll(ctx)
	if err != nil {
		return err
	}
	loaded := 0
	for entry := range watcher.Updates() {
		if entry == nil {
			break
		}
		if entry.Operation() != jetstream.KeyValuePut {
			continue
		}
		gt.upsertAreaFromKV(entry.Key(), entry.Value())
		loaded++
	}
	_ = watcher.Stop()
	log.Printf("[geotruth] seeded %d areas from KV", loaded)
	return nil
}

func (gt *GeoTruth) watchAreas(ctx context.Context) {
	watcher, err := gt.kvAreas.WatchAll(ctx)
	if err != nil {
		log.Printf("[geotruth] areas watcher error: %v", err)
		return
	}
	defer watcher.Stop()

	for {
		entry, ok := <-watcher.Updates()
		if !ok {
			return
		}
		if entry == nil {
			continue
		}
		select {
		case <-ctx.Done():
			return
		default:
		}

		switch entry.Operation() {
		case jetstream.KeyValuePut:
			gt.upsertAreaFromKV(entry.Key(), entry.Value())
			log.Printf("[geotruth] area upserted from KV: %s", entry.Key())

		case jetstream.KeyValueDelete, jetstream.KeyValuePurge:
			// Area removal is just an R-tree cleanup in v1.
			// No GT_EVENTS commit for area lifecycle.
			gt.eng.RemoveArea(entry.Key())
			log.Printf("[geotruth] area removed from KV: %s", entry.Key())
		}
	}
}

func (gt *GeoTruth) upsertAreaFromKV(id string, value []byte) {
	var kv natspublish.AreaKV
	if err := json.Unmarshal(value, &kv); err != nil {
		log.Printf("[geotruth] bad area KV entry %s: %v", id, err)
		return
	}

	if err := gt.eng.RegisterArea(id, kv.Region, kv.Points); err != nil {
		log.Printf("[geotruth] register area %s: %v", id, err)
	}
}

func (gt *GeoTruth) startQueryHandlers() error {
	queries := []struct {
		subj    string
		handler nats.MsgHandler
	}{
		{natsquery.QueryNearby, gt.handleNearby},
		{natsquery.QueryNearbyOf, gt.handleNearbyOf},
		{natsquery.QueryWithinArea, gt.handleWithinArea},
		{natsquery.QueryAreasContainingObj, gt.handleAreasContaining},
		{natsquery.QueryAreasAtPoint, gt.handleAreasAtPoint},
		{natsquery.QueryIntersecting, gt.handleIntersecting},
		{natsquery.QueryObjectBounds, gt.handleBounds},
		{natsquery.QueryObjectData, gt.handleObjectData},
		{natsquery.QueryNearbyAreas, gt.handleNearbyAreas},
		{natsquery.QueryArea, gt.handleArea},
		{natsquery.QueryAllObjects, gt.handleAllObjects},
		{natsquery.QueryAllObjectsOriented, gt.handleAllObjectsOriented},
		{natsquery.QueryRegionOf, gt.handleRegionOf},
		{natsquery.QueryRegionFromPoint, gt.handleRegionFromPoint},
		{natsquery.QueryAllAreas, gt.handleAllAreas},
	}
	for _, q := range queries {
		if _, err := gt.nc.Subscribe(q.subj, q.handler); err != nil {
			return fmt.Errorf("subscribe %s: %w", q.subj, err)
		}
		log.Printf("[geotruth] listening on %s", q.subj)
	}
	return nil
}

func queryRegex(pattern *string) (*regexp.Regexp, error) {
	if pattern == nil {
		return nil, nil
	}
	re, err := regexp.Compile(*pattern)
	if err != nil {
		return nil, fmt.Errorf("invalid regex: %w", err)
	}
	return re, nil
}

func (gt *GeoTruth) handleNearby(msg *nats.Msg) {
	if !gt.ready.Load() {
		_ = msg.Respond(messages.ErrResp(fmt.Errorf("recovering, retry")))
		return
	}
	var req natsquery.NearbyReq
	if err := json.Unmarshal(msg.Data, &req); err != nil {
		_ = msg.Respond(messages.ErrResp(err))
		return
	}
	re, err := queryRegex(req.Regex)
	if err != nil {
		_ = msg.Respond(messages.ErrResp(err))
		return
	}
	results, err := gt.eng.Nearby(req.Region, req.X, req.Y, req.RadiusMeters, re)
	if err != nil {
		_ = msg.Respond(messages.ErrResp(err))
		return
	}
	_ = msg.Respond(messages.OKDataResp(results))
}

func (gt *GeoTruth) handleNearbyOf(msg *nats.Msg) {
	if !gt.ready.Load() {
		_ = msg.Respond(messages.ErrResp(fmt.Errorf("recovering, retry")))
		return
	}
	var req natsquery.NearbyOfReq
	if err := json.Unmarshal(msg.Data, &req); err != nil {
		_ = msg.Respond(messages.ErrResp(err))
		return
	}
	re, err := queryRegex(req.Regex)
	if err != nil {
		_ = msg.Respond(messages.ErrResp(err))
		return
	}
	results, err := gt.eng.NearbyOf(req.ObjectID, req.RadiusMeters, re)
	if err != nil {
		_ = msg.Respond(messages.ErrResp(err))
		return
	}
	_ = msg.Respond(messages.OKDataResp(results))
}

func (gt *GeoTruth) handleWithinArea(msg *nats.Msg) {
	if !gt.ready.Load() {
		_ = msg.Respond(messages.ErrResp(fmt.Errorf("recovering, retry")))
		return
	}
	var req natsquery.WithinAreaReq
	if err := json.Unmarshal(msg.Data, &req); err != nil {
		_ = msg.Respond(messages.ErrResp(err))
		return
	}
	re, err := queryRegex(req.Regex)
	if err != nil {
		_ = msg.Respond(messages.ErrResp(err))
		return
	}
	results, err := gt.eng.WithinArea(req.Region, req.AreaID, re)
	if err != nil {
		_ = msg.Respond(messages.ErrResp(err))
		return
	}
	_ = msg.Respond(messages.OKDataResp(results))
}

func (gt *GeoTruth) handleAreasAtPoint(msg *nats.Msg) {
	if !gt.ready.Load() {
		_ = msg.Respond(messages.ErrResp(fmt.Errorf("recovering, retry")))
		return
	}
	var req natsquery.AreasAtPointReq
	if err := json.Unmarshal(msg.Data, &req); err != nil {
		_ = msg.Respond(messages.ErrResp(err))
		return
	}
	re, err := queryRegex(req.Regex)
	if err != nil {
		_ = msg.Respond(messages.ErrResp(err))
		return
	}
	results, err := gt.eng.AreasAtPoint(req.Region, req.X, req.Y, re)
	if err != nil {
		_ = msg.Respond(messages.ErrResp(err))
		return
	}
	_ = msg.Respond(messages.OKDataResp(results))
}

func (gt *GeoTruth) handleAreasContaining(msg *nats.Msg) {
	if !gt.ready.Load() {
		_ = msg.Respond(messages.ErrResp(fmt.Errorf("recovering, retry")))
		return
	}
	var req natsquery.AreasContainingReq
	if err := json.Unmarshal(msg.Data, &req); err != nil {
		_ = msg.Respond(messages.ErrResp(err))
		return
	}
	re, err := queryRegex(req.Regex)
	if err != nil {
		_ = msg.Respond(messages.ErrResp(err))
		return
	}
	results, err := gt.eng.AreasContaining(req.ObjectID, re)
	if err != nil {
		_ = msg.Respond(messages.ErrResp(err))
		return
	}
	_ = msg.Respond(messages.OKDataResp(results))
}

func (gt *GeoTruth) handleIntersecting(msg *nats.Msg) {
	if !gt.ready.Load() {
		_ = msg.Respond(messages.ErrResp(fmt.Errorf("recovering, retry")))
		return
	}
	var req natsquery.IntersectingReq
	if err := json.Unmarshal(msg.Data, &req); err != nil {
		_ = msg.Respond(messages.ErrResp(err))
		return
	}
	re, err := queryRegex(req.Regex)
	if err != nil {
		_ = msg.Respond(messages.ErrResp(err))
		return
	}
	results, err := gt.eng.Intersecting(req.ObjectID, re)
	if err != nil {
		_ = msg.Respond(messages.ErrResp(err))
		return
	}
	_ = msg.Respond(messages.OKDataResp(results))
}

func (gt *GeoTruth) handleBounds(msg *nats.Msg) {
	if !gt.ready.Load() {
		_ = msg.Respond(messages.ErrResp(fmt.Errorf("recovering, retry")))
		return
	}
	var req natsquery.BoundsReq
	if err := json.Unmarshal(msg.Data, &req); err != nil {
		_ = msg.Respond(messages.ErrResp(err))
		return
	}
	bounds, err := gt.eng.Bounds(req.ObjectID)
	if err != nil {
		_ = msg.Respond(messages.ErrResp(err))
		return
	}
	_ = msg.Respond(messages.OKDataResp(bounds))
}

func (gt *GeoTruth) handleObjectData(msg *nats.Msg) {
	if !gt.ready.Load() {
		_ = msg.Respond(messages.ErrResp(fmt.Errorf("recovering, retry")))
		return
	}
	var req natsquery.ObjectDataReq
	if err := json.Unmarshal(msg.Data, &req); err != nil {
		_ = msg.Respond(messages.ErrResp(err))
		return
	}
	obj, err := gt.eng.ObjectPosition(req.ObjectID)
	if err != nil {
		_ = msg.Respond(messages.ErrResp(err))
		return
	}
	_ = msg.Respond(messages.OKDataResp(obj))
}

func (gt *GeoTruth) handleNearbyAreas(msg *nats.Msg) {
	if !gt.ready.Load() {
		_ = msg.Respond(messages.ErrResp(fmt.Errorf("recovering, retry")))
		return
	}
	var req natsquery.NearbyAreasReq
	if err := json.Unmarshal(msg.Data, &req); err != nil {
		_ = msg.Respond(messages.ErrResp(err))
		return
	}
	re, err := queryRegex(req.Regex)
	if err != nil {
		_ = msg.Respond(messages.ErrResp(err))
		return
	}
	results, err := gt.eng.NearbyAreas(req.Region, req.X, req.Y, req.RadiusMeters, re)
	if err != nil {
		_ = msg.Respond(messages.ErrResp(err))
		return
	}
	_ = msg.Respond(messages.OKDataResp(results))
}

func (gt *GeoTruth) handleArea(msg *nats.Msg) {
	if !gt.ready.Load() {
		_ = msg.Respond(messages.ErrResp(fmt.Errorf("recovering, retry")))
		return
	}
	var req natsquery.AreaReq
	if err := json.Unmarshal(msg.Data, &req); err != nil {
		_ = msg.Respond(messages.ErrResp(err))
		return
	}
	area, err := gt.eng.Area(req.AreaID)
	if err != nil {
		_ = msg.Respond(messages.ErrResp(err))
		return
	}
	_ = msg.Respond(messages.OKDataResp(area))
}

func (gt *GeoTruth) handleAllObjects(msg *nats.Msg) {
	if !gt.ready.Load() {
		_ = msg.Respond(messages.ErrResp(fmt.Errorf("recovering, retry")))
		return
	}
	var req natsquery.AllObjectsReq
	if len(msg.Data) > 0 {
		if err := json.Unmarshal(msg.Data, &req); err != nil {
			_ = msg.Respond(messages.ErrResp(err))
			return
		}
	}
	re, err := queryRegex(req.Regex)
	if err != nil {
		_ = msg.Respond(messages.ErrResp(err))
		return
	}
	resp := gt.eng.AllObjects(re)
	_ = msg.Respond(messages.OKDataResp(resp))
}

func (gt *GeoTruth) handleAllObjectsOriented(msg *nats.Msg) {
	if !gt.ready.Load() {
		_ = msg.Respond(messages.ErrResp(fmt.Errorf("recovering, retry")))
		return
	}
	var req natsquery.AllObjectsOrientedReq
	if len(msg.Data) > 0 {
		if err := json.Unmarshal(msg.Data, &req); err != nil {
			_ = msg.Respond(messages.ErrResp(err))
			return
		}
	}
	re, err := queryRegex(req.Regex)
	if err != nil {
		_ = msg.Respond(messages.ErrResp(err))
		return
	}
	resp := gt.eng.AllObjectsOriented(re)
	_ = msg.Respond(messages.OKDataResp(resp))
}

func (gt *GeoTruth) handleRegionOf(msg *nats.Msg) {
	if !gt.ready.Load() {
		_ = msg.Respond(messages.ErrResp(fmt.Errorf("recovering, retry")))
		return
	}
	var req natsquery.RegionOfReq
	if err := json.Unmarshal(msg.Data, &req); err != nil {
		_ = msg.Respond(messages.ErrResp(err))
		return
	}
	region, err := gt.eng.RegionOf(req.ObjectID)
	if err != nil {
		_ = msg.Respond(messages.ErrResp(err))
		return
	}
	resp := natsquery.RegionOfResp{Region: region}
	_ = msg.Respond(messages.OKDataResp(resp))
}

func (gt *GeoTruth) handleRegionFromPoint(msg *nats.Msg) {
	if !gt.ready.Load() {
		_ = msg.Respond(messages.ErrResp(fmt.Errorf("recovering, retry")))
		return
	}
	var req natsquery.RegionFromPointReq
	if err := json.Unmarshal(msg.Data, &req); err != nil {
		_ = msg.Respond(messages.ErrResp(err))
		return
	}
	region, err := gt.eng.RegionFromPoint(req.X, req.Y, req.Z)
	if err != nil {
		_ = msg.Respond(messages.ErrResp(err))
		return
	}
	resp := natsquery.RegionOfResp{Region: region}
	_ = msg.Respond(messages.OKDataResp(resp))
}

func (gt *GeoTruth) handleAllAreas(msg *nats.Msg) {
	if !gt.ready.Load() {
		_ = msg.Respond(messages.ErrResp(fmt.Errorf("recovering, retry")))
		return
	}
	var req natsquery.AllAreasReq
	if len(msg.Data) > 0 {
		if err := json.Unmarshal(msg.Data, &req); err != nil {
			_ = msg.Respond(messages.ErrResp(err))
			return
		}
	}
	re, err := queryRegex(req.Regex)
	if err != nil {
		_ = msg.Respond(messages.ErrResp(err))
		return
	}
	resp := gt.eng.AllAreas(re)
	_ = msg.Respond(messages.OKDataResp(resp))
}
