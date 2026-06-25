package ingester

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/midtxwn/geotruth/internal/geo"
	privKeys "github.com/midtxwn/geotruth/internal/natskeys"
	"github.com/midtxwn/geotruth/internal/spatialstream"
	"github.com/midtxwn/geotruth/internal/streampressure"
	pkgingester "github.com/midtxwn/geotruth/pkg/ingester"
	"github.com/midtxwn/geotruth/pkg/messages"
	"github.com/midtxwn/geotruth/pkg/natspublish"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

const (
	defaultShardQueueCapacity = 4096
	maxDefaultShardCount      = 64
	publishAttemptTimeout     = 5 * time.Second
	publishInitialBackoff     = 50 * time.Millisecond
	publishMaxBackoff         = 2 * time.Second
	publishLogInterval        = 5 * time.Second
	startupFlushTimeout       = 5 * time.Second
	subscriptionDrainTimeout  = 5 * time.Second
)

type Ingester struct {
	js                 jetstream.JetStream
	nc                 *nats.Conn
	kvAreas            jetstream.KeyValue
	pressure           *streampressure.Monitor
	shardCount         int
	shardQueueCapacity int
	shardChans         []chan shardWork
	shardConns         []*nats.Conn
	shardJS            []jetstream.JetStream
	connectNATS        pkgingester.NATSConnector
	shardWG            sync.WaitGroup
	chReady            chan struct{}
	chDone             chan struct{}
	errMu              sync.Mutex
	err                error
}

func newIngester(cfg pkgingester.Config, deps pkgingester.Dependencies) (*Ingester, error) {
	if deps.NATS == nil {
		return nil, fmt.Errorf("ingester: NATS dependency is required")
	}
	if cfg.ShardCount < 0 {
		return nil, fmt.Errorf("ShardCount must be >= 0, got %d", cfg.ShardCount)
	}
	if cfg.ShardQueueCapacity < 0 {
		return nil, fmt.Errorf("ShardQueueCapacity must be >= 0, got %d", cfg.ShardQueueCapacity)
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
	shardCount := cfg.ShardCount
	if shardCount == 0 {
		shardCount = minInt(maxDefaultShardCount, maxInt(1, runtime.GOMAXPROCS(0)*4))
	}
	shardQueueCapacity := cfg.ShardQueueCapacity
	if shardQueueCapacity == 0 {
		shardQueueCapacity = defaultShardQueueCapacity
	}

	ctx := context.Background()
	_, err = spatialstream.EnsureStream(ctx, js, spatialstream.Config{
		Storage:  storage,
		MaxBytes: &cfg.SpatialMaxBytes,
		Replicas: &cfg.Replicas,
	})
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

	ing := &Ingester{
		js:                 js,
		nc:                 nc,
		kvAreas:            kvAreas,
		shardCount:         shardCount,
		shardQueueCapacity: shardQueueCapacity,
		shardChans:         make([]chan shardWork, shardCount),
		shardConns:         make([]*nats.Conn, shardCount),
		shardJS:            make([]jetstream.JetStream, shardCount),
		connectNATS:        deps.NATS,
		pressure: streampressure.New(nc, js, privKeys.StreamName, streampressure.Config{
			Enabled:            cfg.PressureMonitor.Enabled,
			WarnRatio:          cfg.PressureMonitor.WarnRatio,
			CriticalRatio:      cfg.PressureMonitor.CriticalRatio,
			MinRefreshInterval: cfg.PressureMonitor.MinRefreshInterval,
			MinBytesDelta:      cfg.PressureMonitor.MinBytesDelta,
		}),
		chReady: make(chan struct{}),
		chDone:  make(chan struct{}),
	}
	for i := range ing.shardChans {
		ing.shardChans[i] = make(chan shardWork, shardQueueCapacity)
	}
	cleanupMain = false
	return ing, nil
}

func (ing *Ingester) Ready() <-chan struct{} {
	return ing.chReady
}

func (ing *Ingester) Done() <-chan struct{} {
	return ing.chDone
}

func (ing *Ingester) Err() error {
	ing.errMu.Lock()
	defer ing.errMu.Unlock()
	return ing.err
}

func (ing *Ingester) finish(err error) {
	ing.errMu.Lock()
	ing.err = err
	ing.errMu.Unlock()
	close(ing.chDone)
}

func validateID(id string) error {
	if strings.Contains(id, ".") {
		return fmt.Errorf("id %q contains '.' which is not allowed in NATS subjects", id)
	}
	if id == "" {
		return fmt.Errorf("id must not be empty")
	}
	return nil
}

type sourceSeqPayload struct {
	SourceSeq uint64 `json:"source_seq"`
}

type shardWorkKind int

const (
	shardWorkRegister shardWorkKind = iota
	shardWorkRemove
	shardWorkPosition
	shardWorkPositionSync
)

type shardWork struct {
	kind     shardWorkKind
	objectID string
	payload  []byte
	reply    string
}

func parseObjectIngressSubject(subject string) (string, shardWorkKind, error) {
	parts := strings.Split(subject, ".")
	if len(parts) != 4 || parts[0] != "ingester" || parts[1] != "object" {
		return "", 0, fmt.Errorf("invalid object ingress subject %q", subject)
	}
	objectID := parts[2]
	if err := validateID(objectID); err != nil {
		return "", 0, err
	}
	switch parts[3] {
	case natspublish.IngesterOpRegister:
		return objectID, shardWorkRegister, nil
	case natspublish.IngesterOpRemove:
		return objectID, shardWorkRemove, nil
	case natspublish.IngesterOpPosition:
		return objectID, shardWorkPosition, nil
	case natspublish.IngesterOpPositionSync:
		return objectID, shardWorkPositionSync, nil
	default:
		return "", 0, fmt.Errorf("unknown object ingress operation %q", parts[3])
	}
}

func (ing *Ingester) routeObjectCommand(ctx context.Context, msg *nats.Msg) {
	objectID, kind, err := parseObjectIngressSubject(msg.Subject)
	if err != nil {
		_ = msg.Respond(messages.ErrResp(err))
		return
	}

	work := shardWork{
		kind:     kind,
		objectID: objectID,
		payload:  append([]byte(nil), msg.Data...),
		reply:    msg.Reply,
	}
	idx := objectShardIndex(objectID, len(ing.shardChans))
	ch := ing.shardChans[idx]
	select {
	case ch <- work:
	case <-ctx.Done():
		_ = msg.Respond(messages.ErrResp(ctx.Err()))
	}
}

func buildRegisterPublish(subjectObjectID string, data []byte) (string, []byte, error) {
	var req natspublish.RegisterObjectReq
	if err := json.Unmarshal(data, &req); err != nil {
		return "", nil, err
	}
	if err := validateID(req.ID); err != nil {
		return "", nil, err
	}
	if req.ID != subjectObjectID {
		return "", nil, fmt.Errorf("subject object ID %q does not match request ID %q", subjectObjectID, req.ID)
	}

	reg := natspublish.ObjectRegisterMsg{ID: req.ID, Dims: req.Dims, ClientOpID: req.ClientOpID}
	rb, err := json.Marshal(reg)
	if err != nil {
		return "", nil, fmt.Errorf("json: %w", err)
	}
	return privKeys.SubjectCmdObjRegister, rb, nil
}

func buildRemovePublish(subjectObjectID string, data []byte) (string, []byte, error) {
	var req natspublish.RemoveObjectReq
	if err := json.Unmarshal(data, &req); err != nil {
		return "", nil, err
	}

	if err := validateID(req.ID); err != nil {
		return "", nil, err
	}
	if req.ID != subjectObjectID {
		return "", nil, fmt.Errorf("subject object ID %q does not match request ID %q", subjectObjectID, req.ID)
	}

	rm := natspublish.ObjectRemoveMsg{ID: req.ID, ClientOpID: req.ClientOpID}
	rb, err := json.Marshal(rm)
	if err != nil {
		return "", nil, fmt.Errorf("json: %w", err)
	}
	return privKeys.SubjectCmdObjRemove, rb, nil
}

func buildPositionPublish(subjectObjectID string, data []byte) (string, []byte, error) {
	var req natspublish.UpdatePositionReq
	if err := json.Unmarshal(data, &req); err != nil {
		return "", nil, err
	}

	if err := validateID(req.ID); err != nil {
		return "", nil, err
	}
	if req.ID != subjectObjectID {
		return "", nil, fmt.Errorf("subject object ID %q does not match request ID %q", subjectObjectID, req.ID)
	}

	pos := natspublish.PositionMsg{ID: req.ID, X: req.X, Y: req.Y, Z: req.Z, RotY: req.RotY, ClientOpID: req.ClientOpID}
	b, err := json.Marshal(pos)
	if err != nil {
		return "", nil, fmt.Errorf("json: %w", err)
	}
	subj := privKeys.SubjectPosRawObject(req.ID)
	return subj, b, nil
}

func (ing *Ingester) handleShardWork(idx int, work shardWork) {
	var subj string
	var payload []byte
	var err error

	switch work.kind {
	case shardWorkRegister:
		subj, payload, err = buildRegisterPublish(work.objectID, work.payload)
	case shardWorkRemove:
		subj, payload, err = buildRemovePublish(work.objectID, work.payload)
	case shardWorkPosition, shardWorkPositionSync:
		subj, payload, err = buildPositionPublish(work.objectID, work.payload)
	default:
		err = fmt.Errorf("unknown shard work kind %d", work.kind)
	}
	if err != nil {
		ing.respond(idx, work.reply, messages.ErrResp(err))
		return
	}

	if work.kind == shardWorkPosition {
		ing.respond(idx, work.reply, messages.OKResp())
	}

	ack := ing.publishWithRetry(idx, work, subj, payload)
	if work.kind != shardWorkPosition {
		ing.respond(idx, work.reply, messages.OKDataResp(sourceSeqPayload{SourceSeq: ack.Sequence}))
	}
}

func (ing *Ingester) respond(idx int, reply string, data []byte) {
	if reply == "" {
		log.Printf("[ingester] shard %d response dropped: empty reply subject", idx)
		return
	}
	if err := ing.shardConns[idx].Publish(reply, data); err != nil {
		log.Printf("[ingester] shard %d respond %s: %v", idx, reply, err)
	}
}

func (ing *Ingester) publishWithRetry(idx int, work shardWork, subj string, payload []byte) *jetstream.PubAck {
	backoff := publishInitialBackoff
	var attempts uint64
	lastLog := time.Time{}
	for {
		attempts++
		ctx, cancel := context.WithTimeout(context.Background(), publishAttemptTimeout)
		ack, err := ing.shardJS[idx].PublishMsg(ctx, &nats.Msg{
			Subject: subj,
			Data:    payload,
		})
		cancel()
		if err == nil {
			ing.pressure.ObservePublish(subj, ack.Sequence, uint64(len(payload)))
			if attempts > 1 {
				log.Printf("[ingester] shard %d publish recovered for %s after %d attempts", idx, subj, attempts)
			}
			return ack
		}

		ing.pressure.ObservePublishError(subj, err)
		if lastLog.IsZero() || time.Since(lastLog) >= publishLogInterval {
			log.Printf("[ingester] shard %d publish retrying %s object=%s attempt=%d: %v", idx, subj, work.objectID, attempts, err)
			lastLog = time.Now()
		}

		sleep := backoff
		if backoff > 1 {
			sleep += time.Duration(rand.Int63n(int64(backoff / 2)))
		}
		time.Sleep(sleep)
		backoff *= 2
		if backoff > publishMaxBackoff {
			backoff = publishMaxBackoff
		}
	}
}

func (ing *Ingester) handleRegisterArea(msg *nats.Msg) {
	var req natspublish.RegisterAreaReq
	if err := json.Unmarshal(msg.Data, &req); err != nil {
		_ = msg.Respond(messages.ErrResp(err))
		return
	}

	if err := validateID(req.ID); err != nil {
		_ = msg.Respond(messages.ErrResp(err))
		return
	}

	if len(req.Points) < 3 {
		_ = msg.Respond(messages.ErrResp(fmt.Errorf("area %s needs at least 3 points, got %d", req.ID, len(req.Points))))
		return
	}

	if !geo.IsSimplePolygon(geo.Polygon(req.Points)) {
		_ = msg.Respond(messages.ErrResp(fmt.Errorf("area %s is self-intersecting", req.ID)))
		return
	}

	kv := natspublish.AreaKV{Region: req.Region, Points: req.Points}
	b, err := json.Marshal(kv)
	if err != nil {
		_ = msg.Respond(messages.ErrResp(fmt.Errorf("json: %w", err)))
		return
	}

	if _, err := ing.kvAreas.Put(context.Background(), req.ID, b); err != nil {
		_ = msg.Respond(messages.ErrResp(fmt.Errorf("register area %s: %w", req.ID, err)))
		return
	}

	log.Printf("[ingester] registered area %s (region %s)", req.ID, req.Region)
	_ = msg.Respond(messages.OKResp())
}

func (ing *Ingester) handleRemoveArea(msg *nats.Msg) {
	var req natspublish.RemoveAreaReq
	if err := json.Unmarshal(msg.Data, &req); err != nil {
		_ = msg.Respond(messages.ErrResp(err))
		return
	}

	if err := validateID(req.ID); err != nil {
		_ = msg.Respond(messages.ErrResp(err))
		return
	}

	if err := ing.kvAreas.Delete(context.Background(), req.ID); err != nil {
		_ = msg.Respond(messages.ErrResp(fmt.Errorf("delete area %s: %w", req.ID, err)))
		return
	}

	log.Printf("[ingester] removed area %s", req.ID)
	_ = msg.Respond(messages.OKResp())
}

func (ing *Ingester) startShards() error {
	for i := 0; i < ing.shardCount; i++ {
		nc, err := ing.connectNATS(fmt.Sprintf("shard-%d", i))
		if err != nil {
			ing.closeShardConns()
			return fmt.Errorf("connect shard %d: %w", i, err)
		}
		js, err := jetstream.New(nc)
		if err != nil {
			nc.Close()
			ing.closeShardConns()
			return fmt.Errorf("jetstream shard %d: %w", i, err)
		}
		ing.shardConns[i] = nc
		ing.shardJS[i] = js
	}

	for i := 0; i < ing.shardCount; i++ {
		ing.shardWG.Add(1)
		go ing.shardRunner(i)
	}
	log.Printf("[ingester] started %d object publish shards (queue=%d)", ing.shardCount, ing.shardQueueCapacity)
	return nil
}

func (ing *Ingester) shardRunner(idx int) {
	defer ing.shardWG.Done()
	ch := ing.shardChans[idx]
	for work := range ch {
		ing.handleShardWork(idx, work)
	}
}

func (ing *Ingester) closeShardInputs() {
	for _, ch := range ing.shardChans {
		close(ch)
	}
}

func (ing *Ingester) closeShardConns() {
	for _, nc := range ing.shardConns {
		if nc != nil {
			nc.Close()
		}
	}
}

func (ing *Ingester) drainMainConn() {
	if ing.nc == nil {
		return
	}
	if err := ing.nc.Drain(); err != nil {
		log.Printf("[ingester] drain main NATS connection: %v", err)
		ing.nc.Close()
	}
}

func drainSubscriptions(subs []*nats.Subscription, timeout time.Duration) bool {
	type watch struct {
		sub    *nats.Subscription
		closed <-chan nats.SubStatus
	}
	watches := make([]watch, 0, len(subs))
	for _, sub := range subs {
		ch := sub.StatusChanged(nats.SubscriptionClosed)
		if err := sub.Drain(); err != nil {
			log.Printf("[ingester] drain subscription: %v", err)
		}
		watches = append(watches, watch{sub: sub, closed: ch})
	}

	deadline := time.Now().Add(timeout)
	for _, watch := range watches {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			log.Printf("[ingester] drain subscription %s timed out", watch.sub.Subject)
			return false
		}
		select {
		case <-watch.closed:
		case <-time.After(remaining):
			log.Printf("[ingester] drain subscription %s timed out", watch.sub.Subject)
			return false
		}
	}
	return true
}

func objectShardIndex(objectID string, shardCount int) int {
	var hash uint32 = 2166136261
	for i := 0; i < len(objectID); i++ {
		hash ^= uint32(objectID[i])
		hash *= 16777619
	}
	return int(hash % uint32(shardCount))
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

func (ing *Ingester) run(ctx context.Context) error {
	if err := ing.startShards(); err != nil {
		return err
	}
	defer ing.closeShardConns()
	defer ing.drainMainConn()

	subs := []struct {
		subj    string
		handler nats.MsgHandler
	}{
		{natspublish.IngesterObjectWildcard, func(msg *nats.Msg) { ing.routeObjectCommand(ctx, msg) }},
		{natspublish.AreaRegister, ing.handleRegisterArea},
		{natspublish.AreaRemove, ing.handleRemoveArea},
	}
	activeSubs := make([]*nats.Subscription, 0, len(subs))
	for _, s := range subs {
		sub, err := ing.nc.Subscribe(s.subj, s.handler)
		if err != nil {
			return fmt.Errorf("subscribe %s: %w", s.subj, err)
		}
		activeSubs = append(activeSubs, sub)
		log.Printf("[ingester] listening on %s", s.subj)
	}
	if err := ing.nc.FlushTimeout(startupFlushTimeout); err != nil {
		return fmt.Errorf("flush subscriptions: %w", err)
	}

	close(ing.chReady)

	<-ctx.Done()
	log.Println("[ingester] shutting down")
	if drainSubscriptions(activeSubs, subscriptionDrainTimeout) {
		ing.closeShardInputs()
		ing.shardWG.Wait()
	} else {
		log.Println("[ingester] subscription drain timed out; leaving shard queues open to avoid racing active handlers")
	}
	return nil
}

func Run(ctx context.Context, cfg pkgingester.Config, deps pkgingester.Dependencies) (*Ingester, error) {
	ing, err := newIngester(cfg, deps)
	if err != nil {
		return nil, fmt.Errorf("ingester init: %w", err)
	}
	go func() {
		ing.finish(ing.run(ctx))
	}()
	return ing, nil
}
