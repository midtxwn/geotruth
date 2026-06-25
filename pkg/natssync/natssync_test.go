package natssync

import (
	"context"
	"fmt"
	"math"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/midtxwn/geotruth/embedded"
	"github.com/midtxwn/geotruth/pkg/domain"
	"github.com/midtxwn/geotruth/pkg/natskeys"
	"github.com/midtxwn/geotruth/pkg/natspublish"
	"github.com/midtxwn/geotruth/pkg/natsquery"

	"github.com/nats-io/nats.go/jetstream"
)

func startSyncServices(tb testing.TB) (*embedded.Services, *Client, natsquery.Query) {
	tb.Helper()
	cfg := embedded.DefaultConfig
	svc, err := embedded.Run(context.Background(), cfg, embedded.DefaultDependencies)
	if err != nil {
		tb.Fatalf("start services: %v", err)
	}
	tb.Cleanup(func() { svc.Shutdown() })

	nc := svc.NATSConn()
	syncCfg := DefaultConfig()
	syncClient, err := New(nc, syncCfg)
	if err != nil {
		tb.Fatalf("natssync New: %v", err)
	}
	tb.Cleanup(func() { syncClient.Close() })

	return svc, syncClient, natsquery.New(nc)
}

func TestSyncRegisterObject(t *testing.T) {
	_, syncClient, _ := startSyncServices(t)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	dims := domain.ObjectDimensions{Width: 1.0, Height: 1.0}
	err := syncClient.RegisterObject(ctx, "sync-reg-obj1", dims)
	if err != nil {
		t.Fatalf("RegisterObject: %v", err)
	}
}

func TestSyncDuplicateRegisterCompletesWithoutOverwritingDims(t *testing.T) {
	_, syncClient, query := startSyncServices(t)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	objectID := "sync-dup-reg-obj1"
	if err := syncClient.RegisterObject(ctx, objectID, domain.ObjectDimensions{Width: 2.0, Height: 4.0}); err != nil {
		t.Fatalf("first RegisterObject: %v", err)
	}
	if err := syncClient.UpdateObjectPosition(ctx, objectID, 10.0, 10.0, 1.0, 0.0); err != nil {
		t.Fatalf("UpdateObjectPosition: %v", err)
	}
	if err := syncClient.RegisterObject(ctx, objectID, domain.ObjectDimensions{Width: 8.0, Height: 8.0}); err != nil {
		t.Fatalf("duplicate RegisterObject: %v", err)
	}

	bounds, err := query.ObjectBounds(ctx, objectID)
	if err != nil {
		t.Fatalf("ObjectBounds: %v", err)
	}
	width := math.Hypot(bounds.TR.X-bounds.TL.X, bounds.TR.Y-bounds.TL.Y)
	if math.Abs(width-2.0) > 0.001 {
		t.Fatalf("duplicate register should not overwrite width, got %.3f", width)
	}
}

func TestSyncUpdatePosition(t *testing.T) {
	_, syncClient, query := startSyncServices(t)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	dims := domain.ObjectDimensions{Width: 2.0, Height: 3.0}
	if err := syncClient.RegisterObject(ctx, "sync-pos-obj1", dims); err != nil {
		t.Fatalf("RegisterObject: %v", err)
	}

	if err := syncClient.UpdateObjectPosition(ctx, "sync-pos-obj1", 5.0, 5.0, 1.0, 0.0); err != nil {
		t.Fatalf("UpdateObjectPosition: %v", err)
	}

	obj, err := query.ObjectData(ctx, "sync-pos-obj1")
	if err != nil {
		t.Fatalf("ObjectData after sync update: %v", err)
	}
	if obj.X != 5.0 || obj.Y != 5.0 {
		t.Errorf("expected position (5,5), got (%f,%f)", obj.X, obj.Y)
	}
}

func TestSyncReusesObjectWatcherForUpdates(t *testing.T) {
	_, syncClient, _ := startSyncServices(t)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	objectID := "sync-cache-obj1"
	dims := domain.ObjectDimensions{Width: 1.0, Height: 1.0}
	if err := syncClient.RegisterObject(ctx, objectID, dims); err != nil {
		t.Fatalf("RegisterObject: %v", err)
	}

	syncClient.mu.Lock()
	firstWatcher := syncClient.watchers[objectID]
	watcherCount := len(syncClient.watchers)
	syncClient.mu.Unlock()
	if firstWatcher == nil {
		t.Fatal("expected cached watcher after register")
	}
	if watcherCount != 1 {
		t.Fatalf("expected 1 cached watcher, got %d", watcherCount)
	}

	for i := 0; i < 5; i++ {
		if err := syncClient.UpdateObjectPosition(ctx, objectID, float64(i), float64(i), 1.0, 0.0); err != nil {
			t.Fatalf("UpdateObjectPosition %d: %v", i, err)
		}
	}

	syncClient.mu.Lock()
	currentWatcher := syncClient.watchers[objectID]
	watcherCount = len(syncClient.watchers)
	syncClient.mu.Unlock()
	if currentWatcher != firstWatcher {
		t.Fatal("expected updates to reuse the cached watcher")
	}
	if watcherCount != 1 {
		t.Fatalf("expected 1 cached watcher after updates, got %d", watcherCount)
	}

	if err := syncClient.RemoveObject(ctx, objectID); err != nil {
		t.Fatalf("RemoveObject: %v", err)
	}

	syncClient.mu.Lock()
	_, stillCached := syncClient.watchers[objectID]
	syncClient.mu.Unlock()
	if stillCached {
		t.Fatal("expected watcher to be evicted after remove")
	}
}

func TestSyncRemoveObject(t *testing.T) {
	_, syncClient, query := startSyncServices(t)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	dims := domain.ObjectDimensions{Width: 1.0, Height: 1.0}
	if err := syncClient.RegisterObject(ctx, "sync-rm-obj1", dims); err != nil {
		t.Fatalf("RegisterObject: %v", err)
	}

	if err := syncClient.UpdateObjectPosition(ctx, "sync-rm-obj1", 5.0, 5.0, 1.0, 0.0); err != nil {
		t.Fatalf("UpdateObjectPosition: %v", err)
	}

	if err := syncClient.RemoveObject(ctx, "sync-rm-obj1"); err != nil {
		t.Fatalf("RemoveObject: %v", err)
	}

	nearby, err := query.NearbyObjects(ctx, "0", 5.0, 5.0, 10.0, nil)
	if err != nil {
		t.Fatalf("NearbyObjects after sync remove: %v", err)
	}
	for _, obj := range nearby {
		if obj.ID == "sync-rm-obj1" {
			t.Fatal("removed object should not appear in NearbyObjects")
		}
	}
}

func TestSyncRemoveMissingObjectCompletes(t *testing.T) {
	_, syncClient, _ := startSyncServices(t)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := syncClient.RemoveObject(ctx, "sync-missing-rm-obj1"); err != nil {
		t.Fatalf("RemoveObject missing object: %v", err)
	}
}

func TestSyncConcurrentSameObject(t *testing.T) {
	_, syncClient, query := startSyncServices(t)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	dims := domain.ObjectDimensions{Width: 1.0, Height: 1.0}
	if err := syncClient.RegisterObject(ctx, "sync-conc-obj1", dims); err != nil {
		t.Fatalf("RegisterObject: %v", err)
	}

	if err := syncClient.UpdateObjectPosition(ctx, "sync-conc-obj1", 0.0, 0.0, 1.0, 0.0); err != nil {
		t.Fatalf("first UpdateObjectPosition: %v", err)
	}

	var wg sync.WaitGroup
	errs := make([]error, 3)

	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			x := float64(idx * 10)
			errs[idx] = syncClient.UpdateObjectPosition(ctx, "sync-conc-obj1", x, x, 1.0, 0.0)
		}(i)
	}
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Errorf("concurrent update %d: %v", i, err)
		}
	}

	obj, err := query.ObjectData(ctx, "sync-conc-obj1")
	if err != nil {
		t.Fatalf("ObjectData after concurrent updates: %v", err)
	}
	validX := obj.X == 0.0 || obj.X == 10.0 || obj.X == 20.0
	if !validX {
		t.Errorf("unexpected final position x=%f, expected one of {0,10,20}", obj.X)
	}
}

func TestSyncConcurrentDifferentObjects(t *testing.T) {
	_, syncClient, _ := startSyncServices(t)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	dims := domain.ObjectDimensions{Width: 1.0, Height: 1.0}
	for i := 0; i < 3; i++ {
		id := fmt.Sprintf("sync-diff-obj%d", i)
		if err := syncClient.RegisterObject(ctx, id, dims); err != nil {
			t.Fatalf("RegisterObject %s: %v", id, err)
		}
	}

	var wg sync.WaitGroup
	errs := make([]error, 3)

	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			id := fmt.Sprintf("sync-diff-obj%d", idx)
			errs[idx] = syncClient.UpdateObjectPosition(ctx, id, float64(idx), float64(idx), 1.0, 0.0)
		}(i)
	}
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Errorf("concurrent different-object update %d: %v", i, err)
		}
	}
}

func TestSyncRemoveThenReregister(t *testing.T) {
	_, syncClient, query := startSyncServices(t)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	dims := domain.ObjectDimensions{Width: 1.0, Height: 1.0}
	if err := syncClient.RegisterObject(ctx, "sync-rr-obj1", dims); err != nil {
		t.Fatalf("RegisterObject first: %v", err)
	}

	if err := syncClient.UpdateObjectPosition(ctx, "sync-rr-obj1", 5.0, 5.0, 1.0, 0.0); err != nil {
		t.Fatalf("UpdateObjectPosition first: %v", err)
	}

	if err := syncClient.RemoveObject(ctx, "sync-rr-obj1"); err != nil {
		t.Fatalf("RemoveObject: %v", err)
	}

	if err := syncClient.RegisterObject(ctx, "sync-rr-obj1", dims); err != nil {
		t.Fatalf("RegisterObject second: %v", err)
	}

	if err := syncClient.UpdateObjectPosition(ctx, "sync-rr-obj1", 10.0, 10.0, 1.0, 0.0); err != nil {
		t.Fatalf("UpdateObjectPosition second: %v", err)
	}

	obj, err := query.ObjectData(ctx, "sync-rr-obj1")
	if err != nil {
		t.Fatalf("ObjectData after reregister: %v", err)
	}
	if obj.X != 10.0 || obj.Y != 10.0 {
		t.Errorf("expected position (10,10), got (%f,%f)", obj.X, obj.Y)
	}
}

func TestSyncTimeout(t *testing.T) {
	cfg := embedded.DefaultConfig
	svc, err := embedded.Run(context.Background(), cfg, embedded.DefaultDependencies)
	if err != nil {
		t.Fatalf("start services: %v", err)
	}
	defer svc.Shutdown()

	nc := svc.NATSConn()

	syncCfg := DefaultConfig()
	syncClient, err := New(nc, syncCfg)
	if err != nil {
		t.Fatalf("natssync New: %v", err)
	}
	defer syncClient.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Nanosecond)
	defer cancel()
	time.Sleep(time.Millisecond)

	dims := domain.ObjectDimensions{Width: 1.0, Height: 1.0}
	err = syncClient.RegisterObject(ctx, "sync-timeout-obj1", dims)
	if err == nil {
		t.Fatal("expected timeout error, got nil")
	}
}

func TestObjectWatcherTimeoutDoesNotPoisonWatcher(t *testing.T) {
	watcher := &objectWatcher{
		objectID:    "sync-timeout-cache-obj1",
		cancel:      func() {},
		unsubscribe: func() {},
		waiters:     make(map[syncEventKey][]chan error),
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Nanosecond)
	time.Sleep(time.Millisecond)
	defer cancel()

	key, ch, err := watcher.addWaiter(syncEventPositionUpdated, "op-timeout")
	if err != nil {
		t.Fatalf("addWaiter: %v", err)
	}
	if err := watcher.wait(ctx, key, ch); err == nil {
		t.Fatal("expected timeout waiting for event")
	}

	watcher.mu.Lock()
	waiterCount := len(watcher.waiters[key])
	watcher.mu.Unlock()
	if waiterCount != 0 {
		t.Fatalf("expected timed-out waiter to be removed, got %d", waiterCount)
	}

	ctx2, cancel2 := context.WithTimeout(context.Background(), time.Second)
	defer cancel2()
	key2, ch2, err := watcher.addWaiter(syncEventPositionUpdated, "op-success")
	if err != nil {
		t.Fatalf("second addWaiter: %v", err)
	}
	watcher.observe(key2)
	if err := watcher.wait(ctx2, key2, ch2); err != nil {
		t.Fatalf("wait after matching event: %v", err)
	}
}

func TestObjectWatcherIgnoresWrongClientOpID(t *testing.T) {
	watcher := &objectWatcher{
		objectID:    "sync-opid-cache-obj1",
		cancel:      func() {},
		unsubscribe: func() {},
		waiters:     make(map[syncEventKey][]chan error),
	}

	key, ch, err := watcher.addWaiter(syncEventPositionUpdated, "wanted-op")
	if err != nil {
		t.Fatalf("addWaiter: %v", err)
	}
	watcher.observe(syncEventKey{kind: syncEventPositionUpdated, clientOpID: "other-op"})

	ctx, cancel := context.WithTimeout(context.Background(), time.Nanosecond)
	time.Sleep(time.Millisecond)
	defer cancel()
	if err := watcher.wait(ctx, key, ch); err == nil {
		t.Fatal("expected timeout for wrong client_op_id")
	}
}

func TestSyncGeofence(t *testing.T) {
	svc, syncClient, query := startSyncServices(t)
	nc := svc.NATSConn()
	pub := natspublish.New(nc)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	points := []domain.Point{{X: 0, Y: 0}, {X: 10, Y: 0}, {X: 10, Y: 10}, {X: 0, Y: 10}}
	if err := pub.RegisterArea(ctx, "sync-gf-zone", "0", points); err != nil {
		t.Fatalf("RegisterArea: %v", err)
	}

	assertEventually(t, func() bool {
		_, err := query.Area(ctx, "sync-gf-zone")
		return err == nil
	}, 5*time.Second)

	dims := domain.ObjectDimensions{Width: 1.0, Height: 1.0}
	if err := syncClient.RegisterObject(ctx, "sync-gf-obj1", dims); err != nil {
		t.Fatalf("RegisterObject: %v", err)
	}

	if err := syncClient.UpdateObjectPosition(ctx, "sync-gf-obj1", 5.0, 5.0, 1.0, 0.0); err != nil {
		t.Fatalf("UpdateObjectPosition: %v", err)
	}

	areas, err := query.AreasContainingObject(ctx, "sync-gf-obj1", nil)
	if err != nil {
		t.Fatalf("AreasContainingObject: %v", err)
	}
	found := false
	for _, a := range areas {
		if a.ID == "sync-gf-zone" {
			found = true
		}
	}
	if !found {
		t.Fatal("expected object inside sync-gf-zone")
	}
}

func TestDefaultConfig(t *testing.T) {
	cfg := DefaultConfig()
	if cfg.ConsumerName != "" {
		t.Fatalf("expected empty ConsumerName default, got %q", cfg.ConsumerName)
	}
	if cfg.AckWait != 60*time.Second {
		t.Fatalf("expected AckWait 60s, got %v", cfg.AckWait)
	}
	if cfg.MaxDeliver != -1 {
		t.Fatalf("expected MaxDeliver -1, got %d", cfg.MaxDeliver)
	}
	if cfg.MemoryStorage {
		t.Fatal("expected MemoryStorage false by default")
	}
	if cfg.Description != "" {
		t.Fatalf("expected empty Description default, got %q", cfg.Description)
	}
}

func TestNewWithGeneratedDurableName(t *testing.T) {
	svc, _, _ := startSyncServices(t)
	nc := svc.NATSConn()

	syncClient, err := New(nc, Config{})
	if err != nil {
		t.Fatalf("New with zero Config: %v", err)
	}
	defer syncClient.Close()

	if _, err := syncClient.ensureObjectWatcher("sync-generated-name-obj1"); err != nil {
		t.Fatalf("ensureObjectWatcher: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream: %v", err)
	}
	stream, err := js.Stream(ctx, natskeys.GTStreamName)
	if err != nil {
		t.Fatalf("GT_EVENTS stream: %v", err)
	}
	names := stream.ConsumerNames(ctx)
	foundGenerated := false
	for name := range names.Name() {
		if strings.HasPrefix(name, "syncreq-") {
			foundGenerated = true
			cons, err := js.Consumer(ctx, natskeys.GTStreamName, name)
			if err != nil {
				t.Fatalf("generated consumer lookup: %v", err)
			}
			info := cons.CachedInfo()
			if info.Config.Durable != name {
				t.Fatalf("expected generated consumer durable %q, got %q", name, info.Config.Durable)
			}
			if info.Config.InactiveThreshold != 0 {
				t.Fatalf("expected no inactive threshold, got %v", info.Config.InactiveThreshold)
			}
			break
		}
	}
	if err := names.Err(); err != nil {
		t.Fatalf("list consumer names: %v", err)
	}
	if !foundGenerated {
		t.Fatal("expected generated syncreq durable consumer")
	}
}

func TestNewPassesConsumerOptions(t *testing.T) {
	svc, _, _ := startSyncServices(t)
	nc := svc.NATSConn()

	cfg := Config{
		ConsumerName:  "sync-options-consumer",
		AckWait:       25 * time.Second,
		MaxDeliver:    7,
		MemoryStorage: true,
		Description:   "natssync options test",
	}
	syncClient, err := New(nc, cfg)
	if err != nil {
		t.Fatalf("New with options: %v", err)
	}
	defer syncClient.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if _, err := syncClient.ensureObjectWatcher("sync-options-obj1"); err != nil {
		t.Fatalf("ensureObjectWatcher: %v", err)
	}

	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream: %v", err)
	}
	cons, err := js.Consumer(ctx, natskeys.GTStreamName, cfg.ConsumerName)
	if err != nil {
		t.Fatalf("consumer lookup: %v", err)
	}
	info := cons.CachedInfo()
	if info.Name != cfg.ConsumerName {
		t.Fatalf("expected consumer name %q, got %q", cfg.ConsumerName, info.Name)
	}
	if info.Config.Durable != cfg.ConsumerName {
		t.Fatalf("expected durable name %q, got %q", cfg.ConsumerName, info.Config.Durable)
	}
	if info.Config.DeliverPolicy != jetstream.DeliverLastPerSubjectPolicy {
		t.Fatalf("expected DeliverLastPerSubjectPolicy, got %v", info.Config.DeliverPolicy)
	}
	if info.Config.AckWait != cfg.AckWait {
		t.Fatalf("expected AckWait %v, got %v", cfg.AckWait, info.Config.AckWait)
	}
	if info.Config.MaxDeliver != cfg.MaxDeliver {
		t.Fatalf("expected MaxDeliver %d, got %d", cfg.MaxDeliver, info.Config.MaxDeliver)
	}
	if !info.Config.MemoryStorage {
		t.Fatal("expected MemoryStorage true")
	}
	if info.Config.Description != cfg.Description {
		t.Fatalf("expected Description %q, got %q", cfg.Description, info.Config.Description)
	}
	if info.Config.InactiveThreshold != 0 {
		t.Fatalf("expected no inactive threshold, got %v", info.Config.InactiveThreshold)
	}
}

func assertEventually(tb testing.TB, fn func() bool, timeout time.Duration) {
	tb.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if fn() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	tb.Fatalf("condition never met within %s", timeout)
}
