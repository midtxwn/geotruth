package engine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/midtxwn/geotruth/internal/gtevents"
	"github.com/midtxwn/geotruth/internal/natskeys"
	"github.com/midtxwn/geotruth/pkg/domain"
	"github.com/midtxwn/geotruth/pkg/natspublish"
	"github.com/midtxwn/geotruth/pkg/regionresolver"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

var (
	squarePoints   = []domain.Point{{X: 0, Y: 0}, {X: 10, Y: 0}, {X: 10, Y: 10}, {X: 0, Y: 10}}
	largeSquarePts = []domain.Point{{X: 0, Y: 0}, {X: 100, Y: 0}, {X: 100, Y: 100}, {X: 0, Y: 100}}
	smallSquarePts = []domain.Point{{X: 0, Y: 0}, {X: 5, Y: 0}, {X: 5, Y: 5}, {X: 0, Y: 5}}
)

type testFlatResolver struct {
	floorHeight float64
	hysteresis  float64
	floors      int
}

func newTestFlatResolver(floors int) *testFlatResolver {
	return &testFlatResolver{floorHeight: 4.0, hysteresis: 0.2, floors: floors}
}

func (r *testFlatResolver) Resolve(_, _, z float64, prevRegion *string) (string, error) {
	if z < 0 {
		z = 0
	}
	naiveFloor := int(z / r.floorHeight)
	if naiveFloor >= r.floors {
		naiveFloor = r.floors - 1
	}

	if prevRegion == nil {
		return strconv.Itoa(naiveFloor), nil
	}

	prev, err := strconv.Atoi(*prevRegion)
	if err != nil {
		return strconv.Itoa(naiveFloor), nil
	}

	base := float64(prev) * r.floorHeight
	if z > base+r.floorHeight+r.hysteresis && prev < r.floors-1 {
		return strconv.Itoa(prev + 1), nil
	}
	if z < base-r.hysteresis && prev > 0 {
		return strconv.Itoa(prev - 1), nil
	}
	return *prevRegion, nil
}

func (r *testFlatResolver) KnownRegions() []string {
	regions := make([]string, r.floors)
	for i := 0; i < r.floors; i++ {
		regions[i] = strconv.Itoa(i)
	}
	return regions
}

type scriptedResolver struct {
	known   []string
	resolve func(x, y, z float64, prevRegion *string) (string, error)
}

func (r scriptedResolver) Resolve(x, y, z float64, prevRegion *string) (string, error) {
	return r.resolve(x, y, z, prevRegion)
}

func (r scriptedResolver) KnownRegions() []string {
	return r.known
}

func TestNewEngineRejectsInvalidKnownRegions(t *testing.T) {
	tests := []struct {
		name  string
		known []string
	}{
		{name: "empty", known: nil},
		{name: "empty ID", known: []string{""}},
		{name: "duplicate ID", known: []string{"0", "0"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			defer func() {
				if recover() == nil {
					t.Fatal("expected panic")
				}
			}()

			NewEngine(scriptedResolver{
				known: tt.known,
				resolve: func(_, _, _ float64, _ *string) (string, error) {
					return "", nil
				},
			})
		})
	}
}

// applyPrevInside simulates the dispatcher's post-commit deferred mutation.
// Workers no longer write prevInside - the dispatcher applies it after the
// GT_EVENTS commit confirms. Unit tests driving workers directly must call
// this to simulate the post-commit write, otherwise subsequent worker calls
// won't see correct geofence transition state.
func applyPrevInside(e *Engine, objectID string, region string, currentInside map[string]bool) {
	rs := e.regions[region]
	rs.mu.Lock()
	rs.prevInside[objectID] = currentInside
	rs.mu.Unlock()
}

type mockPublisher struct {
	commitCh chan *gtevents.CommitEnvelope
	resultCh chan gtevents.CommitResult
}

func newMockPublisher() *mockPublisher {
	return &mockPublisher{
		commitCh: make(chan *gtevents.CommitEnvelope, 4096),
		resultCh: make(chan gtevents.CommitResult, 4096),
	}
}

func (m *mockPublisher) Submit(env *gtevents.CommitEnvelope) {
	m.commitCh <- env
}

func (m *mockPublisher) Results() <-chan gtevents.CommitResult {
	return m.resultCh
}

func (m *mockPublisher) Run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case env := <-m.commitCh:
			m.resultCh <- gtevents.CommitResult{
				ObjectID:  env.ObjectID,
				SourceSeq: env.SourceSeq,
				Err:       nil,
			}
		}
	}
}

func newTestDispatcher(e *Engine, msgCh chan jetstream.Msg, numFloors int) (*Dispatcher, *mockPublisher) {
	mp := newMockPublisher()
	resolver := newTestFlatResolver(numFloors)
	d := NewDispatcher(e, msgCh, mp, resolver, nil)
	return d, mp
}

func newTestDispatcherWithResolver(e *Engine, resolver regionresolver.Resolver) (*Dispatcher, *mockPublisher) {
	mp := newMockPublisher()
	d := NewDispatcher(e, make(chan jetstream.Msg, 1), mp, resolver, nil)
	return d, mp
}

type mockMsg struct {
	subject string
	data    []byte
	seq     uint64
	deliver uint64
	termErr error
	acked   atomic.Bool
	nacked  atomic.Bool
	inprog  atomic.Bool
	termed  atomic.Bool
}

func (m *mockMsg) Ack() error                         { m.acked.Store(true); return nil }
func (m *mockMsg) Nak() error                         { m.nacked.Store(true); return nil }
func (m *mockMsg) NakWithDelay(_ time.Duration) error { m.nacked.Store(true); return nil }
func (m *mockMsg) Term() error                        { m.termed.Store(true); return m.termErr }
func (m *mockMsg) TermWithReason(_ string) error      { return nil }
func (m *mockMsg) InProgress() error                  { m.inprog.Store(true); return nil }
func (m *mockMsg) Data() []byte                       { return m.data }
func (m *mockMsg) Subject() string                    { return m.subject }
func (m *mockMsg) Reply() string                      { return "" }
func (m *mockMsg) Headers() nats.Header               { return nil }
func (m *mockMsg) Respond(_ []byte) error             { return nil }
func (m *mockMsg) RespondMsg(_ *nats.Msg) error       { return nil }
func (m *mockMsg) DoubleAck(_ context.Context) error  { return m.Ack() }

func (m *mockMsg) Metadata() (*jetstream.MsgMetadata, error) {
	return &jetstream.MsgMetadata{
		Sequence:     jetstream.SequencePair{Stream: m.seq},
		NumDelivered: m.deliver,
	}, nil
}

func makePosMsg(id string, x, y, z, rotY float64) *mockMsg {
	pos := natspublish.PositionMsg{ID: id, X: x, Y: y, Z: z, RotY: rotY}
	b, _ := json.Marshal(pos)
	return &mockMsg{
		subject: natskeys.SubjectPosRawObject(id),
		data:    b,
		seq:     1,
	}
}

func makeRegisterMsg(id string, w, h float64) *mockMsg {
	reg := natspublish.ObjectRegisterMsg{ID: id, Dims: domain.ObjectDimensions{Width: w, Height: h}}
	b, _ := json.Marshal(reg)
	return &mockMsg{
		subject: natskeys.SubjectCmdObjRegister,
		data:    b,
		seq:     1,
	}
}

func makeRemoveMsg(id string) *mockMsg {
	rm := natspublish.ObjectRemoveMsg{ID: id}
	b, _ := json.Marshal(rm)
	return &mockMsg{
		subject: natskeys.SubjectCmdObjRemove,
		data:    b,
		seq:     2,
	}
}

func placeObject(e *Engine, id string, x, y, z float64, dims domain.ObjectDimensions) {
	e.RegisterObject(id, dims)
	resolver := e.Resolver()
	region, _ := resolver.Resolve(x, y, z, regionresolver.NoPrevRegion)
	e.BootstrapPlaceObject(id, x, y, z, 0, region)
}

func TestBootstrapPlaceObject(t *testing.T) {
	e := NewEngine(newTestFlatResolver(2))
	e.RegisterObject("r1", domain.ObjectDimensions{Width: 0.6, Height: 0.8})
	e.BootstrapPlaceObject("r1", 5, 5, 1.0, 0, "0")

	all := e.AllObjects(nil)
	if len(all.Regions["0"]) != 1 || all.Regions["0"][0].ID != "r1" {
		t.Fatalf("expected r1 on region 0, got %v", all.Regions["0"])
	}
	if all.Regions["0"][0].X != 5 || all.Regions["0"][0].Y != 5 {
		t.Fatalf("expected position (5,5), got (%v,%v)", all.Regions["0"][0].X, all.Regions["0"][0].Y)
	}
}

func TestWorkerInit(t *testing.T) {
	e := NewEngine(newTestFlatResolver(2))
	e.RegisterObject("r1", domain.ObjectDimensions{Width: 0.6, Height: 0.8})

	doneCh := make(chan WorkerResult, 1)
	w := NewRegionWorker("0", e, doneCh)
	go w.Run()
	defer close(w.inbox)

	msg := makePosMsg("r1", 5, 5, 1.0, 0)
	msg.seq = 1
	w.inbox <- WorkerTask{
		Kind: taskInit, Msg: msg, ID: "r1",
		X: 5, Y: 5, Z: 1.0, RotY: 0,
		Dims:      domain.ObjectDimensions{Width: 0.6, Height: 0.8},
		Region:    "0",
		StreamSeq: 1,
		Resp:      doneCh,
	}

	select {
	case res := <-doneCh:
		if res.Outcome != outcomeReady {
			t.Fatalf("expected outcomeReady, got %d", res.Outcome)
		}
		if res.NewRegion != "0" {
			t.Fatalf("expected region 0, got %s", res.NewRegion)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for worker result")
	}

	all := e.AllObjects(nil)
	if len(all.Regions["0"]) != 1 || all.Regions["0"][0].ID != "r1" {
		t.Fatalf("expected r1 on region 0, got %v", all.Regions["0"])
	}
}

func TestFloorTransitionWorker(t *testing.T) {
	e := NewEngine(newTestFlatResolver(2))
	doneCh := make(chan WorkerResult, 64)

	w0 := NewRegionWorker("0", e, doneCh)
	w1 := NewRegionWorker("1", e, doneCh)
	tx := NewTransitionExec(e, doneCh)

	go w0.Run()
	go w1.Run()
	go tx.Run()

	e.RegisterObject("r1", domain.ObjectDimensions{Width: 0.6, Height: 0.8})

	initMsg := makePosMsg("r1", 5, 5, 1.0, 0)
	initMsg.seq = 1
	w0.inbox <- WorkerTask{
		Kind: taskInit, Msg: initMsg, ID: "r1",
		X: 5, Y: 5, Z: 1.0, RotY: 0,
		Dims:   domain.ObjectDimensions{Width: 0.6, Height: 0.8},
		Region: "0", StreamSeq: 1, Resp: doneCh,
	}
	res := <-doneCh
	if res.Outcome != outcomeReady {
		t.Fatalf("init failed: outcome=%d", res.Outcome)
	}

	all := e.AllObjects(nil)
	if len(all.Regions["0"]) != 1 {
		t.Fatal("r1 should be on region 0 before transition")
	}

	transMsg := makePosMsg("r1", 5, 5, 5.5, 0)
	transMsg.seq = 2
	tx.inbox <- WorkerTask{
		Kind: taskTransition, Msg: transMsg, ID: "r1",
		X: 5, Y: 5, Z: 5.5, RotY: 0,
		Dims:      domain.ObjectDimensions{Width: 0.6, Height: 0.8},
		OldRegion: "0", Region: "1", StreamSeq: 2, Resp: doneCh,
	}
	res = <-doneCh
	if res.Outcome != outcomeReady {
		t.Fatalf("transition failed: outcome=%d", res.Outcome)
	}

	all = e.AllObjects(nil)
	if len(all.Regions["0"]) != 0 {
		t.Fatal("r1 should be removed from region 0 after transition")
	}
	if len(all.Regions["1"]) != 1 || all.Regions["1"][0].ID != "r1" {
		t.Fatal("r1 should be on region 1 after transition")
	}
}

func TestFloorTransitionExitEvents(t *testing.T) {
	e := NewEngine(newTestFlatResolver(2))
	e.RegisterArea("zone-a", "0", squarePoints)
	e.RegisterObject("r1", domain.ObjectDimensions{Width: 0.5, Height: 0.5})

	doneCh := make(chan WorkerResult, 64)
	w0 := NewRegionWorker("0", e, doneCh)
	tx := NewTransitionExec(e, doneCh)
	go w0.Run()
	go tx.Run()

	initMsg := makePosMsg("r1", 5, 5, 1.0, 0)
	initMsg.seq = 1
	w0.inbox <- WorkerTask{
		Kind: taskInit, Msg: initMsg, ID: "r1",
		X: 5, Y: 5, Z: 1.0, RotY: 0,
		Dims:   domain.ObjectDimensions{Width: 0.5, Height: 0.5},
		Region: "0", StreamSeq: 1, Resp: doneCh,
	}
	res, _ := <-doneCh
	applyPrevInside(e, "r1", res.NewRegion, res.PostCurrentInside)

	transMsg := makePosMsg("r1", 5, 5, 5.5, 0)
	transMsg.seq = 2
	tx.inbox <- WorkerTask{
		Kind: taskTransition, Msg: transMsg, ID: "r1",
		X: 5, Y: 5, Z: 5.5, RotY: 0,
		Dims:      domain.ObjectDimensions{Width: 0.5, Height: 0.5},
		OldRegion: "0", Region: "1", StreamSeq: 2, Resp: doneCh,
	}
	res = <-doneCh

	hasExit := false
	for _, tr := range res.Transitions {
		if !tr.Entered && tr.AreaID == "zone-a" {
			hasExit = true
		}
	}
	if !hasExit {
		t.Fatal("expected exit event for zone-a on floor transition")
	}
}

func TestFloorTransitionEventOrder(t *testing.T) {
	e := NewEngine(newTestFlatResolver(2))
	e.RegisterArea("zone-a", "0", squarePoints)
	e.RegisterArea("zone-b", "1", squarePoints)
	e.RegisterObject("r1", domain.ObjectDimensions{Width: 0.5, Height: 0.5})

	doneCh := make(chan WorkerResult, 64)
	w0 := NewRegionWorker("0", e, doneCh)
	w1 := NewRegionWorker("1", e, doneCh)
	tx := NewTransitionExec(e, doneCh)
	go w0.Run()
	go w1.Run()
	go tx.Run()

	initMsg := makePosMsg("r1", 5, 5, 1.0, 0)
	initMsg.seq = 1
	w0.inbox <- WorkerTask{
		Kind: taskInit, Msg: initMsg, ID: "r1",
		X: 5, Y: 5, Z: 1.0, RotY: 0,
		Dims:   domain.ObjectDimensions{Width: 0.5, Height: 0.5},
		Region: "0", StreamSeq: 1, Resp: doneCh,
	}
	initRes := <-doneCh
	applyPrevInside(e, "r1", initRes.NewRegion, initRes.PostCurrentInside)

	transMsg := makePosMsg("r1", 5, 5, 5.5, 0)
	transMsg.seq = 2
	tx.inbox <- WorkerTask{
		Kind: taskTransition, Msg: transMsg, ID: "r1",
		X: 5, Y: 5, Z: 5.5, RotY: 0,
		Dims:      domain.ObjectDimensions{Width: 0.5, Height: 0.5},
		OldRegion: "0", Region: "1", StreamSeq: 2, Resp: doneCh,
	}
	res := <-doneCh

	exitIdx := -1
	enterIdx := -1
	for i, tr := range res.Transitions {
		if !tr.Entered && tr.AreaID == "zone-a" {
			exitIdx = i
		}
		if tr.Entered && tr.AreaID == "zone-b" {
			enterIdx = i
		}
	}
	if exitIdx == -1 {
		t.Fatal("expected exit event for zone-a")
	}
	if enterIdx == -1 {
		t.Fatal("expected enter event for zone-b")
	}
	if exitIdx > enterIdx {
		t.Fatalf("exit event (idx %d) should come before enter event (idx %d)", exitIdx, enterIdx)
	}
}

func TestRemoveObjectBootstrap(t *testing.T) {
	e := NewEngine(newTestFlatResolver(2))
	e.RegisterArea("zone-a", "0", squarePoints)
	placeObject(e, "r1", 5, 5, 1.0, domain.ObjectDimensions{Width: 0.5, Height: 0.5})

	// BootstrapPlaceObject doesn't compute geofence events (correct for boot
	// recovery where prevInside is restored from GT_EVENTS state). We manually
	// set prevInside here to simulate the recovered detector state.
	rs := e.regions["0"]
	rs.mu.Lock()
	rs.prevInside["r1"] = map[string]bool{"zone-a": true}
	rs.mu.Unlock()

	transitions := e.DirectRemoveObject("r1")

	all := e.AllObjects(nil)
	if len(all.Regions["0"]) != 0 {
		t.Fatal("r1 should be removed from R-tree")
	}

	hasExit := false
	for _, tr := range transitions {
		if !tr.Entered && tr.AreaID == "zone-a" {
			hasExit = true
		}
	}
	if !hasExit {
		t.Fatal("expected exit transition for zone-a on object removal")
	}
}

func TestRemoveObjectNotYetPositioned(t *testing.T) {
	e := NewEngine(newTestFlatResolver(2))
	e.RegisterObject("r1", domain.ObjectDimensions{Width: 0.5, Height: 0.5})

	transitions := e.DirectRemoveObject("r1")
	_ = transitions

	if e.ObjectCount() != 0 {
		t.Fatal("r1 should be removed from objects map")
	}
}

func TestRemoveArea(t *testing.T) {
	e := NewEngine(newTestFlatResolver(2))
	e.RegisterArea("zone-a", "0", squarePoints)
	placeObject(e, "r1", 5, 5, 1.0, domain.ObjectDimensions{Width: 0.5, Height: 0.5})

	// RemoveArea cleans up the R-tree and prevInside.
	// In v1, area lifecycle events are not published to GT_EVENTS.
	e.RemoveArea("zone-a")

	_, err := e.Area("zone-a")
	if err == nil {
		t.Fatal("zone-a should be removed from area R-tree")
	}
}

func TestGeofenceEnterExitWorker(t *testing.T) {
	e := NewEngine(newTestFlatResolver(2))
	e.RegisterArea("zone-a", "0", squarePoints)
	e.RegisterObject("r1", domain.ObjectDimensions{Width: 0.5, Height: 0.5})

	doneCh := make(chan WorkerResult, 64)
	w0 := NewRegionWorker("0", e, doneCh)
	go w0.Run()

	initMsg := makePosMsg("r1", 5, 5, 1.0, 0)
	initMsg.seq = 1
	w0.inbox <- WorkerTask{
		Kind: taskInit, Msg: initMsg, ID: "r1",
		X: 5, Y: 5, Z: 1.0, RotY: 0,
		Dims:   domain.ObjectDimensions{Width: 0.5, Height: 0.5},
		Region: "0", StreamSeq: 1, Resp: doneCh,
	}
	res := <-doneCh

	if len(res.Transitions) == 0 {
		t.Fatal("expected enter transition, got none")
	}
	tr := res.Transitions[0]
	if !tr.Entered {
		t.Fatalf("expected enter transition, got exit")
	}
	if tr.AreaID != "zone-a" {
		t.Fatalf("expected zone-a, got %s", tr.AreaID)
	}

	// Simulate post-commit prevInside write.
	applyPrevInside(e, "r1", res.NewRegion, res.PostCurrentInside)

	updateMsg := makePosMsg("r1", 1000, 1000, 1.0, 0)
	updateMsg.seq = 2
	w0.inbox <- WorkerTask{
		Kind: taskUpdate, Msg: updateMsg, ID: "r1",
		X: 1000, Y: 1000, Z: 1.0, RotY: 0,
		Dims:   domain.ObjectDimensions{Width: 0.5, Height: 0.5},
		Region: "0", StreamSeq: 2, Resp: doneCh,
	}
	res = <-doneCh

	if len(res.Transitions) == 0 {
		t.Fatal("expected exit transition after move out")
	}
	exitTr := res.Transitions[0]
	if exitTr.Entered {
		t.Fatalf("expected exit transition, got enter for %s", exitTr.AreaID)
	}
}

func TestGeofenceNoSpuriousEvents(t *testing.T) {
	e := NewEngine(newTestFlatResolver(2))
	e.RegisterArea("zone-b", "0", largeSquarePts)
	e.RegisterObject("r1", domain.ObjectDimensions{Width: 0.5, Height: 0.5})

	doneCh := make(chan WorkerResult, 64)
	w0 := NewRegionWorker("0", e, doneCh)
	go w0.Run()

	initMsg := makePosMsg("r1", 5, 5, 1.0, 0)
	initMsg.seq = 1
	w0.inbox <- WorkerTask{
		Kind: taskInit, Msg: initMsg, ID: "r1",
		X: 5, Y: 5, Z: 1.0, RotY: 0,
		Dims:   domain.ObjectDimensions{Width: 0.5, Height: 0.5},
		Region: "0", StreamSeq: 1, Resp: doneCh,
	}
	initRes := <-doneCh
	applyPrevInside(e, "r1", initRes.NewRegion, initRes.PostCurrentInside)

	updateMsg := makePosMsg("r1", 5, 5, 1.0, 0)
	updateMsg.seq = 2
	w0.inbox <- WorkerTask{
		Kind: taskUpdate, Msg: updateMsg, ID: "r1",
		X: 5, Y: 5, Z: 1.0, RotY: 0,
		Dims:   domain.ObjectDimensions{Width: 0.5, Height: 0.5},
		Region: "0", StreamSeq: 2, Resp: doneCh,
	}
	res := <-doneCh

	if len(res.Transitions) != 0 {
		t.Fatalf("expected no transitions on duplicate position, got %d", len(res.Transitions))
	}
}

func TestNearby(t *testing.T) {
	e := NewEngine(newTestFlatResolver(1))
	placeObject(e, "r1", 0, 0, 1.0, domain.ObjectDimensions{Width: 0.5, Height: 0.5})
	placeObject(e, "r2", 3, 3, 1.0, domain.ObjectDimensions{Width: 0.5, Height: 0.5})

	results, err := e.Nearby("0", 0, 0, 5.0, nil)
	if err != nil {
		t.Fatalf("Nearby error: %v", err)
	}
	if len(results) != 2 {
		t.Fatalf("expected 2 nearby objects, got %d", len(results))
	}
}

func TestNearbyUsesOBBIntersection(t *testing.T) {
	e := NewEngine(newTestFlatResolver(1))
	e.RegisterObject("edge", domain.ObjectDimensions{Width: 4, Height: 2})
	e.BootstrapPlaceObject("edge", 4.5, 0, 1.0, 0, "0")
	e.RegisterObject("miss", domain.ObjectDimensions{Width: 1, Height: 1})
	e.BootstrapPlaceObject("miss", 3.6, 3.6, 1.0, 0, "0")

	results, err := e.Nearby("0", 0, 0, 3.0, nil)
	if err != nil {
		t.Fatalf("Nearby error: %v", err)
	}

	found := make(map[string]bool)
	for _, obj := range results {
		found[obj.ID] = true
	}
	if !found["edge"] {
		t.Fatal("expected edge object because its OBB intersects the radius")
	}
	if found["miss"] {
		t.Fatal("miss object should be filtered after AABB broad phase")
	}
}

func TestNearbyOf(t *testing.T) {
	e := NewEngine(newTestFlatResolver(1))
	placeObject(e, "r1", 0, 0, 1.0, domain.ObjectDimensions{Width: 0.5, Height: 0.5})
	placeObject(e, "r2", 3, 3, 1.0, domain.ObjectDimensions{Width: 0.5, Height: 0.5})

	results, err := e.NearbyOf("r1", 5.0, nil)
	if err != nil {
		t.Fatalf("NearbyOf error: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("expected 1 nearby object, got %d", len(results))
	}
	if results[0].ID != "r2" {
		t.Fatalf("expected r2, got %s", results[0].ID)
	}
	if results[0].Position.X != 3 || results[0].Position.Y != 3 {
		t.Fatalf("expected position (3,3), got (%v,%v)", results[0].Position.X, results[0].Position.Y)
	}
}

func TestNearbyOfUsesCandidateOBBAndOriginCenter(t *testing.T) {
	e := NewEngine(newTestFlatResolver(1))
	e.RegisterObject("origin", domain.ObjectDimensions{Width: 20, Height: 20})
	e.BootstrapPlaceObject("origin", 0, 0, 1.0, 0, "0")
	e.RegisterObject("edge", domain.ObjectDimensions{Width: 4, Height: 2})
	e.BootstrapPlaceObject("edge", 4.5, 0, 1.0, 0, "0")
	e.RegisterObject("outside", domain.ObjectDimensions{Width: 1, Height: 1})
	e.BootstrapPlaceObject("outside", 8, 0, 1.0, 0, "0")

	results, err := e.NearbyOf("origin", 3.0, nil)
	if err != nil {
		t.Fatalf("NearbyOf error: %v", err)
	}

	found := make(map[string]bool)
	for _, obj := range results {
		found[obj.ID] = true
	}
	if !found["edge"] {
		t.Fatal("expected edge candidate because its OBB intersects the origin-centered radius")
	}
	if found["outside"] {
		t.Fatal("outside candidate should not be included by origin object size")
	}
}

func TestWithinArea(t *testing.T) {
	e := NewEngine(newTestFlatResolver(1))
	e.RegisterArea("zone-a", "0", squarePoints)
	placeObject(e, "r1", 5, 5, 1.0, domain.ObjectDimensions{Width: 0.5, Height: 0.5})

	results, err := e.WithinArea("0", "zone-a", nil)
	if err != nil {
		t.Fatalf("WithinArea error: %v", err)
	}
	if len(results) != 1 || results[0].ID != "r1" {
		t.Fatalf("expected r1 within zone-a, got %v", results)
	}
}

func TestAreasContaining(t *testing.T) {
	e := NewEngine(newTestFlatResolver(1))
	e.RegisterArea("zone-a", "0", squarePoints)
	placeObject(e, "r1", 5, 5, 1.0, domain.ObjectDimensions{Width: 0.5, Height: 0.5})

	results, err := e.AreasContaining("r1", nil)
	if err != nil {
		t.Fatalf("AreasContaining error: %v", err)
	}
	if len(results) != 1 || results[0].ID != "zone-a" {
		t.Fatalf("expected zone-a containing r1, got %v", results)
	}
}

func TestAreasAtPoint(t *testing.T) {
	e := NewEngine(newTestFlatResolver(1))
	e.RegisterArea("zone-a", "0", squarePoints)

	results, err := e.AreasAtPoint("0", 5, 5, nil)
	if err != nil {
		t.Fatalf("AreasAtPoint error: %v", err)
	}
	if len(results) != 1 || results[0].ID != "zone-a" {
		t.Fatalf("expected zone-a containing point, got %v", results)
	}

	results, err = e.AreasAtPoint("0", 50, 50, nil)
	if err != nil {
		t.Fatalf("AreasAtPoint error: %v", err)
	}
	if len(results) != 0 {
		t.Fatalf("expected no areas containing point (50,50), got %v", results)
	}
}

func TestIntersecting(t *testing.T) {
	e := NewEngine(newTestFlatResolver(1))
	placeObject(e, "r1", 0, 0, 1.0, domain.ObjectDimensions{Width: 2, Height: 2})
	placeObject(e, "r2", 1, 0, 1.0, domain.ObjectDimensions{Width: 2, Height: 2})

	results, err := e.Intersecting("r1", nil)
	if err != nil {
		t.Fatalf("Intersecting error: %v", err)
	}
	if len(results) != 1 || results[0].ID != "r2" {
		t.Fatalf("expected r2 intersecting r1, got %v", results)
	}
}

func TestBounds(t *testing.T) {
	e := NewEngine(newTestFlatResolver(1))
	placeObject(e, "r1", 5, 5, 1.0, domain.ObjectDimensions{Width: 2, Height: 2})

	bounds, err := e.Bounds("r1")
	if err != nil {
		t.Fatalf("Bounds error: %v", err)
	}
	if bounds.TL.X != 4 || bounds.TL.Y != 4 {
		t.Fatalf("expected TL (4,4), got (%v,%v)", bounds.TL.X, bounds.TL.Y)
	}
	if bounds.BR.X != 6 || bounds.BR.Y != 6 {
		t.Fatalf("expected BR (6,6), got (%v,%v)", bounds.BR.X, bounds.BR.Y)
	}
}

func TestNearbyAreas(t *testing.T) {
	e := NewEngine(newTestFlatResolver(1))
	e.RegisterArea("zone-a", "0", smallSquarePts)
	e.RegisterArea("zone-b", "0", []domain.Point{{X: 50, Y: 50}, {X: 55, Y: 50}, {X: 55, Y: 55}, {X: 50, Y: 55}})

	results, err := e.NearbyAreas("0", 0, 0, 10, nil)
	if err != nil {
		t.Fatalf("NearbyAreas error: %v", err)
	}
	found := false
	for _, a := range results {
		if a.ID == "zone-a" {
			found = true
		}
	}
	if !found {
		t.Fatal("expected zone-a in nearby areas")
	}
}

func TestNearbyAreasUsesPolygonCircleIntersection(t *testing.T) {
	e := NewEngine(newTestFlatResolver(1))
	if err := e.RegisterArea("edge-area", "0", []domain.Point{
		{X: 3.8, Y: -0.5},
		{X: 5.0, Y: -0.5},
		{X: 5.0, Y: 0.5},
		{X: 3.8, Y: 0.5},
	}); err != nil {
		t.Fatalf("RegisterArea edge-area: %v", err)
	}
	if err := e.RegisterArea("gap-area", "0", []domain.Point{
		{X: 0, Y: 0},
		{X: 10, Y: 0},
		{X: 10, Y: 10},
		{X: 8, Y: 10},
		{X: 8, Y: 2},
		{X: 2, Y: 2},
		{X: 2, Y: 10},
		{X: 0, Y: 10},
	}); err != nil {
		t.Fatalf("RegisterArea gap-area: %v", err)
	}

	results, err := e.NearbyAreas("0", 0, 0, 4, nil)
	if err != nil {
		t.Fatalf("NearbyAreas error: %v", err)
	}
	found := false
	for _, area := range results {
		if area.ID == "edge-area" {
			found = true
		}
	}
	if !found {
		t.Fatal("expected edge-area because its polygon intersects the radius")
	}

	results, err = e.NearbyAreas("0", 5, 6, 1, nil)
	if err != nil {
		t.Fatalf("NearbyAreas gap query error: %v", err)
	}
	for _, area := range results {
		if area.ID == "gap-area" {
			t.Fatal("gap-area should not match a circle inside its concave gap")
		}
	}
}

func TestArea(t *testing.T) {
	e := NewEngine(newTestFlatResolver(1))
	e.RegisterArea("zone-a", "0", squarePoints)

	area, err := e.Area("zone-a")
	if err != nil {
		t.Fatalf("Area error: %v", err)
	}
	if area.ID != "zone-a" {
		t.Fatalf("expected zone-a, got %s", area.ID)
	}
	if area.Region != "0" {
		t.Fatalf("expected region 0, got %s", area.Region)
	}
	if len(area.Triangles) != 2 {
		t.Fatalf("expected 2 triangles for square, got %d", len(area.Triangles))
	}

	_, err = e.Area("nonexistent")
	if err == nil {
		t.Fatal("expected error for nonexistent area")
	}
}

func TestAllObjects(t *testing.T) {
	e := NewEngine(newTestFlatResolver(2))
	placeObject(e, "r1", 5, 5, 1.0, domain.ObjectDimensions{Width: 0.6, Height: 0.8})
	placeObject(e, "r2", 10, 10, 5.5, domain.ObjectDimensions{Width: 1.0, Height: 1.0})

	resp := e.AllObjects(nil)
	if len(resp.Regions["0"]) != 1 || resp.Regions["0"][0].ID != "r1" {
		t.Fatalf("expected r1 on region 0, got %v", resp.Regions["0"])
	}
	if len(resp.Regions["1"]) != 1 || resp.Regions["1"][0].ID != "r2" {
		t.Fatalf("expected r2 on region 1, got %v", resp.Regions["1"])
	}
}

func TestAllAreas(t *testing.T) {
	e := NewEngine(newTestFlatResolver(2))
	e.RegisterArea("zone-a", "0", squarePoints)
	e.RegisterArea("zone-b", "1", smallSquarePts)

	resp := e.AllAreas(nil)
	if len(resp.Regions["0"]) != 1 || resp.Regions["0"][0].ID != "zone-a" {
		t.Fatalf("expected zone-a on region 0, got %v", resp.Regions["0"])
	}
	if len(resp.Regions["1"]) != 1 || resp.Regions["1"][0].ID != "zone-b" {
		t.Fatalf("expected zone-b on region 1, got %v", resp.Regions["1"])
	}
}

func TestObjectQueriesRegex(t *testing.T) {
	e := NewEngine(newTestFlatResolver(1))
	placeObject(e, "robot-alpha", 0, 0, 1.0, domain.ObjectDimensions{Width: 0.5, Height: 0.5})
	placeObject(e, "robot-beta", 3, 3, 1.0, domain.ObjectDimensions{Width: 0.5, Height: 0.5})

	alphaOnly := regexp.MustCompile("alpha")
	results, err := e.Nearby("0", 0, 0, 5.0, alphaOnly)
	if err != nil {
		t.Fatalf("Nearby error: %v", err)
	}
	if len(results) != 1 || results[0].ID != "robot-alpha" {
		t.Fatalf("expected only robot-alpha, got %v", results)
	}

	all := e.AllObjects(regexp.MustCompile(""))
	if len(all.Regions["0"]) != 2 {
		t.Fatalf("empty regex should match all objects, got %v", all.Regions["0"])
	}
}

func TestAreaQueriesRegex(t *testing.T) {
	e := NewEngine(newTestFlatResolver(1))
	e.RegisterArea("zone-alpha", "0", squarePoints)
	e.RegisterArea("zone-beta", "0", largeSquarePts)

	alphaOnly := regexp.MustCompile("alpha")
	results, err := e.AreasAtPoint("0", 5, 5, alphaOnly)
	if err != nil {
		t.Fatalf("AreasAtPoint error: %v", err)
	}
	if len(results) != 1 || results[0].ID != "zone-alpha" {
		t.Fatalf("expected only zone-alpha, got %v", results)
	}

	all := e.AllAreas(regexp.MustCompile(""))
	if len(all.Regions["0"]) != 2 {
		t.Fatalf("empty regex should match all areas, got %v", all.Regions["0"])
	}
}

func TestRegisterAreaTooFewPoints(t *testing.T) {
	e := NewEngine(newTestFlatResolver(2))

	if err := e.RegisterArea("bad", "0", nil); err == nil {
		t.Fatal("expected error for nil points")
	}
	if err := e.RegisterArea("bad", "0", []domain.Point{{X: 0, Y: 0}}); err == nil {
		t.Fatal("expected error for 1 point")
	}
	if err := e.RegisterArea("bad", "0", []domain.Point{{X: 0, Y: 0}, {X: 1, Y: 0}}); err == nil {
		t.Fatal("expected error for 2 points")
	}
}

func TestRegisterAreaNonexistentRegion(t *testing.T) {
	e := NewEngine(newTestFlatResolver(1))

	if err := e.RegisterArea("bad", "5", squarePoints); err == nil {
		t.Fatal("expected error for nonexistent region")
	}
}

func TestConcurrentPositionUpdates(t *testing.T) {
	e := NewEngine(newTestFlatResolver(2))
	msgCh := make(chan jetstream.Msg, 256)
	d, mp := newTestDispatcher(e, msgCh, 2)
	d.Start(context.Background())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go mp.Run(ctx)
	go d.Run(ctx)

	for i := 0; i < 10; i++ {
		id := string(rune('A' + i))
		regMsg := makeRegisterMsg(id, 0.5, 0.5)
		regMsg.seq = uint64(i + 1)
		msgCh <- regMsg
	}
	time.Sleep(50 * time.Millisecond)

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		id := string(rune('A' + i))
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				posMsg := makePosMsg(id, float64(j), float64(j), 1.0, 0)
				posMsg.seq = uint64(100 + j)
				msgCh <- posMsg
			}
		}()
	}
	wg.Wait()
	time.Sleep(3 * time.Second)

	if e.ObjectCount() != 10 {
		t.Fatalf("expected 10 objects, got %d", e.ObjectCount())
	}

	all := e.AllObjects(nil)
	total := 0
	for _, objs := range all.Regions {
		total += len(objs)
	}
	if total != 10 {
		t.Fatalf("expected 10 objects across all regions, got %d", total)
	}
}

func TestClassifyInit(t *testing.T) {
	e := NewEngine(newTestFlatResolver(2))
	d, _ := newTestDispatcher(e, make(chan jetstream.Msg, 1), 2)

	ctl := newObjCtl(domain.ObjectDimensions{Width: 0.5, Height: 0.5})
	kind, region, err := d.classify(ctl, natspublish.PositionMsg{Z: 1.0})
	if err != nil {
		t.Fatalf("classify: %v", err)
	}
	if kind != taskInit {
		t.Fatalf("expected taskInit, got %d", kind)
	}
	if region != "" {
		t.Fatalf("init should not pre-resolve region, got %q", region)
	}
}

func TestClassifyUpdate(t *testing.T) {
	e := NewEngine(newTestFlatResolver(2))
	d, _ := newTestDispatcher(e, make(chan jetstream.Msg, 1), 2)

	ctl := newObjCtl(domain.ObjectDimensions{Width: 0.5, Height: 0.5})
	ctl.routeRegion = "0"
	ctl.hasRouteRegion = true
	kind, region, err := d.classify(ctl, natspublish.PositionMsg{Z: 1.0})
	if err != nil {
		t.Fatalf("classify: %v", err)
	}
	if kind != taskUpdate {
		t.Fatalf("expected taskUpdate, got %d", kind)
	}
	if region != "0" {
		t.Fatalf("expected resolved region 0, got %q", region)
	}
}

func TestClassifyTransition(t *testing.T) {
	e := NewEngine(newTestFlatResolver(2))
	d, _ := newTestDispatcher(e, make(chan jetstream.Msg, 1), 2)

	ctl := newObjCtl(domain.ObjectDimensions{Width: 0.5, Height: 0.5})
	ctl.routeRegion = "0"
	ctl.hasRouteRegion = true
	kind, region, err := d.classify(ctl, natspublish.PositionMsg{Z: 5.5})
	if err != nil {
		t.Fatalf("classify: %v", err)
	}
	if kind != taskTransition {
		t.Fatalf("expected taskTransition, got %d", kind)
	}
	if region != "1" {
		t.Fatalf("expected resolved region 1, got %q", region)
	}
}

func TestClassifyResolverError(t *testing.T) {
	resolver := scriptedResolver{
		known: []string{"0"},
		resolve: func(x, _, _ float64, prevRegion *string) (string, error) {
			if prevRegion != nil && x == 99 {
				return "", errors.New("outside all regions")
			}
			return "0", nil
		},
	}
	e := NewEngine(resolver)
	e.RegisterObject("r1", domain.ObjectDimensions{Width: 0.5, Height: 0.5})
	if !e.BootstrapPlaceObject("r1", 5, 5, 1, 0, "0") {
		t.Fatal("bootstrap should place object")
	}
	d, _ := newTestDispatcherWithResolver(e, resolver)

	msg := makePosMsg("r1", 99, 5, 1, 0)
	msg.seq = 10
	d.onMsg(msg)

	if !msg.termed.Load() {
		t.Fatal("resolver error should terminate the position message")
	}
	obj, err := e.ObjectPosition("r1")
	if err != nil {
		t.Fatalf("object should remain positioned: %v", err)
	}
	if obj.X != 5 {
		t.Fatalf("rejected position should not be committed, got x=%v", obj.X)
	}
}

func TestClassifyUnknownRegion(t *testing.T) {
	resolver := scriptedResolver{
		known: []string{"0"},
		resolve: func(x, _, _ float64, prevRegion *string) (string, error) {
			if prevRegion != nil && x == 42 {
				return "missing", nil
			}
			return "0", nil
		},
	}
	e := NewEngine(resolver)
	e.RegisterObject("r1", domain.ObjectDimensions{Width: 0.5, Height: 0.5})
	if !e.BootstrapPlaceObject("r1", 5, 5, 1, 0, "0") {
		t.Fatal("bootstrap should place object")
	}
	d, _ := newTestDispatcherWithResolver(e, resolver)

	msg := makePosMsg("r1", 42, 5, 1, 0)
	msg.seq = 11
	d.onMsg(msg)

	if !msg.termed.Load() {
		t.Fatal("unknown resolved region should terminate the position message")
	}
}

func TestTermPoisonMessage(t *testing.T) {
	e := NewEngine(newTestFlatResolver(1))
	d, _ := newTestDispatcher(e, make(chan jetstream.Msg, 1), 1)

	msg := &mockMsg{
		subject: natskeys.SubjectPosRawObject("r1"),
		data:    []byte("{bad json"),
		seq:     12,
	}
	d.onMsg(msg)

	if !msg.termed.Load() {
		t.Fatal("malformed position JSON should be terminated")
	}
	if msg.acked.Load() || msg.nacked.Load() {
		t.Fatal("malformed position JSON should not be acked or nacked")
	}
}

func TestValidJSONInvalidObjectIDTerms(t *testing.T) {
	e := NewEngine(newTestFlatResolver(1))
	d, mp := newTestDispatcher(e, make(chan jetstream.Msg, 1), 1)

	reg := makeRegisterMsg("", 0.5, 0.5)
	reg.seq = 30
	d.onMsg(reg)
	if !reg.termed.Load() {
		t.Fatal("empty register ID should be terminated")
	}

	rm := makeRemoveMsg("bad.id")
	rm.seq = 31
	d.onMsg(rm)
	if !rm.termed.Load() {
		t.Fatal("dotted remove ID should be terminated")
	}

	pos := makePosMsg("bad.id", 1, 1, 1, 0)
	pos.seq = 32
	d.onMsg(pos)
	if !pos.termed.Load() {
		t.Fatal("dotted position ID should be terminated")
	}

	select {
	case env := <-mp.commitCh:
		t.Fatalf("invalid ID should not produce commit envelope: %+v", env)
	default:
	}
}

func TestPositionSubjectBodyMismatchTerms(t *testing.T) {
	e := NewEngine(newTestFlatResolver(1))
	e.RegisterObject("b", domain.ObjectDimensions{Width: 0.5, Height: 0.5})
	d, _ := newTestDispatcher(e, make(chan jetstream.Msg, 1), 1)

	msg := makePosMsg("b", 1, 1, 1, 0)
	msg.subject = natskeys.SubjectPosRawObject("a")
	msg.seq = 33
	d.onMsg(msg)

	if !msg.termed.Load() {
		t.Fatal("position subject/body ID mismatch should be terminated")
	}
	if msg.nacked.Load() || msg.acked.Load() {
		t.Fatal("position subject/body ID mismatch should not ack or nack")
	}
}

func TestPositionInvalidSubjectIDTerms(t *testing.T) {
	e := NewEngine(newTestFlatResolver(1))
	d, _ := newTestDispatcher(e, make(chan jetstream.Msg, 1), 1)

	msg := makePosMsg("r1", 1, 1, 1, 0)
	msg.subject = "pos.raw."
	msg.seq = 34
	d.onMsg(msg)

	if !msg.termed.Load() {
		t.Fatal("invalid position subject ID should be terminated")
	}
}

func TestTermFailurePreservesMailbox(t *testing.T) {
	resolver := scriptedResolver{
		known: []string{"0"},
		resolve: func(x, _, _ float64, prevRegion *string) (string, error) {
			if prevRegion != nil && x == 99 {
				return "", errors.New("outside all regions")
			}
			return "0", nil
		},
	}
	e := NewEngine(resolver)
	e.RegisterObject("r1", domain.ObjectDimensions{Width: 0.5, Height: 0.5})
	if !e.BootstrapPlaceObject("r1", 5, 5, 1, 0, "0") {
		t.Fatal("bootstrap should place object")
	}
	d, _ := newTestDispatcherWithResolver(e, resolver)

	poison := makePosMsg("r1", 99, 5, 1, 0)
	poison.seq = 35
	poison.termErr = errors.New("term failed")
	queued := makePosMsg("r1", 6, 5, 1, 0)
	queued.seq = 36

	ctl, _ := e.lookupCtl("r1")
	ctl.head = &Envelope{Msg: poison, ObjectID: "r1", StreamSeq: poison.seq, Pos: natspublish.PositionMsg{ID: "r1", X: 99, Y: 5, Z: 1}}
	ctl.pendingSeqs[poison.seq] = struct{}{}
	ctl.queue = append(ctl.queue, &Envelope{Msg: queued, ObjectID: "r1", StreamSeq: queued.seq, Pos: natspublish.PositionMsg{ID: "r1", X: 6, Y: 5, Z: 1}})

	d.termPoisonMsg(ctl, poison, poison.seq, "injected poison")

	if !poison.termed.Load() {
		t.Fatal("poison message should attempt Term")
	}
	if ctl.head == nil || ctl.head.StreamSeq != poison.seq {
		t.Fatal("failed Term should preserve poison head")
	}
	if _, ok := ctl.pendingSeqs[poison.seq]; !ok {
		t.Fatal("failed Term should preserve pending seq")
	}
	if len(ctl.queue) != 1 || ctl.queue[0].StreamSeq != queued.seq {
		t.Fatal("failed Term should not release queued message")
	}
}

func TestPositionBeforeRegisterBoundedRetry(t *testing.T) {
	e := NewEngine(newTestFlatResolver(1))
	d, _ := newTestDispatcher(e, make(chan jetstream.Msg, 1), 1)

	retry := makePosMsg("missing", 1, 1, 1, 0)
	retry.seq = 13
	retry.deliver = maxPositionBeforeRegisterDeliveries - 1
	d.onMsg(retry)
	if !retry.nacked.Load() {
		t.Fatal("position before register should NAK before threshold")
	}
	if retry.termed.Load() {
		t.Fatal("position before register should not term before threshold")
	}

	poison := makePosMsg("missing", 1, 1, 1, 0)
	poison.seq = 14
	poison.deliver = maxPositionBeforeRegisterDeliveries
	d.onMsg(poison)
	if !poison.termed.Load() {
		t.Fatal("position before register should term at threshold")
	}
}

func TestDuplicateSuppressionStale(t *testing.T) {
	e := NewEngine(newTestFlatResolver(1))
	e.RegisterObject("r1", domain.ObjectDimensions{Width: 0.5, Height: 0.5})

	ctl, _ := e.lookupCtl("r1")
	ctl.lastAppliedSeq = 100

	msg := makePosMsg("r1", 5, 5, 1.0, 0)
	msg.seq = 50

	d, _ := newTestDispatcher(e, make(chan jetstream.Msg, 1), 1)
	d.onMsg(msg)

	if !msg.acked.Load() {
		t.Fatal("stale duplicate should be acked immediately")
	}
}

func TestDuplicateSuppressionPending(t *testing.T) {
	e := NewEngine(newTestFlatResolver(1))
	e.RegisterObject("r1", domain.ObjectDimensions{Width: 0.5, Height: 0.5})

	ctl, _ := e.lookupCtl("r1")
	ctl.lastAppliedSeq = 100
	ctl.pendingSeqs[200] = struct{}{}

	msg := makePosMsg("r1", 5, 5, 1.0, 0)
	msg.seq = 200

	d, _ := newTestDispatcher(e, make(chan jetstream.Msg, 1), 1)
	d.onMsg(msg)

	if msg.acked.Load() || msg.nacked.Load() {
		t.Fatal("pending duplicate should not be acked or nacked")
	}
	if !msg.inprog.Load() {
		t.Fatal("pending duplicate should receive InProgress")
	}
}

func TestDispatcherRegisterIdempotent(t *testing.T) {
	e := NewEngine(newTestFlatResolver(1))
	msgCh := make(chan jetstream.Msg, 16)
	d, mp := newTestDispatcher(e, msgCh, 1)

	reg1 := makeRegisterMsg("r1", 0.5, 0.5)
	reg1.seq = 1
	d.onMsg(reg1)

	// Process the register commit result.
	env := <-mp.commitCh
	d.onCommitResult(gtevents.CommitResult{ObjectID: env.ObjectID, SourceSeq: env.SourceSeq})

	if e.ObjectCount() != 1 {
		t.Fatalf("expected 1 object after first register, got %d", e.ObjectCount())
	}

	reg2 := makeRegisterMsg("r1", 1.0, 1.0)
	reg2.seq = 2
	d.onMsg(reg2)

	dupEnv := <-mp.commitCh
	if dupEnv.SourceSeq != 2 {
		t.Fatalf("duplicate register commit source seq = %d, want 2", dupEnv.SourceSeq)
	}
	if reg2.acked.Load() {
		t.Fatal("duplicate register should not ack before commit")
	}

	var regEvt gtevents.ObjectRegisteredEvent
	if err := json.Unmarshal(dupEnv.Messages[0].Data, &regEvt); err != nil {
		t.Fatalf("unmarshal duplicate register event: %v", err)
	}
	if regEvt.SourceSeq != 2 {
		t.Fatalf("duplicate register event source seq = %d, want 2", regEvt.SourceSeq)
	}

	var st gtevents.ObjectStateRecord
	if err := json.Unmarshal(dupEnv.Messages[len(dupEnv.Messages)-1].Data, &st); err != nil {
		t.Fatalf("unmarshal duplicate register state: %v", err)
	}
	if st.DetectorStateSeq != 2 {
		t.Fatalf("duplicate register state seq = %d, want 2", st.DetectorStateSeq)
	}

	d.onCommitResult(gtevents.CommitResult{ObjectID: dupEnv.ObjectID, SourceSeq: dupEnv.SourceSeq})
	if !reg2.acked.Load() {
		t.Fatal("duplicate register should ack after commit")
	}

	ctl, ok := e.lookupCtl("r1")
	if !ok {
		t.Fatal("r1 should exist")
	}
	if ctl.dims.Width != 0.5 {
		t.Fatalf("dims should remain 0.5 (first register), got %v", ctl.dims.Width)
	}
}

func TestMailboxQueueing(t *testing.T) {
	e := NewEngine(newTestFlatResolver(1))
	doneCh := make(chan WorkerResult, 64)
	w0 := NewRegionWorker("0", e, doneCh)
	go w0.Run()

	e.RegisterObject("r1", domain.ObjectDimensions{Width: 0.5, Height: 0.5})

	msgCh := make(chan jetstream.Msg, 16)
	d, mp := newTestDispatcher(e, msgCh, 1)
	d.workers = map[string]*RegionWorker{"0": w0}
	d.doneCh = doneCh

	pos1 := makePosMsg("r1", 5, 5, 1.0, 0)
	pos1.seq = 1
	d.onMsg(pos1)

	ctl, _ := e.lookupCtl("r1")
	if ctl.head == nil {
		t.Fatal("head should be set after first message")
	}
	if len(ctl.queue) != 0 {
		t.Fatal("queue should be empty after first message")
	}

	pos2 := makePosMsg("r1", 6, 6, 1.0, 0)
	pos2.seq = 2
	d.onMsg(pos2)

	ctl, _ = e.lookupCtl("r1")
	if len(ctl.queue) != 1 {
		t.Fatalf("queue should have 1 item, got %d", len(ctl.queue))
	}

	// Worker completes the task.
	res := <-doneCh
	d.onResult(res)

	// Process the commit result to ack and drain the queue.
	env := <-mp.commitCh
	d.onCommitResult(gtevents.CommitResult{ObjectID: env.ObjectID, SourceSeq: env.SourceSeq})

	ctl, _ = e.lookupCtl("r1")
	if ctl.head == nil || ctl.head.StreamSeq != 2 {
		t.Fatal("second message should be dequeued as new head after first completes")
	}

	// Process the second position update through the commit stage.
	res = <-doneCh
	d.onResult(res)
	env = <-mp.commitCh
	d.onCommitResult(gtevents.CommitResult{ObjectID: env.ObjectID, SourceSeq: env.SourceSeq})
}

func TestRemoveDrainsQueue(t *testing.T) {
	e := NewEngine(newTestFlatResolver(1))
	doneCh := make(chan WorkerResult, 64)
	w0 := NewRegionWorker("0", e, doneCh)
	go w0.Run()

	e.RegisterObject("r1", domain.ObjectDimensions{Width: 0.5, Height: 0.5})

	msgCh := make(chan jetstream.Msg, 16)
	d, mp := newTestDispatcher(e, msgCh, 1)
	d.workers = map[string]*RegionWorker{"0": w0}
	d.doneCh = doneCh

	pos1 := makePosMsg("r1", 5, 5, 1.0, 0)
	pos1.seq = 1
	d.onMsg(pos1)
	<-doneCh

	// onResult builds the commit envelope for this position update.
	d.onResult(WorkerResult{ObjectID: "r1", StreamSeq: 1, NewRegion: "0", Outcome: outcomeReady,
		PostX: 5, PostY: 5, PostZ: 1.0, PostRotY: 0,
		PostDims: domain.ObjectDimensions{Width: 0.5, Height: 0.5}})

	// Process the commit result to complete the position update.
	env := <-mp.commitCh
	d.onCommitResult(gtevents.CommitResult{ObjectID: env.ObjectID, SourceSeq: env.SourceSeq})

	ctl, _ := e.lookupCtl("r1")
	if ctl.head != nil {
		t.Fatal("head should be nil after first result processed")
	}

	pos2 := makePosMsg("r1", 6, 6, 1.0, 0)
	pos2.seq = 2
	d.onMsg(pos2)

	pos3 := makePosMsg("r1", 7, 7, 1.0, 0)
	pos3.seq = 3
	d.onMsg(pos3)

	rmMsg := makeRemoveMsg("r1")
	rmMsg.seq = 4
	d.onMsg(rmMsg)

	ctl, _ = e.lookupCtl("r1")
	if len(ctl.queue) != 2 {
		t.Fatalf("expected 2 queued items (pos3 + remove), got %d", len(ctl.queue))
	}

	// Process the position update (seq=2) through worker → onResult → commit.
	res := <-doneCh
	d.onResult(res)
	env = <-mp.commitCh
	d.onCommitResult(gtevents.CommitResult{ObjectID: env.ObjectID, SourceSeq: env.SourceSeq})

	// Process the position update (seq=3) through worker → onResult → commit.
	res = <-doneCh
	d.onResult(res)
	env = <-mp.commitCh
	d.onCommitResult(gtevents.CommitResult{ObjectID: env.ObjectID, SourceSeq: env.SourceSeq})

	// Process the remove through worker → onResult → commit.
	res = <-doneCh
	d.onResult(res)
	env = <-mp.commitCh
	d.onCommitResult(gtevents.CommitResult{ObjectID: env.ObjectID, SourceSeq: env.SourceSeq})

	_, ok := e.lookupCtl("r1")
	if ok {
		t.Fatal("r1 should be removed from ctls after remove completes")
	}
}

func TestRemoveNeverPositionedObject(t *testing.T) {
	e := NewEngine(newTestFlatResolver(1))
	msgCh := make(chan jetstream.Msg, 16)
	d, mp := newTestDispatcher(e, msgCh, 1)

	// Register the object (no position update - it never enters the R-tree).
	reg := makeRegisterMsg("r1", 0.5, 0.5)
	reg.seq = 1
	d.onMsg(reg)

	// Drain the register commit.
	env := <-mp.commitCh
	d.onCommitResult(gtevents.CommitResult{ObjectID: env.ObjectID, SourceSeq: env.SourceSeq})

	if e.ObjectCount() != 1 {
		t.Fatalf("expected 1 object after register, got %d", e.ObjectCount())
	}

	// Remove the object that was never positioned.
	rm := makeRemoveMsg("r1")
	rm.seq = 2
	d.onMsg(rm)

	// The dispatchRemove else-branch should build a commit envelope
	// (no geofence exits) without deleting from ctls prematurely.
	env = <-mp.commitCh

	// Before onCommitResult, the source message should NOT be acked.
	if rm.acked.Load() {
		t.Fatal("remove source message should not be acked before commit confirms")
	}

	// The object should still be in ctls (not prematurely deleted).
	if e.ObjectCount() != 1 {
		t.Fatal("object should still be in ctls before commit confirms")
	}

	// Confirm the commit - onCommitResult should Ack and delete from ctls.
	d.onCommitResult(gtevents.CommitResult{ObjectID: env.ObjectID, SourceSeq: env.SourceSeq})

	if !rm.acked.Load() {
		t.Fatal("remove source message should be acked after commit confirms")
	}

	if e.ObjectCount() != 0 {
		t.Fatalf("expected 0 objects after remove commit, got %d", e.ObjectCount())
	}
}

func TestRemoveCleansUpPrevInside(t *testing.T) {
	e := NewEngine(newTestFlatResolver(1))
	if err := e.RegisterArea("zone-a", "0", squarePoints); err != nil {
		t.Fatalf("RegisterArea: %v", err)
	}
	e.RegisterObject("r1", domain.ObjectDimensions{Width: 0.5, Height: 0.5})
	if !e.BootstrapPlaceObject("r1", 5, 5, 1, 0, "0") {
		t.Fatal("bootstrap should place object")
	}
	applyPrevInside(e, "r1", "0", map[string]bool{"zone-a": true})

	doneCh := make(chan WorkerResult, 64)
	w0 := NewRegionWorker("0", e, doneCh)
	go w0.Run()

	d, mp := newTestDispatcher(e, make(chan jetstream.Msg, 1), 1)
	d.workers = map[string]*RegionWorker{"0": w0}
	d.doneCh = doneCh

	rm := makeRemoveMsg("r1")
	rm.seq = 21
	d.onMsg(rm)

	res := <-doneCh
	if res.PostOldRegion != "0" {
		t.Fatalf("remove result PostOldRegion = %q, want 0", res.PostOldRegion)
	}
	d.onResult(res)
	env := <-mp.commitCh
	if env.Mutation.Kind != gtevents.PrevInsideDelete || env.Mutation.OldRegion != "0" {
		t.Fatalf("remove mutation = %+v, want delete on region 0", env.Mutation)
	}

	d.onCommitResult(gtevents.CommitResult{ObjectID: env.ObjectID, SourceSeq: env.SourceSeq})
	rs := e.regions["0"]
	rs.mu.RLock()
	_, ok := rs.prevInside["r1"]
	rs.mu.RUnlock()
	if ok {
		t.Fatal("remove commit should delete prevInside for object")
	}
}

func TestRemoveMissingObjectCommitsIdempotentRemovedEvent(t *testing.T) {
	e := NewEngine(newTestFlatResolver(1))
	msgCh := make(chan jetstream.Msg, 16)
	d, mp := newTestDispatcher(e, msgCh, 1)

	rm := makeRemoveMsg("missing")
	rm.seq = 9
	d.onMsg(rm)

	env := <-mp.commitCh
	if env.ObjectID != "missing" || env.SourceSeq != 9 {
		t.Fatalf("unexpected remove envelope object=%s seq=%d", env.ObjectID, env.SourceSeq)
	}
	if rm.acked.Load() {
		t.Fatal("missing remove should not ack before commit")
	}

	var evt gtevents.ObjectRemovedEvent
	if err := json.Unmarshal(env.Messages[0].Data, &evt); err != nil {
		t.Fatalf("unmarshal removed event: %v", err)
	}
	if evt.SourceSeq != 9 || evt.ObjectID != "missing" {
		t.Fatalf("unexpected removed event object=%s seq=%d", evt.ObjectID, evt.SourceSeq)
	}

	d.onCommitResult(gtevents.CommitResult{ObjectID: env.ObjectID, SourceSeq: env.SourceSeq})
	if !rm.acked.Load() {
		t.Fatal("missing remove should ack after commit")
	}
	if e.ObjectCount() != 0 {
		t.Fatalf("missing remove tombstone ctl should be deleted, count=%d", e.ObjectCount())
	}
}

func TestBootstrapUnknownRegion(t *testing.T) {
	e := NewEngine(newTestFlatResolver(1))
	missing := "missing"
	e.BootstrapFromState(&gtevents.ObjectStateRecord{
		ObjectID:         "r1",
		Lifecycle:        gtevents.LifecycleActive,
		DetectorStateSeq: 7,
		Region:           &missing,
		Position:         &gtevents.EventPosition{X: 1, Y: 1, Z: 1},
		Dims:             gtevents.EventDims{Width: 0.5, Height: 0.5},
		InsideAreaIDs:    []string{"zone-a"},
	})

	if e.ObjectCount() != 1 {
		t.Fatalf("object should remain registered in ctls, count=%d", e.ObjectCount())
	}
	if _, err := e.RegionOf("r1"); err == nil {
		t.Fatal("object with unknown bootstrap region should not be query-visible")
	}
	if len(e.AllObjects(nil).Regions["0"]) != 0 {
		t.Fatal("object with unknown bootstrap region should not be placed in R-tree")
	}
}

func TestFlatResolverRegion(t *testing.T) {
	resolver := newTestFlatResolver(2)
	region, err := resolver.Resolve(0, 0, 1.0, regionresolver.NoPrevRegion)
	if err != nil || region != "0" {
		t.Fatalf("z=1.0 should resolve to region 0, got %q err=%v", region, err)
	}
	region, err = resolver.Resolve(0, 0, 5.5, regionresolver.NoPrevRegion)
	if err != nil || region != "1" {
		t.Fatalf("z=5.5 should resolve to region 1, got %q err=%v", region, err)
	}
	region, err = resolver.Resolve(0, 0, -1, regionresolver.NoPrevRegion)
	if err != nil || region != "0" {
		t.Fatalf("z=-1 should resolve to region 0 (clamped), got %q err=%v", region, err)
	}
}

func TestCommitFailureResubmit(t *testing.T) {
	e := NewEngine(newTestFlatResolver(1))
	e.RegisterArea("zone-a", "0", squarePoints)
	e.RegisterObject("r1", domain.ObjectDimensions{Width: 0.5, Height: 0.5})

	doneCh := make(chan WorkerResult, 64)
	w0 := NewRegionWorker("0", e, doneCh)
	go w0.Run()

	msgCh := make(chan jetstream.Msg, 16)
	d, mp := newTestDispatcher(e, msgCh, 1)
	d.workers = map[string]*RegionWorker{"0": w0}
	d.doneCh = doneCh

	pos1 := makePosMsg("r1", 5, 5, 1.0, 0)
	pos1.seq = 1
	d.onMsg(pos1)

	// Worker completes.
	res := <-doneCh
	d.onResult(res)

	// First commit - simulate failure (the error path in onCommitResult).
	env := <-mp.commitCh
	d.onCommitResult(gtevents.CommitResult{
		ObjectID:  env.ObjectID,
		SourceSeq: env.SourceSeq,
		Err:       fmt.Errorf("injected failure"),
	})

	// Dispatcher re-submitted the same immutable envelope.
	env2 := <-mp.commitCh
	if env2.ObjectID != env.ObjectID || env2.SourceSeq != env.SourceSeq {
		t.Fatal("resubmitted envelope should match original")
	}
	if env2.Mutation.Kind != gtevents.PrevInsideSet {
		t.Fatalf("expected PrevInsideSet mutation in resubmitted envelope, got %d", env2.Mutation.Kind)
	}

	// Second commit - succeeds. Exercises the success path after resubmit.
	d.onCommitResult(gtevents.CommitResult{
		ObjectID:  env2.ObjectID,
		SourceSeq: env2.SourceSeq,
	})

	// After successful commit, source should be acked.
	if !pos1.acked.Load() {
		t.Fatal("source message should be acked after successful commit")
	}

	// deferred prevInside should be applied post-commit.
	rs := e.regions["0"]
	rs.mu.RLock()
	pi := rs.prevInside["r1"]
	rs.mu.RUnlock()
	if pi == nil || !pi["zone-a"] {
		t.Fatal("prevInside should be set post-commit after successful retry")
	}

	// ctls should be intact (not removed - this wasn't a removal commit).
	_, ok := e.lookupCtl("r1")
	if !ok {
		t.Fatal("r1 should still be in ctls after successful retry")
	}
}

func TestNumRegions(t *testing.T) {
	e := NewEngine(newTestFlatResolver(2))
	if e.NumRegions() != 2 {
		t.Fatalf("expected 2 regions, got %d", e.NumRegions())
	}
}

func TestRegionOf(t *testing.T) {
	e := NewEngine(newTestFlatResolver(2))
	placeObject(e, "r1", 5, 5, 1.0, domain.ObjectDimensions{Width: 0.6, Height: 0.8})

	region, err := e.RegionOf("r1")
	if err != nil {
		t.Fatalf("RegionOf error: %v", err)
	}
	if region != "0" {
		t.Fatalf("expected region 0, got %s", region)
	}
}

func TestRegionFromPoint(t *testing.T) {
	e := NewEngine(newTestFlatResolver(2))

	region, err := e.RegionFromPoint(5, 5, 5.5)
	if err != nil {
		t.Fatalf("RegionFromPoint error: %v", err)
	}
	if region != "1" {
		t.Fatalf("expected region 1, got %s", region)
	}
}

func TestRegionFromPointUnknownRegion(t *testing.T) {
	e := NewEngine(scriptedResolver{
		known: []string{"0"},
		resolve: func(_, _, _ float64, prevRegion *string) (string, error) {
			if prevRegion != nil {
				t.Fatal("RegionFromPoint should resolve without previous region")
			}
			return "missing", nil
		},
	})

	if _, err := e.RegionFromPoint(5, 5, 1); err == nil {
		t.Fatal("expected error for unknown resolved region")
	}
}
