package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/midtxwn/geotruth/embedded"
	"github.com/midtxwn/geotruth/pkg/domain"
	pkggeotruth "github.com/midtxwn/geotruth/pkg/geotruth"
	"github.com/midtxwn/geotruth/pkg/messages"
	"github.com/midtxwn/geotruth/pkg/natskeys"
	"github.com/midtxwn/geotruth/pkg/natspublish"
	"github.com/midtxwn/geotruth/pkg/natsquery"
	"github.com/midtxwn/geotruth/pkg/natswatch"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func startServices(tb testing.TB, numFloors int) (*embedded.Services, natspublish.Publish, natsquery.Query) {
	tb.Helper()
	cfg := embedded.DefaultConfig
	deps := embedded.DefaultDependencies
	deps.Resolver = embedded.NewFlatResolver(numFloors)
	svc, err := embedded.Run(context.Background(), cfg, deps)
	if err != nil {
		tb.Fatalf("start services: %v", err)
	}
	tb.Cleanup(func() { svc.Shutdown() })
	nc := svc.NATSConn()
	return svc, natspublish.New(nc), natsquery.New(nc)
}

func startGeoTruthOnNATS(tb testing.TB, parent context.Context, url string, numFloors int) (embedded.Service, func()) {
	tb.Helper()
	ctx, cancel := context.WithCancel(parent)
	svc, err := embedded.RunGeoTruth(ctx, embedded.DefaultConfig.GeoTruth, pkggeotruth.Dependencies{
		NATS: func(role string) (*nats.Conn, error) {
			return nats.Connect(url,
				nats.Name("geotruth-restart-test-"+role),
				nats.RetryOnFailedConnect(true),
				nats.MaxReconnects(-1),
				nats.ReconnectWait(2*time.Second),
			)
		},
		Resolver: embedded.NewFlatResolver(numFloors),
	})
	if err != nil {
		cancel()
		tb.Fatalf("start geotruth: %v", err)
	}
	stop := func() {
		cancel()
		select {
		case <-svc.Done():
		case <-time.After(5 * time.Second):
			tb.Fatalf("geotruth did not stop within timeout")
		}
	}
	return svc, stop
}

func connectTestNATS(tb testing.TB, url string) *nats.Conn {
	tb.Helper()
	nc, err := nats.Connect(url,
		nats.Name("geotruth-integration-test-client"),
		nats.RetryOnFailedConnect(true),
		nats.MaxReconnects(-1),
		nats.ReconnectWait(2*time.Second),
	)
	if err != nil {
		tb.Fatalf("connect nats: %v", err)
	}
	tb.Cleanup(func() { nc.Close() })
	return nc
}

func startPersistentNATS(tb testing.TB) (*embedded.NATSServer, context.Context, context.CancelFunc) {
	tb.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	svc, err := embedded.RunNATSServer(ctx, embedded.DefaultConfig.NATS)
	if err != nil {
		cancel()
		tb.Fatalf("start embedded nats: %v", err)
	}
	tb.Cleanup(func() {
		cancel()
		svc.Shutdown()
	})
	return svc, ctx, cancel
}

func gtEventStream(tb testing.TB, ctx context.Context, nc *nats.Conn) jetstream.Stream {
	tb.Helper()
	js, err := jetstream.New(nc)
	if err != nil {
		tb.Fatalf("jetstream: %v", err)
	}
	stream, err := js.Stream(ctx, natskeys.GTStreamName)
	if err != nil {
		tb.Fatalf("stream %s: %v", natskeys.GTStreamName, err)
	}
	return stream
}

func latestGTEvent(tb testing.TB, ctx context.Context, stream jetstream.Stream, subject string) *jetstream.RawStreamMsg {
	tb.Helper()
	msg, err := stream.GetLastMsgForSubject(ctx, subject)
	if err != nil {
		tb.Fatalf("latest %s: %v", subject, err)
	}
	return msg
}

func eventIDFromGTEvent(tb testing.TB, data []byte) string {
	tb.Helper()
	var evt struct {
		EventID string `json:"event_id"`
	}
	if err := json.Unmarshal(data, &evt); err != nil {
		tb.Fatalf("decode gt event: %v", err)
	}
	if evt.EventID == "" {
		tb.Fatalf("gt event has empty event_id: %s", string(data))
	}
	return evt.EventID
}

func registerTestArea(tb testing.TB, ctx context.Context, pub natspublish.Publish, query natsquery.Query, areaID string) {
	tb.Helper()
	points := []domain.Point{{X: 0, Y: 0}, {X: 10, Y: 0}, {X: 10, Y: 10}, {X: 0, Y: 10}}
	if err := pub.RegisterArea(ctx, areaID, "0", points); err != nil {
		tb.Fatalf("RegisterArea: %v", err)
	}
	assertEventually(tb, func() bool {
		_, err := query.Area(ctx, areaID)
		return err == nil
	}, 5*time.Second)
}

func TestEndToEnd_ObjectWritesReturnProcessedCommitAck(t *testing.T) {
	_, pub, _ := startServices(t, 1)
	ctx := context.Background()

	reg, err := pub.RegisterObject(ctx, "ack-obj", domain.ObjectDimensions{Width: 1, Height: 1})
	if err != nil {
		t.Fatalf("RegisterObject: %v", err)
	}
	if reg.InstanceID == "" || reg.CommitSeq != 1 {
		t.Fatalf("register ack = %+v, want instance and commit_seq 1", reg)
	}

	pos, err := pub.UpdateObjectPosition(ctx, "ack-obj", 1, 2, 1, 0)
	if err != nil {
		t.Fatalf("UpdateObjectPosition: %v", err)
	}
	if pos.InstanceID != reg.InstanceID || pos.CommitSeq != 2 {
		t.Fatalf("position ack = %+v, want instance %q commit_seq 2", pos, reg.InstanceID)
	}

	rem, err := pub.RemoveObject(ctx, "ack-obj")
	if err != nil {
		t.Fatalf("RemoveObject: %v", err)
	}
	if rem.InstanceID != reg.InstanceID || rem.CommitSeq != 3 {
		t.Fatalf("remove ack = %+v, want instance %q commit_seq 3", rem, reg.InstanceID)
	}
}

func TestEndToEnd_ObjectWriteRejectsSubjectBodyIDMismatch(t *testing.T) {
	svc, _, _ := startServices(t, 1)
	ctx := context.Background()

	data, err := json.Marshal(natspublish.UpdatePositionReq{ID: "body-obj", X: 1, Y: 2, Z: 1})
	if err != nil {
		t.Fatalf("marshal request: %v", err)
	}
	resp, err := svc.NATSConn().RequestWithContext(ctx, natspublish.GeoTruthUpdatePositionSubject("subject-obj"), data)
	if err != nil {
		t.Fatalf("request mismatch: %v", err)
	}
	if err := messages.Err(resp.Data); err == nil {
		t.Fatalf("expected subject/body mismatch error, got %s", string(resp.Data))
	}
}

func TestGeoTruthRestartRecoversActiveObjectFromGTEvents(t *testing.T) {
	natsSvc, rootCtx, _ := startPersistentNATS(t)
	_, stop := startGeoTruthOnNATS(t, rootCtx, natsSvc.NATSURL(), 1)

	nc := connectTestNATS(t, natsSvc.NATSURL())
	pub := natspublish.New(nc)
	query := natsquery.New(nc)
	ctx := context.Background()

	if _, err := pub.RegisterObject(ctx, "recover-obj", domain.ObjectDimensions{Width: 2, Height: 3}); err != nil {
		t.Fatalf("RegisterObject: %v", err)
	}
	if _, err := pub.UpdateObjectPosition(ctx, "recover-obj", 5, 6, 1, 0.25); err != nil {
		t.Fatalf("UpdateObjectPosition: %v", err)
	}

	stop()
	_, stop = startGeoTruthOnNATS(t, rootCtx, natsSvc.NATSURL(), 1)
	defer stop()

	assertEventually(t, func() bool {
		obj, err := query.ObjectData(ctx, "recover-obj")
		if err != nil || obj == nil {
			return false
		}
		return obj.ID == "recover-obj" && obj.X == 5 && obj.Y == 6 && obj.Z == 1
	}, 5*time.Second)
}

func TestGeoTruthRestartRepairsMissingGeofenceEnterProjection(t *testing.T) {
	natsSvc, rootCtx, _ := startPersistentNATS(t)
	_, stop := startGeoTruthOnNATS(t, rootCtx, natsSvc.NATSURL(), 1)

	nc := connectTestNATS(t, natsSvc.NATSURL())
	pub := natspublish.New(nc)
	query := natsquery.New(nc)
	ctx := context.Background()

	registerTestArea(t, ctx, pub, query, "repair-enter-zone")
	if _, err := pub.RegisterObject(ctx, "repair-enter-obj", domain.ObjectDimensions{Width: 1, Height: 1}); err != nil {
		t.Fatalf("RegisterObject: %v", err)
	}
	if _, err := pub.UpdateObjectPosition(ctx, "repair-enter-obj", 5, 5, 1, 0); err != nil {
		t.Fatalf("UpdateObjectPosition: %v", err)
	}

	stream := gtEventStream(t, ctx, nc)
	subject := natswatch.GTSubjectGeofenceEntered("repair-enter-obj", "repair-enter-zone")
	original := latestGTEvent(t, ctx, stream, subject)
	expectedID := eventIDFromGTEvent(t, original.Data)
	if err := stream.DeleteMsg(ctx, original.Sequence); err != nil {
		t.Fatalf("delete projection: %v", err)
	}

	stop()
	_, stop = startGeoTruthOnNATS(t, rootCtx, natsSvc.NATSURL(), 1)
	defer stop()

	assertEventually(t, func() bool {
		repaired, err := stream.GetLastMsgForSubject(ctx, subject)
		if err != nil {
			return false
		}
		return repaired.Sequence != original.Sequence && eventIDFromGTEvent(t, repaired.Data) == expectedID
	}, 5*time.Second)
}

func TestGeoTruthRestartRepairsMissingRemoveExitProjection(t *testing.T) {
	natsSvc, rootCtx, _ := startPersistentNATS(t)
	_, stop := startGeoTruthOnNATS(t, rootCtx, natsSvc.NATSURL(), 1)

	nc := connectTestNATS(t, natsSvc.NATSURL())
	pub := natspublish.New(nc)
	query := natsquery.New(nc)
	ctx := context.Background()

	registerTestArea(t, ctx, pub, query, "repair-exit-zone")
	if _, err := pub.RegisterObject(ctx, "repair-exit-obj", domain.ObjectDimensions{Width: 1, Height: 1}); err != nil {
		t.Fatalf("RegisterObject: %v", err)
	}
	if _, err := pub.UpdateObjectPosition(ctx, "repair-exit-obj", 5, 5, 1, 0); err != nil {
		t.Fatalf("UpdateObjectPosition: %v", err)
	}
	if _, err := pub.RemoveObject(ctx, "repair-exit-obj"); err != nil {
		t.Fatalf("RemoveObject: %v", err)
	}

	stream := gtEventStream(t, ctx, nc)
	subject := natswatch.GTSubjectGeofenceExited("repair-exit-obj", "repair-exit-zone")
	original := latestGTEvent(t, ctx, stream, subject)
	expectedID := eventIDFromGTEvent(t, original.Data)
	if err := stream.DeleteMsg(ctx, original.Sequence); err != nil {
		t.Fatalf("delete projection: %v", err)
	}

	stop()
	_, stop = startGeoTruthOnNATS(t, rootCtx, natsSvc.NATSURL(), 1)
	defer stop()

	assertEventually(t, func() bool {
		repaired, err := stream.GetLastMsgForSubject(ctx, subject)
		if err != nil {
			return false
		}
		return repaired.Sequence != original.Sequence && eventIDFromGTEvent(t, repaired.Data) == expectedID
	}, 5*time.Second)
}

func TestEndToEnd_ObjectLifecycle(t *testing.T) {
	_, pub, query := startServices(t, 2)

	ctx := context.Background()

	dims := domain.ObjectDimensions{Width: 2.0, Height: 3.0}
	if _, err := pub.RegisterObject(ctx, "obj1", dims); err != nil {
		t.Fatalf("RegisterObject failed: %v", err)
	}

	if _, err := pub.UpdateObjectPosition(ctx, "obj1", 5.0, 5.0, 1.0, 0.0); err != nil {
		t.Fatalf("UpdateObjectPosition failed: %v", err)
	}

	assertEventually(t, func() bool {
		nearby, err := query.NearbyObjects(ctx, "0", 5.0, 5.0, 10.0, nil)
		if err != nil {
			return false
		}
		for _, obj := range nearby {
			if obj.ID == "obj1" && obj.X == 5.0 && obj.Y == 5.0 {
				return true
			}
		}
		return false
	}, 5*time.Second)

	bounds, err := query.ObjectBounds(ctx, "obj1")
	if err != nil {
		t.Fatalf("ObjectBounds failed: %v", err)
	}
	if bounds == nil {
		t.Fatal("expected non-nil bounds")
	}
	if bounds.TL.X == bounds.BR.X || bounds.TL.Y == bounds.BR.Y {
		t.Error("bounds appear to be degenerate")
	}

	if _, err := pub.RemoveObject(ctx, "obj1"); err != nil {
		t.Fatalf("RemoveObject failed: %v", err)
	}

	assertEventually(t, func() bool {
		nearby, err := query.NearbyObjects(ctx, "0", 5.0, 5.0, 10.0, nil)
		if err != nil {
			return false
		}
		for _, obj := range nearby {
			if obj.ID == "obj1" {
				return false
			}
		}
		return true
	}, 5*time.Second)
}

func TestEndToEnd_QueryInvalidRegex(t *testing.T) {
	_, _, query := startServices(t, 1)

	ctx := context.Background()
	pattern := "["
	_, err := query.NearbyObjects(ctx, "0", 0, 0, 10.0, &pattern)
	if err == nil {
		t.Fatal("expected invalid regex error")
	}
}

func TestEndToEnd_AreaLifecycle(t *testing.T) {
	_, pub, query := startServices(t, 2)

	ctx := context.Background()

	points := []domain.Point{{X: 0, Y: 0}, {X: 10, Y: 0}, {X: 10, Y: 10}, {X: 0, Y: 10}}
	if err := pub.RegisterArea(ctx, "zone-a", "0", points); err != nil {
		t.Fatalf("RegisterArea failed: %v", err)
	}

	assertEventually(t, func() bool {
		_, err := query.Area(ctx, "zone-a")
		return err == nil
	}, 5*time.Second)

	dims := domain.ObjectDimensions{Width: 1.0, Height: 1.0}
	if _, err := pub.RegisterObject(ctx, "obj1", dims); err != nil {
		t.Fatalf("RegisterObject failed: %v", err)
	}
	if _, err := pub.UpdateObjectPosition(ctx, "obj1", 5.0, 5.0, 1.0, 0.0); err != nil {
		t.Fatalf("UpdateObjectPosition failed: %v", err)
	}

	assertEventually(t, func() bool {
		areas, err := query.AreasAtPoint(ctx, "0", 5.0, 5.0, nil)
		if err != nil {
			return false
		}
		for _, area := range areas {
			if area.ID == "zone-a" {
				return true
			}
		}
		return false
	}, 5*time.Second)

	assertEventually(t, func() bool {
		objs, err := query.ObjectsWithinArea(ctx, "0", "zone-a", nil)
		if err != nil {
			return false
		}
		for _, obj := range objs {
			if obj.ID == "obj1" {
				return true
			}
		}
		return false
	}, 5*time.Second)

	if err := pub.RemoveArea(ctx, "zone-a"); err != nil {
		t.Fatalf("RemoveArea failed: %v", err)
	}

	assertEventually(t, func() bool {
		_, err := query.Area(ctx, "zone-a")
		return err != nil
	}, 5*time.Second)
}

func TestEndToEnd_NearbyQueries(t *testing.T) {
	_, pub, query := startServices(t, 2)

	ctx := context.Background()

	positions := []struct {
		id string
		x  float64
		y  float64
	}{
		{"obj1", 0, 0},
		{"obj2", 5, 5},
		{"obj3", 10, 10},
		{"obj4", 100, 100},
	}

	for _, pos := range positions {
		dims := domain.ObjectDimensions{Width: 1.0, Height: 1.0}
		_, err := pub.RegisterObject(ctx, pos.id, dims)
		if err != nil {
			t.Fatalf("RegisterObject %s failed: %v", pos.id, err)
		}
		_, err = pub.UpdateObjectPosition(ctx, pos.id, pos.x, pos.y, 1.0, 0.0)
		if err != nil {
			t.Fatalf("UpdateObjectPosition %s failed: %v", pos.id, err)
		}
	}

	assertEventually(t, func() bool {
		nearby, err := query.NearbyObjects(ctx, "0", 0, 0, 8.0, nil)
		if err != nil {
			return false
		}
		found := make(map[string]bool)
		for _, obj := range nearby {
			found[obj.ID] = true
		}
		return found["obj1"] && found["obj2"] && !found["obj3"] && !found["obj4"]
	}, 5*time.Second)

	nearbyOf, err := query.NearbyObjectsOf(ctx, "obj1", 8.0, nil)
	if err != nil {
		t.Fatalf("NearbyObjectsOf failed: %v", err)
	}
	found := make(map[string]bool)
	for _, obj := range nearbyOf {
		found[obj.ID] = true
	}
	if !found["obj2"] {
		t.Errorf("obj2 should be nearby obj1 within radius 8")
	}
}

func TestEndToEnd_IntersectionDetection(t *testing.T) {
	_, pub, query := startServices(t, 2)

	ctx := context.Background()

	dims := domain.ObjectDimensions{Width: 2.0, Height: 2.0}

	if _, err := pub.RegisterObject(ctx, "obj1", dims); err != nil {
		t.Fatalf("RegisterObject obj1 failed: %v", err)
	}
	if _, err := pub.UpdateObjectPosition(ctx, "obj1", 0, 0, 1.0, 0.0); err != nil {
		t.Fatalf("UpdateObjectPosition obj1 failed: %v", err)
	}

	if _, err := pub.RegisterObject(ctx, "obj2", dims); err != nil {
		t.Fatalf("RegisterObject obj2 failed: %v", err)
	}
	if _, err := pub.UpdateObjectPosition(ctx, "obj2", 1, 1, 1.0, 0.0); err != nil {
		t.Fatalf("UpdateObjectPosition obj2 failed: %v", err)
	}

	assertEventually(t, func() bool {
		intersecting, err := query.IntersectingObjects(ctx, "obj1", nil)
		if err != nil {
			return false
		}
		for _, obj := range intersecting {
			if obj.ID == "obj2" {
				return true
			}
		}
		return false
	}, 5*time.Second)

	if _, err := pub.UpdateObjectPosition(ctx, "obj2", 10, 10, 1.0, 0.0); err != nil {
		t.Fatalf("UpdateObjectPosition obj2 failed: %v", err)
	}

	assertEventually(t, func() bool {
		intersecting, err := query.IntersectingObjects(ctx, "obj1", nil)
		if err != nil {
			return false
		}
		for _, obj := range intersecting {
			if obj.ID == "obj2" {
				return false
			}
		}
		return true
	}, 5*time.Second)
}

func TestEndToEnd_MultiFloor(t *testing.T) {
	_, pub, query := startServices(t, 2)

	ctx := context.Background()

	dims := domain.ObjectDimensions{Width: 1.0, Height: 1.0}
	if _, err := pub.RegisterObject(ctx, "obj1", dims); err != nil {
		t.Fatalf("RegisterObject failed: %v", err)
	}

	if _, err := pub.UpdateObjectPosition(ctx, "obj1", 5.0, 5.0, 1.0, 0.0); err != nil {
		t.Fatalf("UpdateObjectPosition failed: %v", err)
	}

	assertEventually(t, func() bool {
		nearby, err := query.NearbyObjects(ctx, "0", 5.0, 5.0, 10.0, nil)
		if err != nil {
			return false
		}
		for _, obj := range nearby {
			if obj.ID == "obj1" {
				return true
			}
		}
		return false
	}, 5*time.Second)

	nearby, err := query.NearbyObjects(ctx, "1", 5.0, 5.0, 10.0, nil)
	if err != nil {
		t.Fatalf("NearbyObjects on floor 1 failed: %v", err)
	}
	for _, obj := range nearby {
		if obj.ID == "obj1" {
			t.Error("obj1 should not be on floor 1")
		}
	}
}

func TestEndToEnd_AllQueries(t *testing.T) {
	_, pub, query := startServices(t, 2)

	ctx := context.Background()

	area1Pts := []domain.Point{{X: 0, Y: 0}, {X: 10, Y: 0}, {X: 10, Y: 10}, {X: 0, Y: 10}}
	area2Pts := []domain.Point{{X: 20, Y: 20}, {X: 30, Y: 20}, {X: 30, Y: 30}, {X: 20, Y: 30}}

	if err := pub.RegisterArea(ctx, "zone-a", "0", area1Pts); err != nil {
		t.Fatalf("RegisterArea zone-a failed: %v", err)
	}
	if err := pub.RegisterArea(ctx, "zone-b", "0", area2Pts); err != nil {
		t.Fatalf("RegisterArea zone-b failed: %v", err)
	}

	assertEventually(t, func() bool {
		areaA, err := query.Area(ctx, "zone-a")
		if err != nil {
			return false
		}
		if areaA == nil || areaA.ID != "zone-a" {
			return false
		}
		areaB, err := query.Area(ctx, "zone-b")
		if err != nil {
			return false
		}
		return areaB != nil && areaB.ID == "zone-b"
	}, 5*time.Second)

	for i := 1; i <= 3; i++ {
		dims := domain.ObjectDimensions{Width: 1.0, Height: 1.0}
		id := fmt.Sprintf("obj%d", i)
		if _, err := pub.RegisterObject(ctx, id, dims); err != nil {
			t.Fatalf("RegisterObject %s failed: %v", id, err)
		}
		if _, err := pub.UpdateObjectPosition(ctx, id, float64(i*5), float64(i*5), 1.0, 0.0); err != nil {
			t.Fatalf("UpdateObjectPosition %s failed: %v", id, err)
		}
	}

	assertEventually(t, func() bool {
		all, err := query.NearbyObjects(ctx, "0", 0, 0, 1000.0, nil)
		if err != nil {
			return false
		}
		return len(all) >= 3
	}, 5*time.Second)
}

func TestEndToEnd_RegionOf(t *testing.T) {
	_, pub, query := startServices(t, 2)

	ctx := context.Background()

	dims := domain.ObjectDimensions{Width: 1.0, Height: 1.0}
	if _, err := pub.RegisterObject(ctx, "obj1", dims); err != nil {
		t.Fatalf("RegisterObject failed: %v", err)
	}
	if _, err := pub.UpdateObjectPosition(ctx, "obj1", 5.0, 5.0, 1.0, 0.0); err != nil {
		t.Fatalf("UpdateObjectPosition failed: %v", err)
	}

	assertEventually(t, func() bool {
		region, err := query.RegionOf(ctx, "obj1")
		if err != nil {
			return false
		}
		return region == "0"
	}, 5*time.Second)
}

func TestEndToEnd_RegionFromPoint(t *testing.T) {
	_, _, query := startServices(t, 2)

	ctx := context.Background()

	region, err := query.RegionFromPoint(ctx, 5.0, 5.0, 1.0)
	if err != nil {
		t.Fatalf("RegionFromPoint z=1.0 failed: %v", err)
	}
	if region != "0" {
		t.Fatalf("expected region 0, got %s", region)
	}

	region, err = query.RegionFromPoint(ctx, 5.0, 5.0, 5.5)
	if err != nil {
		t.Fatalf("RegionFromPoint z=5.5 failed: %v", err)
	}
	if region != "1" {
		t.Fatalf("expected region 1, got %s", region)
	}
}

func TestEndToEnd_AllObjectsOriented(t *testing.T) {
	_, pub, query := startServices(t, 2)

	ctx := context.Background()

	dims := domain.ObjectDimensions{Width: 1.0, Height: 1.0}
	if _, err := pub.RegisterObject(ctx, "obj1", dims); err != nil {
		t.Fatalf("RegisterObject failed: %v", err)
	}
	if _, err := pub.UpdateObjectPosition(ctx, "obj1", 5.0, 5.0, 1.0, 0.0); err != nil {
		t.Fatalf("UpdateObjectPosition failed: %v", err)
	}

	assertEventually(t, func() bool {
		resp, err := query.AllObjectsOriented(ctx, nil)
		if err != nil {
			return false
		}
		if len(resp.Regions["0"]) < 1 {
			return false
		}
		for _, obj := range resp.Regions["0"] {
			if obj.ID == "obj1" && obj.Position.X == 5.0 && obj.Position.Y == 5.0 && obj.Position.Z == 1.0 {
				return true
			}
		}
		return false
	}, 5*time.Second)
}

func TestEndToEnd_AllObjects(t *testing.T) {
	_, pub, query := startServices(t, 2)

	ctx := context.Background()

	dims := domain.ObjectDimensions{Width: 1.0, Height: 1.0}
	if _, err := pub.RegisterObject(ctx, "obj1", dims); err != nil {
		t.Fatalf("RegisterObject failed: %v", err)
	}
	if _, err := pub.UpdateObjectPosition(ctx, "obj1", 5.0, 5.0, 1.0, 0.0); err != nil {
		t.Fatalf("UpdateObjectPosition failed: %v", err)
	}

	assertEventually(t, func() bool {
		resp, err := query.AllObjects(ctx, nil)
		if err != nil {
			return false
		}
		if len(resp.Regions["0"]) < 1 {
			return false
		}
		for _, obj := range resp.Regions["0"] {
			if obj.ID == "obj1" && obj.X == 5.0 && obj.Y == 5.0 && obj.Z == 1.0 {
				return true
			}
		}
		return false
	}, 5*time.Second)
}
