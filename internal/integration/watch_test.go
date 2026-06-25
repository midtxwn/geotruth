package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/midtxwn/geotruth/pkg/domain"
	"github.com/midtxwn/geotruth/pkg/messages"
	"github.com/midtxwn/geotruth/pkg/natsconsumer"
	"github.com/midtxwn/geotruth/pkg/natskeys"
	"github.com/midtxwn/geotruth/pkg/natspublish"
	"github.com/midtxwn/geotruth/pkg/natswatch"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func createWatchConsumer(t *testing.T, ctx context.Context, js jetstream.JetStream, name string) (*natsconsumer.Consumer, func()) {
	t.Helper()
	cfg := natsconsumer.Config{
		Name:              name,
		DeliverPolicy:     jetstream.DeliverLastPerSubjectPolicy,
		AckWait:           60 * time.Second,
		MaxDeliver:        -1,
		InactiveThreshold: 30 * time.Second,
	}
	cons, err := natsconsumer.New(js, natskeys.GTStreamName, cfg)
	if err != nil {
		t.Fatalf("create consumer %s: %v", name, err)
	}
	cleanup := func() {
		cons.Close()
	}
	return cons, cleanup
}

func requestObjectCommand(t *testing.T, ctx context.Context, nc requestConn, subject string, req interface{}) {
	t.Helper()
	data, err := json.Marshal(req)
	if err != nil {
		t.Fatalf("marshal request: %v", err)
	}
	resp, err := nc.RequestWithContext(ctx, subject, data)
	if err != nil {
		t.Fatalf("request %s: %v", subject, err)
	}
	if err := messages.Err(resp.Data); err != nil {
		t.Fatalf("response %s: %v", subject, err)
	}
}

type requestConn interface {
	RequestWithContext(context.Context, string, []byte) (*nats.Msg, error)
}

func TestWatch_ObjectRegistered(t *testing.T) {
	svc, _, _ := startServices(t, 2)
	nc := svc.NATSConn()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream: %v", err)
	}

	cons, cleanup := createWatchConsumer(t, ctx, js, fmt.Sprintf("test_reg_%d", time.Now().UnixNano()))
	defer cleanup()

	events, unsub, err := natswatch.AllObjectRegistered(ctx, cons)
	if err != nil {
		t.Fatalf("AllObjectRegistered: %v", err)
	}
	defer unsub()

	pub := natspublish.New(nc)
	dims := domain.ObjectDimensions{Width: 1.0, Height: 1.0}
	if _, err := pub.RegisterObject(ctx, "watch-obj1", dims); err != nil {
		t.Fatalf("RegisterObject: %v", err)
	}

	select {
	case evt := <-events:
		if evt.ObjectID != "watch-obj1" {
			t.Errorf("expected ObjectID watch-obj1, got %s", evt.ObjectID)
		}
		if evt.EventType != natswatch.EventTypeObjectRegistered {
			t.Errorf("expected EventType %s, got %s", natswatch.EventTypeObjectRegistered, evt.EventType)
		}
		if evt.Dims.Width != 1.0 || evt.Dims.Height != 1.0 {
			t.Errorf("dims: got w=%f h=%f", evt.Dims.Width, evt.Dims.Height)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for ObjectRegistered event")
	}
}

func TestWatch_PositionUpdates(t *testing.T) {
	svc, _, _ := startServices(t, 2)
	nc := svc.NATSConn()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream: %v", err)
	}

	cons, cleanup := createWatchConsumer(t, ctx, js, fmt.Sprintf("test_pos_%d", time.Now().UnixNano()))
	defer cleanup()

	events, unsub, err := natswatch.PositionUpdates(ctx, cons, "watch-pos-obj1")
	if err != nil {
		t.Fatalf("PositionUpdates: %v", err)
	}
	defer unsub()

	pub := natspublish.New(nc)
	dims := domain.ObjectDimensions{Width: 2.0, Height: 3.0}
	if _, err := pub.RegisterObject(ctx, "watch-pos-obj1", dims); err != nil {
		t.Fatalf("RegisterObject: %v", err)
	}

	if _, err := pub.UpdateObjectPosition(ctx, "watch-pos-obj1", 5.0, 5.0, 1.0, 0.0); err != nil {
		t.Fatalf("UpdateObjectPosition: %v", err)
	}

	select {
	case evt := <-events:
		if evt.ObjectID != "watch-pos-obj1" {
			t.Errorf("expected ObjectID watch-pos-obj1, got %s", evt.ObjectID)
		}
		if evt.EventType != natswatch.EventTypePositionUpdated {
			t.Errorf("expected EventType %s, got %s", natswatch.EventTypePositionUpdated, evt.EventType)
		}
		if evt.Position.X != 5.0 || evt.Position.Y != 5.0 {
			t.Errorf("position: got x=%f y=%f", evt.Position.X, evt.Position.Y)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for PositionUpdated event")
	}
}

func TestWatch_GeofenceTransitions(t *testing.T) {
	svc, _, query := startServices(t, 2)
	nc := svc.NATSConn()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream: %v", err)
	}

	cons, cleanup := createWatchConsumer(t, ctx, js, fmt.Sprintf("test_gf_%d", time.Now().UnixNano()))
	defer cleanup()

	events, unsub, err := natswatch.GeofenceTransitions(ctx, cons, "gf-obj1")
	if err != nil {
		t.Fatalf("GeofenceTransitions: %v", err)
	}
	defer unsub()

	pub := natspublish.New(nc)

	points := []domain.Point{{X: 0, Y: 0}, {X: 10, Y: 0}, {X: 10, Y: 10}, {X: 0, Y: 10}}
	if err := pub.RegisterArea(ctx, "gf-zone", "0", points); err != nil {
		t.Fatalf("RegisterArea: %v", err)
	}

	assertEventually(t, func() bool {
		_, err := query.Area(ctx, "gf-zone")
		return err == nil
	}, 5*time.Second)

	dims := domain.ObjectDimensions{Width: 1.0, Height: 1.0}
	if _, err := pub.RegisterObject(ctx, "gf-obj1", dims); err != nil {
		t.Fatalf("RegisterObject: %v", err)
	}

	if _, err := pub.UpdateObjectPosition(ctx, "gf-obj1", 5.0, 5.0, 1.0, 0.0); err != nil {
		t.Fatalf("UpdateObjectPosition: %v", err)
	}

	select {
	case evt := <-events:
		if evt.ObjectID != "gf-obj1" {
			t.Errorf("expected ObjectID gf-obj1, got %s", evt.ObjectID)
		}
		if evt.EventType != natswatch.EventTypeGeofenceEntered {
			t.Errorf("expected EventType %s, got %s", natswatch.EventTypeGeofenceEntered, evt.EventType)
		}
		if evt.AreaID != "gf-zone" {
			t.Errorf("expected AreaID gf-zone, got %s", evt.AreaID)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for GeofenceTransition event")
	}
}

func TestWatch_ClientOpIDPropagatesThroughObjectEvents(t *testing.T) {
	svc, pub, query := startServices(t, 2)
	nc := svc.NATSConn()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream: %v", err)
	}

	cons, cleanup := createWatchConsumer(t, ctx, js, fmt.Sprintf("test_client_op_%d", time.Now().UnixNano()))
	defer cleanup()

	events, unsub, err := natswatch.WatchObject(ctx, cons, "client-op-obj1")
	if err != nil {
		t.Fatalf("WatchObject: %v", err)
	}
	defer unsub()

	points := []domain.Point{{X: 0, Y: 0}, {X: 10, Y: 0}, {X: 10, Y: 10}, {X: 0, Y: 10}}
	if err := pub.RegisterArea(ctx, "client-op-zone", "0", points); err != nil {
		t.Fatalf("RegisterArea: %v", err)
	}
	assertEventually(t, func() bool {
		_, err := query.Area(ctx, "client-op-zone")
		return err == nil
	}, 5*time.Second)

	requestObjectCommand(t, ctx, nc, natspublish.GeoTruthRegisterObjectSubject("client-op-obj1"), natspublish.RegisterObjectReq{
		ID:         "client-op-obj1",
		Dims:       domain.ObjectDimensions{Width: 1.0, Height: 1.0},
		ClientOpID: "op-register",
	})
	requestObjectCommand(t, ctx, nc, natspublish.GeoTruthUpdatePositionSubject("client-op-obj1"), natspublish.UpdatePositionReq{
		ID:         "client-op-obj1",
		X:          5.0,
		Y:          5.0,
		Z:          1.0,
		RotY:       0.0,
		ClientOpID: "op-update",
	})
	requestObjectCommand(t, ctx, nc, natspublish.GeoTruthRemoveObjectSubject("client-op-obj1"), natspublish.RemoveObjectReq{
		ID:         "client-op-obj1",
		ClientOpID: "op-remove",
	})

	seen := map[string]bool{}
	deadline := time.After(5 * time.Second)
	for !(seen["registered"] && seen["position"] && seen["geofence"] && seen["removed"]) {
		select {
		case evt := <-events:
			switch {
			case evt.ObjectRegistered != nil:
				if evt.ObjectRegistered.ClientOpID == "op-register" {
					seen["registered"] = true
				}
			case evt.PositionUpdated != nil:
				if evt.PositionUpdated.ClientOpID == "op-update" {
					seen["position"] = true
				}
			case evt.GeofenceEntered != nil:
				if evt.GeofenceEntered.ClientOpID == "op-update" {
					seen["geofence"] = true
				}
			case evt.ObjectRemoved != nil:
				if evt.ObjectRemoved.ClientOpID == "op-remove" {
					seen["removed"] = true
				}
			}
		case <-deadline:
			t.Fatalf("timeout waiting for client_op_id events, seen=%v", seen)
		}
	}
}

func TestWatch_WatchObject(t *testing.T) {
	svc, _, _ := startServices(t, 2)
	nc := svc.NATSConn()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream: %v", err)
	}

	cons, cleanup := createWatchConsumer(t, ctx, js, fmt.Sprintf("test_wo_%d", time.Now().UnixNano()))
	defer cleanup()

	events, unsub, err := natswatch.WatchObject(ctx, cons, "wo-obj1")
	if err != nil {
		t.Fatalf("WatchObject: %v", err)
	}
	defer unsub()

	pub := natspublish.New(nc)
	dims := domain.ObjectDimensions{Width: 1.0, Height: 1.0}
	if _, err := pub.RegisterObject(ctx, "wo-obj1", dims); err != nil {
		t.Fatalf("RegisterObject: %v", err)
	}

	select {
	case evt := <-events:
		if evt.ObjectRegistered == nil {
			t.Fatal("expected ObjectRegistered event, got nil")
		}
		if evt.ObjectRegistered.ObjectID != "wo-obj1" {
			t.Errorf("expected ObjectID wo-obj1, got %s", evt.ObjectRegistered.ObjectID)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for WatchObject registered event")
	}

	if _, err := pub.UpdateObjectPosition(ctx, "wo-obj1", 3.0, 3.0, 1.0, 0.0); err != nil {
		t.Fatalf("UpdateObjectPosition: %v", err)
	}

	select {
	case evt := <-events:
		if evt.PositionUpdated == nil {
			t.Fatal("expected PositionUpdated event, got nil")
		}
		if evt.PositionUpdated.ObjectID != "wo-obj1" {
			t.Errorf("expected ObjectID wo-obj1, got %s", evt.PositionUpdated.ObjectID)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for WatchObject position event")
	}
}

func TestWatch_AllGeofenceTransitions(t *testing.T) {
	svc, _, query := startServices(t, 2)
	nc := svc.NATSConn()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream: %v", err)
	}

	cons, cleanup := createWatchConsumer(t, ctx, js, fmt.Sprintf("test_allgf_%d", time.Now().UnixNano()))
	defer cleanup()

	events, unsub, err := natswatch.AllGeofenceTransitions(ctx, cons)
	if err != nil {
		t.Fatalf("AllGeofenceTransitions: %v", err)
	}
	defer unsub()

	pub := natspublish.New(nc)

	points := []domain.Point{{X: 0, Y: 0}, {X: 10, Y: 0}, {X: 10, Y: 10}, {X: 0, Y: 10}}
	if err := pub.RegisterArea(ctx, "all-gf-zone", "0", points); err != nil {
		t.Fatalf("RegisterArea: %v", err)
	}

	assertEventually(t, func() bool {
		_, err := query.Area(ctx, "all-gf-zone")
		return err == nil
	}, 5*time.Second)

	dims := domain.ObjectDimensions{Width: 1.0, Height: 1.0}
	if _, err := pub.RegisterObject(ctx, "all-gf-obj1", dims); err != nil {
		t.Fatalf("RegisterObject: %v", err)
	}

	if _, err := pub.UpdateObjectPosition(ctx, "all-gf-obj1", 5.0, 5.0, 1.0, 0.0); err != nil {
		t.Fatalf("UpdateObjectPosition: %v", err)
	}

	select {
	case evt := <-events:
		if evt.ObjectID != "all-gf-obj1" {
			t.Errorf("expected ObjectID all-gf-obj1, got %s", evt.ObjectID)
		}
		if evt.EventType != natswatch.EventTypeGeofenceEntered {
			t.Errorf("expected EventType %s, got %s", natswatch.EventTypeGeofenceEntered, evt.EventType)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for AllGeofenceTransitions event")
	}
}

func TestWatch_ObjectRemoved(t *testing.T) {
	svc, _, query := startServices(t, 2)
	nc := svc.NATSConn()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream: %v", err)
	}

	cons, cleanup := createWatchConsumer(t, ctx, js, fmt.Sprintf("test_rm_%d", time.Now().UnixNano()))
	defer cleanup()

	events, unsub, err := natswatch.AllObjectRemoved(ctx, cons)
	if err != nil {
		t.Fatalf("AllObjectRemoved: %v", err)
	}
	defer unsub()

	pub := natspublish.New(nc)

	dims := domain.ObjectDimensions{Width: 1.0, Height: 1.0}
	if _, err := pub.RegisterObject(ctx, "rm-obj1", dims); err != nil {
		t.Fatalf("RegisterObject: %v", err)
	}
	if _, err := pub.UpdateObjectPosition(ctx, "rm-obj1", 5.0, 5.0, 1.0, 0.0); err != nil {
		t.Fatalf("UpdateObjectPosition: %v", err)
	}

	assertEventually(t, func() bool {
		nearby, err := query.NearbyObjects(ctx, "0", 5.0, 5.0, 10.0, nil)
		if err != nil {
			return false
		}
		for _, obj := range nearby {
			if obj.ID == "rm-obj1" {
				return true
			}
		}
		return false
	}, 5*time.Second)

	if _, err := pub.RemoveObject(ctx, "rm-obj1"); err != nil {
		t.Fatalf("RemoveObject: %v", err)
	}

	select {
	case evt := <-events:
		if evt.ObjectID != "rm-obj1" {
			t.Errorf("expected ObjectID rm-obj1, got %s", evt.ObjectID)
		}
		if evt.EventType != natswatch.EventTypeObjectRemoved {
			t.Errorf("expected EventType %s, got %s", natswatch.EventTypeObjectRemoved, evt.EventType)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for ObjectRemoved event")
	}
}
