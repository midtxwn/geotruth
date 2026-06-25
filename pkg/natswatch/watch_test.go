package natswatch

import (
	"context"
	"encoding/json"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/midtxwn/geotruth/pkg/natsconsumer"
	"github.com/midtxwn/geotruth/pkg/natskeys"

	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

var watchTestConsumerSeq uint64

func TestSubjectBuildersAndEventTypeName(t *testing.T) {
	tests := []struct {
		subject string
		want    eventType
	}{
		{GTSubjectPositionUpdated("obj1"), eventTypePositionUpdated},
		{GTSubjectGeofenceEntered("obj1", "zone1"), eventTypeGeofenceEntered},
		{GTSubjectGeofenceExited("obj1", "zone1"), eventTypeGeofenceExited},
		{GTSubjectObjectRegistered("obj1"), eventTypeObjectRegistered},
		{GTSubjectObjectRemoved("obj1"), eventTypeObjectRemoved},
		{GTGeoTruthBooted, eventTypeGeoTruthBooted},
		{"gt.events.v1.object.obj1.unknown", ""},
	}
	for _, tt := range tests {
		if got := eventTypeName(tt.subject); got != tt.want {
			t.Fatalf("eventTypeName(%q) = %q, want %q", tt.subject, got, tt.want)
		}
	}

	if got := GTSubjectObjectEvents("obj1"); got != "gt.events.v1.object.obj1.>" {
		t.Fatalf("GTSubjectObjectEvents = %q", got)
	}
	if got := GTSubjectObjectGeofence("obj1"); got != "gt.events.v1.object.obj1.geofence.>" {
		t.Fatalf("GTSubjectObjectGeofence = %q", got)
	}
	if got := GTSubjectAreaGeofence("zone1"); got != "gt.events.v1.object.*.geofence.zone1.>" {
		t.Fatalf("GTSubjectAreaGeofence = %q", got)
	}
}

func TestObjectSpecificWatchersDecodeEvents(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, js := startWatchTestNATS(t)

	t.Run("position updates", func(t *testing.T) {
		cons := newWatchTestConsumer(t, js)
		ch, unsub, err := PositionUpdates(ctx, cons, "obj1")
		if err != nil {
			t.Fatalf("PositionUpdates: %v", err)
		}
		defer unsub()

		publishWatchEvent(t, ctx, js, GTSubjectGeofenceEntered("obj1", "zone1"), geofenceEvent("obj1", "zone1", EventTypeGeofenceEntered))
		publishWatchEvent(t, ctx, js, GTSubjectPositionUpdated("obj1"), positionEvent("obj1"))

		got := recvWatchEvent(t, ch)
		if got.ObjectID != "obj1" || got.Position.X != 1 {
			t.Fatalf("position event = %+v", got)
		}
	})

	t.Run("geofence transitions", func(t *testing.T) {
		cons := newWatchTestConsumer(t, js)
		ch, unsub, err := GeofenceTransitions(ctx, cons, "obj2")
		if err != nil {
			t.Fatalf("GeofenceTransitions: %v", err)
		}
		defer unsub()

		publishWatchEvent(t, ctx, js, GTSubjectGeofenceExited("obj2", "zone2"), geofenceEvent("obj2", "zone2", EventTypeGeofenceExited))

		got := recvWatchEvent(t, ch)
		if got.ObjectID != "obj2" || got.AreaID != "zone2" || got.EventType != EventTypeGeofenceExited {
			t.Fatalf("geofence event = %+v", got)
		}
	})

	t.Run("registered and removed", func(t *testing.T) {
		consReg := newWatchTestConsumer(t, js)
		regCh, regUnsub, err := ObjectRegistered(ctx, consReg, "obj3")
		if err != nil {
			t.Fatalf("ObjectRegistered: %v", err)
		}
		defer regUnsub()
		publishWatchEvent(t, ctx, js, GTSubjectObjectRegistered("obj3"), registeredEvent("obj3"))
		if got := recvWatchEvent(t, regCh); got.ObjectID != "obj3" || got.EventType != EventTypeObjectRegistered {
			t.Fatalf("registered event = %+v", got)
		}

		consRem := newWatchTestConsumer(t, js)
		remCh, remUnsub, err := ObjectRemoved(ctx, consRem, "obj3")
		if err != nil {
			t.Fatalf("ObjectRemoved: %v", err)
		}
		defer remUnsub()
		publishWatchEvent(t, ctx, js, GTSubjectObjectRemoved("obj3"), removedEvent("obj3"))
		if got := recvWatchEvent(t, remCh); got.ObjectID != "obj3" || got.EventType != EventTypeObjectRemoved {
			t.Fatalf("removed event = %+v", got)
		}
	})
}

func TestWatchObjectRoutesAllObjectEventKinds(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, js := startWatchTestNATS(t)
	cons := newWatchTestConsumer(t, js)

	ch, unsub, err := WatchObject(ctx, cons, "obj1")
	if err != nil {
		t.Fatalf("WatchObject: %v", err)
	}
	defer unsub()

	publishWatchEvent(t, ctx, js, GTSubjectObjectRegistered("obj1"), registeredEvent("obj1"))
	publishWatchEvent(t, ctx, js, GTSubjectPositionUpdated("obj1"), positionEvent("obj1"))
	publishWatchEvent(t, ctx, js, GTSubjectGeofenceEntered("obj1", "zone1"), geofenceEvent("obj1", "zone1", EventTypeGeofenceEntered))
	publishWatchEvent(t, ctx, js, GTSubjectGeofenceExited("obj1", "zone1"), geofenceEvent("obj1", "zone1", EventTypeGeofenceExited))
	publishWatchEvent(t, ctx, js, GTSubjectObjectRemoved("obj1"), removedEvent("obj1"))

	seen := map[string]bool{}
	for i := 0; i < 5; i++ {
		got := recvWatchEvent(t, ch)
		switch {
		case got.ObjectRegistered != nil:
			seen[EventTypeObjectRegistered] = true
		case got.PositionUpdated != nil:
			seen[EventTypePositionUpdated] = true
		case got.GeofenceEntered != nil:
			seen[EventTypeGeofenceEntered] = true
		case got.GeofenceExited != nil:
			seen[EventTypeGeofenceExited] = true
		case got.ObjectRemoved != nil:
			seen[EventTypeObjectRemoved] = true
		default:
			t.Fatalf("empty object event: %+v", got)
		}
	}

	for _, want := range []string{EventTypeObjectRegistered, EventTypePositionUpdated, EventTypeGeofenceEntered, EventTypeGeofenceExited, EventTypeObjectRemoved} {
		if !seen[want] {
			t.Fatalf("missing object event type %q in %v", want, seen)
		}
	}
}

func TestAllAndAreaWatchersDecodeEvents(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, js := startWatchTestNATS(t)

	t.Run("all position updates", func(t *testing.T) {
		cons := newWatchTestConsumer(t, js)
		ch, unsub, err := AllPositionUpdates(ctx, cons)
		if err != nil {
			t.Fatalf("AllPositionUpdates: %v", err)
		}
		defer unsub()
		publishWatchEvent(t, ctx, js, GTSubjectObjectRegistered("obj1"), registeredEvent("obj1"))
		publishWatchEvent(t, ctx, js, GTSubjectPositionUpdated("obj1"), positionEvent("obj1"))
		if got := recvWatchEvent(t, ch); got.EventType != EventTypePositionUpdated {
			t.Fatalf("all position event = %+v", got)
		}
	})

	t.Run("all geofence transitions", func(t *testing.T) {
		cons := newWatchTestConsumer(t, js)
		ch, unsub, err := AllGeofenceTransitions(ctx, cons)
		if err != nil {
			t.Fatalf("AllGeofenceTransitions: %v", err)
		}
		defer unsub()
		publishWatchEvent(t, ctx, js, GTSubjectPositionUpdated("obj2"), positionEvent("obj2"))
		publishWatchEvent(t, ctx, js, GTSubjectGeofenceEntered("obj2", "zone2"), geofenceEvent("obj2", "zone2", EventTypeGeofenceEntered))
		if got := recvWatchEvent(t, ch); got.AreaID != "zone2" {
			t.Fatalf("all geofence event = %+v", got)
		}
	})

	t.Run("all lifecycle", func(t *testing.T) {
		consReg := newWatchTestConsumer(t, js)
		regCh, regUnsub, err := AllObjectRegistered(ctx, consReg)
		if err != nil {
			t.Fatalf("AllObjectRegistered: %v", err)
		}
		defer regUnsub()
		publishWatchEvent(t, ctx, js, GTSubjectObjectRegistered("obj3"), registeredEvent("obj3"))
		if got := recvWatchEvent(t, regCh); got.EventType != EventTypeObjectRegistered {
			t.Fatalf("all registered event = %+v", got)
		}

		consRem := newWatchTestConsumer(t, js)
		remCh, remUnsub, err := AllObjectRemoved(ctx, consRem)
		if err != nil {
			t.Fatalf("AllObjectRemoved: %v", err)
		}
		defer remUnsub()
		publishWatchEvent(t, ctx, js, GTSubjectObjectRemoved("obj3"), removedEvent("obj3"))
		if got := recvWatchEvent(t, remCh); got.EventType != EventTypeObjectRemoved {
			t.Fatalf("all removed event = %+v", got)
		}
	})

	t.Run("area and boot", func(t *testing.T) {
		consArea := newWatchTestConsumer(t, js)
		areaCh, areaUnsub, err := AreaGeofenceTransitions(ctx, consArea, "zone4")
		if err != nil {
			t.Fatalf("AreaGeofenceTransitions: %v", err)
		}
		defer areaUnsub()
		publishWatchEvent(t, ctx, js, GTSubjectGeofenceEntered("obj4", "zone4"), geofenceEvent("obj4", "zone4", EventTypeGeofenceEntered))
		if got := recvWatchEvent(t, areaCh); got.AreaID != "zone4" {
			t.Fatalf("area geofence event = %+v", got)
		}

		consBoot := newWatchTestConsumer(t, js)
		bootCh, bootUnsub, err := GeoTruthBooted(ctx, consBoot)
		if err != nil {
			t.Fatalf("GeoTruthBooted: %v", err)
		}
		defer bootUnsub()
		publishWatchEvent(t, ctx, js, GTGeoTruthBooted, bootedEvent())
		if got := recvWatchEvent(t, bootCh); got.EventType != EventTypeGeoTruthBooted || got.BootEpochSeq != 42 {
			t.Fatalf("boot event = %+v", got)
		}
	})

	t.Run("watch area alias", func(t *testing.T) {
		cons := newWatchTestConsumer(t, js)
		ch, unsub, err := WatchArea(ctx, cons, "zone5")
		if err != nil {
			t.Fatalf("WatchArea: %v", err)
		}
		defer unsub()
		publishWatchEvent(t, ctx, js, GTSubjectGeofenceExited("obj5", "zone5"), geofenceEvent("obj5", "zone5", EventTypeGeofenceExited))
		if got := recvWatchEvent(t, ch); got.AreaID != "zone5" || got.EventType != EventTypeGeofenceExited {
			t.Fatalf("watch area event = %+v", got)
		}
	})
}

func startWatchTestNATS(tb testing.TB) (*nats.Conn, jetstream.JetStream) {
	tb.Helper()
	opts := &natsserver.Options{
		Port:      -1,
		JetStream: true,
		StoreDir:  tb.TempDir(),
		NoLog:     true,
		NoSigs:    true,
	}
	s, err := natsserver.NewServer(opts)
	if err != nil {
		tb.Fatalf("new nats server: %v", err)
	}
	go s.Start()
	if !s.ReadyForConnections(5 * time.Second) {
		s.Shutdown()
		tb.Fatal("nats server not ready")
	}
	tb.Cleanup(func() { s.Shutdown() })

	nc, err := nats.Connect(s.ClientURL())
	if err != nil {
		tb.Fatalf("connect nats: %v", err)
	}
	tb.Cleanup(func() { nc.Close() })

	js, err := jetstream.New(nc)
	if err != nil {
		tb.Fatalf("jetstream: %v", err)
	}
	_, err = js.CreateOrUpdateStream(context.Background(), jetstream.StreamConfig{
		Name:     natskeys.GTStreamName,
		Subjects: []string{GTEventsWildcard},
		Storage:  jetstream.MemoryStorage,
	})
	if err != nil {
		tb.Fatalf("create stream: %v", err)
	}
	return nc, js
}

func newWatchTestConsumer(tb testing.TB, js jetstream.JetStream) *natsconsumer.Consumer {
	tb.Helper()
	seq := atomic.AddUint64(&watchTestConsumerSeq, 1)
	cons, err := natsconsumer.New(js, natskeys.GTStreamName, natsconsumer.Config{
		Name:          fmt.Sprintf("watch-test-%d", seq),
		DeliverPolicy: jetstream.DeliverNewPolicy,
		AckWait:       time.Second,
		MaxDeliver:    -1,
		MemoryStorage: true,
	})
	if err != nil {
		tb.Fatalf("new consumer: %v", err)
	}
	tb.Cleanup(func() { cons.Close() })
	return cons
}

func publishWatchEvent(tb testing.TB, ctx context.Context, js jetstream.JetStream, subject string, event any) {
	tb.Helper()
	data, err := json.Marshal(event)
	if err != nil {
		tb.Fatalf("marshal event: %v", err)
	}
	if _, err := js.Publish(ctx, subject, data); err != nil {
		tb.Fatalf("publish %s: %v", subject, err)
	}
}

func recvWatchEvent[T any](tb testing.TB, ch <-chan T) T {
	tb.Helper()
	select {
	case got, ok := <-ch:
		if !ok {
			tb.Fatal("watch channel closed")
		}
		return got
	case <-time.After(5 * time.Second):
		tb.Fatal("timed out waiting for watch event")
		var zero T
		return zero
	}
}

func positionEvent(objectID string) PositionUpdatedEvent {
	return PositionUpdatedEvent{
		ObjectEventMeta: objectMeta(EventTypePositionUpdated, objectID, 2),
		Region:          "0",
		Position:        EventPosition{X: 1, Y: 2, Z: 3, RotY: 0.5},
		Dims:            EventDims{Width: 1, Height: 2},
		InsideAreaIDs:   []string{"zone1"},
	}
}

func geofenceEvent(objectID, areaID, eventType string) GeofenceTransitionEvent {
	return GeofenceTransitionEvent{
		ObjectEventMeta:    objectMeta(eventType, objectID, 3),
		AreaID:             areaID,
		Region:             "0",
		Position:           EventPosition{X: 4, Y: 5, Z: 6},
		Dims:               EventDims{Width: 1, Height: 2},
		InsideAreaIDsAfter: []string{areaID},
	}
}

func registeredEvent(objectID string) ObjectRegisteredEvent {
	return ObjectRegisteredEvent{
		ObjectEventMeta: objectMeta(EventTypeObjectRegistered, objectID, 1),
		Dims:            EventDims{Width: 1, Height: 2},
	}
}

func removedEvent(objectID string) ObjectRemovedEvent {
	return ObjectRemovedEvent{
		ObjectEventMeta:     objectMeta(EventTypeObjectRemoved, objectID, 4),
		Dims:                EventDims{Width: 1, Height: 2},
		InsideAreaIDsBefore: []string{"zone1"},
		PositionBefore:      &EventPosition{X: 1, Y: 2, Z: 3},
	}
}

func bootedEvent() GeoTruthBootedEvent {
	return GeoTruthBootedEvent{
		EventID:      "gt:boot:42",
		EventType:    EventTypeGeoTruthBooted,
		BootEpochSeq: 42,
		OccurredAt:   time.Now(),
	}
}

func objectMeta(eventType, objectID string, commitSeq uint64) ObjectEventMeta {
	return ObjectEventMeta{
		EventMeta: EventMeta{
			EventID:    eventType + ":" + objectID,
			EventType:  eventType,
			OccurredAt: time.Now(),
		},
		ObjectID:   objectID,
		InstanceID: "b1i1",
		CommitSeq:  commitSeq,
	}
}
