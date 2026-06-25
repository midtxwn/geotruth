package natswatch

import (
	"encoding/json"
	"testing"
	"time"
)

func stringPtr(s string) *string { return &s }

func TestGTSubjects(t *testing.T) {
	objID := "sensor-1"
	areaID := "zone-a"

	posSubj := GTSubjectPositionUpdated(objID)
	if posSubj != "gt.events.v1.object.sensor-1.position.updated" {
		t.Errorf("GTSubjectPositionUpdated(%q) = %q", objID, posSubj)
	}

	enterSubj := GTSubjectGeofenceEntered(objID, areaID)
	if enterSubj != "gt.events.v1.object.sensor-1.geofence.zone-a.entered" {
		t.Errorf("GTSubjectGeofenceEntered(%q, %q) = %q", objID, areaID, enterSubj)
	}

	exitSubj := GTSubjectGeofenceExited(objID, areaID)
	if exitSubj != "gt.events.v1.object.sensor-1.geofence.zone-a.exited" {
		t.Errorf("GTSubjectGeofenceExited(%q, %q) = %q", objID, areaID, exitSubj)
	}

	regSubj := GTSubjectObjectRegistered(objID)
	if regSubj != "gt.events.v1.object.sensor-1.registered" {
		t.Errorf("GTSubjectObjectRegistered(%q) = %q", objID, regSubj)
	}

	rmSubj := GTSubjectObjectRemoved(objID)
	if rmSubj != "gt.events.v1.object.sensor-1.removed" {
		t.Errorf("GTSubjectObjectRemoved(%q) = %q", objID, rmSubj)
	}

	objEventsSubj := GTSubjectObjectEvents(objID)
	if objEventsSubj != "gt.events.v1.object.sensor-1.>" {
		t.Errorf("GTSubjectObjectEvents(%q) = %q", objID, objEventsSubj)
	}

	geofenceSubj := GTSubjectObjectGeofence(objID)
	if geofenceSubj != "gt.events.v1.object.sensor-1.geofence.>" {
		t.Errorf("GTSubjectObjectGeofence(%q) = %q", objID, geofenceSubj)
	}

	areaSubj := GTSubjectAreaGeofence(areaID)
	if areaSubj != "gt.events.v1.object.*.geofence.zone-a.>" {
		t.Errorf("GTSubjectAreaGeofence(%q) = %q", areaID, areaSubj)
	}

	if GTDetectorBooted != "gt.events.v1.detector.booted" {
		t.Errorf("GTDetectorBooted = %q", GTDetectorBooted)
	}

	if GTEventsWildcard != "gt.events.v1.>" {
		t.Errorf("GTEventsWildcard = %q", GTEventsWildcard)
	}
}

func TestEventPositionJSONRoundTrip(t *testing.T) {
	pos := EventPosition{X: 1.5, Y: 2.5, Z: 3.5, RotY: 0.78}
	data, err := json.Marshal(pos)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var parsed EventPosition
	if err := json.Unmarshal(data, &parsed); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if parsed.X != pos.X || parsed.Y != pos.Y || parsed.Z != pos.Z || parsed.RotY != pos.RotY {
		t.Errorf("round-trip failed: got %+v, want %+v", parsed, pos)
	}
}

func TestEventDimsJSONRoundTrip(t *testing.T) {
	dims := EventDims{Width: 2.0, Height: 3.0}
	data, err := json.Marshal(dims)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var parsed EventDims
	if err := json.Unmarshal(data, &parsed); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if parsed.Width != dims.Width || parsed.Height != dims.Height {
		t.Errorf("round-trip failed: got %+v, want %+v", parsed, dims)
	}
}

func assertFlatJSONField(t *testing.T, data []byte, field string) {
	t.Helper()

	var fields map[string]json.RawMessage
	if err := json.Unmarshal(data, &fields); err != nil {
		t.Fatalf("unmarshal object: %v", err)
	}
	if _, ok := fields[field]; !ok {
		t.Fatalf("missing flat JSON field %q in %s", field, data)
	}
	if _, ok := fields["ObjectEventMeta"]; ok {
		t.Fatalf("embedded ObjectEventMeta leaked into JSON: %s", data)
	}
	if _, ok := fields["EventMeta"]; ok {
		t.Fatalf("embedded EventMeta leaked into JSON: %s", data)
	}
}

func TestPositionUpdatedEventJSONRoundTrip(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Millisecond)
	evt := PositionUpdatedEvent{
		ObjectEventMeta: ObjectEventMeta{
			EventMeta: EventMeta{
				EventID:      "gt:position:sensor_1:spatial:42",
				EventType:    EventTypePositionUpdated,
				SourceStream: "SPATIAL",
				SourceSeq:    42,
				OccurredAt:   now,
			},
			ObjectID: "sensor.1",
		},
		Region:        "0",
		Position:      EventPosition{X: 10.0, Y: 20.0, Z: 1.0, RotY: 0.5},
		Dims:          EventDims{Width: 2.0, Height: 3.0},
		InsideAreaIDs: []string{"zone-a", "zone-b"},
	}
	data, err := json.Marshal(evt)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var parsed PositionUpdatedEvent
	if err := json.Unmarshal(data, &parsed); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if parsed.ObjectID != evt.ObjectID {
		t.Errorf("ObjectID: got %q, want %q", parsed.ObjectID, evt.ObjectID)
	}
	if parsed.EventType != EventTypePositionUpdated {
		t.Errorf("EventType: got %q, want %q", parsed.EventType, EventTypePositionUpdated)
	}
	if parsed.Region != evt.Region {
		t.Errorf("Region: got %q, want %q", parsed.Region, evt.Region)
	}
	if parsed.Position.X != evt.Position.X {
		t.Errorf("Position.X: got %f, want %f", parsed.Position.X, evt.Position.X)
	}
	if len(parsed.InsideAreaIDs) != 2 {
		t.Errorf("InsideAreaIDs: got %d, want 2", len(parsed.InsideAreaIDs))
	}
}

func TestGeofenceTransitionEventJSONRoundTrip(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Millisecond)
	evt := GeofenceTransitionEvent{
		ObjectEventMeta: ObjectEventMeta{
			EventMeta: EventMeta{
				EventID:      "gt:geofence:entered:zone_a:sensor_1:spatial:42",
				EventType:    EventTypeGeofenceEntered,
				SourceStream: "SPATIAL",
				SourceSeq:    42,
				OccurredAt:   now,
			},
			ObjectID: "sensor.1",
		},
		AreaID:             "zone-a",
		Region:             "0",
		Position:           EventPosition{X: 10.0, Y: 20.0, Z: 1.0, RotY: 0.0},
		Dims:               EventDims{Width: 2.0, Height: 3.0},
		InsideAreaIDsAfter: []string{"zone-a"},
	}
	data, err := json.Marshal(evt)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var parsed GeofenceTransitionEvent
	if err := json.Unmarshal(data, &parsed); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if parsed.AreaID != evt.AreaID {
		t.Errorf("AreaID: got %q, want %q", parsed.AreaID, evt.AreaID)
	}
	if parsed.EventType != EventTypeGeofenceEntered {
		t.Errorf("EventType: got %q, want %q", parsed.EventType, EventTypeGeofenceEntered)
	}
}

func TestObjectRegisteredEventJSONRoundTrip(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Millisecond)
	evt := ObjectRegisteredEvent{
		ObjectEventMeta: ObjectEventMeta{
			EventMeta: EventMeta{
				EventID:      "gt:object:registered:sensor_1:spatial:1",
				EventType:    EventTypeObjectRegistered,
				SourceStream: "SPATIAL",
				SourceSeq:    1,
				OccurredAt:   now,
			},
			ObjectID: "sensor.1",
		},
		Dims: EventDims{Width: 2.0, Height: 3.0},
	}
	data, err := json.Marshal(evt)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	assertFlatJSONField(t, data, "event_id")
	assertFlatJSONField(t, data, "object_id")
	var parsed ObjectRegisteredEvent
	if err := json.Unmarshal(data, &parsed); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if parsed.ObjectID != evt.ObjectID {
		t.Errorf("ObjectID: got %q, want %q", parsed.ObjectID, evt.ObjectID)
	}
}

func TestObjectRemovedEventJSONRoundTrip(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Millisecond)
	evt := ObjectRemovedEvent{
		ObjectEventMeta: ObjectEventMeta{
			EventMeta: EventMeta{
				EventID:      "gt:object:removed:sensor_1:spatial:10",
				EventType:    EventTypeObjectRemoved,
				SourceStream: "SPATIAL",
				SourceSeq:    10,
				OccurredAt:   now,
			},
			ObjectID: "sensor.1",
		},
		Region:              stringPtr("0"),
		InsideAreaIDsBefore: []string{"zone-a"},
	}
	data, err := json.Marshal(evt)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	assertFlatJSONField(t, data, "event_id")
	assertFlatJSONField(t, data, "object_id")
	var parsed ObjectRemovedEvent
	if err := json.Unmarshal(data, &parsed); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if parsed.ObjectID != evt.ObjectID {
		t.Errorf("ObjectID: got %q, want %q", parsed.ObjectID, evt.ObjectID)
	}
	if len(parsed.InsideAreaIDsBefore) != 1 {
		t.Errorf("InsideAreaIDsBefore: got %d, want 1", len(parsed.InsideAreaIDsBefore))
	}
}

func TestDetectorBootedEventJSONRoundTrip(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Millisecond)
	evt := DetectorBootedEvent{
		EventID:    "gt:detector:booted:abc123",
		EventType:  EventTypeDetectorBooted,
		BootID:     "abc123",
		OccurredAt: now,
	}
	data, err := json.Marshal(evt)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var parsed DetectorBootedEvent
	if err := json.Unmarshal(data, &parsed); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if parsed.BootID != evt.BootID {
		t.Errorf("BootID: got %q, want %q", parsed.BootID, evt.BootID)
	}
}

func TestEventTypeConstants(t *testing.T) {
	if EventTypePositionUpdated != "position.updated" {
		t.Errorf("EventTypePositionUpdated = %q", EventTypePositionUpdated)
	}
	if EventTypeGeofenceEntered != "geofence.entered" {
		t.Errorf("EventTypeGeofenceEntered = %q", EventTypeGeofenceEntered)
	}
	if EventTypeGeofenceExited != "geofence.exited" {
		t.Errorf("EventTypeGeofenceExited = %q", EventTypeGeofenceExited)
	}
	if EventTypeObjectRegistered != "object.registered" {
		t.Errorf("EventTypeObjectRegistered = %q", EventTypeObjectRegistered)
	}
	if EventTypeObjectRemoved != "object.removed" {
		t.Errorf("EventTypeObjectRemoved = %q", EventTypeObjectRemoved)
	}
	if EventTypeDetectorBooted != "detector.booted" {
		t.Errorf("EventTypeDetectorBooted = %q", EventTypeDetectorBooted)
	}
}

func TestLifecycleConstants(t *testing.T) {
	if LifecycleActive != "active" {
		t.Errorf("LifecycleActive = %q", LifecycleActive)
	}
	if LifecycleRemoved != "removed" {
		t.Errorf("LifecycleRemoved = %q", LifecycleRemoved)
	}
}
