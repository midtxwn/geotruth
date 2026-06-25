package geotruthops

import (
	"testing"

	"github.com/midtxwn/geotruth/pkg/natswatch"

	"github.com/nats-io/nats.go/jetstream"
)

func TestParseEventRecordReadsGeofenceTransitions(t *testing.T) {
	msg := &jetstream.RawStreamMsg{
		Sequence: 7,
		Subject:  natswatch.GTSubjectPositionUpdated("obj1"),
		Data: []byte(`{
			"event_id":"gt:p:obj1:b1i1:2",
			"event_type":"position.updated",
			"object_id":"obj1",
			"instance_id":"b1i1",
			"commit_seq":2,
			"state_after":{
				"object_id":"obj1",
				"instance_id":"b1i1",
				"commit_seq":2,
				"lifecycle":"active",
				"inside_area_ids":["zone-a"],
				"geofence_transitions":[
					{"area_id":"zone-a","entered":true}
				],
				"updated_at":"2026-06-11T00:00:00Z"
			}
		}`),
	}

	rec, ok := parseEventRecord(msg)
	if !ok {
		t.Fatal("parseEventRecord returned ok=false")
	}
	if !rec.IsCommit {
		t.Fatal("record was not marked as commit")
	}
	if rec.StreamSeq != 7 || rec.ObjectID != "obj1" || rec.InstanceID != "b1i1" || rec.CommitSeq != 2 {
		t.Fatalf("bad record identity: %+v", rec)
	}
	if rec.Lifecycle != natswatch.LifecycleActive {
		t.Fatalf("lifecycle = %q, want %q", rec.Lifecycle, natswatch.LifecycleActive)
	}
	if len(rec.GeofenceTransitions) != 1 {
		t.Fatalf("geofence transitions = %d, want 1", len(rec.GeofenceTransitions))
	}
	if got := rec.GeofenceTransitions[0]; got.AreaID != "zone-a" || !got.Entered {
		t.Fatalf("geofence transition = %+v, want zone-a entered", got)
	}
}
