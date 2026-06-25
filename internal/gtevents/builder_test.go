package gtevents

import (
	"bytes"
	"encoding/json"
	"testing"
)

func TestBuildCommitMsgsUsesObjectCommitAsMarker(t *testing.T) {
	msgs, err := BuildCommitMsgs(CommitInput{
		ObjectID:                "obj1",
		InstanceID:              "b1i1",
		CommitSeq:               2,
		ClientOpID:              "client-op",
		Region:                  "0",
		Position:                EventPosition{X: 1, Y: 2, Z: 3, RotY: 4},
		Dims:                    EventDims{Width: 1, Height: 1},
		HasPosition:             true,
		ProjectionPositionKnown: true,
		InsideAreaIDs:           []string{"zone-a"},
		Lifecycle:               LifecycleActive,
		GeofenceTransitions:     []GeofenceTransition{{AreaID: "zone-a", Entered: true}},
	})
	if err != nil {
		t.Fatalf("BuildCommitMsgs: %v", err)
	}
	if msgs.Commit == nil {
		t.Fatal("commit message is nil")
	}
	if msgs.Commit.Subject != SubjectPositionUpdated("obj1") {
		t.Fatalf("commit subject = %q", msgs.Commit.Subject)
	}
	if got := msgs.Commit.Header.Get("Nats-Msg-Id"); got != "" {
		t.Fatalf("commit Nats-Msg-Id header = %q, want empty", got)
	}
	if len(msgs.Projections) != 1 {
		t.Fatalf("projections = %d, want 1", len(msgs.Projections))
	}
	if got := msgs.Projections[0].Header.Get("Nats-Msg-Id"); got != "" {
		t.Fatalf("projection Nats-Msg-Id header = %q, want empty", got)
	}

	var commit PositionUpdatedEvent
	if err := json.Unmarshal(msgs.Commit.Data, &commit); err != nil {
		t.Fatalf("decode commit: %v", err)
	}
	if commit.EventID != PositionEventID("obj1", "b1i1", 2) {
		t.Fatalf("commit event_id = %q", commit.EventID)
	}
	if bytes.Contains(msgs.Commit.Data, []byte("state_after")) {
		t.Fatalf("public commit carried state_after: %s", string(msgs.Commit.Data))
	}
	state, ok, err := CheckpointStateFromPublicEvent(msgs.Commit.Data)
	if err != nil {
		t.Fatalf("decode checkpoint: %v", err)
	}
	if !ok || state.InstanceID != "b1i1" || state.CommitSeq != 2 {
		t.Fatalf("bad checkpoint state: ok=%v state=%+v", ok, state)
	}
	if state.UpdatedAt.IsZero() {
		t.Fatal("checkpoint updated_at was not reconstructed from occurred_at")
	}
	if state.Position == nil || state.Position.X != 1 || state.Position.RotY != 4 {
		t.Fatalf("checkpoint position = %+v", state.Position)
	}
	if len(state.GeofenceTransitions) != 1 || !state.GeofenceTransitions[0].Entered {
		t.Fatalf("checkpoint geofence transitions = %+v", state.GeofenceTransitions)
	}

	var projection GeofenceTransitionEvent
	if err := json.Unmarshal(msgs.Projections[0].Data, &projection); err != nil {
		t.Fatalf("decode projection: %v", err)
	}
	if projection.EventID != GeofenceEnterEventID("zone-a", "obj1", "b1i1", 2) {
		t.Fatalf("projection event_id = %q", projection.EventID)
	}
	if projection.InstanceID != commit.InstanceID || projection.CommitSeq != commit.CommitSeq {
		t.Fatalf("projection commit coordinates = %s/%d", projection.InstanceID, projection.CommitSeq)
	}
}

func TestBuildRemovedCommitCarriesPositionBefore(t *testing.T) {
	msgs, err := BuildCommitMsgs(CommitInput{
		ObjectID:                "obj1",
		InstanceID:              "b1i1",
		CommitSeq:               3,
		Region:                  "0",
		Position:                EventPosition{X: 5, Y: 6, Z: 1, RotY: 0},
		Dims:                    EventDims{Width: 2, Height: 3},
		ProjectionPositionKnown: true,
		Lifecycle:               LifecycleRemoved,
		GeofenceTransitions:     []GeofenceTransition{{AreaID: "zone-a", Entered: false}},
	})
	if err != nil {
		t.Fatalf("BuildCommitMsgs: %v", err)
	}

	var removed ObjectRemovedEvent
	if err := json.Unmarshal(msgs.Commit.Data, &removed); err != nil {
		t.Fatalf("decode remove: %v", err)
	}
	if removed.PositionBefore == nil {
		t.Fatal("position_before is nil")
	}
	if removed.PositionBefore.X != 5 || removed.Dims.Width != 2 {
		t.Fatalf("bad removal context: pos=%+v dims=%+v", removed.PositionBefore, removed.Dims)
	}
	if bytes.Contains(msgs.Commit.Data, []byte("state_after")) {
		t.Fatalf("public remove carried state_after: %s", string(msgs.Commit.Data))
	}
	state, ok, err := CheckpointStateFromPublicEvent(msgs.Commit.Data)
	if err != nil {
		t.Fatalf("decode checkpoint: %v", err)
	}
	if !ok || state.Lifecycle != LifecycleRemoved || state.Position != nil {
		t.Fatalf("bad removed checkpoint: ok=%v state=%+v", ok, state)
	}
	if len(state.GeofenceTransitions) != 1 || state.GeofenceTransitions[0].Entered {
		t.Fatalf("checkpoint geofence transitions = %+v", state.GeofenceTransitions)
	}
	if len(msgs.Projections) != 1 {
		t.Fatalf("projections = %d, want 1", len(msgs.Projections))
	}
}

func TestMarshalCheckpointEventRejectsUnknownLifecycle(t *testing.T) {
	_, err := marshalCheckpointEvent(ObjectRegisteredEvent{}, &ObjectStateRecord{Lifecycle: Lifecycle(99)})
	if err == nil {
		t.Fatal("marshalCheckpointEvent accepted unknown lifecycle")
	}
}

func TestCheckpointStateFromPublicEventRejectsUnknownLifecycle(t *testing.T) {
	_, _, err := CheckpointStateFromPublicEvent([]byte(`{
		"e":"gt:o:r:obj1:b1i1:1",
		"k":"object.registered",
		"o":"obj1",
		"i":"b1i1",
		"s":1,
		"cp":{"l":"z"}
	}`))
	if err == nil {
		t.Fatal("CheckpointStateFromPublicEvent accepted unknown lifecycle")
	}
}
