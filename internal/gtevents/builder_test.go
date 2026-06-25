package gtevents

import (
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
	if commit.StateAfter == nil || commit.StateAfter.InstanceID != "b1i1" || commit.StateAfter.CommitSeq != 2 {
		t.Fatalf("bad state_after: %+v", commit.StateAfter)
	}
	if len(commit.StateAfter.GeofenceTransitions) != 1 || !commit.StateAfter.GeofenceTransitions[0].Entered {
		t.Fatalf("state_after geofence transitions = %+v", commit.StateAfter.GeofenceTransitions)
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
	if len(msgs.Projections) != 1 {
		t.Fatalf("projections = %d, want 1", len(msgs.Projections))
	}
}

func TestBuildRegisterCurrentStateCommitMsgsPreservesRegisterSemantics(t *testing.T) {
	region := "0"
	msgs, err := BuildRegisterCurrentStateCommitMsgs("obj1", "b1i1", 4, "client-op", ObjectStateRecord{
		Lifecycle: LifecycleActive,
		Region:    &region,
		Position:  &EventPosition{X: 7, Y: 8, Z: 1, RotY: 0.5},
		Dims:      EventDims{Width: 2, Height: 3},
		InsideAreaIDs: []string{
			"zone-a",
		},
		GeofenceTransitions: []GeofenceTransitionRecord{{AreaID: "stale", Entered: true}},
	})
	if err != nil {
		t.Fatalf("BuildRegisterCurrentStateCommitMsgs: %v", err)
	}
	if msgs.Commit == nil {
		t.Fatal("commit message is nil")
	}
	if msgs.Commit.Subject != SubjectObjectRegistered("obj1") {
		t.Fatalf("commit subject = %q", msgs.Commit.Subject)
	}
	if len(msgs.Projections) != 0 {
		t.Fatalf("projections = %d, want 0", len(msgs.Projections))
	}

	var registered ObjectRegisteredEvent
	if err := json.Unmarshal(msgs.Commit.Data, &registered); err != nil {
		t.Fatalf("decode register: %v", err)
	}
	if registered.EventType != EventTypeObjectRegistered {
		t.Fatalf("event_type = %q", registered.EventType)
	}
	if registered.EventID != ObjectRegisteredEventID("obj1", "b1i1", 4) {
		t.Fatalf("event_id = %q", registered.EventID)
	}
	if registered.StateAfter == nil || registered.StateAfter.Position == nil {
		t.Fatalf("missing state_after position: %+v", registered.StateAfter)
	}
	if registered.StateAfter.Position.X != 7 || registered.StateAfter.Position.Y != 8 {
		t.Fatalf("state_after position = %+v", registered.StateAfter.Position)
	}
	if len(registered.StateAfter.GeofenceTransitions) != 0 {
		t.Fatalf("state_after geofence transitions = %+v, want none", registered.StateAfter.GeofenceTransitions)
	}
}
