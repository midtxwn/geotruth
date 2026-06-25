package gtevents

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/midtxwn/geotruth/pkg/domain"
)

func TestBuildCommitMsgsIncludesClientOpID(t *testing.T) {
	msgs, err := BuildCommitMsgs(CommitInput{
		ObjectID:      "obj1",
		SourceSeq:     42,
		ClientOpID:    "op-update",
		Region:        "0",
		Position:      EventPosition{X: 1, Y: 2, Z: 1, RotY: 0},
		Dims:          EventDims{Width: 1, Height: 1},
		HasPosition:   true,
		InsideAreaIDs: []string{"zone1"},
		Lifecycle:     LifecycleActive,
		Transitions:   []GeofenceTransition{{AreaID: "zone1", Entered: true}},
	})
	if err != nil {
		t.Fatalf("BuildCommitMsgs: %v", err)
	}
	if len(msgs) != 3 {
		t.Fatalf("expected 3 messages, got %d", len(msgs))
	}

	var geofence GeofenceTransitionEvent
	if err := json.Unmarshal(msgs[0].Data, &geofence); err != nil {
		t.Fatalf("unmarshal geofence: %v", err)
	}
	if geofence.ClientOpID != "op-update" {
		t.Fatalf("geofence ClientOpID = %q, want op-update", geofence.ClientOpID)
	}

	var position PositionUpdatedEvent
	if err := json.Unmarshal(msgs[1].Data, &position); err != nil {
		t.Fatalf("unmarshal position: %v", err)
	}
	if position.ClientOpID != "op-update" {
		t.Fatalf("position ClientOpID = %q, want op-update", position.ClientOpID)
	}
}

func TestBuildRegisterCommitMsgsIncludesClientOpID(t *testing.T) {
	msgs, err := BuildRegisterCommitMsgs("obj1", 7, "op-register", domain.ObjectDimensions{Width: 1, Height: 1})
	if err != nil {
		t.Fatalf("BuildRegisterCommitMsgs: %v", err)
	}

	var event ObjectRegisteredEvent
	if err := json.Unmarshal(msgs[0].Data, &event); err != nil {
		t.Fatalf("unmarshal register: %v", err)
	}
	if event.ClientOpID != "op-register" {
		t.Fatalf("register ClientOpID = %q, want op-register", event.ClientOpID)
	}
}

func TestBuildCommitMsgsOmitsEmptyClientOpID(t *testing.T) {
	msgs, err := BuildCommitMsgs(CommitInput{
		ObjectID:    "obj1",
		SourceSeq:   42,
		Region:      "0",
		Dims:        EventDims{Width: 1, Height: 1},
		Lifecycle:   LifecycleRemoved,
		HasPosition: false,
	})
	if err != nil {
		t.Fatalf("BuildCommitMsgs: %v", err)
	}
	if strings.Contains(string(msgs[0].Data), "client_op_id") {
		t.Fatalf("empty client_op_id should be omitted: %s", string(msgs[0].Data))
	}
}
