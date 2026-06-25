package gtevents

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

type fakeMessagePublisher struct {
	publishAck *jetstream.PubAck
	publishErr error
	latest     *jetstream.RawStreamMsg
	latestErr  error
}

func (p fakeMessagePublisher) PublishMsg(context.Context, *nats.Msg, ...jetstream.PublishOpt) (*jetstream.PubAck, error) {
	return p.publishAck, p.publishErr
}

func (p fakeMessagePublisher) GetLastMsgForSubject(context.Context, string) (*jetstream.RawStreamMsg, error) {
	return p.latest, p.latestErr
}

func TestExpectedProjectionMsgsUsesGeofenceTransitions(t *testing.T) {
	region := "0"
	projections, err := expectedProjectionMsgs(RecoveredCommit{
		State: ObjectStateRecord{
			ObjectID:      "obj1",
			InstanceID:    "b1i1",
			CommitSeq:     2,
			Lifecycle:     LifecycleActive,
			Region:        &region,
			Position:      &EventPosition{X: 1, Y: 2, Z: 1, RotY: 0},
			Dims:          EventDims{Width: 1, Height: 1},
			InsideAreaIDs: []string{"zone-a"},
			GeofenceTransitions: []GeofenceTransitionRecord{
				{AreaID: "zone-a", Entered: true},
			},
		},
	})
	if err != nil {
		t.Fatalf("expectedProjectionMsgs: %v", err)
	}
	if len(projections) != 1 {
		t.Fatalf("projections = %d, want 1", len(projections))
	}
	if projections[0].Subject != SubjectGeofenceEntered("obj1", "zone-a") {
		t.Fatalf("projection subject = %q", projections[0].Subject)
	}

	var evt GeofenceTransitionEvent
	if err := json.Unmarshal(projections[0].Data, &evt); err != nil {
		t.Fatalf("decode projection: %v", err)
	}
	if evt.EventID != GeofenceEnterEventID("zone-a", "obj1", "b1i1", 2) {
		t.Fatalf("event_id = %q", evt.EventID)
	}
}

func TestPublishOrVerifyOnceUsesPayloadEventID(t *testing.T) {
	msg := msgWithEventID(t, SubjectPositionUpdated("obj1"), "gt:p:obj1:b1i1:2")
	ack, err := publishOrVerifyOnce(context.Background(), fakeMessagePublisher{
		publishErr: errors.New("publish failed after store"),
		latest: &jetstream.RawStreamMsg{
			Sequence: 42,
			Subject:  msg.Subject,
			Data:     msg.Data,
		},
	}, msg)
	if err != nil {
		t.Fatalf("publishOrVerifyOnce: %v", err)
	}
	if ack == nil || !ack.Duplicate || ack.Sequence != 42 {
		t.Fatalf("ack = %+v, want duplicate seq 42", ack)
	}
}

func TestPublishOrVerifyOnceRejectsDifferentLatestEventID(t *testing.T) {
	publishErr := errors.New("publish failed")
	msg := msgWithEventID(t, SubjectPositionUpdated("obj1"), "gt:p:obj1:b1i1:2")
	_, err := publishOrVerifyOnce(context.Background(), fakeMessagePublisher{
		publishErr: publishErr,
		latest: &jetstream.RawStreamMsg{
			Sequence: 42,
			Subject:  msg.Subject,
			Data:     msgWithEventID(t, msg.Subject, "gt:p:obj1:b1i1:3").Data,
		},
	}, msg)
	if !errors.Is(err, publishErr) {
		t.Fatalf("err = %v, want %v", err, publishErr)
	}
}

func msgWithEventID(t *testing.T, subject, eventID string) *nats.Msg {
	t.Helper()
	data, err := json.Marshal(struct {
		EventID string `json:"event_id"`
	}{EventID: eventID})
	if err != nil {
		t.Fatalf("marshal event id: %v", err)
	}
	return &nats.Msg{Subject: subject, Data: data}
}
