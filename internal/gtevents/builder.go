package gtevents

import (
	"encoding/json"
	"time"

	"github.com/midtxwn/geotruth/pkg/domain"

	"github.com/nats-io/nats.go"
)

func regionPtr(r string) *string {
	if r == "" {
		return nil
	}
	return &r
}

// GeofenceTransition describes a single area enter/exit detected by the engine.
type GeofenceTransition struct {
	AreaID  string
	Entered bool // true=enter, false=exit
}

// CommitInput carries the data needed to build a CommitEnvelope.
// The dispatcher fills this from a WorkerResult or a registration/remove event.
type CommitInput struct {
	ObjectID   string
	SourceSeq  uint64
	ClientOpID string

	// Post-state fields after the SPATIAL message is processed.
	Region        string
	Position      EventPosition // zero if removed
	Dims          EventDims
	HasPosition   bool // false for pure registration without position
	InsideAreaIDs []string
	Lifecycle     string // "active" or "removed"

	// Transitions detected during processing.
	Transitions []GeofenceTransition
}

// BuildCommitMsgs constructs the ordered message slice for a CommitEnvelope.
//
// Message order is load-bearing: geofence transitions -> position/lifecycle
// event -> state record. Public events MUST be published before the state
// record so that if a crash occurs after the state record is persisted, all
// preceding public events are guaranteed to exist. The state record is the
// commit marker - its existence proves all public events were published.
func BuildCommitMsgs(input CommitInput) ([]*nats.Msg, error) {
	var msgs []*nats.Msg
	now := time.Now().UTC()

	// 1. Geofence transition events (no TTL for surveillance events).
	for _, tr := range input.Transitions {
		var eventID, eventType, subject string
		if tr.Entered {
			eventID = GeofenceEnterEventID(tr.AreaID, input.ObjectID, input.SourceSeq)
			eventType = EventTypeGeofenceEntered
			subject = SubjectGeofenceEntered(input.ObjectID, tr.AreaID)
		} else {
			eventID = GeofenceExitEventID(tr.AreaID, input.ObjectID, input.SourceSeq)
			eventType = EventTypeGeofenceExited
			subject = SubjectGeofenceExited(input.ObjectID, tr.AreaID)
		}

		event := GeofenceTransitionEvent{
			ObjectEventMeta: ObjectEventMeta{
				EventMeta: EventMeta{
					EventID:      eventID,
					EventType:    eventType,
					SourceStream: SourceStream,
					SourceSeq:    input.SourceSeq,
					ClientOpID:   input.ClientOpID,
					OccurredAt:   now,
				},
				ObjectID: input.ObjectID,
			},
			AreaID:             tr.AreaID,
			Region:             input.Region,
			Position:           input.Position,
			Dims:               input.Dims,
			InsideAreaIDsAfter: input.InsideAreaIDs,
		}

		data, err := json.Marshal(event)
		if err != nil {
			return nil, err
		}
		msgs = append(msgs, &nats.Msg{
			Subject: subject,
			Data:    data,
			Header:  PublicNoExpiryHeaders(eventID),
		})
	}

	// 2. Position updated event (with TTL) or lifecycle event.
	switch input.Lifecycle {
	case LifecycleActive:
		if input.HasPosition {
			eventID := PositionEventID(input.ObjectID, input.SourceSeq)
			subject := SubjectPositionUpdated(input.ObjectID)
			event := PositionUpdatedEvent{
				ObjectEventMeta: ObjectEventMeta{
					EventMeta: EventMeta{
						EventID:      eventID,
						EventType:    EventTypePositionUpdated,
						SourceStream: SourceStream,
						SourceSeq:    input.SourceSeq,
						ClientOpID:   input.ClientOpID,
						OccurredAt:   now,
					},
					ObjectID: input.ObjectID,
				},
				Region:        input.Region,
				Position:      input.Position,
				Dims:          input.Dims,
				InsideAreaIDs: input.InsideAreaIDs,
			}
			data, err := json.Marshal(event)
			if err != nil {
				return nil, err
			}
			msgs = append(msgs, &nats.Msg{
				Subject: subject,
				Data:    data,
				Header:  PublicHeaders(TTLPosition, eventID),
			})
		}

	case LifecycleRemoved:
		eventID := ObjectRemovedEventID(input.ObjectID, input.SourceSeq)
		subject := SubjectObjectRemoved(input.ObjectID)
		// Collect areas the object was inside before removal for the exit
		// events. The removed event carries inside_area_ids_before.
		var beforeAreas []string
		for _, tr := range input.Transitions {
			if !tr.Entered {
				beforeAreas = append(beforeAreas, tr.AreaID)
			}
		}
		event := ObjectRemovedEvent{
			ObjectEventMeta: ObjectEventMeta{
				EventMeta: EventMeta{
					EventID:      eventID,
					EventType:    EventTypeObjectRemoved,
					SourceStream: SourceStream,
					SourceSeq:    input.SourceSeq,
					ClientOpID:   input.ClientOpID,
					OccurredAt:   now,
				},
				ObjectID: input.ObjectID,
			},
			Region:              regionPtr(input.Region),
			InsideAreaIDsBefore: beforeAreas,
		}
		data, err := json.Marshal(event)
		if err != nil {
			return nil, err
		}
		msgs = append(msgs, &nats.Msg{
			Subject: subject,
			Data:    data,
			Header:  PublicNoExpiryHeaders(eventID),
		})
	}

	// 3. Internal state record (Nats-Rollup:sub, no TTL). Always LAST in
	// the message slice - it is the commit marker. Its existence proves
	// all preceding public events were published. If the object had
	// geofence transitions, they are recorded as diagnostic metadata
	// (not used for boot recovery; InsideAreaIDs serves that role).
	stateSubject := SubjectObjectState(input.ObjectID)
	posPtr := &input.Position
	if !input.HasPosition {
		posPtr = nil
	}

	var transitionRecords []TransitionRecord
	for _, tr := range input.Transitions {
		transitionRecords = append(transitionRecords, TransitionRecord{
			AreaID:  tr.AreaID,
			Entered: tr.Entered,
		})
	}

	state := ObjectStateRecord{
		ObjectID:         input.ObjectID,
		Lifecycle:        input.Lifecycle,
		DetectorStateSeq: input.SourceSeq,
		Region:           regionPtr(input.Region),
		Position:         posPtr,
		Dims:             input.Dims,
		InsideAreaIDs:    input.InsideAreaIDs,
		Transitions:      transitionRecords,
		UpdatedAt:        now,
	}
	stateData, err := json.Marshal(state)
	if err != nil {
		return nil, err
	}

	stateEventID := StateRecordID(input.ObjectID, input.SourceSeq)
	msgs = append(msgs, &nats.Msg{
		Subject: stateSubject,
		Data:    stateData,
		Header:  StateHeaders(stateEventID),
	})

	return msgs, nil
}

// BuildRegisterCommitMsgs creates the message slice for object registration.
// Registration happens via cmd.object.register and has no geofence transitions.
// Message order: registered event -> state record (state is commit marker).
func BuildRegisterCommitMsgs(objectID string, sourceSeq uint64, clientOpID string, dims domain.ObjectDimensions) ([]*nats.Msg, error) {
	now := time.Now().UTC()
	eventID := ObjectRegisteredEventID(objectID, sourceSeq)
	subject := SubjectObjectRegistered(objectID)

	event := ObjectRegisteredEvent{
		ObjectEventMeta: ObjectEventMeta{
			EventMeta: EventMeta{
				EventID:      eventID,
				EventType:    EventTypeObjectRegistered,
				SourceStream: SourceStream,
				SourceSeq:    sourceSeq,
				ClientOpID:   clientOpID,
				OccurredAt:   now,
			},
			ObjectID: objectID,
		},
		Dims: EventDims{
			Width:  dims.Width,
			Height: dims.Height,
		},
	}
	data, err := json.Marshal(event)
	if err != nil {
		return nil, err
	}

	// State record for registration (no position yet).
	stateEventID := StateRecordID(objectID, sourceSeq)
	state := ObjectStateRecord{
		ObjectID:         objectID,
		Lifecycle:        LifecycleActive,
		DetectorStateSeq: sourceSeq,
		Region:           nil,
		Position:         nil,
		Dims: EventDims{
			Width:  dims.Width,
			Height: dims.Height,
		},
		InsideAreaIDs: nil,
		UpdatedAt:     now,
	}
	stateData, err := json.Marshal(state)
	if err != nil {
		return nil, err
	}

	return []*nats.Msg{
		{
			Subject: subject,
			Data:    data,
			Header:  PublicNoExpiryHeaders(eventID),
		},
		{
			Subject: SubjectObjectState(objectID),
			Data:    stateData,
			Header:  StateHeaders(stateEventID),
		},
	}, nil
}

// BuildRegisterCurrentStateCommitMsgs creates a registration event for an
// idempotent duplicate register, followed by a state record that preserves the
// object's current engine state while advancing the SPATIAL commit marker.
func BuildRegisterCurrentStateCommitMsgs(objectID string, sourceSeq uint64, clientOpID string, state ObjectStateRecord) ([]*nats.Msg, error) {
	now := time.Now().UTC()
	eventID := ObjectRegisteredEventID(objectID, sourceSeq)

	event := ObjectRegisteredEvent{
		ObjectEventMeta: ObjectEventMeta{
			EventMeta: EventMeta{
				EventID:      eventID,
				EventType:    EventTypeObjectRegistered,
				SourceStream: SourceStream,
				SourceSeq:    sourceSeq,
				ClientOpID:   clientOpID,
				OccurredAt:   now,
			},
			ObjectID: objectID,
		},
		Dims: state.Dims,
	}
	data, err := json.Marshal(event)
	if err != nil {
		return nil, err
	}

	state.ObjectID = objectID
	state.DetectorStateSeq = sourceSeq
	state.UpdatedAt = now
	state.Transitions = nil
	stateData, err := json.Marshal(state)
	if err != nil {
		return nil, err
	}

	stateEventID := StateRecordID(objectID, sourceSeq)
	return []*nats.Msg{
		{
			Subject: SubjectObjectRegistered(objectID),
			Data:    data,
			Header:  PublicNoExpiryHeaders(eventID),
		},
		{
			Subject: SubjectObjectState(objectID),
			Data:    stateData,
			Header:  StateHeaders(stateEventID),
		},
	}, nil
}
