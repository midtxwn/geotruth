package gtevents

import (
	"encoding/json"
	"fmt"
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
	InstanceID string
	CommitSeq  uint64
	ClientOpID string

	// Post-state fields after the object command is processed.
	Region                  string
	Position                EventPosition
	Dims                    EventDims
	HasPosition             bool // false for pure registration and removed state
	ProjectionPositionKnown bool // true when transition projections can carry a real position
	InsideAreaIDs           []string
	Lifecycle               string // "active" or "removed"

	// GeofenceTransitions detected during processing. These are embedded in the
	// commit event so startup repair can derive missing geofence projections.
	GeofenceTransitions []GeofenceTransition
}

// CommitMessages is the built GT_EVENTS write set for one object commit. Commit
// is the durable object checkpoint and must be published before Projections.
type CommitMessages struct {
	Commit      *nats.Msg
	Projections []*nats.Msg
	StateAfter  ObjectStateRecord
}

// BuildCommitMsgs constructs the commit marker and derived projection messages.
// The object commit event is published first and carries StateAfter. Geofence
// transition messages are repairable projections derived from that commit.
func BuildCommitMsgs(input CommitInput) (CommitMessages, error) {
	if input.InstanceID == "" || input.CommitSeq == 0 {
		return CommitMessages{}, fmt.Errorf("missing instance_id or commit_seq")
	}
	if len(input.GeofenceTransitions) > 0 && !input.ProjectionPositionKnown {
		return CommitMessages{}, fmt.Errorf("geofence transition projections require position context")
	}

	now := time.Now().UTC()
	state := buildStateAfter(input, now)

	commit, err := buildObjectCommitMsg(input, state, now)
	if err != nil {
		return CommitMessages{}, err
	}
	projections, err := buildProjectionMsgs(input, now)
	if err != nil {
		return CommitMessages{}, err
	}

	return CommitMessages{
		Commit:      commit,
		Projections: projections,
		StateAfter:  state,
	}, nil
}

func buildObjectCommitMsg(input CommitInput, state ObjectStateRecord, now time.Time) (*nats.Msg, error) {
	meta := ObjectEventMeta{
		EventMeta: EventMeta{
			ClientOpID: input.ClientOpID,
			OccurredAt: now,
		},
		ObjectID:   input.ObjectID,
		InstanceID: input.InstanceID,
		CommitSeq:  input.CommitSeq,
	}

	switch input.Lifecycle {
	case LifecycleActive:
		if input.HasPosition {
			eventID := PositionEventID(input.ObjectID, input.InstanceID, input.CommitSeq)
			meta.EventID = eventID
			meta.EventType = EventTypePositionUpdated
			event := PositionUpdatedEvent{
				ObjectEventMeta: meta,
				Region:          input.Region,
				Position:        input.Position,
				Dims:            input.Dims,
				InsideAreaIDs:   input.InsideAreaIDs,
				StateAfter:      &state,
			}
			return marshalPublicMsg(SubjectPositionUpdated(input.ObjectID), event)
		}

		eventID := ObjectRegisteredEventID(input.ObjectID, input.InstanceID, input.CommitSeq)
		meta.EventID = eventID
		meta.EventType = EventTypeObjectRegistered
		event := ObjectRegisteredEvent{
			ObjectEventMeta: meta,
			Region:          state.Region,
			Dims:            input.Dims,
			StateAfter:      &state,
		}
		return marshalPublicMsg(SubjectObjectRegistered(input.ObjectID), event)

	case LifecycleRemoved:
		eventID := ObjectRemovedEventID(input.ObjectID, input.InstanceID, input.CommitSeq)
		meta.EventID = eventID
		meta.EventType = EventTypeObjectRemoved
		beforeAreas := removedInsideAreas(input.GeofenceTransitions)
		var posBefore *EventPosition
		if input.ProjectionPositionKnown {
			pos := input.Position
			posBefore = &pos
		}
		event := ObjectRemovedEvent{
			ObjectEventMeta:     meta,
			Region:              regionPtr(input.Region),
			PositionBefore:      posBefore,
			Dims:                input.Dims,
			InsideAreaIDsBefore: beforeAreas,
			StateAfter:          &state,
		}
		return marshalPublicMsg(SubjectObjectRemoved(input.ObjectID), event)

	default:
		return nil, fmt.Errorf("unknown lifecycle %q", input.Lifecycle)
	}
}

func buildProjectionMsgs(input CommitInput, now time.Time) ([]*nats.Msg, error) {
	projections := make([]*nats.Msg, 0, len(input.GeofenceTransitions))
	for _, tr := range input.GeofenceTransitions {
		var eventID, eventType, subject string
		if tr.Entered {
			eventID = GeofenceEnterEventID(tr.AreaID, input.ObjectID, input.InstanceID, input.CommitSeq)
			eventType = EventTypeGeofenceEntered
			subject = SubjectGeofenceEntered(input.ObjectID, tr.AreaID)
		} else {
			eventID = GeofenceExitEventID(tr.AreaID, input.ObjectID, input.InstanceID, input.CommitSeq)
			eventType = EventTypeGeofenceExited
			subject = SubjectGeofenceExited(input.ObjectID, tr.AreaID)
		}

		event := GeofenceTransitionEvent{
			ObjectEventMeta: ObjectEventMeta{
				EventMeta: EventMeta{
					EventID:    eventID,
					EventType:  eventType,
					ClientOpID: input.ClientOpID,
					OccurredAt: now,
				},
				ObjectID:   input.ObjectID,
				InstanceID: input.InstanceID,
				CommitSeq:  input.CommitSeq,
			},
			AreaID:             tr.AreaID,
			Region:             input.Region,
			Position:           input.Position,
			Dims:               input.Dims,
			InsideAreaIDsAfter: input.InsideAreaIDs,
		}

		msg, err := marshalPublicMsg(subject, event)
		if err != nil {
			return nil, err
		}
		projections = append(projections, msg)
	}
	return projections, nil
}

func marshalPublicMsg(subject string, event any) (*nats.Msg, error) {
	data, err := json.Marshal(event)
	if err != nil {
		return nil, err
	}
	return &nats.Msg{
		Subject: subject,
		Data:    data,
	}, nil
}

func buildStateAfter(input CommitInput, now time.Time) ObjectStateRecord {
	var posPtr *EventPosition
	if input.HasPosition {
		pos := input.Position
		posPtr = &pos
	}

	geofenceTransitionRecords := make([]GeofenceTransitionRecord, 0, len(input.GeofenceTransitions))
	for _, tr := range input.GeofenceTransitions {
		geofenceTransitionRecords = append(geofenceTransitionRecords, GeofenceTransitionRecord{
			AreaID:  tr.AreaID,
			Entered: tr.Entered,
		})
	}

	return ObjectStateRecord{
		ObjectID:            input.ObjectID,
		InstanceID:          input.InstanceID,
		CommitSeq:           input.CommitSeq,
		Lifecycle:           input.Lifecycle,
		Region:              regionPtr(input.Region),
		Position:            posPtr,
		Dims:                input.Dims,
		InsideAreaIDs:       input.InsideAreaIDs,
		GeofenceTransitions: geofenceTransitionRecords,
		UpdatedAt:           now,
	}
}

func removedInsideAreas(transitions []GeofenceTransition) []string {
	areas := make([]string, 0, len(transitions))
	for _, tr := range transitions {
		if !tr.Entered {
			areas = append(areas, tr.AreaID)
		}
	}
	return areas
}

// BuildRegisterCommitMsgs creates the commit for object registration.
func BuildRegisterCommitMsgs(objectID, instanceID string, commitSeq uint64, clientOpID string, dims domain.ObjectDimensions) (CommitMessages, error) {
	return BuildCommitMsgs(CommitInput{
		ObjectID:   objectID,
		InstanceID: instanceID,
		CommitSeq:  commitSeq,
		ClientOpID: clientOpID,
		Dims: EventDims{
			Width:  dims.Width,
			Height: dims.Height,
		},
		Lifecycle: LifecycleActive,
	})
}

// BuildRegisterCurrentStateCommitMsgs creates a registration event for an
// idempotent duplicate register with a StateAfter snapshot that preserves the
// object's current engine state while advancing the object commit sequence.
func BuildRegisterCurrentStateCommitMsgs(objectID, instanceID string, commitSeq uint64, clientOpID string, state ObjectStateRecord) (CommitMessages, error) {
	if instanceID == "" || commitSeq == 0 {
		return CommitMessages{}, fmt.Errorf("missing instance_id or commit_seq")
	}
	now := time.Now().UTC()
	state.ObjectID = objectID
	state.InstanceID = instanceID
	state.CommitSeq = commitSeq
	state.Lifecycle = LifecycleActive
	state.UpdatedAt = now
	state.GeofenceTransitions = nil

	meta := ObjectEventMeta{
		EventMeta: EventMeta{
			EventID:    ObjectRegisteredEventID(objectID, instanceID, commitSeq),
			EventType:  EventTypeObjectRegistered,
			ClientOpID: clientOpID,
			OccurredAt: now,
		},
		ObjectID:   objectID,
		InstanceID: instanceID,
		CommitSeq:  commitSeq,
	}
	event := ObjectRegisteredEvent{
		ObjectEventMeta: meta,
		Region:          state.Region,
		Dims:            state.Dims,
		StateAfter:      &state,
	}
	msg, err := marshalPublicMsg(SubjectObjectRegistered(objectID), event)
	if err != nil {
		return CommitMessages{}, err
	}
	return CommitMessages{Commit: msg, StateAfter: state}, nil
}
