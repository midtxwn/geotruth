package natswatch

import "time"

const (
	EventTypePositionUpdated  = "position.updated"
	EventTypeGeofenceEntered  = "geofence.entered"
	EventTypeGeofenceExited   = "geofence.exited"
	EventTypeObjectRegistered = "object.registered"
	EventTypeObjectRemoved    = "object.removed"
	EventTypeGeoTruthBooted   = "geotruth.booted"

	LifecycleActive  = "active"
	LifecycleRemoved = "removed"
)

type EventPosition struct {
	X    float64 `json:"x"`
	Y    float64 `json:"y"`
	Z    float64 `json:"z"`
	RotY float64 `json:"rot_y"`
}

type EventDims struct {
	Width  float64 `json:"w"`
	Height float64 `json:"h"`
}

// GeofenceTransitionRecord is diagnostic and repair metadata stored in a
// commit-bearing object event's StateAfter snapshot.
type GeofenceTransitionRecord struct {
	AreaID  string `json:"area_id"`
	Entered bool   `json:"entered"`
}

// ObjectStateSnapshot is the recovery checkpoint embedded in commit-bearing
// public object events. InstanceID identifies one object incarnation, and
// CommitSeq is monotonic within that incarnation.
type ObjectStateSnapshot struct {
	ObjectID            string                     `json:"object_id"`
	InstanceID          string                     `json:"instance_id"`
	CommitSeq           uint64                     `json:"commit_seq"`
	Lifecycle           string                     `json:"lifecycle"`
	Region              *string                    `json:"region"`
	Position            *EventPosition             `json:"position,omitempty"`
	Dims                EventDims                  `json:"dims"`
	InsideAreaIDs       []string                   `json:"inside_area_ids"`
	GeofenceTransitions []GeofenceTransitionRecord `json:"geofence_transitions,omitempty"`
	UpdatedAt           time.Time                  `json:"updated_at"`
}

type EventMeta struct {
	EventID    string    `json:"event_id"`
	EventType  string    `json:"event_type"`
	ClientOpID string    `json:"client_op_id,omitempty"`
	OccurredAt time.Time `json:"occurred_at"`
}

type ObjectEventMeta struct {
	EventMeta
	ObjectID   string `json:"object_id"`
	InstanceID string `json:"instance_id"`
	CommitSeq  uint64 `json:"commit_seq"`
}

type PositionUpdatedEvent struct {
	ObjectEventMeta
	Region        string               `json:"region"`
	Position      EventPosition        `json:"position"`
	Dims          EventDims            `json:"dims"`
	InsideAreaIDs []string             `json:"inside_area_ids"`
	StateAfter    *ObjectStateSnapshot `json:"state_after,omitempty"`
}

type GeofenceTransitionEvent struct {
	ObjectEventMeta
	AreaID             string        `json:"area_id"`
	Region             string        `json:"region"`
	Position           EventPosition `json:"position"`
	Dims               EventDims     `json:"dims"`
	InsideAreaIDsAfter []string      `json:"inside_area_ids_after"`
}

type ObjectRegisteredEvent struct {
	ObjectEventMeta
	Region     *string              `json:"region"`
	Dims       EventDims            `json:"dims"`
	StateAfter *ObjectStateSnapshot `json:"state_after,omitempty"`
}

type ObjectRemovedEvent struct {
	ObjectEventMeta
	Region              *string              `json:"region"`
	PositionBefore      *EventPosition       `json:"position_before,omitempty"`
	Dims                EventDims            `json:"dims"`
	InsideAreaIDsBefore []string             `json:"inside_area_ids_before"`
	StateAfter          *ObjectStateSnapshot `json:"state_after,omitempty"`
}

type GeoTruthBootedEvent struct {
	EventID      string    `json:"event_id"`
	EventType    string    `json:"event_type"`
	BootEpochSeq uint64    `json:"boot_epoch_seq"`
	OccurredAt   time.Time `json:"occurred_at"`
}
