package natswatch

import "time"

const (
	EventTypePositionUpdated  = "position.updated"
	EventTypeGeofenceEntered  = "geofence.entered"
	EventTypeGeofenceExited   = "geofence.exited"
	EventTypeObjectRegistered = "object.registered"
	EventTypeObjectRemoved    = "object.removed"
	EventTypeDetectorBooted   = "detector.booted"

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

type EventMeta struct {
	EventID      string    `json:"event_id"`
	EventType    string    `json:"event_type"`
	SourceStream string    `json:"source_stream"`
	SourceSeq    uint64    `json:"source_seq"`
	ClientOpID   string    `json:"client_op_id,omitempty"`
	OccurredAt   time.Time `json:"occurred_at"`
}

type ObjectEventMeta struct {
	EventMeta
	ObjectID string `json:"object_id"`
}

type PositionUpdatedEvent struct {
	ObjectEventMeta
	Region        string        `json:"region"`
	Position      EventPosition `json:"position"`
	Dims          EventDims     `json:"dims"`
	InsideAreaIDs []string      `json:"inside_area_ids"`
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
	Region *string   `json:"region"`
	Dims   EventDims `json:"dims"`
}

type ObjectRemovedEvent struct {
	ObjectEventMeta
	Region              *string  `json:"region"`
	InsideAreaIDsBefore []string `json:"inside_area_ids_before"`
}

type DetectorBootedEvent struct {
	EventID    string    `json:"event_id"`
	EventType  string    `json:"event_type"`
	BootID     string    `json:"boot_id"`
	OccurredAt time.Time `json:"occurred_at"`
}
