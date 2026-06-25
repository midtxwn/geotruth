package natswatch

import "time"

const (
	EventTypePositionUpdated  = "position.updated"
	EventTypeGeofenceEntered  = "geofence.entered"
	EventTypeGeofenceExited   = "geofence.exited"
	EventTypeObjectRegistered = "object.registered"
	EventTypeObjectRemoved    = "object.removed"
	EventTypeGeoTruthBooted   = "geotruth.booted"
)

type EventPosition struct {
	X    float64 `json:"x"`
	Y    float64 `json:"y"`
	Z    float64 `json:"z"`
	RotY float64 `json:"q"`
}

type EventDims struct {
	Width  float64 `json:"w"`
	Height float64 `json:"h"`
}

type EventMeta struct {
	EventID    string    `json:"e"`
	EventType  string    `json:"k"`
	ClientOpID string    `json:"c,omitempty"`
	OccurredAt time.Time `json:"t"`
}

type ObjectEventMeta struct {
	EventMeta
	ObjectID   string `json:"o"`
	InstanceID string `json:"i"`
	CommitSeq  uint64 `json:"s"`
}

type PositionUpdatedEvent struct {
	ObjectEventMeta
	Region        string        `json:"r"`
	Position      EventPosition `json:"p"`
	Dims          EventDims     `json:"d"`
	InsideAreaIDs []string      `json:"a,omitempty"`
}

type GeofenceTransitionEvent struct {
	ObjectEventMeta
	AreaID             string        `json:"g"`
	Region             string        `json:"r"`
	Position           EventPosition `json:"p"`
	Dims               EventDims     `json:"d"`
	InsideAreaIDsAfter []string      `json:"a,omitempty"`
}

type ObjectRegisteredEvent struct {
	ObjectEventMeta
	Region *string   `json:"r,omitempty"`
	Dims   EventDims `json:"d"`
}

type ObjectRemovedEvent struct {
	ObjectEventMeta
	Region              *string        `json:"r,omitempty"`
	PositionBefore      *EventPosition `json:"pb,omitempty"`
	Dims                EventDims      `json:"d"`
	InsideAreaIDsBefore []string       `json:"a,omitempty"`
}

type GeoTruthBootedEvent struct {
	EventID      string    `json:"e"`
	EventType    string    `json:"k"`
	BootEpochSeq uint64    `json:"b"`
	OccurredAt   time.Time `json:"t"`
}
